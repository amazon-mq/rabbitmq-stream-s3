%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_read_pipeline).
-moduledoc """
The ranges a remote reader has asked S3 for, and the bytes they assemble into.

Several ranges of a fragment are fetched at once so a reader's bandwidth is not
capped by one connection's transfer rate, which means responses interleave. The
read buffer takes contiguous appends only, so the ranges are held in a queue
ordered by `{fragment, range_start}` that doubles as a reassembly queue: bytes
for a range whose predecessors are unfinished are staged on that range and
appended once it heads its fragment's queue.

This module owns that queue, the buffers it assembles into - the current
fragment's and one per fragment prefetched ahead of it - and the consumer's
read position. It
decides nothing: how much to fetch, when to retry and when to move on are the
core's, which drives this through `push/3`, `ready/2`, `data/4` and `fail/3`.
The split is bytes against policy. Everything here is a pure function of what
was asked for and what S3 said.

## Identity

A range carries an id of its own, minted when it is queued and kept for as long
as the range exists - including across a failure, which restarts the range at
its last flushed byte and so moves the position it would otherwise be addressed
by. Position made a serviceable key, since ranges within a fragment never
overlap, but it is a mutable one: the restart could land a range's key exactly
on its successor's, which had to be detected and dropped rather than being
impossible. The shell records the id against the request it started and hands it
back with every frame.

## Invariants

The queue is ordered by `{fragment, range_start}`, its keys are unique, its
ranges are disjoint, and each range satisfies
`range_start =< flushed =< pos =< range_end + 1`. Under TEST these are checked
after every operation that mutates the queue, so they hold by construction for
any caller rather than by convention across the call sites that build it.

## Buffers

Bytes are kept as the blocks S3 delivered, never copied into place, and reads
come back as iodata for the same reason - see `rabbitmq_stream_s3_read_buffer`.
Reassembly appends each block on its own, so a range delivered as four frames
stays four blocks: flattening them here would undo what the block queue is for.
""".

-include("include/rabbitmq_stream_s3.hrl").

%% One range of one fragment object that the reader still needs.
%%
%% `flushed` is the next byte not yet appended to a buffer; `pos` is the next
%% byte not yet received from S3. The bytes in `[flushed, pos)` are held in
%% `staged` (newest first), which is only non-empty while a predecessor request
%% is unfinished: the read buffer takes contiguous appends only, so a request
%% may flush only once it heads its fragment's queue.
%%
%% Invariant: `range_start =< flushed =< pos =< range_end + 1`.
%%
%% Status is one of:
%%
%% - `ready` - queued; issued at the next pass over the queue
%% - `{backoff, Kind}` - queued, but only released once `Kind`'s retry timer
%%   fires, so a failure is not re-issued the moment a sibling request delivers
%%   something. The kind is the clock the range waits on: a range that hit a
%%   throttling S3 must not be released by the pool's much shorter timer
%% - `inflight` - issued and being answered
%% - `complete` - the response closed; the request is dropped once its bytes
%%   have reached a buffer
-record(req, {
    id :: request_id(),
    fragment :: fragment_offset(),
    frag_ref :: #fragment_ref{},
    key :: rabbitmq_stream_s3:key(),
    range_start :: byte_offset(),
    %% Inclusive, matching the HTTP range header this becomes.
    range_end :: byte_offset(),
    flushed :: byte_offset(),
    pos :: byte_offset(),
    status = ready :: ready | {backoff, backoff()} | inflight | complete,
    %% The backoff clock this range was released from, until it delivers. A
    %% retry round proves nothing about the condition that armed it until the
    %% ranges it released come back, so this is what stops a sibling's delivery
    %% from handing the clock back to its minimum mid-round (see the core's
    %% `reset_idle_backoffs/1`).
    retried = undefined :: undefined | backoff(),
    staged = [] :: [binary()]
}).

-record(pipeline, {
    stream :: stream_id(),

    %% Mints the next range's id. Never rewound, including when the reader is
    %% placed on a new fragment (`replace_fragment/3` carries it over), so an id
    %% is never reused for the life of the shell - which is what matters, since
    %% the shell is what may still have frames in flight for an old one.
    next_id = 1 :: request_id(),

    %% Current fragment
    frag_ref :: #fragment_ref{},
    buffer :: rabbitmq_stream_s3_read_buffer:buffer(),
    %% Where the consumer has read up to. The buffer's own `start_pos` is not a
    %% substitute: it only moves when a whole block falls behind the read, so
    %% up to a block of consumed bytes would keep counting against the window.
    read_pos :: byte_offset(),

    %% Fragments ahead of the current one, nearest first and contiguous in
    %% manifest order. The head is what the next transition moves to.
    %%
    %% An entry appears when a range is first issued into its fragment
    %% (`register_next/2`), not when bytes first arrive for it. That is what
    %% keeps the list in manifest order: ranges are issued strictly forward, but
    %% their answers are not, so a list built from deliveries could seat a later
    %% fragment at the head and skip the one between it and the current one at
    %% the next transition.
    nexts = [] :: [{#fragment_ref{}, rabbitmq_stream_s3_read_buffer:buffer()}],

    %% Whether the fragment following the last of `nexts` - or following the
    %% current one, when `nexts` is empty - is known to be missing.
    %%
    %% Positional rather than an offset, so it is exact only while `nexts` is
    %% contiguous. A fragment is seated when a range is issued into it, and
    %% every fragment with a data region gets one, so the only fragment the
    %% look-ahead can walk past without seating is one whose data region is
    %% empty. Such a fragment holds no chunks, which is what makes recording the
    %% hole one fragment early harmless: neither the prefetch it blocks nor the
    %% iterator refresh that skips past it gives up a byte the consumer wants.
    next_missing = false :: boolean(),

    %% Outstanding ranges, ordered by `{fragment, range_start}` and disjoint.
    %% Normally contiguous within a fragment too, but not while a request that
    %% has delivered every byte it owes waits on its closing frame: the requests
    %% behind it are dropped as they flush, so a gap the buffer already holds can
    %% open up. Also the reassembly queue (see `flush_reqs/1`).
    reqs = [] :: [#req{}]
}).

-opaque pipeline() :: #pipeline{}.
-type fragment_offset() :: osiris:offset().
-doc "Identifies one outstanding range for as long as it exists.".
-type request_id() :: pos_integer().
-doc """
The two backoff clocks: `fault` for an S3 request that failed, `pool_busy` for
one that never left the node because the connection pool had nothing free.
""".
-type backoff() :: fault | pool_busy.
-doc """
What the shell needs to start one range GET: the id to record it against, the
object key, the range, and the fragment it belongs to.
""".
-type start_spec() ::
    {request_id(), rabbitmq_stream_s3:key(), {byte_offset(), byte_offset()}, fragment_offset()}.
-doc """
Something that happened while absorbing a delivery which is the core's business,
not this module's. `empty_completion` is a response that closed without
delivering a byte: the range is queued against the fault clock, and the core
arms it.
""".
-type signal() :: empty_completion.

-export_type([pipeline/0, request_id/0, backoff/0, start_spec/0, signal/0]).

-export([
    new/3,
    replace_fragment/3,
    current_fragment/1,
    current_fragment_offset/1,
    read_position/1,
    frontier/2,
    outstanding/1,
    inflight/1,
    waits_on/2,
    push/3,
    ready/2,
    data/4,
    fail/3,
    release/2,
    drop_fragment/2,
    clear_requests/1,
    drop_prefetch/1,
    prefetch/1,
    prefetching/2,
    prefetch_horizon/1,
    advance/1,
    read/3
]).

-ifdef(TEST).
-export([
    outstanding_ranges/1,
    inflight_ranges/1,
    find_request/3,
    request_count/1,
    queued_on/2
]).
-endif.

%% ------------------------------------------------------------------
%% Construction and navigation
%% ------------------------------------------------------------------

-doc "An empty pipeline reading `FragRef` from `Position`.".
-spec new(stream_id(), #fragment_ref{}, byte_offset()) -> pipeline().
new(StreamId, FragRef, Position) ->
    #pipeline{
        stream = StreamId,
        frag_ref = FragRef,
        buffer = rabbitmq_stream_s3_read_buffer:new(Position),
        read_pos = Position
    }.

-doc """
Places the reader on a different fragment, dropping the queue and both buffers.

For a reader whose iterator has been refreshed past a fragment that is gone:
everything about where it was is invalid, but the ids it handed the shell are
not, so the id counter carries over rather than restarting under frames that may
still arrive for the ranges it just dropped.
""".
-spec replace_fragment(#fragment_ref{}, byte_offset(), pipeline()) -> pipeline().
replace_fragment(FragRef, Position, #pipeline{stream = StreamId, next_id = NextId}) ->
    P = new(StreamId, FragRef, Position),
    P#pipeline{next_id = NextId}.

-spec current_fragment(pipeline()) -> #fragment_ref{}.
current_fragment(#pipeline{frag_ref = FragRef}) ->
    FragRef.

-spec current_fragment_offset(pipeline()) -> fragment_offset().
current_fragment_offset(#pipeline{frag_ref = #fragment_ref{offset = Offset}}) ->
    Offset.

-spec read_position(pipeline()) -> byte_offset().
read_position(#pipeline{read_pos = ReadPos}) ->
    ReadPos.

-doc """
What the prefetch of the next fragment holds: nothing, bytes, or a 404.

A registered fragment that has yet to receive a byte reads as `undefined`, the
same as no prefetch at all. The transition it would answer is one the caller
cannot take yet - there is nothing to serve on the far side of it - and saying
so keeps a fragment's arrival, rather than a range being issued into it, as what
moves the reader onto it.
""".
-spec prefetch(pipeline()) -> undefined | not_found | {#fragment_ref{}, byte_offset()}.
prefetch(#pipeline{nexts = [{FragRef, Buffer} | _]}) ->
    case rabbitmq_stream_s3_read_buffer:end_pos(Buffer) of
        ?SEGMENT_HEADER_B -> undefined;
        EndPos -> {FragRef, EndPos}
    end;
prefetch(#pipeline{next_missing = true}) ->
    not_found;
prefetch(#pipeline{}) ->
    undefined.

-doc """
Whether `Fragment` is one the reader is prefetching.

An answer can still arrive for a fragment the reader has left behind at a
transition, and what such a frame says about the prefetch is nothing: it is
about a fragment nobody is reading any more. Registration happens on issuance,
so this is answered for a fragment that has yet to deliver a byte - which is the
case the caller most needs it for, since that is when a 404 arrives.
""".
-spec prefetching(fragment_offset(), pipeline()) -> boolean().
prefetching(Fragment, #pipeline{nexts = Nexts}) ->
    find_next(Fragment, Nexts) =/= error.

-doc """
The last fragment the reader may still fetch into, or `unlimited`.

A hole bounds the frontier, and the look-ahead can be several fragments deeper
than the hole is: a 404 arrives for a fragment the iterator has already been
walked past, so the core can hold peeks on the far side of it. Rather than have
the core carry a second copy of where the hole is and keep the two in step, it
asks - this module learns of the 404 and owns the answer.
""".
-spec prefetch_horizon(pipeline()) -> unlimited | fragment_offset().
prefetch_horizon(#pipeline{next_missing = false}) ->
    unlimited;
prefetch_horizon(#pipeline{nexts = Nexts, frag_ref = #fragment_ref{offset = Current}}) ->
    last_next_offset(Nexts, Current).

%% Where the hole sits when `next_missing` is set: after the last prefetched
%% fragment, or after the current one when nothing is prefetched.
last_next_offset([], Current) ->
    Current;
last_next_offset(Nexts, _Current) ->
    {#fragment_ref{offset = Offset}, _} = lists:last(Nexts),
    Offset.

-doc """
Records that `Fragment` is known to be missing, so nothing is prefetched at or
past it and the core can decide what to do when the consumer reaches it.

Everything at or past the hole goes, not just the fragment itself: the reader
reaches a fragment only by walking the ones before it, so a buffer or a range
beyond a fragment that is gone is work nothing will ever collect. Returns the
ids of the ranges that had been issued past it, which the caller cancels.
""".
-spec drop_fragment(fragment_offset() | {not_found, fragment_offset()}, pipeline()) ->
    {[request_id()], pipeline()}.
drop_fragment({not_found, Fragment}, #pipeline{nexts = Nexts, reqs = Reqs} = P) ->
    Kept = lists:takewhile(fun({#fragment_ref{offset = O}, _}) -> O < Fragment end, Nexts),
    {Dropped, KeptReqs} = lists:partition(fun(#req{fragment = F}) -> F >= Fragment end, Reqs),
    {
        [Id || #req{id = Id} <- Dropped],
        checked(P#pipeline{nexts = Kept, next_missing = true, reqs = KeptReqs})
    };
drop_fragment(Fragment, #pipeline{reqs = Reqs} = P) ->
    {Dropped, Kept} = lists:partition(fun(#req{fragment = F}) -> F =:= Fragment end, Reqs),
    {[Id || #req{id = Id} <- Dropped], checked(P#pipeline{reqs = Kept})}.

-doc """
Drops every outstanding range, keeping the buffer.

The buffer holds a contiguous run of the current fragment that nothing in
flight contributed to - staged bytes are held on their request, not in it - so
it is exactly as valid afterwards as before.
""".
-spec clear_requests(pipeline()) -> pipeline().
clear_requests(#pipeline{} = P) ->
    checked(P#pipeline{reqs = []}).

%% Give up the bytes prefetched ahead of the current fragment. A bare `not_found`
%% is kept: it holds no bytes, so it costs the window nothing, and a fragment
%% retention has deleted does not come back - forgetting it would spend another
%% GET learning the same 404. That clause changes nothing, so it needs no
%% `checked/1`.
%%
%% Dropping buffered fragments drops the marker with them, because the marker is
%% positional: it says the fragment after the last of `nexts` is gone, so keeping
%% it over an emptied list would move the hole onto the fragment immediately
%% after the current one - which is a fragment the reader has buffered bytes for.
%% Re-peeking costs one GET to learn the same 404 again; getting the position
%% wrong strands the reader on a fragment it believes is missing.
%%
%% The ranges into those fragments go with their seats. A range whose fragment
%% has no seat can never reach a buffer, so leaving them would wedge the queue
%% while holding pooled connections - the caller drops the whole queue in the
%% same breath, but the pairing belongs to whichever operation breaks it.
-spec drop_prefetch(pipeline()) -> pipeline().
drop_prefetch(#pipeline{nexts = []} = P) ->
    P;
drop_prefetch(#pipeline{reqs = Reqs, frag_ref = #fragment_ref{offset = Current}} = P) ->
    Kept = [Req || #req{fragment = F} = Req <- Reqs, F =< Current],
    checked(P#pipeline{nexts = [], next_missing = false, reqs = Kept}).

-doc """
Moves to the nearest prefetched fragment holding bytes, or resets the current
one when there is nothing to move to.

Returns the offset now being read and what the shell must cancel: the ranges
still outstanding against the fragment being left behind sort ahead of the new
current fragment's, so leaving them in the queue would block reassembly forever.
In practice the transition can only happen once the old fragment's data region
is fully buffered, so the list is normally empty; dropping them defensively
costs nothing and turns a possible deadlock into a cancelled request.

Bytes, not a seat in `nexts`, are what make a fragment somewhere to move to. A
fragment is seated as soon as a range is issued into it, so the head can be a
fragment nothing has answered for yet; moving onto that one would put the reader
on an empty buffer and call it progress. The callers divide on exactly this
line - a transition asks `prefetch/1` first, and the become-local path wants the
reset - so the distinction lives here rather than in either of them.
""".
-spec advance(pipeline()) -> {fragment_offset(), all | [request_id()], pipeline()}.
advance(#pipeline{nexts = [{_FragRef, Buffer} | _] = Nexts} = P0) ->
    case rabbitmq_stream_s3_read_buffer:end_pos(Buffer) > ?SEGMENT_HEADER_B of
        true -> advance_to(Nexts, P0);
        false -> reset_current(P0)
    end;
advance(#pipeline{} = P0) ->
    reset_current(P0).

advance_to([{NextFragRef, Buffer} | Rest], #pipeline{frag_ref = FragRef} = P0) ->
    #fragment_ref{offset = NextOffset} = NextFragRef,
    #fragment_ref{offset = CurrentOffset} = FragRef,
    %% Forward navigation must be strictly increasing. A fragment iterator that
    %% mispositions (for example descending into a group at the wrong offset)
    %% can hand back an earlier fragment, which would deliver out-of-order or
    %% duplicate offsets to the consumer with no other signal. Assert the
    %% invariant here so such a bug is a loud crash at the transition rather
    %% than silent data corruption downstream, and so any future iterator
    %% regression fails fast.
    true = NextOffset > CurrentOffset,
    P = P0#pipeline{
        frag_ref = NextFragRef,
        buffer = Buffer,
        read_pos = rabbitmq_stream_s3_read_buffer:start_pos(Buffer),
        nexts = Rest
    },
    {Stale, Reqs} = lists:partition(
        fun(#req{fragment = Fragment}) -> Fragment < NextOffset end, P#pipeline.reqs
    ),
    {NextOffset, [Id || #req{id = Id} <- Stale], checked(P#pipeline{reqs = Reqs})}.

reset_current(#pipeline{frag_ref = #fragment_ref{offset = CurrentOffset}, reqs = Reqs} = P0) ->
    %% There was no prefetched fragment to move to, so the fragment does not
    %% change and the buffer is reset under the requests still reading it. Those
    %% requests can no longer flush - their `flushed` is past the empty buffer's
    %% end for good, and a blocked head skips the rest of its fragment's queue -
    %% so they would wedge the queue and hold their pooled connections. Drop
    %% them.
    Cancels =
        case Reqs of
            [] -> [];
            _ -> all
        end,
    P = P0#pipeline{
        buffer = rabbitmq_stream_s3_read_buffer:new(?SEGMENT_HEADER_B),
        read_pos = ?SEGMENT_HEADER_B,
        nexts = [],
        next_missing = false,
        reqs = []
    },
    {CurrentOffset, Cancels, checked(P)}.

%% ------------------------------------------------------------------
%% Issuance
%% ------------------------------------------------------------------

-doc """
Records a range the core has decided to fetch, and returns what starting it
takes. The range is queued as in flight: the caller starts it in the same pass.
""".
-spec push(#fragment_ref{}, {byte_offset(), byte_offset()}, pipeline()) ->
    {start_spec(), pipeline()}.
push(
    #fragment_ref{offset = Fragment, uid = Uid} = FragRef,
    {Start, End},
    #pipeline{stream = StreamId, next_id = Id, reqs = Reqs} = P
) ->
    Key = rabbitmq_stream_s3:fragment_key(StreamId, Fragment, Uid),
    Req = #req{
        id = Id,
        fragment = Fragment,
        frag_ref = FragRef,
        key = Key,
        range_start = Start,
        range_end = End,
        flushed = Start,
        pos = Start,
        status = inflight
    },
    P1 = register_next(FragRef, P#pipeline{next_id = Id + 1, reqs = insert_req(Req, Reqs)}),
    {start_spec(Req), checked(P1)}.

%% Seat a fragment beyond the current one in `nexts` the moment a range is
%% issued into it, with the empty buffer its deliveries will fill. See the
%% field's own comment for why issuance rather than delivery is what orders the
%% list; appending is enough because `next_range/2` only ever spills forward.
register_next(
    #fragment_ref{offset = Fragment} = FragRef,
    #pipeline{frag_ref = #fragment_ref{offset = Current}, nexts = Nexts} = P
) when Fragment > Current ->
    case find_next(Fragment, Nexts) of
        error ->
            Buffer = rabbitmq_stream_s3_read_buffer:new(?SEGMENT_HEADER_B),
            P#pipeline{nexts = Nexts ++ [{FragRef, Buffer}]};
        {ok, _Buffer} ->
            P
    end;
register_next(_FragRef, #pipeline{} = P) ->
    P.

find_next(_Fragment, []) ->
    error;
find_next(Fragment, [{#fragment_ref{offset = Fragment}, Buffer} | _]) ->
    {ok, Buffer};
find_next(Fragment, [_ | Rest]) ->
    find_next(Fragment, Rest).

replace_next(Fragment, Buffer, [{#fragment_ref{offset = Fragment} = FragRef, _} | Rest]) ->
    [{FragRef, Buffer} | Rest];
replace_next(Fragment, Buffer, [Entry | Rest]) ->
    [Entry | replace_next(Fragment, Buffer, Rest)];
replace_next(Fragment, _Buffer, []) ->
    %% Only reachable with a delivery for a fragment that has no seat, which
    %% `checked/1` rules out and `set_buffer_for/3` never asks for. Named rather
    %% than left to `function_clause`: the pairing between a range and its seat
    %% is maintained across `register_next/2`, `drop_fragment/2`,
    %% `drop_prefetch/1` and `advance/1`, and a break in it reaches the consumer
    %% as `{error, {remote_reader_down, _}}` with nothing at the point of
    %% failure to say which of them dropped the seat.
    error({unseated_fragment, Fragment}).

%% (Re-)issue ranges that are queued and not in flight. Their bytes are already
%% counted in `outstanding/1`, so the caller does not window-gate them - a range
%% the reader has committed to must be fetched, or the buffer never becomes
%% contiguous again and every read behind it stalls until the deadline. They do
%% take a slot, so the depth cap still applies.
-spec ready(pos_integer(), pipeline()) -> {[start_spec()], pipeline()}.
ready(MaxDepth, #pipeline{reqs = Reqs} = P) ->
    {Reqs1, {_, Specs}} = lists:mapfoldl(
        fun
            (#req{status = ready} = Req, {InFlight, Acc}) when InFlight < MaxDepth ->
                {Req#req{status = inflight}, {InFlight + 1, [start_spec(Req) | Acc]}};
            (Req, Acc) ->
                {Req, Acc}
        end,
        {inflight(P), []},
        Reqs
    ),
    {lists:reverse(Specs), checked(P#pipeline{reqs = Reqs1})}.

start_spec(#req{id = Id, key = Key, fragment = Fragment, range_start = Start, range_end = End}) ->
    {Id, Key, {Start, End}, Fragment}.

%% Keep the queue ordered by `{fragment, range_start}`. A new range is always
%% the last one for its own fragment, but not necessarily for the queue: a
%% fragment whose tail had to be re-requested gains a range after the spill into
%% the next fragment has already been queued.
insert_req(#req{fragment = Fragment} = Req, Reqs) ->
    {Before, After} = lists:splitwith(fun(#req{fragment = F}) -> F =< Fragment end, Reqs),
    Before ++ [Req | After].

%% ------------------------------------------------------------------
%% Accounting
%% ------------------------------------------------------------------

%% Where the next range for a fragment starts: past everything already asked
%% for, which is both what the queue covers and what the buffer holds.
%%
%% Both terms are needed. The queue is not always ahead of the buffer: a request
%% that owes nothing stays queued until its closing frame arrives, and the
%% requests behind it flush and are dropped in the meantime, so the highest
%% range end left in the queue can sit below the buffer's end. Taking the queue
%% alone then walks the frontier backwards and re-requests bytes the buffer
%% already holds - a range that can never flush (its `flushed` is behind the
%% buffer's end for good), so it wedges the queue as well as wasting the fetch.
%% Nor is the buffer alone enough: ranges in flight are not in it yet.
%%
%% Taking the maximum cannot skip a hole. The buffer is contiguous, so nothing
%% below its end is missing, and a hole that a short response opened is queued
%% as its own request (see `complete_req/4`), so it is inside the queue's reach.
-spec frontier(fragment_offset(), pipeline()) -> byte_offset().
frontier(Fragment, #pipeline{reqs = Reqs} = P) ->
    Buffered = buffered_end(Fragment, P),
    case [RangeEnd || #req{fragment = F, range_end = RangeEnd} <- Reqs, F =:= Fragment] of
        [] -> Buffered;
        Ends -> max(Buffered, lists:max(Ends) + 1)
    end.

buffered_end(Fragment, #pipeline{frag_ref = #fragment_ref{offset = Fragment}, buffer = Buffer}) ->
    rabbitmq_stream_s3_read_buffer:end_pos(Buffer);
buffered_end(Fragment, #pipeline{nexts = Nexts}) ->
    case find_next(Fragment, Nexts) of
        {ok, Buffer} -> rabbitmq_stream_s3_read_buffer:end_pos(Buffer);
        error -> ?SEGMENT_HEADER_B
    end.

%% How far ahead of the consumer the reader has got: the buffered bytes it has
%% not read yet, plus the part of every outstanding range that is not in a
%% buffer. This is what the prefetch window bounds.
%%
%% Split into its two terms because they answer different questions - how much
%% memory the reader is holding, against how hard it is currently fetching - and
%% the sum is what couples them: a full buffer subtracts directly from the room
%% `has_room/1` will admit for new ranges.
-spec outstanding(pipeline()) -> non_neg_integer().
outstanding(#pipeline{} = P) ->
    buffered(P) + committed(P).

%% Bytes held in a buffer that the consumer has not read: the current
%% fragment's unread run, plus everything prefetched for the fragments after it
%% (none of which has been read, by definition).
-spec buffered(pipeline()) -> non_neg_integer().
buffered(#pipeline{buffer = Buffer, read_pos = ReadPos, nexts = Nexts}) ->
    max(0, rabbitmq_stream_s3_read_buffer:end_pos(Buffer) - ReadPos) +
        lists:sum([
            rabbitmq_stream_s3_read_buffer:end_pos(NextBuffer) - ?SEGMENT_HEADER_B
         || {_FragRef, NextBuffer} <- Nexts
        ]).

%% Bytes the reader has committed to fetching that have not reached a buffer
%% yet. Every queued range counts, not only the ones on the wire: a range in
%% backoff or awaiting a depth slot is just as committed, which is why
%% `ready/2` is not window-gated.
-spec committed(pipeline()) -> non_neg_integer().
committed(#pipeline{reqs = Reqs}) ->
    lists:foldl(
        fun(#req{range_end = RangeEnd, flushed = Flushed}, Acc) -> Acc + RangeEnd + 1 - Flushed end,
        0,
        Reqs
    ).

-spec inflight(pipeline()) -> non_neg_integer().
inflight(#pipeline{reqs = Reqs}) ->
    length([Req || #req{status = inflight} = Req <- Reqs]).

-doc """
Whether anything is waiting on a backoff clock: a range queued against it, or
one it released that has not answered yet.
""".
-spec waits_on(backoff(), pipeline()) -> boolean().
waits_on(Kind, #pipeline{reqs = Reqs}) ->
    lists:any(
        fun(#req{status = Status, retried = Retried}) ->
            Status =:= {backoff, Kind} orelse Retried =:= Kind
        end,
        Reqs
    ).

%% ------------------------------------------------------------------
%% Responses
%% ------------------------------------------------------------------

%% Absorb a delivery for one outstanding range and flush whatever that makes
%% contiguous. Deliveries for a range the pipeline no longer tracks are dropped:
%% a request cancelled by a read deadline or an iterator refresh can still have
%% frames in flight, and appending those bytes would corrupt the buffer's
%% addressing.
-spec data(request_id(), binary(), done | continue, pipeline()) -> {[signal()], pipeline()}.
data(Id, Data, DoneOrContinue, #pipeline{reqs = Reqs} = P) ->
    case take_req(Id, Reqs) of
        {Before, #req{status = inflight} = Req0, After} ->
            Req = stage(Data, Req0),
            case DoneOrContinue of
                continue ->
                    {[], checked(flush_reqs(P#pipeline{reqs = Before ++ [Req | After]}))};
                done ->
                    complete_req(Before, Req, After, P)
            end;
        _ ->
            {[], P}
    end.

%% Retain the delivered block against the request that asked for it. S3 answers
%% a range request with exactly that range, but a backend that over-delivers
%% would otherwise push this request's bytes over its successor's range and
%% double-append them, so the block is clipped to what this request owns.
stage(Data, #req{pos = Pos, range_end = RangeEnd} = Req) ->
    case RangeEnd + 1 - Pos of
        Room when Room =< 0 ->
            Req;
        Room ->
            case byte_size(Data) of
                0 ->
                    Req;
                Size when Size =< Room ->
                    stage_block(Pos + Size, Data, Req);
                _ ->
                    stage_block(Pos + Room, binary:part(Data, 0, Room), Req)
            end
    end.

%% Bytes have landed on this range, which is the whole of what a retry round was
%% waiting to hear, so the stamp that says otherwise comes off with them.
%% Delivering is what `retried` is set until (see the field), and nothing else
%% takes it off a range that keeps its place in the queue: one that has
%% delivered but lingers - blocked behind an unfinished predecessor, or holding
%% its closing frame - would keep `waits_on/2` true for a clock whose round has
%% been answered, and the delay would stay wherever the last burst grew it.
stage_block(Pos, Block, #req{staged = Staged} = Req) ->
    Req#req{pos = Pos, staged = [Block | Staged], retried = undefined}.

complete_req(Before, #req{pos = Pos, range_end = RangeEnd} = Req, After, P) when
    Pos > RangeEnd
->
    {[], checked(flush_reqs(P#pipeline{reqs = Before ++ [Req#req{status = complete} | After]}))};
complete_req(Before, #req{pos = Pos, range_start = Pos} = Req, After, P0) ->
    %% The request ended without delivering a byte. Re-issuing it straight away
    %% would spin against whatever is answering that way, so it goes into the
    %% fault backoff as if it had failed and the core arms that clock.
    P = flush_reqs(P0#pipeline{reqs = Before ++ [backoff_req(fault, Req) | After]}),
    {[empty_completion], checked(P)};
complete_req(Before, #req{pos = Pos, range_end = RangeEnd} = Req0, After, P) ->
    #pipeline{next_id = GapId} = P,
    %% Short completion: the response ended before the range did (a truncated
    %% response, or a fragment smaller than the manifest claims). Shrink the
    %% request to what actually arrived and queue the bytes that never came as a
    %% gap request, keeping the fragment's ranges contiguous.
    %%
    %% The gap is queued even when nothing else is outstanding for the fragment,
    %% rather than being left to the frontier. The core's `extend_frontier/1` is
    %% window-gated and the missing bytes are still counted against the window
    %% until the request is dropped, so a short completion that frees fewer
    %% bytes than a whole request leaves no room to re-request them: the hole
    %% never closes and every read behind it stalls until the read deadline.
    %% `ready/2` is not window-gated for exactly this reason.
    Req = Req0#req{status = complete, range_end = Pos - 1},
    Gap = Req0#req{
        %% The gap is a range of its own and outlives the response that opened
        %% it, so it gets an id of its own: inheriting the completed request's
        %% would put two ranges in the queue under one identity.
        id = GapId,
        status = ready,
        range_start = Pos,
        range_end = RangeEnd,
        flushed = Pos,
        pos = Pos,
        staged = [],
        %% A range of its own, not the retry the response it came from was: that
        %% one delivered, which is what a backoff clock is waiting to hear.
        retried = undefined
    },
    {[], checked(flush_reqs(P#pipeline{next_id = GapId + 1, reqs = Before ++ [Req, Gap | After]}))}.

%% Put a failed range back in the queue. Bytes already appended to a buffer
%% cannot be un-appended, so the range restarts at `flushed`; anything staged
%% beyond that is dropped and fetched again. An error for a range that is not in
%% flight is stale - a duplicate, or one already abandoned - and must not double
%% the backoff for a single failure.
-spec fail(request_id(), backoff(), pipeline()) ->
    {ok, pipeline()} | {dropped, pipeline()} | stale.
fail(Id, Kind, #pipeline{reqs = Reqs} = P) ->
    case take_req(Id, Reqs) of
        {Before, #req{status = inflight, flushed = Flushed, range_end = RangeEnd}, After} when
            Flushed > RangeEnd
        ->
            %% Every byte the range owed is already in a buffer; only its
            %% closing frame was outstanding (see `advance_past/3`), so there is
            %% nothing left to fetch. Restarting it at `flushed` would leave
            %% `range_start = range_end + 1`, an inverted range: re-issuing it
            %% would ask S3 for a backwards range. Drop it instead.
            {dropped, checked(P#pipeline{reqs = Before ++ After})};
        {Before, #req{status = inflight, flushed = Flushed} = Req0, After} ->
            Req = backoff_req(Kind, Req0#req{range_start = Flushed, pos = Flushed, staged = []}),
            {ok, checked(P#pipeline{reqs = Before ++ [Req | After]})};
        _ ->
            stale
    end.

-doc "Release the ranges waiting on this clock, so the next pass re-issues them.".
-spec release(backoff(), pipeline()) -> pipeline().
release(Kind, #pipeline{reqs = Reqs} = P) ->
    checked(P#pipeline{reqs = [release_req(Kind, Req) || Req <- Reqs]}).

release_req(Kind, #req{status = {backoff, Kind}} = Req) ->
    Req#req{status = ready, retried = Kind};
release_req(_Kind, Req) ->
    Req.

%% Put a range on a clock. Every way a range lands in a backoff goes through
%% here, because `status` is only half of it: the `retried` stamp a previous
%% release left has to come off in the same breath. That stamp says "a round of
%% that clock is still owed an answer", and a range that has just failed or come
%% back empty has given the answer. Left on, `waits_on/2` reports the other
%% clock busy for as long as the range lives, and `reset_idle_backoffs/1` never
%% hands its delay back - so the next failure of that kind is paced from
%% whatever the last round had grown to, against a path that has since
%% recovered.
backoff_req(Kind, Req) ->
    Req#req{status = {backoff, Kind}, retried = undefined}.

take_req(Id, Reqs) ->
    take_req(Id, Reqs, []).

take_req(Id, [#req{id = Id} = Req | Rest], Acc) ->
    {lists:reverse(Acc), Req, Rest};
take_req(Id, [Req | Rest], Acc) ->
    take_req(Id, Rest, [Req | Acc]);
take_req(_Id, [], _Acc) ->
    false.

%% ------------------------------------------------------------------
%% Reassembly
%% ------------------------------------------------------------------

%% Append staged bytes into their fragment's buffer for as long as the head of
%% that fragment's queue is contiguous with it. The read buffer takes contiguous
%% appends only, so a request whose predecessor is unfinished has to wait; when
%% the predecessor is dropped, the successor flushes behind it in the same pass.
%%
%% Each fragment's queue is independent: a stuck current-fragment request must
%% not stop the prefetched next fragment from filling its own buffer.
flush_reqs(#pipeline{reqs = Reqs} = P0) ->
    {Reqs1, P} = flush_reqs1(Reqs, P0),
    P#pipeline{reqs = Reqs1}.

flush_reqs1([], P) ->
    {[], P};
flush_reqs1([#req{flushed = Flushed, range_end = RangeEnd} = Req | Rest], P) when
    Flushed > RangeEnd
->
    %% The request owes nothing: every byte it asked for is already in a
    %% buffer, and `flush_req/4` has left it with no staged bytes. Whether it
    %% can still append is not a question worth asking - and must not be asked,
    %% because `advance_past/3` lets its successors flush past it while its
    %% closing frame is outstanding, which moves the buffer's end beyond this
    %% request's `flushed` and would make `flushable/2` call it blocked
    %% forever. Since `flush_other_fragments/3` then skips the whole fragment,
    %% that wedged the queue permanently: no byte of that fragment could ever
    %% reach the buffer again and every read behind it stalled until the read
    %% deadline, including after the closing frame arrived. Decide from the
    %% request's own accounting instead of the buffer's position.
    advance_past(Req, Rest, P);
flush_reqs1([#req{fragment = Fragment} = Req | Rest], P) ->
    case flushable(Req, P) of
        {ok, Buffer} ->
            flush_req(Req, Rest, Buffer, P);
        blocked ->
            {Rest1, P1} = flush_other_fragments(Fragment, Rest, P),
            {[Req | Rest1], P1}
    end.

%% Skip past the rest of `Fragment`'s queue - nothing in it can flush while a
%% request ahead of it still owes bytes - and flush the fragments behind it.
%% Each fragment's queue is independent, so a stuck current-fragment request
%% must not stop the prefetched next fragment from filling its own buffer.
flush_other_fragments(Fragment, Reqs, P) ->
    {Same, Other} = lists:splitwith(fun(#req{fragment = F}) -> F =:= Fragment end, Reqs),
    {Other1, P1} = flush_reqs1(Other, P),
    {Same ++ Other1, P1}.

flush_req(#req{flushed = Pos, pos = Pos} = Req, Rest, _Buffer, P) ->
    %% Nothing new to append. Leave the buffer alone: writing an empty one back
    %% would create the next fragment's buffer, which is what says the prefetch
    %% has delivered something and lets a fragment transition go ahead.
    advance_past(Req, Rest, P);
flush_req(#req{pos = Pos, staged = Staged} = Req, Rest, Buffer, P0) ->
    %% Block by block, never concatenated: the buffer keeps the bytes as S3
    %% delivered them so that reads share them instead of copying (see
    %% `rabbitmq_stream_s3_read_buffer`), and flattening them here would undo
    %% exactly that.
    Buffer1 = lists:foldl(
        fun(Block, Acc) -> rabbitmq_stream_s3_read_buffer:append(Block, Acc) end,
        Buffer,
        lists:reverse(Staged)
    ),
    P = set_buffer_for(Req, Buffer1, P0),
    advance_past(Req#req{flushed = Pos, staged = []}, Rest, P).

advance_past(#req{status = complete, flushed = Flushed, range_end = RangeEnd}, Rest, P) when
    Flushed > RangeEnd
->
    %% Fully delivered and fully flushed. Drop it so its successor can flush
    %% into the space it just filled.
    flush_reqs1(Rest, P);
advance_past(#req{flushed = Flushed, range_end = RangeEnd} = Req, Rest, P) when
    Flushed > RangeEnd
->
    %% Every byte is in the buffer but the closing frame has not arrived yet.
    %% That is no reason to hold up the successor.
    {Rest1, P1} = flush_reqs1(Rest, P),
    {[Req | Rest1], P1};
advance_past(#req{fragment = Fragment} = Req, Rest, P) ->
    %% Still owing bytes, so nothing behind it in its own fragment can flush.
    %% The other fragments' queues still can.
    {Rest1, P1} = flush_other_fragments(Fragment, Rest, P),
    {[Req | Rest1], P1}.

%% The buffer a request's bytes belong in, but only while the request is
%% positioned to append to it. Only ever consulted for a request that still
%% owes bytes: one that owes nothing is decided in `flush_reqs1/2`, since the
%% buffer's end may legitimately have moved past it.
flushable(#req{flushed = Flushed} = Req, P) ->
    case buffer_for(Req, P) of
        {ok, Buffer} ->
            case rabbitmq_stream_s3_read_buffer:end_pos(Buffer) =:= Flushed of
                true -> {ok, Buffer};
                false -> blocked
            end;
        error ->
            blocked
    end.

buffer_for(
    #req{fragment = Fragment},
    #pipeline{frag_ref = #fragment_ref{offset = Fragment}, buffer = Buffer}
) ->
    {ok, Buffer};
buffer_for(#req{fragment = Fragment}, #pipeline{nexts = Nexts}) ->
    %% `error` is a fragment with no seat in `nexts`: one the reader has moved
    %% past, or one dropped behind a 404. Every fragment it is still prefetching
    %% was seated when its first range was issued, so a delivery never creates a
    %% buffer here. That is what stops one being created over a hole and putting
    %% the reader back to fetching an object retention has deleted.
    find_next(Fragment, Nexts).

set_buffer_for(
    #req{fragment = Fragment},
    Buffer,
    #pipeline{frag_ref = #fragment_ref{offset = Fragment}} = P
) ->
    P#pipeline{buffer = Buffer};
set_buffer_for(#req{fragment = Fragment}, Buffer, #pipeline{nexts = Nexts} = P) ->
    P#pipeline{nexts = replace_next(Fragment, Buffer, Nexts)}.

%% ------------------------------------------------------------------
%% Reading
%% ------------------------------------------------------------------

-doc """
Serves `Bytes` at `Offset` from the current fragment's buffer.

`past_end` means the offset is beyond the fragment's data region and the caller
has to decide where to go next; `await` means the bytes have not arrived yet.
The reply is iodata - the blocks the buffer holds - so a range spanning two of
them is not copied to build one binary. See `rabbitmq_stream_s3_read_buffer`.
""".
-spec read(byte_offset(), non_neg_integer(), pipeline()) ->
    {ok, [binary()], pipeline()} | await | past_end.
read(Offset, _Bytes, #pipeline{frag_ref = #fragment_ref{size = FragSize}}) when
    Offset >= ?SEGMENT_HEADER_B + FragSize
->
    past_end;
read(Offset, Bytes, #pipeline{frag_ref = #fragment_ref{size = FragSize}, buffer = Buffer} = P) ->
    EndPos = rabbitmq_stream_s3_read_buffer:end_pos(Buffer),
    IdxStartPos = ?SEGMENT_HEADER_B + FragSize,
    case Offset + Bytes > EndPos of
        true when EndPos >= IdxStartPos ->
            %% Not enough data buffered, but all chunk data is buffered (the
            %% index region, which follows the chunk data, has started), so
            %% nothing more is coming for a read within the chunk-data range:
            %% this is the consumer over-reading a chunk header at the fragment
            %% tail. Cap the read at the index boundary and serve the remaining
            %% chunk data.
            %%
            %% The only read that overshoots EndPos is the header over-read,
            %% which read_header1/1 always issues at a chunk boundary, so
            %% IdxStartPos - Offset is the full last chunk (>= CHUNK_HEADER_B +
            %% FilterSize) and read_header2/2 has enough to parse. A previous
            %% `Offset + 64 =< EndPos` guard additionally required 64 bytes
            %% available; a final chunk plus index totalling fewer than 64 bytes
            %% failed it, so the reader awaited data that would never arrive and
            %% the consumer hung. (The 64 was also arbitrary: the over-read is
            %% CHUNK_HEADER_B + MAX_FILTER_SIZE bytes, not 64.)
            read(Offset, EndPos - Offset, P);
        true ->
            %% Chunk data is still streaming in (EndPos has not reached the
            %% index boundary). Wait for more.
            await;
        false ->
            ReadBytes = min(Bytes, IdxStartPos - Offset),
            Data = rabbitmq_stream_s3_read_buffer:read_iodata(Offset, ReadBytes, Buffer),
            %% Reads are non-decreasing, so blocks entirely below this read's
            %% start are consumed; drop them so they are freed incrementally.
            Buffer1 = rabbitmq_stream_s3_read_buffer:drop_before(Offset, Buffer),
            %% Past the end of what was served, not its start: the bytes just
            %% handed to the consumer are read, and counting them as unread
            %% would hold a read's worth of the prefetch window shut until the
            %% next read arrives.
            {ok, Data, P#pipeline{buffer = Buffer1, read_pos = Offset + ReadBytes}}
    end.

%% ------------------------------------------------------------------
%% Invariants
%% ------------------------------------------------------------------

-ifdef(TEST).

%% The queue is ordered by `{fragment, range_start}` and its ranges are
%% disjoint, which is what lets `flush_reqs/1` treat it as a reassembly queue
%% and what keeps `{Fragment, RangeStart}` a key. Two ranges of one fragment
%% that overlap mean bytes fetched twice and a buffer that can never be appended
%% to contiguously; a duplicate key means a delivery routed to the wrong
%% request. Checked after every operation that mutates the queue, so no caller
%% can construct a queue that breaks them.
%%
%% Every mutator routes through here rather than the ones that look risky,
%% because these are whole-queue properties: an operation that touches one
%% request can break an invariant that spans two, and the resulting queue is
%% still well-formed enough to keep working - a read stalls or a range is
%% fetched twice, far from the edit that caused it. Routing all of them through
%% one place is what makes the check a net rather than a spot assertion, and it
%% costs nothing in production (`checked/1` is a no-op outside TEST).
checked(
    #pipeline{
        reqs = Reqs,
        nexts = Nexts,
        next_missing = Missing,
        frag_ref = #fragment_ref{offset = Current}
    } = P
) ->
    %% The prefetched fragments run forward from the current one, without
    %% repeats. `advance/1` promotes the head whole, so a list out of order
    %% would move the reader onto a fragment past one it never read - delivering
    %% a hole to the consumer with nothing else to signal it.
    Offsets = [Offset || {#fragment_ref{offset = Offset}, _} <- Nexts],
    (Offsets =:= lists:sort(Offsets) andalso length(lists:usort(Offsets)) =:= length(Offsets)) orelse
        error({prefetch_out_of_order, Current, Offsets}),
    lists:all(fun(Offset) -> Offset > Current end, Offsets) orelse
        error({prefetch_below_current, Current, Offsets}),
    %% Every range beyond the current fragment has a seat in `nexts`. `push/3`
    %% makes one, and every path that takes a seat away takes that fragment's
    %% ranges with it. Without the pairing a delivery finds no buffer to append
    %% to (`buffer_for/2` creates none), so its bytes are dropped while the range
    %% stays queued holding a pooled connection - a stall with nothing at the
    %% point of failure to say why.
    lists:all(
        fun(#req{fragment = F}) -> F =< Current orelse find_next(F, Nexts) =/= error end,
        Reqs
    ) orelse
        error({unseated_request, Current, Offsets, [F || #req{fragment = F} <- Reqs]}),
    %% Nothing is fetched past a fragment known to be missing. Such a range
    %% could not reach a buffer - `buffer_for/2` seats none beyond the hole - so
    %% it would wedge the queue while holding a pooled connection.
    (not Missing orelse
        lists:all(
            fun(#req{fragment = F}) -> F =< last_next_offset(Nexts, Current) end, Reqs
        )) orelse
        error({prefetch_after_not_found, Current, Reqs}),
    Ids = [Id || #req{id = Id} <- Reqs],
    length(lists:usort(Ids)) =:= length(Ids) orelse error({duplicate_request_ids, Ids}),
    Positions = [{F, S} || #req{fragment = F, range_start = S} <- Reqs],
    Positions =:= lists:sort(Positions) orelse error({queue_out_of_order, Positions}),
    lists:foreach(
        fun(#req{range_start = Start, range_end = End, flushed = Flushed, pos = Pos} = Req) ->
            (Start =< Flushed andalso Flushed =< Pos andalso Pos =< End + 1) orelse
                error({range_positions_out_of_order, Req})
        end,
        Reqs
    ),
    assert_disjoint(Reqs),
    P.

assert_disjoint([#req{fragment = F, range_end = End} | [#req{fragment = F} = Next | _] = Rest]) ->
    Next#req.range_start > End orelse error({overlapping_ranges, End, Next}),
    assert_disjoint(Rest);
assert_disjoint([_ | Rest]) ->
    assert_disjoint(Rest);
assert_disjoint([]) ->
    ok.

%% The ranges still outstanding, in queue order. Tests describe what S3 answers
%% rather than which byte range the reader happened to ask for, so they use this
%% to address a delivery to the right request.
-spec outstanding_ranges(pipeline()) -> [{fragment_offset(), byte_offset(), byte_offset()}].
outstanding_ranges(#pipeline{reqs = Reqs}) ->
    [
        {Fragment, RangeStart, RangeEnd}
     || #req{fragment = Fragment, range_start = RangeStart, range_end = RangeEnd} <- Reqs
    ].

%% The ranges actually on the wire. S3 only answers those, so a test that
%% describes what S3 says addresses its deliveries with this rather than with
%% every range the queue holds.
-spec inflight_ranges(pipeline()) -> [{fragment_offset(), byte_offset(), byte_offset()}].
inflight_ranges(#pipeline{reqs = Reqs}) ->
    [
        {Fragment, RangeStart, RangeEnd}
     || #req{fragment = Fragment, range_start = RangeStart, range_end = RangeEnd, status = inflight} <-
            Reqs
    ].

%% The id of the range a fragment has outstanding at a position. Tests describe
%% what S3 answers - a range of bytes - rather than which id the pipeline
%% happened to mint for it, so this is how they address a delivery.
-spec find_request(fragment_offset(), byte_offset(), pipeline()) ->
    {ok, request_id()} | error.
find_request(Fragment, RangeStart, #pipeline{reqs = Reqs}) ->
    case
        [
            Id
         || #req{id = Id, fragment = F, range_start = S} <- Reqs, F =:= Fragment, S =:= RangeStart
        ]
    of
        [Id | _] -> {ok, Id};
        [] -> error
    end.

-spec request_count(pipeline()) -> non_neg_integer().
request_count(#pipeline{reqs = Reqs}) ->
    length(Reqs).

%% The clocks ranges are queued against, which is what the core's "nothing waits
%% on a clock that is not running" invariant is about. Narrower than
%% `waits_on/2`: a range released by a retry round is not waiting on anything.
-spec queued_on(backoff(), pipeline()) -> boolean().
queued_on(Kind, #pipeline{reqs = Reqs}) ->
    lists:any(fun(#req{status = Status}) -> Status =:= {backoff, Kind} end, Reqs).

-else.

checked(P) ->
    P.

-endif.
