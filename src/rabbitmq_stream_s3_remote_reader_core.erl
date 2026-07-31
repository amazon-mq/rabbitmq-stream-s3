%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_remote_reader_core).
-moduledoc """
Functional core for the remote read path.

This module contains the pure decision logic for reading stream data from the
remote tier. It manages buffer state, prefetch sizing and concurrency, fragment
transitions, and retry/timeout decisions. It produces effects that the
imperative shell (the remote reader gen_server) executes.

The core never performs I/O. It receives events describing what happened and
returns a new state plus a list of effects describing what should happen next.

## Events (inputs)

- `{read, Offset, Bytes, Hint}` - caller wants data at this position
- `{data, Fragment, RangeStart, Data, done | continue}` - S3 delivered bytes
- `{request_error, Fragment, RangeStart, Reason}` - S3 request failed
- `{retry, Kind}` - the retry timer of that backoff kind fired
- `deadline_expired` - pending read exceeded its deadline
- `{iterator_refreshed, Iterator}` - manifest cache provided new iterator

Requests are identified by the fragment they read and the start of the range
they were issued for. Ranges never overlap, so that pair is unique, and using
it rather than an opaque id keeps the core free of `make_ref/0`.

## Effects (outputs)

- `{reply, Result}` - respond to the pending read
- `{start_request, Key, Range, Fragment}` - initiate an S3 GET
- `{cancel_request, {Fragment, RangeStart}}` - abandon one in-flight GET
- `{cancel_requests, all}` - abandon every in-flight GET
- `{set_timer, Kind, Duration}` - schedule that backoff kind's retry timer
- `{cancel_timers, all}` - drop every armed retry timer, and any `retry` event
  an already-fired one has left in the shell's mailbox
- `{refresh_iterator, Offset}` - rebuild iterator past the given offset
- `{observe, Kind, Window}` - report a notable read-path event for metrics
- `{fatal_error, Reason}` - a non-retryable error is stopping the reader; report it (log + metric) before `stop`
- `stop` - shut down the remote reader

## Design

The core is structured around `try_read/1` which examines the buffer and
fragment state to determine if a pending read can be served. When it cannot,
the core returns effects to fetch more data. When data arrives, the shell
feeds it back and the core re-evaluates.

Buffered data is held in a `rabbitmq_stream_s3_read_buffer`: a queue of the
delivered binaries as-is rather than one flat binary, so a delivery is never
copied into place and consumed data is freed block-by-block (see that
module's docs for why flat-binary appends degrade here).

Several ranges of a fragment may be in flight at once, so their responses can
interleave, but the read buffer only accepts contiguous appends. Requests are
therefore held in an ordered queue which doubles as a reassembly queue: bytes
for a request whose predecessors have not finished are held in that request's
`staged` list and appended once it reaches the head. See `flush_reqs/1`.

Prefetch is sized by one number, the window: the bytes to hold or have
outstanding ahead of the consumer. Request size is fixed, so the window is
really a concurrency control - a buffer miss doubles it, sustained hits give a
request back. See "prefetch window control" below.

Fragment transitions happen when the read position exceeds the current
fragment's data region. The core checks for pre-fetched next-fragment data
and transitions immediately if available, or signals that more data is needed.
""".

-include_lib("stdlib/include/assert.hrl").
-include("include/rabbitmq_stream_s3.hrl").

%% Bounds of the `pool_busy` backoff. It starts low to catch a connection as
%% soon as its handshake completes and caps well below the retry backoff, since
%% a saturated pool is a local condition, not a reason to leave S3 alone.
-define(MIN_POOL_BUSY_DELAY_MS, 25).
-define(MAX_POOL_BUSY_DELAY_MS, 500).

%% ------------------------------------------------------------------
%% Types
%% ------------------------------------------------------------------

-record(cfg, {
    %% Bytes per range request. Fixed: concurrency, not size, is what scales a
    %% remote reader's bandwidth past one connection's transfer rate.
    request_size :: pos_integer(),
    %% Ceiling on the prefetch window, and so on this reader's memory: it can
    %% hold or have outstanding at most `window_max + request_size` bytes.
    window_max :: pos_integer(),
    %% Most requests that may be in flight at once, across both fragments.
    max_depth :: pos_integer(),
    min_retry_delay_ms :: pos_integer(),
    max_retry_delay_ms :: pos_integer()
}).

-record(pending, {
    offset :: byte_offset(),
    bytes :: pos_integer(),
    hint :: chunk_boundary | within_chunk
}).

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
    %% from handing the clock back to its minimum mid-round (see
    %% `reset_idle_backoffs/1`).
    retried = undefined :: undefined | backoff(),
    staged = [] :: [binary()]
}).

-record(state, {
    stream :: stream_id(),
    cfg :: #cfg{},

    %% Prefetch window: how far ahead of the consumer to fetch. See the
    %% "prefetch window control" section.
    window :: pos_integer(),
    bytes_served_since_miss = 0 :: non_neg_integer(),
    %% The pending read has already been counted as a miss.
    missed_pending = false :: boolean(),

    %% Retry state: one backoff clock per kind, grown and reset independently.
    %% They measure unrelated conditions - a throttling or unreachable S3
    %% against a pool that has no free connection yet - and are three orders of
    %% magnitude apart, so sharing either the delay or the timer would let the
    %% pool's clock re-issue a throttled range 25ms after S3 asked for a second.
    retry_delay :: pos_integer(),
    pool_busy_delay = ?MIN_POOL_BUSY_DELAY_MS :: pos_integer(),

    %% Current fragment
    fragment_ref :: #fragment_ref{},
    key :: rabbitmq_stream_s3:key(),
    buffer :: rabbitmq_stream_s3_read_buffer:buffer(),
    %% Where the consumer has read up to. The buffer's own `start_pos` is not a
    %% substitute: it only moves when a whole block falls behind the read, so
    %% up to a block of consumed bytes would keep counting against the window.
    read_pos :: byte_offset(),

    %% Next fragment (pre-fetched)
    next :: {#fragment_ref{}, rabbitmq_stream_s3_read_buffer:buffer()} | undefined | not_found,

    %% Fragment iterator
    iterator :: rabbitmq_stream_s3_fragment_iterator:iterator(),

    %% The entry the iterator points at, and the iterator advanced past it,
    %% memoised. `next/1` is a synchronous S3 GET whenever that entry sits
    %% behind a group node, so the answer is kept for as long as the iterator is
    %% the same one (it is only ever replaced wholesale, never advanced in
    %% place), and the fragment transition reuses it rather than fetching the
    %% group a second time. `unknown` means "not looked up yet"; `failed` means
    %% the group fetch failed transiently, which is not an answer to keep but is
    %% a reason not to ask again until a retry timer fires.
    next_peek = unknown ::
        unknown
        | failed
        | none
        | {ok, #fragment_ref{}, rabbitmq_stream_s3_fragment_iterator:iterator()},

    %% Outstanding ranges, ordered by `{fragment, range_start}` and disjoint.
    %% Normally contiguous within a fragment too, but not while a request that
    %% has delivered every byte it owes waits on its closing frame: the requests
    %% behind it are dropped as they flush, so a gap the buffer already holds can
    %% open up. Also the reassembly queue (see `flush_reqs/1`).
    reqs = [] :: [#req{}],

    %% The backoff kinds whose retry timer is armed and has not fired yet.
    %% Without this a batch of N failing requests would arm N timers and drive N
    %% retry passes; keeping it per kind stops one kind's timer from standing in
    %% for the other's, which would both suppress that other backoff's growth
    %% and release its ranges on the wrong clock.
    timers = #{} :: #{backoff() => armed},

    %% Pending read (at most one)
    pending :: #pending{} | undefined,

    %% Current fragment returned 404
    current_not_found = false :: boolean()
}).

-type state() :: #state{}.
-type fragment_offset() :: osiris:offset().
-doc "Identifies one outstanding range: the fragment read and where it starts.".
-type request_key() :: {fragment_offset(), byte_offset()}.
-doc """
The two backoff clocks: `fault` for an S3 request that failed, `pool_busy` for
one that never left the node because the connection pool had nothing free.
""".
-type backoff() :: fault | pool_busy.

-type event() ::
    {read, byte_offset(), pos_integer(), chunk_boundary | within_chunk}
    | {data, fragment_offset(), byte_offset(), binary(), done | continue}
    | {request_error, fragment_offset(), byte_offset(), term()}
    | {retry, backoff()}
    | deadline_expired
    | {iterator_refreshed, rabbitmq_stream_s3_fragment_iterator:iterator() | end_of_manifest}.

-type observe_kind() :: hit | miss | fragment_transition.

-type effect() ::
    {reply, read_result()}
    | {start_request, rabbitmq_stream_s3:key(), {byte_offset(), byte_offset()}, fragment_offset()}
    | {cancel_request, request_key()}
    | {cancel_requests, all}
    | {set_timer, backoff(), pos_integer()}
    | {cancel_timers, all}
    | {refresh_iterator, osiris:offset()}
    | {observe, observe_kind(), pos_integer()}
    | {fatal_error, term()}
    | stop.

-type read_result() ::
    {ok, binary()}
    | {error, timeout}
    | {next_fragment, osiris:offset()}
    | {become_local, osiris:offset()}
    | end_of_stream.

-export_type([state/0, event/0, effect/0, read_result/0, request_key/0, backoff/0]).

%% ------------------------------------------------------------------
%% API
%% ------------------------------------------------------------------

-export([
    init/5,
    step/2,
    pending/1,
    current_fragment_offset/1
]).

-ifdef(TEST).
%% The ranges the core is still waiting on, in queue order. Tests describe what
%% S3 answers rather than which byte range the core happened to ask for, so
%% they use this to address a delivery to the right request.
-export([outstanding_ranges/1, window_bytes/1, read_position/1, load/1]).
-endif.

%% @doc Initialize the read core.
%% `Position` is the byte offset within the fragment to start reading from
%% (typically `?SEGMENT_HEADER_B` for the beginning, or further in if the
%% consumer attached mid-fragment).
%% `Opts` is a map of configuration overrides (see `#cfg{}`). Pass `#{}`
%% for defaults.
-spec init(
    stream_id(),
    #fragment_ref{},
    byte_offset(),
    rabbitmq_stream_s3_fragment_iterator:iterator(),
    map()
) ->
    {state(), [effect()]}.
init(StreamId, FragRef, Position, Iterator, Opts) ->
    #fragment_ref{offset = Offset, uid = Uid} = FragRef,
    Key = rabbitmq_stream_s3:fragment_key(StreamId, Offset, Uid),
    Cfg = build_cfg(Opts),
    %% The iterator arrives already advanced past the current entry
    %% (done by find_position in the log reader). It points at the next
    %% fragment, ready for prefetch and forward navigation.
    State = #state{
        stream = StreamId,
        cfg = Cfg,
        window = Cfg#cfg.request_size,
        retry_delay = Cfg#cfg.min_retry_delay_ms,
        fragment_ref = FragRef,
        key = Key,
        buffer = rabbitmq_stream_s3_read_buffer:new(Position),
        read_pos = Position,
        iterator = Iterator,
        next = undefined
    },
    %% Immediately request data for the current fragment.
    {State1, Effects} = start_current_request(State),
    {State1, Effects}.

%% @doc Feed an event into the core, get back new state and effects.
-spec step(state(), event()) -> {state(), [effect()]}.
step(State, {read, Offset, Bytes, Hint}) ->
    State1 = State#state{
        pending = #pending{offset = Offset, bytes = Bytes, hint = Hint},
        missed_pending = false
    },
    try_serve(State1);
step(State0, {data, Fragment, RangeStart, Data, DoneOrContinue}) ->
    %% The backoffs are judged after the delivery has been absorbed, not before:
    %% that is when the range it closes has left the queue, and when a response
    %% that closed without delivering a byte has been put back into backoff.
    {State1, Effects1} = add_data(Fragment, RangeStart, Data, DoneOrContinue, State0),
    State2 = reset_idle_backoffs(State1),
    {State3, Effects2} = maybe_start_requests(State2),
    {State4, Effects3} = try_serve(State3),
    {State4, Effects1 ++ Effects2 ++ Effects3};
step(State0, {request_error, Fragment, _RangeStart, not_found}) ->
    case Fragment =:= current_fragment_offset(State0) of
        true ->
            %% Current fragment 404. Refresh the iterator past this offset.
            State = State0#state{current_not_found = true, reqs = []},
            case State#state.pending of
                undefined -> {State, [{cancel_requests, all}]};
                _ -> {State, [{cancel_requests, all}, {refresh_iterator, Fragment}]}
            end;
        false ->
            %% Next fragment 404. Mark it and try to serve (may trigger refresh
            %% when the consumer reads past the current fragment).
            {Dropped, Reqs} = drop_fragment_reqs(Fragment, State0#state.reqs),
            State = State0#state{next = not_found, reqs = Reqs},
            {State1, Effects} = try_serve(State),
            {State1, [{cancel_request, Key} || Key <- Dropped] ++ Effects}
    end;
step(State0, {request_error, Fragment, RangeStart, Reason}) when
    Reason =:= slow_down;
    Reason =:= internal_error;
    Reason =:= timeout;
    Reason =:= stream_error;
    Reason =:= connection_error
->
    %% Transient error. Put the range back in the queue and retry it with
    %% exponential backoff. Only the failed range is retried: co-pending
    %% requests keep streaming, and the range is restarted at the last byte that
    %% reached a buffer so no bytes are re-fetched or lost.
    case fail_req(Fragment, RangeStart, fault, State0) of
        {ok, State1} ->
            arm_retry(fault, State1);
        {dropped, State1} ->
            drop_req_done(State1);
        stale ->
            {State0, []}
    end;
step(State0, {request_error, Fragment, RangeStart, pool_busy}) ->
    %% Pool is growing — a connection becomes available once its TLS handshake
    %% completes (fast on same-region S3, but not instant). Use a mild backoff
    %% (25, 50, 100, 200, 400, 500, 500...) starting low to catch the connection
    %% as soon as it is ready, doubling up to a 500ms cap so we don't spin if the
    %% pool cannot grow (e.g. S3 unreachable).
    case fail_req(Fragment, RangeStart, pool_busy, State0) of
        {ok, State1} ->
            arm_retry(pool_busy, State1);
        {dropped, State1} ->
            drop_req_done(State1);
        stale ->
            {State0, []}
    end;
step(State0, {request_error, _Fragment, _RangeStart, Reason}) ->
    %% Non-retryable error (e.g. 403 AccessDenied, an unexpected status). Report
    %% the reason before stopping so an operator can answer "why did this
    %% consumer's remote read stop?". Without this the shutdown is silent: the
    %% API layer's per-status counters tick, but nothing ties a stopped reader
    %% to a cause. The report effect must precede `stop`.
    %%
    %% Deliberately not gated on the request still being live: a 403 is about
    %% the reader's credentials, not about one range, so a stale one still
    %% means every subsequent request would fail too.
    {State0, [{fatal_error, Reason}, stop]};
step(State0, {retry, Kind}) ->
    %% Only the ranges waiting on this clock are released. The other kind's
    %% ranges keep waiting for their own timer: a pool_busy retry that released
    %% them would put a range S3 has just asked us to slow down straight back on
    %% the wire, 25ms after a `slow_down`.
    %% A `failed` look-ahead memo needs no clearing here: it is honoured only
    %% while the fault clock is armed (see `peek_next_fragment/2`), and this
    %% round has just given that clock up.
    State1 = State0#state{
        timers = maps:remove(Kind, State0#state.timers),
        reqs = [release(Kind, Req) || Req <- State0#state.reqs]
    },
    {State2, Effects} = maybe_start_requests(State1),
    {State3, Effects2} = try_serve(State2),
    {State3, Effects ++ Effects2};
step(#state{cfg = #cfg{min_retry_delay_ms = MinDelay}} = State0, deadline_expired) ->
    %% The shell's pending-read deadline fired. Reply with an error and drop
    %% everything the retry cannot use: the ranges in flight, the bytes staged
    %% behind them, and both backoff clocks.
    %% See: https://github.com/amazon-mq/rabbitmq-stream-s3/issues/157
    %% See: https://github.com/amazon-mq/rabbitmq-stream-s3/issues/161
    %%
    %% The buffer is kept. It holds a contiguous run of the current fragment
    %% that nothing in flight contributed to - staged bytes are held on their
    %% request, not in it - so it is exactly as valid after the deadline as
    %% before it, and the log reader's retry is usually a read it can answer
    %% outright. Nor can the retry fall below it: `try_read/3` frees blocks
    %% below each read's start as it serves it, so a read below `start_pos` is
    %% out of the buffer's bounds with or without a deadline.
    %%
    %% Emptying it re-based the fetch frontier at byte 0 of the current
    %% fragment: `frontier/3` reads the next byte to ask for off the buffer's
    %% end, and `fetch_ceiling/1` measures the pending read against `read_pos`,
    %% which was reset with it. One expiry therefore re-downloaded the whole
    %% fragment the consumer had already read through - up to
    %% `fragment_target_size`, twice the `window_max` this reader is meant to
    %% hold - before asking for the next fragment the read was actually waiting
    %% on. Serving nothing in the meantime, the retry expired again and
    %% repeated the download.
    %%
    %% Both retry timers are disowned along with the requests they were armed
    %% for: leaving a kind marked armed would make `arm_retry/2` a no-op for the
    %% next read's failures of that kind, which would then wait out the stale
    %% timer - up to `max_retry_delay_ms` - before anything re-issued them.
    %%
    %% Disowning them is not enough on its own, which is what `cancel_timers`
    %% is for. A stale timer carries the delay it was armed with, up to
    %% `max_retry_delay_ms`, so it can land in the middle of a *later* backoff
    %% round: it would release that round's ranges early - putting a range back
    %% on the wire before the pause S3 asked for had elapsed - and clear the
    %% kind from `timers`, so the round's own timer would then be taken for a
    %% fresh one.
    %%
    %% The prefetched next fragment is dropped along with them. Its own ranges
    %% are cancelled with the rest, but its bytes keep counting against the
    %% prefetch window (see `outstanding/1`), so keeping it would hold window
    %% space that nothing is left to fetch into. With the window at its ceiling
    %% that is permanent: `has_room/1` stays false however many misses the reader
    %% takes, `extend_frontier/1` issues nothing, and no range is ever requested
    %% for the current fragment again - every subsequent read waits out its own
    %% deadline. Below the ceiling it costs at least one more of them before the
    %% window doubles past what the stale prefetch holds.
    %% The look-ahead memo is left alone. An answered one is the truth about an
    %% iterator this event does not touch, and re-resolving it would spend a
    %% group GET on an answer already in hand; a `failed` one needs no clearing,
    %% because dropping the fault clock is itself what licenses the next attempt
    %% (see `peek_next_fragment/2`).
    State = State0#state{
        next = drop_prefetch(State0#state.next),
        pending = undefined,
        reqs = [],
        timers = #{},
        retry_delay = MinDelay,
        pool_busy_delay = ?MIN_POOL_BUSY_DELAY_MS
    },
    {State, [{cancel_requests, all}, {cancel_timers, all}, {reply, {error, timeout}}]};
step(State0, {iterator_refreshed, end_of_manifest}) ->
    %% No new entries. Become local.
    {State, Effects} = goto_next_fragment(State0),
    {State, Effects ++ [{reply, {become_local, current_fragment_offset(State0)}}]};
step(State0, {iterator_refreshed, Iterator}) ->
    %% Iterator has been refreshed past the 404'd fragment. Reinitialize
    %% at the next available fragment.
    %%
    %% The shell cancels every in-flight request before stepping this event, so
    %% no frame for one can arrive any more: they have to leave the queue here.
    %% One left behind would never be re-issued (`issue_ready/1` only starts
    %% `ready` requests) and `flushable/2` would report it `blocked` for the rest
    %% of the read, holding back every byte of its fragment queued behind it.
    Cancelled = State0#state{reqs = []},
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, FragRef, Iterator1} ->
            #fragment_ref{offset = Offset, uid = Uid} = FragRef,
            StreamId = State0#state.stream,
            Cfg = State0#state.cfg,
            Key = rabbitmq_stream_s3:fragment_key(StreamId, Offset, Uid),
            State = #state{
                stream = StreamId,
                cfg = Cfg,
                window = Cfg#cfg.request_size,
                retry_delay = Cfg#cfg.min_retry_delay_ms,
                fragment_ref = FragRef,
                key = Key,
                buffer = rabbitmq_stream_s3_read_buffer:new(?SEGMENT_HEADER_B),
                read_pos = ?SEGMENT_HEADER_B,
                iterator = Iterator1,
                next = undefined
            },
            {State1, Effects} = start_current_request(State),
            %% The state is rebuilt from scratch, which disowns both backoffs
            %% along with the requests they were armed for, so their timers are
            %% cancelled too. One left running would land part-way through a
            %% later backoff round and release its ranges early - the same
            %% hazard as at `deadline_expired`, which resets the same fields.
            {State1, [
                {cancel_requests, all},
                {cancel_timers, all},
                {reply, {next_fragment, Offset}}
                | Effects
            ]};
        end_of_manifest ->
            %% Iterator exhausted after refresh. Become local.
            {Cancelled, [{reply, {become_local, current_fragment_offset(Cancelled)}}]};
        {error, {group_fetch_failed, _Reason}} ->
            %% A group object could not be fetched while advancing the
            %% refreshed iterator. Transient S3 error, not end of manifest:
            %% retry rather than routing to a local tier that may lack the
            %% data. The refresh was asked for by a pending read, and the retry
            %% re-serves it, so it asks for the refresh again.
            retry_group_fetch(Cancelled)
    end.

%% @doc Returns the pending read, if any.
-spec pending(state()) -> undefined | {byte_offset(), pos_integer()}.
pending(#state{pending = undefined}) -> undefined;
pending(#state{pending = #pending{offset = O, bytes = B}}) -> {O, B}.

%% @doc Returns the offset of the fragment currently being read.
-spec current_fragment_offset(state()) -> osiris:offset().
current_fragment_offset(#state{fragment_ref = #fragment_ref{offset = Off}}) -> Off.

-ifdef(TEST).
-spec outstanding_ranges(state()) -> [{fragment_offset(), byte_offset(), byte_offset()}].
outstanding_ranges(#state{reqs = Reqs}) ->
    [
        {Fragment, RangeStart, RangeEnd}
     || #req{fragment = Fragment, range_start = RangeStart, range_end = RangeEnd} <- Reqs
    ].

-spec window_bytes(state()) -> pos_integer().
window_bytes(#state{window = Window}) ->
    Window.

%% Where the consumer has read up to, so a test can carry on reading from
%% wherever an earlier phase left the reader rather than assuming a position.
-spec read_position(state()) -> byte_offset().
read_position(#state{read_pos = ReadPos}) ->
    ReadPos.

%% Bytes held or asked for ahead of the consumer, and how many requests are in
%% flight: the two quantities the prefetch window and depth cap bound.
-spec load(state()) -> {non_neg_integer(), non_neg_integer()}.
load(State) ->
    {outstanding(State), inflight_count(State)}.
-endif.

%% ------------------------------------------------------------------
%% Internal: try to serve the pending read
%% ------------------------------------------------------------------

try_serve(#state{pending = undefined} = State) ->
    {State, []};
try_serve(#state{pending = #pending{offset = Offset, bytes = Bytes}} = State) ->
    case try_read(State, Offset, Bytes) of
        {ok, Data, State1} ->
            State2 = note_hit(byte_size(Data), State1#state{pending = undefined}),
            {State3, Effects} = maybe_start_requests(State2),
            {State3, [
                {reply, {ok, Data}},
                {observe, hit, window(State3)}
                | Effects
            ]};
        {next_fragment, NextOffset, CancelEffects, State1} ->
            State2 = State1#state{pending = undefined},
            {State3, Effects} = maybe_start_requests(State2),
            {State3,
                CancelEffects ++
                    [
                        {reply, {next_fragment, NextOffset}},
                        {observe, fragment_transition, window(State3)}
                        | Effects
                    ]};
        {await, State1} ->
            {State2, MissEffects} = note_miss(State1),
            {State3, Effects} = maybe_start_requests(State2),
            {State3, MissEffects ++ Effects};
        {not_found_check_range, State1} ->
            not_found_refresh(State1);
        {refresh_iterator, State1} ->
            %% Iterator exhausted. Refresh past current fragment.
            {State1, [{refresh_iterator, current_fragment_offset(State1)}]};
        {group_fetch_failed, State1} ->
            %% A group fetch failed transiently while advancing. Retry rather
            %% than becoming local.
            retry_group_fetch(State1)
    end.

%% ------------------------------------------------------------------
%% Internal: buffer read logic
%% ------------------------------------------------------------------

try_read(#state{fragment_ref = #fragment_ref{size = FragSize}} = State, Offset, _Bytes) when
    Offset >= ?SEGMENT_HEADER_B + FragSize
->
    %% Past end of current fragment. Transition.
    try_fragment_transition(State);
try_read(
    #state{fragment_ref = #fragment_ref{size = FragSize}, buffer = Buffer} = State,
    Offset,
    Bytes
) ->
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
            try_read(State, Offset, EndPos - Offset);
        true ->
            %% Chunk data is still streaming in (EndPos has not reached the
            %% index boundary). Wait for more, unless the fragment 404'd.
            case State of
                #state{current_not_found = true} ->
                    {not_found_check_range, State};
                _ ->
                    {await, State}
            end;
        false ->
            ReadBytes = min(Bytes, IdxStartPos - Offset),
            Data = rabbitmq_stream_s3_read_buffer:read(Offset, ReadBytes, Buffer),
            %% Reads are non-decreasing, so blocks entirely below this read's
            %% start are consumed; drop them so they are freed incrementally.
            Buffer1 = rabbitmq_stream_s3_read_buffer:drop_before(Offset, Buffer),
            {ok, Data, State#state{buffer = Buffer1, read_pos = Offset}}
    end.

try_fragment_transition(
    #state{next = {#fragment_ref{offset = NextOffset}, _}} = State0
) ->
    {State, Effects} = goto_next_fragment(State0),
    {next_fragment, NextOffset, Effects, State};
try_fragment_transition(
    #state{next = not_found} = State
) ->
    %% Next fragment 404. Need manifest range to decide.
    {not_found_check_range, State};
try_fragment_transition(
    #state{next = undefined, next_peek = Peek0} = State0
) ->
    {Peek, _Attempted} = peek_next_fragment(State0, Peek0),
    State = State0#state{next_peek = Peek},
    case State#state.next_peek of
        {ok, _FragRef, _Advanced} ->
            {await, State};
        none ->
            {refresh_iterator, State};
        failed ->
            %% A group object referenced by the manifest could not be fetched.
            %% This is a transient S3 error, not the end of the manifest (a
            %% group deleted by retention surfaces as `not_found`, which the
            %% iterator skips). The group is part of the remote tier we are
            %% reading, so becoming local here would risk serving missing or
            %% wrong data (Tier overlap). Retry instead.
            {group_fetch_failed, State}
    end.

%% A group object could not be fetched (a transient S3 error). Keep the pending
%% read and retry with backoff rather than routing the consumer to the local
%% tier. The pending-read deadline bounds the loop and surfaces
%% `{error, timeout}` if S3 stays unavailable.
%%
%% The ranges in flight are left alone, except on the `iterator_refreshed` path
%% where the shell has already cancelled them. The failure is in advancing the
%% iterator, which says nothing about the fragment GETs already on the wire:
%% they may still be delivering the very bytes the pending read is waiting for,
%% and the retry re-attempts only the group fetch. Dropping them also costs more
%% than the bytes, since `cancel_async/2` closes the pooled connection to stop
%% the response draining: a failed group fetch used to tear down up to
%% `max_depth` healthy connections out of a pool shared with the manifest,
%% group and index GETs.
retry_group_fetch(State) ->
    arm_retry(fault, State).

%% ------------------------------------------------------------------
%% Internal: fragment navigation
%% ------------------------------------------------------------------

%% Decide which offset to refresh the iterator past after a 404. Two paths
%% reach `not_found_check_range`, and they 404'd different fragments, so they
%% must refresh past different offsets:
%%
%%  1. `try_fragment_transition` matching `next = not_found`: the consumer read
%%     past the current fragment and the prefetched *next* fragment 404'd. The
%%     iterator still points at that 404'd next entry, so `next_fragment_offset`
%%     returns its offset and we refresh past it. `current_not_found` is false.
%%
%%  2. `try_read` matching `current_not_found = true`: the *current* fragment
%%     404'd. The iterator was advanced past the current entry at init, so it
%%     points at the entry AFTER the 404'd one - a fragment that is still live.
%%     Refreshing past *that* offset would silently skip the live fragment
%%     (issue #173), so refresh past the current fragment's own offset instead.
%%
%% In both cases the shell's refresh_iterator/2 advances past anything at or
%% below the given offset and resumes at the first surviving fragment, so
%% passing the actually-404'd offset is what keeps a live fragment from being
%% skipped.
not_found_refresh(#state{current_not_found = true} = State) ->
    %% Path 2: the current fragment 404'd. Refresh past its own offset.
    {State, [{refresh_iterator, current_fragment_offset(State)}]};
not_found_refresh(#state{next_peek = Peek0} = State0) ->
    %% Path 1: the prefetched next fragment 404'd. Refresh past it, falling
    %% back to the current offset if the iterator is exhausted (the 404'd
    %% next was the last entry in the manifest).
    %%
    %% Through the memo, like every other look-ahead: asking the iterator
    %% directly here spent a synchronous group GET on an answer this reader has
    %% usually already paid for - and it is the answer that told it to prefetch
    %% the fragment that has just 404'd, so it is memoised by definition.
    {Peek, _Attempted} = peek_next_fragment(State0, Peek0),
    State = State0#state{next_peek = Peek},
    case Peek of
        {ok, #fragment_ref{offset = NotFoundOffset}, _Advanced} ->
            {State, [{refresh_iterator, NotFoundOffset}]};
        none ->
            {State, [{refresh_iterator, current_fragment_offset(State)}]};
        failed ->
            %% Probing the next entry hit a transient group fetch error. Retry
            %% rather than becoming local.
            retry_group_fetch(State)
    end.

%% Moves to the fragment `next` was prefetching. Returns the cancel effects for
%% any request still outstanding against the fragment being left behind: those
%% sort ahead of the new current fragment's requests, so leaving them in the
%% queue would block reassembly forever. In practice the transition can only
%% happen once the old fragment's data region is fully buffered, so the list is
%% normally empty; dropping them defensively costs nothing and turns a possible
%% deadlock into a cancelled request.
goto_next_fragment(State0) ->
    {State, NewOffset, Effects0} = goto_next_fragment1(State0),
    {Stale, Reqs} = lists:partition(
        fun(#req{fragment = Fragment}) -> Fragment < NewOffset end, State#state.reqs
    ),
    Effects = Effects0 ++ [{cancel_request, req_key(Req)} || Req <- Stale],
    {State#state{reqs = Reqs}, Effects}.

goto_next_fragment1(
    #state{
        stream = StreamId,
        next = Next0,
        fragment_ref = #fragment_ref{offset = CurrentOffset}
    } = State
) ->
    Iterator = advance_iterator(State),
    case Next0 of
        {#fragment_ref{offset = NextOffset, uid = NextUid} = NextFragRef, Buffer} ->
            %% Forward navigation must be strictly increasing. A fragment
            %% iterator that mispositions (for example descending into a group
            %% at the wrong offset) can hand back an earlier fragment, which
            %% would deliver out-of-order or duplicate offsets to the consumer
            %% with no other signal. Assert the invariant here so such a bug is
            %% a loud crash at the transition rather than silent data corruption
            %% downstream, and so any future iterator regression fails fast.
            ?assert(NextOffset > CurrentOffset),
            Key = rabbitmq_stream_s3:fragment_key(StreamId, NextOffset, NextUid),
            {
                State#state{
                    buffer = Buffer,
                    read_pos = rabbitmq_stream_s3_read_buffer:start_pos(Buffer),
                    fragment_ref = NextFragRef,
                    key = Key,
                    iterator = Iterator,
                    next_peek = unknown,
                    next = undefined,
                    current_not_found = false
                },
                NextOffset,
                []
            };
        _ ->
            %% There was no prefetched fragment to move to, so the fragment does
            %% not change and the buffer is reset under the requests still
            %% reading it. Those requests can no longer flush - their `flushed`
            %% is past the empty buffer's end for good, and a blocked head skips
            %% the rest of its fragment's queue - so they would wedge the queue
            %% and hold their pooled connections. Drop them.
            Cancels =
                case State#state.reqs of
                    [] -> [];
                    _ -> [{cancel_requests, all}]
                end,
            {
                State#state{
                    buffer = rabbitmq_stream_s3_read_buffer:new(?SEGMENT_HEADER_B),
                    read_pos = ?SEGMENT_HEADER_B,
                    iterator = Iterator,
                    next_peek = unknown,
                    next = undefined,
                    reqs = [],
                    current_not_found = false
                },
                CurrentOffset,
                Cancels
            }
    end.

%% Give up the bytes prefetched for the next fragment. A `not_found` next is
%% kept: it holds no bytes, so it costs the window nothing, and a fragment
%% retention has deleted does not come back - forgetting it would spend another
%% GET learning the same 404.
drop_prefetch(not_found) -> not_found;
drop_prefetch(_) -> undefined.

%% The peek already advanced the iterator past the entry being moved to, and
%% paid for the group fetch that took, so take its answer rather than descending
%% into the same group again.
advance_iterator(#state{next_peek = {ok, _FragRef, Advanced}}) ->
    Advanced;
advance_iterator(#state{iterator = Iterator}) ->
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, _, It} -> It;
        _ -> Iterator
    end.

%% ------------------------------------------------------------------
%% Internal: data buffering and reassembly
%% ------------------------------------------------------------------

%% Absorb a delivery for one outstanding range and flush whatever that makes
%% contiguous. Deliveries for a range the core no longer tracks are dropped:
%% a request cancelled by a read deadline or an iterator refresh can still have
%% frames in flight, and appending those bytes would corrupt the buffer's
%% addressing.
add_data(Fragment, RangeStart, Data, DoneOrContinue, #state{reqs = Reqs} = State) ->
    case take_req(Fragment, RangeStart, Reqs) of
        {Before, #req{status = inflight} = Req0, After} ->
            Req = stage(Data, Req0),
            case DoneOrContinue of
                continue ->
                    {flush_reqs(State#state{reqs = Before ++ [Req | After]}), []};
                done ->
                    complete_req(Before, Req, After, State)
            end;
        _ ->
            {State, []}
    end.

%% Retain the delivered block against the request that asked for it. S3 answers
%% a range request with exactly that range, but a backend that over-delivers
%% would otherwise push this request's bytes over its successor's range and
%% double-append them, so the block is clipped to what this request owns.
stage(Data, #req{pos = Pos, range_end = RangeEnd, staged = Staged} = Req) ->
    case RangeEnd + 1 - Pos of
        Room when Room =< 0 ->
            Req;
        Room ->
            case byte_size(Data) of
                0 ->
                    Req;
                Size when Size =< Room ->
                    Req#req{pos = Pos + Size, staged = [Data | Staged]};
                _ ->
                    Req#req{pos = Pos + Room, staged = [binary:part(Data, 0, Room) | Staged]}
            end
    end.

complete_req(Before, #req{pos = Pos, range_end = RangeEnd} = Req, After, State) when
    Pos > RangeEnd
->
    {flush_reqs(State#state{reqs = Before ++ [Req#req{status = complete} | After]}), []};
complete_req(Before, #req{pos = Pos, range_start = Pos} = Req, After, State0) ->
    %% The request ended without delivering a byte. Re-issuing it straight away
    %% would spin against whatever is answering that way, so back off as if it
    %% had failed.
    State1 = flush_reqs(State0#state{reqs = Before ++ [Req#req{status = {backoff, fault}} | After]}),
    arm_retry(fault, State1);
complete_req(Before, #req{pos = Pos, range_end = RangeEnd} = Req0, After, State) ->
    %% Short completion: the response ended before the range did (a truncated
    %% response, or a fragment smaller than the manifest claims). Shrink the
    %% request to what actually arrived and queue the bytes that never came as a
    %% gap request, keeping the fragment's ranges contiguous.
    %%
    %% The gap is queued even when nothing else is outstanding for the fragment,
    %% rather than being left to the frontier. `extend_frontier/1` is
    %% window-gated and the missing bytes are still counted against the window
    %% until the request is dropped, so a short completion that frees fewer
    %% bytes than a whole request leaves no room to re-request them: the hole
    %% never closes and every read behind it stalls until the read deadline.
    %% `issue_ready/1` is not window-gated for exactly this reason.
    Req = Req0#req{status = complete, range_end = Pos - 1},
    Gap = Req0#req{
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
    {flush_reqs(State#state{reqs = Before ++ [Req, Gap | After]}), []}.

%% Append staged bytes into their fragment's buffer for as long as the head of
%% that fragment's queue is contiguous with it. The read buffer takes contiguous
%% appends only, so a request whose predecessor is unfinished has to wait; when
%% the predecessor is dropped, the successor flushes behind it in the same pass.
%%
%% Each fragment's queue is independent: a stuck current-fragment request must
%% not stop the prefetched next fragment from filling its own buffer.
flush_reqs(#state{reqs = Reqs} = State0) ->
    {Reqs1, State} = flush_reqs1(Reqs, State0),
    State#state{reqs = Reqs1}.

flush_reqs1([], State) ->
    {[], State};
flush_reqs1([#req{flushed = Flushed, range_end = RangeEnd} = Req | Rest], State) when
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
    advance_past(Req, Rest, State);
flush_reqs1([#req{fragment = Fragment} = Req | Rest], State) ->
    case flushable(Req, State) of
        {ok, Buffer} ->
            flush_req(Req, Rest, Buffer, State);
        blocked ->
            {Rest1, State1} = flush_other_fragments(Fragment, Rest, State),
            {[Req | Rest1], State1}
    end.

%% Skip past the rest of `Fragment`'s queue - nothing in it can flush while a
%% request ahead of it still owes bytes - and flush the fragments behind it.
%% Each fragment's queue is independent, so a stuck current-fragment request
%% must not stop the prefetched next fragment from filling its own buffer.
flush_other_fragments(Fragment, Reqs, State) ->
    {Same, Other} = lists:splitwith(fun(#req{fragment = F}) -> F =:= Fragment end, Reqs),
    {Other1, State1} = flush_reqs1(Other, State),
    {Same ++ Other1, State1}.

flush_req(#req{flushed = Pos, pos = Pos} = Req, Rest, _Buffer, State) ->
    %% Nothing new to append. Leave the buffer alone: writing an empty one back
    %% would create the next fragment's buffer, which is what says the prefetch
    %% has delivered something and lets a fragment transition go ahead.
    advance_past(Req, Rest, State);
flush_req(#req{pos = Pos, staged = Staged} = Req, Rest, Buffer, State0) ->
    Buffer1 = lists:foldl(
        fun(Block, Acc) -> rabbitmq_stream_s3_read_buffer:append(Block, Acc) end,
        Buffer,
        lists:reverse(Staged)
    ),
    State = set_buffer_for(Req, Buffer1, State0),
    advance_past(Req#req{flushed = Pos, staged = []}, Rest, State).

advance_past(#req{status = complete, flushed = Flushed, range_end = RangeEnd}, Rest, State) when
    Flushed > RangeEnd
->
    %% Fully delivered and fully flushed. Drop it so its successor can flush
    %% into the space it just filled.
    flush_reqs1(Rest, State);
advance_past(#req{flushed = Flushed, range_end = RangeEnd} = Req, Rest, State) when
    Flushed > RangeEnd
->
    %% Every byte is in the buffer but the closing frame has not arrived yet.
    %% That is no reason to hold up the successor.
    {Rest1, State1} = flush_reqs1(Rest, State),
    {[Req | Rest1], State1};
advance_past(#req{fragment = Fragment} = Req, Rest, State) ->
    %% Still owing bytes, so nothing behind it in its own fragment can flush.
    %% The other fragments' queues still can.
    {Rest1, State1} = flush_other_fragments(Fragment, Rest, State),
    {[Req | Rest1], State1}.

%% The buffer a request's bytes belong in, but only while the request is
%% positioned to append to it. Only ever consulted for a request that still
%% owes bytes: one that owes nothing is decided in `flush_reqs1/2`, since the
%% buffer's end may legitimately have moved past it.
flushable(#req{flushed = Flushed} = Req, State) ->
    case buffer_for(Req, State) of
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
    #state{fragment_ref = #fragment_ref{offset = Fragment}, buffer = Buffer}
) ->
    {ok, Buffer};
buffer_for(#req{fragment = Fragment}, #state{next = {#fragment_ref{offset = Fragment}, Buffer}}) ->
    {ok, Buffer};
buffer_for(#req{fragment = Fragment}, #state{fragment_ref = #fragment_ref{offset = Current}}) when
    Fragment > Current
->
    %% First bytes to reach a buffer for the prefetched next fragment.
    {ok, rabbitmq_stream_s3_read_buffer:new(?SEGMENT_HEADER_B)};
buffer_for(_Req, _State) ->
    %% A fragment the reader has already moved past.
    error.

set_buffer_for(
    #req{fragment = Fragment},
    Buffer,
    #state{fragment_ref = #fragment_ref{offset = Fragment}} = State
) ->
    State#state{buffer = Buffer};
set_buffer_for(#req{frag_ref = FragRef}, Buffer, State) ->
    State#state{next = {FragRef, Buffer}}.

%% ------------------------------------------------------------------
%% Internal: request queue
%% ------------------------------------------------------------------

req_key(#req{fragment = Fragment, range_start = RangeStart}) ->
    {Fragment, RangeStart}.

take_req(Fragment, RangeStart, Reqs) ->
    take_req(Fragment, RangeStart, Reqs, []).

take_req(
    Fragment, RangeStart, [#req{fragment = Fragment, range_start = RangeStart} = Req | Rest], Acc
) ->
    {lists:reverse(Acc), Req, Rest};
take_req(Fragment, RangeStart, [Req | Rest], Acc) ->
    take_req(Fragment, RangeStart, Rest, [Req | Acc]);
take_req(_Fragment, _RangeStart, [], _Acc) ->
    false.

drop_fragment_reqs(Fragment, Reqs) ->
    {Dropped, Kept} = lists:partition(fun(#req{fragment = F}) -> F =:= Fragment end, Reqs),
    {[req_key(Req) || Req <- Dropped], Kept}.

%% Put a failed range back in the queue. Bytes already appended to a buffer
%% cannot be un-appended, so the range restarts at `flushed`; anything staged
%% beyond that is dropped and fetched again. An error for a range that is not in
%% flight is stale - a duplicate, or one already abandoned - and must not double
%% the backoff for a single failure.
fail_req(Fragment, RangeStart, Kind, #state{reqs = Reqs} = State) ->
    case take_req(Fragment, RangeStart, Reqs) of
        {Before, #req{status = inflight, flushed = Flushed, range_end = RangeEnd}, After} when
            Flushed > RangeEnd
        ->
            %% Every byte the range owed is already in a buffer; only its
            %% closing frame was outstanding (see `advance_past/3`), so there is
            %% nothing left to fetch. Restarting it at `flushed` would leave
            %% `range_start = range_end + 1`: an inverted range whose key is
            %% exactly its successor's, so deliveries for the successor would be
            %% routed to it and dropped, and re-issuing it would ask S3 for a
            %% backwards range. Drop it instead.
            {dropped, State#state{reqs = Before ++ After}};
        {Before, #req{status = inflight, flushed = Flushed} = Req0, After} ->
            Req = Req0#req{
                range_start = Flushed, pos = Flushed, staged = [], status = {backoff, Kind}
            },
            {ok, State#state{reqs = Before ++ [Req | After]}};
        _ ->
            stale
    end.

%% A failed range that owed nothing was dropped rather than re-queued. No
%% backoff and no retry timer: nothing failed to arrive. The freed depth slot
%% may let a new range start.
drop_req_done(State) ->
    maybe_start_requests(State).

release(Kind, #req{status = {backoff, Kind}} = Req) -> Req#req{status = ready, retried = Kind};
release(_Kind, Req) -> Req.

%% Arm this kind's retry timer unless one is already pending for it: a batch of
%% failing requests must not arm a timer each, every one of them driving a full
%% retry pass over the queue.
%%
%% The backoff grows on the same terms, once per armed round rather than once
%% per failed range. One fault - a reset connection, a pool that cannot grow -
%% fails every pipelined range at once, and doubling per range would reach the
%% cap in a single round, leaving the next round to wait out a delay the reader
%% never earned.
arm_retry(Kind, #state{timers = Timers} = State) ->
    case is_map_key(Kind, Timers) of
        true ->
            {State, []};
        false ->
            Delay = delay(Kind, State),
            {
                grow_delay(Kind, State#state{timers = Timers#{Kind => armed}}),
                [{set_timer, Kind, Delay}]
            }
    end.

delay(fault, #state{retry_delay = Delay}) -> Delay;
delay(pool_busy, #state{pool_busy_delay = Delay}) -> Delay.

grow_delay(fault, #state{cfg = #cfg{max_retry_delay_ms = Max}, retry_delay = Delay} = State) ->
    State#state{retry_delay = min(Delay * 2, Max)};
grow_delay(pool_busy, #state{pool_busy_delay = Delay} = State) ->
    State#state{pool_busy_delay = min(Delay * 2, ?MAX_POOL_BUSY_DELAY_MS)}.

%% A delivery says the path a range took is working again, so its clock goes
%% back to the minimum. Only a clock nothing is waiting on is reset: with the
%% pipeline several ranges deep, S3 answering some while throttling others is
%% what throttling looks like from here, and resetting on every delivered frame
%% would hand back the delay the failing ranges just earned - leaving the reader
%% retrying a throttling S3 at the minimum delay indefinitely.
%%
%% A round the clock's own timer has just released counts as waiting on it, not
%% only a range still queued behind it. The timer fires, every range it held
%% goes back on the wire, and at that instant no timer is armed and nothing is
%% in backoff - so without `#req.retried` the first range S3 answered reset the
%% clock, and the ranges it throttled in the same breath armed a fresh minimum
%% round. That is the same failure the guard is for, one step later: the delay
%% oscillated between the first two steps and never approached
%% `max_retry_delay_ms`, however long S3 kept throttling.
reset_idle_backoffs(#state{cfg = #cfg{min_retry_delay_ms = MinDelay}} = State0) ->
    State1 =
        case idle(fault, State0) of
            true -> State0#state{retry_delay = MinDelay};
            false -> State0
        end,
    case idle(pool_busy, State1) of
        true -> State1#state{pool_busy_delay = ?MIN_POOL_BUSY_DELAY_MS};
        false -> State1
    end.

%% Nothing is waiting on this backoff clock: no timer armed for it, no range
%% queued against it, and no range it released still to answer.
idle(Kind, #state{timers = Timers, reqs = Reqs}) ->
    not is_map_key(Kind, Timers) andalso
        not lists:any(
            fun(#req{status = Status, retried = Retried}) ->
                Status =:= {backoff, Kind} orelse Retried =:= Kind
            end,
            Reqs
        ).

inflight_count(#state{reqs = Reqs}) ->
    length([Req || #req{status = inflight} = Req <- Reqs]).

%% How far ahead of the consumer the reader has got: the buffered bytes it has
%% not read yet, plus the part of every outstanding range that is not in a
%% buffer. This is what the prefetch window bounds.
outstanding(#state{buffer = Buffer, read_pos = ReadPos, next = Next, reqs = Reqs}) ->
    Unread =
        max(0, rabbitmq_stream_s3_read_buffer:end_pos(Buffer) - ReadPos) +
            case Next of
                {_, NextBuffer} ->
                    rabbitmq_stream_s3_read_buffer:end_pos(NextBuffer) - ?SEGMENT_HEADER_B;
                _ ->
                    0
            end,
    lists:foldl(
        fun(#req{range_end = RangeEnd, flushed = Flushed}, Acc) -> Acc + RangeEnd + 1 - Flushed end,
        Unread,
        Reqs
    ).

%% ------------------------------------------------------------------
%% Internal: request issuance
%% ------------------------------------------------------------------

maybe_start_requests(State0) ->
    {State1, Effects1} = issue_ready(State0),
    {State2, Effects2} = extend_frontier(State1),
    {State2, Effects1 ++ Effects2}.

%% The first pass over a fragment the reader has just been placed on, from
%% `init/5` or from an iterator refresh. It differs from
%% `maybe_start_requests/1` in that it does not resolve the next-fragment peek:
%% passing `none` for it holds the frontier inside the current fragment without
%% touching the iterator, and `#state.next_peek` is left `unknown` so the first
%% delivery resolves it instead.
%%
%% That matters because `init/5` runs inside the shell's `gen_server:init/1`,
%% which is to say in the *consumer* process - blocked in `gen_server:start/3`
%% until it returns - and the peek can be a synchronous group GET (see
%% `peek_next_fragment/2`). Without this a consumer attaching within one request
%% of a fragment's end, or to a fragment shorter than that, would issue the
%% ranges the current fragment has left, still find window room, and peek:
%% `range_in_fragment/3` returns `none` and `extend_frontier/1` spills into the
%% next fragment. The refresh path does not run in the consumer process, but it
%% shares this entry point: the peek it defers is one delivery away, and there
%% is no more reason to spend a blocking group GET inside an event there.
%%
%% `issue_ready/1` is skipped rather than reordered: the queue is empty at both
%% call sites.
start_current_request(State) ->
    {State1, _Peek, _Attempted, Effects} = extend_frontier(State, none, []),
    {State1, Effects}.

%% (Re-)issue ranges that are queued and not in flight. Their bytes are already
%% counted in `outstanding/1`, so they are not window-gated - a range the reader
%% has committed to must be fetched, or the buffer never becomes contiguous
%% again and every read behind it stalls until the deadline. They do take a
%% slot, so the depth cap still applies; running before `extend_frontier/1`
%% keeps new ranges from taking the slots they are waiting for.
issue_ready(#state{cfg = #cfg{max_depth = MaxDepth}, reqs = Reqs} = State) ->
    {Reqs1, {_, Effects}} = lists:mapfoldl(
        fun
            (#req{status = ready} = Req, {InFlight, Acc}) when InFlight < MaxDepth ->
                {Req#req{status = inflight}, {InFlight + 1, [start_request_effect(Req) | Acc]}};
            (Req, Acc) ->
                {Req, Acc}
        end,
        {inflight_count(State), []},
        Reqs
    ),
    {State#state{reqs = Reqs1}, lists:reverse(Effects)}.

%% Append new ranges at the fetch frontier while the depth cap and the prefetch
%% window allow, spilling into the prefetched next fragment once every byte of
%% the current one has been spoken for.
%%
%% Nothing is fetched while the current fragment is known to be 404. Retention
%% deleting a fragment mid-read leaves the reader holding buffered bytes below
%% the fragment's index boundary, so the frontier still points into an object
%% that is gone; the reads those buffered bytes can still serve would each fire
%% a full `max_depth` of range GETs at it, and every one of the 404s they earn
%% wipes the queue and cancels the next fragment's prefetch along with it. The
%% refresh that resolves this is driven by the first read that cannot be served
%% (see `not_found_refresh/1`), so the only thing to do until then is wait.
extend_frontier(#state{current_not_found = true} = State) ->
    {State, []};
extend_frontier(#state{next_peek = Peek0} = State) ->
    {State1, Peek, Attempted, Effects} = extend_frontier(State, Peek0, []),
    arm_peek_retry(Peek, Attempted, State1#state{next_peek = Peek}, Effects).

%% A group fetch that failed while looking ahead has to arm the retry itself.
%% Nothing else in this pass will: the ranges already queued are healthy, so no
%% `fail_req/4` runs. Without a timer nothing would pace the next attempt, and
%% `peek_next_fragment/2` re-attempts precisely when no fault clock is armed -
%% so the reader would spend a synchronous group GET on every pass, against an
%% S3 already having trouble.
%%
%% Armed on the attempt rather than on the memo's value, and so on every failed
%% re-attempt rather than only the first: comparing against the memo's previous
%% value would arm nothing for a peek that was already `failed` when the pass
%% began, which is every re-attempt after the first - each landing exactly when
%% the clock has just been given up.
%%
%% A pass that never reached the look-ahead must not arm, even with the memo
%% sitting at `failed`. It owes nothing: nothing was asked of S3. Arming anyway
%% keeps a clock alive over a memo no attempt is pacing, and an armed clock is
%% what suppresses the next attempt - so the reader would look ahead only after
%% a retry round it did nothing to earn, having stopped prefetching in the
%% meantime. That is the stranding this derivation exists to make impossible,
%% one door along.
arm_peek_retry(failed, true, State, Effects) ->
    {State1, RetryEffects} = arm_retry(fault, State),
    {State1, Effects ++ RetryEffects};
arm_peek_retry(_Peek, _Attempted, State, Effects) ->
    {State, Effects}.

extend_frontier(State, Peek0, Acc) ->
    extend_frontier(State, Peek0, false, Acc).

extend_frontier(#state{reqs = Reqs} = State, Peek0, Attempted0, Acc) ->
    case has_room(State) of
        false ->
            {State, Peek0, Attempted0, lists:reverse(Acc)};
        true ->
            case next_range(State, Peek0) of
                {Peek, Attempted, #req{} = Req} ->
                    State1 = State#state{reqs = insert_req(Req#req{status = inflight}, Reqs)},
                    extend_frontier(
                        State1,
                        Peek,
                        Attempted0 orelse Attempted,
                        [start_request_effect(Req) | Acc]
                    );
                {Peek, Attempted, none} ->
                    {State, Peek, Attempted0 orelse Attempted, lists:reverse(Acc)}
            end
    end.

%% Keep the queue ordered by `{fragment, range_start}`. A new range is always
%% the last one for its own fragment, but not necessarily for the queue: a
%% fragment whose tail had to be re-requested gains a range after the spill into
%% the next fragment has already been queued.
insert_req(#req{fragment = Fragment} = Req, Reqs) ->
    {Before, After} = lists:splitwith(fun(#req{fragment = F}) -> F =< Fragment end, Reqs),
    Before ++ [Req | After].

start_request_effect(#req{key = Key, fragment = Fragment, range_start = Start, range_end = End}) ->
    {start_request, Key, {Start, End}, Fragment}.

has_room(#state{cfg = #cfg{max_depth = MaxDepth}} = State) ->
    inflight_count(State) < MaxDepth andalso outstanding(State) < fetch_ceiling(State).

%% How far ahead the reader may fetch. That is the prefetch window, except that
%% the window bounds *prefetch* and the read in hand is not optional: a read of
%% N bytes cannot be served while fewer than N are outstanding. Gating on the
%% window alone therefore trapped any read larger than `window_max`. Once
%% `outstanding` reached the ceiling `has_room/1` stayed false, `note_miss/1`
%% could not grow the window past `min(WindowMax, _)`, and `extend_frontier/1`
%% issued nothing - forever. The read deadline was no escape either: it clears
%% the buffer, the reader refetches to the same ceiling, and wedges again, so
%% the consumer loops on deadlines instead of making progress. Reads are chunk
%% sized (`data_size` from the chunk header), so a chunk larger than the
%% window - or a smaller one while a next-fragment prefetch is held, since that
%% counts against `outstanding/1` too - was enough.
%%
%% Flooring at what the pending read needs is the whole fix. The other terms in
%% `outstanding/1` cannot starve it: the next-fragment buffer only holds bytes
%% once the current fragment is entirely buffered or in flight, and a pending
%% read that `try_read/3` left awaiting is below that fragment's index
%% boundary, so what is already on the wire completes it.
fetch_ceiling(#state{window = Window, read_pos = ReadPos, pending = #pending{} = Pending}) ->
    #pending{offset = Offset, bytes = Bytes} = Pending,
    max(Window, Offset + Bytes - ReadPos);
fetch_ceiling(#state{window = Window}) ->
    Window.

%% The next range to request: the tail of the current fragment's data region,
%% or the head of the prefetched next fragment once the current one is fully
%% spoken for. Every range is clamped to its own fragment's data region, so a
%% request can never reach into the index region that follows it or run past
%% the end of the object.
%%
%% `Peek` carries the result of looking one fragment ahead, memoised in the
%% state and threaded through the pass. It is not a micro-optimisation:
%% `rabbitmq_stream_s3_fragment_iterator:next/1` fetches a group object through
%% the iterator's (uncached) get-group fun when the next entry sits behind a
%% group node, which is a synchronous S3 GET. Peeking per queued range would
%% spend `max_depth` of them on the same object; peeking per pass would spend
%% one per delivered frame, since a current fragment that is fully spoken for
%% reaches this on every pass until the transition. Either blocks the reader
%% while the caller's read deadline burns.
next_range(#state{fragment_ref = FragRef, key = Key, buffer = Buffer} = State, Peek) ->
    #fragment_ref{offset = Fragment, size = FragSize} = FragRef,
    Frontier = frontier(
        Fragment, State, rabbitmq_stream_s3_read_buffer:end_pos(Buffer)
    ),
    case range_in_fragment(Frontier, FragSize, State) of
        {Start, End} -> {Peek, false, new_req(Fragment, FragRef, Key, Start, End)};
        none -> next_fragment_range(State, Peek)
    end.

next_fragment_range(#state{next = not_found}, Peek) ->
    {Peek, false, none};
next_fragment_range(State, Peek0) ->
    {Peek, Attempted} = peek_next_fragment(State, Peek0),
    {Peek, Attempted, next_fragment_range1(Peek, State)}.

next_fragment_range1(none, _State) ->
    none;
next_fragment_range1(failed, _State) ->
    %% The peek could not be resolved (a transient group fetch failure). Nothing
    %% to prefetch for the next fragment until the retry clears it.
    none;
next_fragment_range1(
    {ok, #fragment_ref{offset = Offset, uid = Uid, size = Size} = FragRef, _Advanced},
    #state{stream = StreamId, next = Next} = State
) ->
    Buffered =
        case Next of
            {#fragment_ref{offset = Offset}, Buffer} ->
                rabbitmq_stream_s3_read_buffer:end_pos(Buffer);
            _ ->
                ?SEGMENT_HEADER_B
        end,
    case range_in_fragment(frontier(Offset, State, Buffered), Size, State) of
        {Start, End} ->
            Key = rabbitmq_stream_s3:fragment_key(StreamId, Offset, Uid),
            new_req(Offset, FragRef, Key, Start, End);
        none ->
            none
    end.

%% What the iterator points at. The iterator is only ever replaced, never
%% advanced in place, so the answer holds until it is - which is where the memo
%% in `#state.next_peek` is dropped. A group fetch that failed transiently is
%% recorded as `failed` rather than as `none`: `none` is what says the manifest
%% ends here, and remembering that of a fetch that merely failed would leave the
%% reader awaiting a next fragment it has stopped asking for.
%%
%% `failed` says only that the last attempt failed. Whether to attempt again is
%% not stored alongside it but read off the fault clock: the memo is honoured
%% while that clock is armed and re-attempted once it is not. Each attempt is a
%% synchronous group GET inside the core, so something has to pace them, and the
%% clock is what paces every other retry here - `min_retry_delay_ms` to
%% `max_retry_delay_ms`, one round at a time. The `pool_busy` clock cannot
%% stand in: it runs 25-500ms and fires for as long as the pool has no free
%% connection, which says nothing about whether the group object is fetchable.
%%
%% Deriving it rather than storing it is what keeps the two from drifting apart.
%% A memo that recorded "waiting on the fault clock" as a fact of its own had to
%% be reset by hand at every site that disowns that clock, and a site that forgot
%% - `deadline_expired` did - stranded it: nothing re-attempted the fetch and
%% nothing armed a clock to pace one, so the frontier stopped spilling into the
%% next fragment for the rest of the current one. Read off the clock, that state
%% cannot be reached: dropping the clock is what licenses the next attempt.
%% Returns `{Peek, Attempted}`. Whether a fetch was actually attempted is what
%% decides the retry clock: a pass that never reached the look-ahead owes
%% nothing, and arming for it would keep the clock alive - and an armed clock is
%% exactly what suppresses the next attempt.
peek_next_fragment(#state{timers = Timers}, failed) when is_map_key(fault, Timers) ->
    {failed, false};
peek_next_fragment(#state{iterator = Iterator}, Peek) when Peek =:= unknown; Peek =:= failed ->
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, FragRef, Advanced} -> {{ok, FragRef, Advanced}, true};
        end_of_manifest -> {none, true};
        {error, {group_fetch_failed, _}} -> {failed, true}
    end;
peek_next_fragment(_State, Peek) ->
    {Peek, false}.

range_in_fragment(Frontier, FragSize, State) ->
    IdxStartPos = ?SEGMENT_HEADER_B + FragSize,
    case Frontier < IdxStartPos of
        true -> {Frontier, min(Frontier + request_size(State) - 1, IdxStartPos - 1)};
        false -> none
    end.

new_req(Fragment, FragRef, Key, Start, End) ->
    #req{
        fragment = Fragment,
        frag_ref = FragRef,
        key = Key,
        range_start = Start,
        range_end = End,
        flushed = Start,
        pos = Start
    }.

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
frontier(Fragment, #state{reqs = Reqs}, Buffered) ->
    case [RangeEnd || #req{fragment = F, range_end = RangeEnd} <- Reqs, F =:= Fragment] of
        [] -> Buffered;
        Ends -> max(Buffered, lists:max(Ends) + 1)
    end.

%% ------------------------------------------------------------------
%% Internal: configuration
%% ------------------------------------------------------------------

build_cfg(Opts) ->
    RequestSize = maps:get(request_size, Opts, 4_194_304),
    #cfg{
        request_size = RequestSize,
        %% Never below one request. These are plain app-env settings with no
        %% schema to reject the combination, and a ceiling under the floor
        %% inverts the signal the whole design rests on: the window starts at
        %% one request, so `note_miss/1`'s `min(WindowMax, Window * 2)` would
        %% *shrink* it when the reader is not fetching far enough ahead, and
        %% `has_room/1` would never admit a second range - a lagging consumer
        %% would be served by one serial request. It also puts every observed
        %% window in the bottom bucket of a histogram whose boundaries are
        %% derived from these same two numbers.
        window_max = max(RequestSize, maps:get(window_max, Opts, 33_554_432)),
        max_depth = maps:get(max_depth, Opts, 8),
        min_retry_delay_ms = maps:get(min_retry_delay_ms, Opts, 1_000),
        max_retry_delay_ms = maps:get(max_retry_delay_ms, Opts, 30_000)
    }.

%% ------------------------------------------------------------------
%% Internal: prefetch window control
%%
%% One knob: `window`, the bytes to hold or have outstanding ahead of the
%% consumer. Request size is fixed, so the window sets how many requests run
%% concurrently (up to `#cfg.max_depth`), which is what determines a remote
%% reader's bandwidth - a single S3 connection tops out around 40 MB/s no
%% matter how large a range is asked of it.
%%
%% A buffer miss means the reader is not fetching far enough ahead, so the
%% window doubles. Sustained hits mean it is further ahead than it needs to be,
%% so it gives a request back. This is the opposite of a congestion window: the
%% miss is a starvation signal, not a signal to back off. The read size this
%% replaced halved on every miss, so it collapsed to its floor exactly when a
%% consumer was falling behind and never recovered.
%% ------------------------------------------------------------------

%% Bytes per range request.
request_size(#state{cfg = #cfg{request_size = RequestSize}}) ->
    RequestSize.

window(#state{window = Window}) ->
    Window.

%% Only the first miss for a given pending read counts. `try_serve/1` re-runs on
%% every delivery while a read waits, so counting each of those would run the
%% window to its ceiling on one slow read - and inflate `buffer_miss`, which
%% counts reads that had to wait, not deliveries they waited through. Growing
%% once per missed read also keeps a consumer that reads a couple of records and
%% disconnects from provoking a full window: it only ever misses once or twice.
note_miss(#state{missed_pending = true} = State) ->
    {State, []};
note_miss(#state{cfg = #cfg{window_max = WindowMax}, window = Window} = State0) ->
    State = State0#state{
        window = min(WindowMax, Window * 2),
        bytes_served_since_miss = 0,
        missed_pending = true
    },
    {State, [{observe, miss, State#state.window}]}.

note_hit(Bytes, #state{bytes_served_since_miss = Since} = State) ->
    decay(State#state{bytes_served_since_miss = Since + Bytes}).

%% A window's worth of reads served without a miss: hand a request back. Decaying
%% by bytes rather than by a count of hits keeps this proportional to the window.
%% Reads come two per chunk (a header over-read and the body), so a hit count
%% would shrink the window after a handful of chunks however large it is.
decay(#state{cfg = #cfg{request_size = RequestSize}, window = Window} = State) when
    State#state.bytes_served_since_miss > Window
->
    State#state{
        window = max(RequestSize, Window - RequestSize),
        bytes_served_since_miss = 0
    };
decay(State) ->
    State.
