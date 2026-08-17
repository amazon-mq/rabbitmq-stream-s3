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
- `{data, Id, Data, done | continue}` - S3 delivered bytes for that request
- `{request_error, Id, Fragment, Reason}` - that request failed. The fragment
  travels with it because a 404 is about the object, not the request: it is
  acted on even when the request it arrived for is long gone
- `{retry, Kind}` - the retry timer of that backoff kind fired
- `deadline_expired` - pending read exceeded its deadline
- `{iterator_refreshed, Iterator}` - manifest cache provided new iterator

Requests are identified by an id the pipeline mints when it queues the range and
keeps for as long as the range exists. The shell records it against the request
it starts and hands it back with every frame.

## Effects (outputs)

- `{reply, Result}` - respond to the pending read
- `{start_request, Id, Key, Range, Fragment}` - initiate an S3 GET
- `{cancel_request, Id}` - abandon one in-flight GET
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
`staged` list and appended once it reaches the head. See
`rabbitmq_stream_s3_read_pipeline`.

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
    %% Ceiling on the prefetch window: the most the reader may have on the wire
    %% at once, over and above what the read in hand needs.
    window_max :: pos_integer(),
    %% Ceiling on the bytes the reader may hold that the consumer has not read.
    %% Together with `window_max` this bounds its memory at
    %% `window_max + buffer_max + request_size` - the two are separate budgets
    %% (see `has_room/1`), so they add rather than share.
    buffer_max :: pos_integer(),
    %% Most requests that may be in flight at once, across every fragment.
    max_depth :: pos_integer(),
    %% Most fragments the reader may look ahead to beyond the one it is reading.
    %%
    %% A backstop, not the working limit: what governs reach is the window and
    %% the depth cap in `has_room/1`, and the look-ahead only ever extends when
    %% every fragment it already holds is spoken for. This bounds the walk for
    %% the cases those do not - a run of fragments with empty data regions
    %% consumes no request, so nothing else would stop it.
    max_lookahead :: pos_integer(),
    min_retry_delay_ms :: pos_integer(),
    max_retry_delay_ms :: pos_integer()
}).

-record(pending, {
    offset :: byte_offset(),
    bytes :: pos_integer(),
    hint :: chunk_boundary | within_chunk
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

    %% The ranges asked for and the bytes they assemble into: the current
    %% fragment's buffer, the prefetched next one's, and the reassembly queue
    %% over both. This module decides what to fetch; that one records it.
    pipeline :: rabbitmq_stream_s3_read_pipeline:pipeline(),

    %% Fragment iterator
    iterator :: rabbitmq_stream_s3_fragment_iterator:iterator(),

    %% The entries the iterator has been walked forward onto, nearest first,
    %% each paired with the iterator advanced past it. `next/1` is a synchronous
    %% S3 GET whenever an entry sits behind a group node, so every answer is kept
    %% for as long as the iterator is the same one (it is only ever replaced
    %% wholesale, never advanced in place), and a fragment transition promotes
    %% the head rather than fetching the group a second time.
    %%
    %% Walking more than one fragment ahead is what lets the window exceed the
    %% reach of a single fragment: at a fragment's tail one entry reaches `F`
    %% bytes, so a window above `F` would otherwise have nowhere to go. Each
    %% entry memoises its own advanced iterator, so walking N ahead costs at
    %% most N descents in total, and normally far fewer - a run of leaf entries
    %% behind one group node is walked in memory once the first of them has been
    %% descended to.
    peeks = [] :: [{#fragment_ref{}, rabbitmq_stream_s3_fragment_iterator:iterator()}],

    %% What the iterator said when it was last walked past the end of `peeks`.
    %% `unknown` means it has not been asked; `none` that the manifest ends
    %% there; `failed` that the group fetch failed transiently, which is not an
    %% answer to keep but is a reason not to ask again until a retry timer fires.
    peek_tail = unknown :: unknown | none | failed,

    %% The backoff kinds whose retry timer is armed and has not fired yet.
    %% Without this a batch of N failing requests would arm N timers and drive N
    %% retry passes; keeping it per kind stops one kind's timer from standing in
    %% for the other's, which would both suppress that other backoff's growth
    %% and release its ranges on the wrong clock.
    timers = #{} :: #{backoff() => armed},

    %% Pending read (at most one)
    pending :: #pending{} | undefined,

    %% Current fragment returned 404
    current_not_found = false :: boolean(),

    %% Bytes S3 has delivered since the last tick, and the rate that came out of
    %% the tick before it.
    %%
    %% This module has no clock: `step/2` takes events and hands back effects,
    %% and reading the time inside it would make every result depend on when it
    %% was computed. Rate control needs elapsed time all the same, so time
    %% arrives as an input - the shell ticks on a timer and stamps each tick with
    %% the micros actually elapsed, which is both what keeps this pure and what
    %% lets a test drive a whole minute of tuning in virtual time.
    sample_bytes = 0 :: non_neg_integer(),
    %% Bytes per second over the last completed sample, or `undefined` before the
    %% first tick. Measured on what S3 delivered, since that is the quantity
    %% concurrency moves.
    fetch_rate :: undefined | non_neg_integer()
}).

-type state() :: #state{}.
-type fragment_offset() :: osiris:offset().
-type request_id() :: rabbitmq_stream_s3_read_pipeline:request_id().
-type backoff() :: rabbitmq_stream_s3_read_pipeline:backoff().

-type event() ::
    {read, byte_offset(), pos_integer(), chunk_boundary | within_chunk}
    | {data, request_id(), binary(), done | continue}
    | {request_error, request_id(), fragment_offset(), term()}
    | {retry, backoff()}
    %% Micros elapsed since the previous tick, measured by the shell. The only
    %% way time enters this module; see `#state.sample_bytes`.
    | {tune_tick, non_neg_integer()}
    | deadline_expired
    | {iterator_refreshed, rabbitmq_stream_s3_fragment_iterator:iterator() | end_of_manifest}.

-type observe_kind() :: hit | miss | fragment_transition.

-type effect() ::
    {reply, read_result()}
    | {start_request, request_id(), rabbitmq_stream_s3:key(), {byte_offset(), byte_offset()},
        fragment_offset()}
    | {cancel_request, request_id()}
    | {cancel_requests, all}
    | {set_timer, backoff(), pos_integer()}
    | {cancel_timers, all}
    | {refresh_iterator, osiris:offset()}
    | {observe, observe_kind(), pos_integer()}
    | {fatal_error, term()}
    | stop.

%% A served read carries iodata - the buffer's own blocks, so a read spanning
%% two of them is not copied to build one binary. See
%% `rabbitmq_stream_s3_read_buffer`.
-type read_result() ::
    {ok, [binary()]}
    | {error, timeout}
    | {next_fragment, osiris:offset()}
    | {become_local, osiris:offset()}
    | end_of_stream.

-export_type([state/0, event/0, effect/0, read_result/0, request_id/0, backoff/0]).

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
-export([
    outstanding_ranges/1, window_bytes/1, read_position/1, load/1, request_id/3, fetch_rate/1
]).

%% The id the pipeline minted for the range a fragment has outstanding at a
%% position. Tests describe what S3 answers - a range of bytes - rather than
%% which id it happened to be given.
-spec request_id(state(), fragment_offset(), byte_offset()) ->
    {ok, rabbitmq_stream_s3_read_pipeline:request_id()} | error.
request_id(State, Fragment, RangeStart) ->
    rabbitmq_stream_s3_read_pipeline:find_request(Fragment, RangeStart, pipeline(State)).
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
    Cfg = build_cfg(Opts),
    %% The iterator arrives already advanced past the current entry
    %% (done by find_position in the log reader). It points at the next
    %% fragment, ready for prefetch and forward navigation.
    State = #state{
        stream = StreamId,
        cfg = Cfg,
        window = Cfg#cfg.request_size,
        retry_delay = Cfg#cfg.min_retry_delay_ms,
        pipeline = rabbitmq_stream_s3_read_pipeline:new(StreamId, FragRef, Position),
        iterator = Iterator
    },
    %% Immediately request data for the current fragment.
    {State1, Effects} = start_current_request(State),
    checked({State1, Effects}, init).

%% @doc Feed an event into the core, get back new state and effects.
-spec step(state(), event()) -> {state(), [effect()]}.
step(State, Event) ->
    checked(step_(State, Event), Event).

-ifdef(TEST).
%% Every transition passes through here, so this is where the invariants the
%% state machine is written against are checked rather than described. They cost
%% nothing in a release build (`checked/2` is the identity there) and everything
%% that drives the core - the suite's cases, the property suite's random event
%% sequences - checks them on every step for free.
%%
%% This is aimed at a specific recurring defect: a fact that is derived being
%% stored as a field of its own, so that each clause has to remember to reset it
%% along with the thing it was derived from. What that produces is not a crash
%% but a quiet degradation - a reader that has stopped fetching, a clock nobody
%% will ever fire - which every safety property passes and no unit test thinks
%% to look for.
checked({State, Effects}, Event) ->
    assert_clocks_have_waiters(State, Event),
    assert_not_wedged(State, Effects, Event),
    {State, Effects}.
-else.
checked(Result, _Event) ->
    Result.
-endif.

step_(State, {read, Offset, Bytes, Hint}) ->
    State1 = State#state{
        pending = #pending{offset = Offset, bytes = Bytes, hint = Hint},
        missed_pending = false
    },
    try_serve(State1);
step_(State0, {data, Id, Data, DoneOrContinue}) ->
    %% The backoffs are judged after the delivery has been absorbed, not before:
    %% that is when the range it closes has left the queue, and when a response
    %% that closed without delivering a byte has been put back into backoff.
    {Signals, Pipeline} = rabbitmq_stream_s3_read_pipeline:data(
        Id, Data, DoneOrContinue, pipeline(State0)
    ),
    Sample = State0#state.sample_bytes + iolist_size(Data),
    {State1, Effects1} = absorb_signals(
        Signals, State0#state{pipeline = Pipeline, sample_bytes = Sample}
    ),
    State2 = reset_idle_backoffs(State1),
    {State3, Effects2} = maybe_start_requests(State2),
    {State4, Effects3} = try_serve(State3),
    {State4, Effects1 ++ Effects2 ++ Effects3};
step_(State, {tune_tick, ElapsedUs}) ->
    %% Close the sample. An elapsed of zero would divide by it, and says nothing
    %% either way, so it is left to accumulate into the next one - which is also
    %% what makes the tick safe to deliver early or twice.
    case ElapsedUs > 0 of
        true ->
            Rate = State#state.sample_bytes * 1_000_000 div ElapsedUs,
            {State#state{sample_bytes = 0, fetch_rate = Rate}, []};
        false ->
            {State, []}
    end;
step_(State0, {request_error, _Id, Fragment, not_found}) ->
    case Fragment =:= current_fragment_offset(State0) of
        true ->
            %% Current fragment 404. Refresh the iterator past this offset.
            State = State0#state{
                current_not_found = true,
                pipeline = rabbitmq_stream_s3_read_pipeline:clear_requests(pipeline(State0))
            },
            case State#state.pending of
                undefined -> {State, [{cancel_requests, all}]};
                _ -> {State, [{cancel_requests, all}, {refresh_iterator, Fragment}]}
            end;
        false ->
            other_fragment_not_found(Fragment, State0)
    end;
step_(State0, {request_error, Id, _Fragment, Reason}) when
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
    fail_range(Id, fault, State0);
step_(State0, {request_error, Id, _Fragment, pool_busy}) ->
    %% Pool is growing — a connection becomes available once its TLS handshake
    %% completes (fast on same-region S3, but not instant). Use a mild backoff
    %% (25, 50, 100, 200, 400, 500, 500...) starting low to catch the connection
    %% as soon as it is ready, doubling up to a 500ms cap so we don't spin if the
    %% pool cannot grow (e.g. S3 unreachable).
    fail_range(Id, pool_busy, State0);
step_(State0, {request_error, _Id, _Fragment, Reason}) ->
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
step_(State0, {retry, Kind}) ->
    %% Only the ranges waiting on this clock are released. The other kind's
    %% ranges keep waiting for their own timer: a pool_busy retry that released
    %% them would put a range S3 has just asked us to slow down straight back on
    %% the wire, 25ms after a `slow_down`.
    %% A `failed` look-ahead memo needs no clearing here: it is honoured only
    %% while the fault clock is armed (see `peek_next_fragment/2`), and this
    %% round has just given that clock up.
    State1 = State0#state{
        timers = maps:remove(Kind, State0#state.timers),
        pipeline = rabbitmq_stream_s3_read_pipeline:release(Kind, pipeline(State0))
    },
    {State2, Effects} = maybe_start_requests(State1),
    {State3, Effects2} = try_serve(State2),
    {State3, Effects ++ Effects2};
step_(#state{cfg = #cfg{min_retry_delay_ms = MinDelay}} = State0, deadline_expired) ->
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
    %% buffer budget (see `buffered/1`), so keeping it would hold budget that
    %% nothing is left to fetch into. With the window at its ceiling that is
    %% permanent: `has_room/1` stays false however many misses the reader takes,
    %% `extend_frontier/1` issues nothing, and no range is ever requested for the
    %% current fragment again - every subsequent read waits out its own deadline.
    %% Below the ceiling it costs at least one more of them before the window
    %% doubles past what the stale prefetch holds.
    %% The look-ahead memo is left alone. An answered one is the truth about an
    %% iterator this event does not touch, and re-resolving it would spend a
    %% group GET on an answer already in hand; a `failed` one needs no clearing,
    %% because dropping the fault clock is itself what licenses the next attempt
    %% (see `peek_next_fragment/2`).
    Pipeline = rabbitmq_stream_s3_read_pipeline:clear_requests(
        rabbitmq_stream_s3_read_pipeline:drop_prefetch(pipeline(State0))
    ),
    State = State0#state{
        pipeline = Pipeline,
        pending = undefined,
        timers = #{},
        retry_delay = MinDelay,
        pool_busy_delay = ?MIN_POOL_BUSY_DELAY_MS
    },
    {State, [{cancel_requests, all}, {cancel_timers, all}, {reply, {error, timeout}}]};
step_(State0, {iterator_refreshed, end_of_manifest}) ->
    %% No new entries. Become local.
    %%
    %% The queue goes, as it does at any transition, but the iterator is left
    %% where it is: advancing it can cost a synchronous group GET (see
    %% `peek_next_fragment/2`) for an answer nothing will ever read, since the
    %% reply below hands the consumer to the local tier and the shell stops the
    %% reader. The pending read is answered by that reply and must be cleared
    %% with it, or a frame still in flight would step `try_serve/1` for a read
    %% that has already been answered - counting a hit or a miss for it, and
    %% growing the window on the way out.
    %%
    %% The clocks go with it. A `retry` already in the shell's mailbox is
    %% honoured until its timer is cancelled, and it would land on a state this
    %% event has just emptied: `advance/1` has re-based the buffer at the start
    %% of the fragment, so the pass it drives re-requests that fragment from
    %% byte 8 - GETs and pooled connections spent on bytes the consumer is on
    %% its way to reading locally, and a look-ahead group GET on top, which is
    %% the very cost this clause avoids by leaving the iterator alone.
    {_Offset, Cancels, Pipeline} = rabbitmq_stream_s3_read_pipeline:advance(pipeline(State0)),
    State = State0#state{
        pipeline = Pipeline,
        pending = undefined,
        peeks = [],
        peek_tail = unknown,
        current_not_found = false,
        timers = #{}
    },
    {State,
        cancel_effects(Cancels) ++
            [{cancel_timers, all}, {reply, {become_local, current_fragment_offset(State0)}}]};
step_(State0, {iterator_refreshed, Iterator}) ->
    %% Iterator has been refreshed past the 404'd fragment. Reinitialize
    %% at the next available fragment.
    %%
    %% The shell cancels every in-flight request before stepping this event, so
    %% no frame for one can arrive any more: they have to leave the queue here.
    %% One left behind would never be re-issued (`issue_ready/1` only starts
    %% `ready` requests) and the pipeline would report it `blocked` for the rest
    %% of the read, holding back every byte of its fragment queued behind it.
    Cancelled = State0#state{
        pipeline = rabbitmq_stream_s3_read_pipeline:clear_requests(pipeline(State0))
    },
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, #fragment_ref{offset = Offset} = FragRef, Iterator1} ->
            StreamId = State0#state.stream,
            Cfg = State0#state.cfg,
            State = #state{
                stream = StreamId,
                cfg = Cfg,
                window = Cfg#cfg.request_size,
                %% The sample carries across the rebuild for the same reason
                %% its elapsed time does: the shell stamps `sample_at` on the
                %% tick and nothing here can move it, so dropping the bytes
                %% would hand the next tick a part-sample's bytes over a whole
                %% sample's time and call the difference a slower reader. Both
                %% terms of the measurement describe the interval since the last
                %% tick, and a refresh part-way through it changes neither.
                sample_bytes = State0#state.sample_bytes,
                retry_delay = Cfg#cfg.min_retry_delay_ms,
                pipeline = rabbitmq_stream_s3_read_pipeline:replace_fragment(
                    FragRef, ?SEGMENT_HEADER_B, pipeline(State0)
                ),
                iterator = Iterator1
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
            %% Iterator exhausted after refresh. Become local, dropping the
            %% clocks with the read for the same reason the other become-local
            %% path does.
            Local = Cancelled#state{pending = undefined, timers = #{}},
            {Local, [
                {cancel_timers, all}, {reply, {become_local, current_fragment_offset(Local)}}
            ]};
        {error, {group_fetch_failed, _Reason}} ->
            %% A group object could not be fetched while advancing the
            %% refreshed iterator. Transient S3 error, not end of manifest:
            %% retry rather than routing to a local tier that may lack the
            %% data. The refresh was asked for by a pending read, and the retry
            %% re-serves it, so it asks for the refresh again.
            retry_group_fetch(Cancelled)
    end.

%% A 404 for a fragment other than the one being read. Its ranges are dropped
%% either way, but only the fragment actually being prefetched may record the
%% 404 against the prefetch: a frame can still arrive for a fragment the reader
%% has left behind at a transition, and marking the prefetch missing for it
%% would throw away a next fragment that is there - the consumer would then be
%% repositioned past a live fragment when it read on. Whether such a frame can
%% reach here is the shell's business (it cancels the ranges it leaves behind in
%% the same effect batch); whether it can do damage is this module's.
other_fragment_not_found(Fragment, State0) ->
    Prefetched = is_prefetched_fragment(Fragment, State0),
    {Dropped, Pipeline0} = rabbitmq_stream_s3_read_pipeline:drop_fragment(
        Fragment, pipeline(State0)
    ),
    {DroppedPast, Pipeline} =
        case Prefetched of
            true ->
                rabbitmq_stream_s3_read_pipeline:drop_fragment(
                    {not_found, Fragment}, Pipeline0
                );
            false ->
                {[], Pipeline0}
        end,
    %% Try to serve: a recorded 404 may trigger a refresh when the consumer
    %% reads past the current fragment.
    {State, Effects} = try_serve(State0#state{pipeline = Pipeline}),
    {State, [{cancel_request, Id} || Id <- Dropped ++ DroppedPast] ++ Effects}.

%% Whether `Fragment` is the fragment the reader is prefetching. The pipeline
%% knows it once bytes have arrived for it; before that the look-ahead memo is
%% what says which fragment that is, and it stays answered across a current
%% fragment 404 - which drops the prefetch's ranges without changing what is
%% being prefetched.
%%
%% Read off the memo rather than the iterator: resolving a peek can cost a
%% synchronous group GET (see `peek_next_fragment/2`). A memo that has not been
%% answered costs nothing to be strict about, since a prefetch is only ever
%% started for a peek that resolved.
is_prefetched_fragment(Fragment, #state{peeks = Peeks} = State) ->
    rabbitmq_stream_s3_read_pipeline:prefetching(Fragment, pipeline(State)) orelse
        lists:any(fun({#fragment_ref{offset = Offset}, _}) -> Offset =:= Fragment end, Peeks).

%% @doc Returns the pending read, if any.
-spec pending(state()) -> undefined | {byte_offset(), pos_integer()}.
pending(#state{pending = undefined}) -> undefined;
pending(#state{pending = #pending{offset = O, bytes = B}}) -> {O, B}.

%% @doc Returns the offset of the fragment currently being read.
-spec current_fragment_offset(state()) -> osiris:offset().
current_fragment_offset(State) ->
    rabbitmq_stream_s3_read_pipeline:current_fragment_offset(pipeline(State)).

pipeline(#state{pipeline = Pipeline}) -> Pipeline.

-ifdef(TEST).
-spec outstanding_ranges(state()) -> [{fragment_offset(), byte_offset(), byte_offset()}].
outstanding_ranges(State) ->
    rabbitmq_stream_s3_read_pipeline:outstanding_ranges(pipeline(State)).

-spec window_bytes(state()) -> pos_integer().
window_bytes(#state{window = Window}) ->
    Window.

%% Where the consumer has read up to, so a test can carry on reading from
%% wherever an earlier phase left the reader rather than assuming a position.
-spec read_position(state()) -> byte_offset().
read_position(State) ->
    rabbitmq_stream_s3_read_pipeline:read_position(pipeline(State)).

%% What the two budgets and the depth cap bound, split the way `has_room/1`
%% bounds them: bytes on the wire, bytes held unread, and requests in flight.
-spec load(state()) -> {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
load(State) ->
    {committed(State), buffered(State), inflight_count(State)}.

%% Bytes per second over the last completed sample; `undefined` before the first
%% tick closes one.
-spec fetch_rate(state()) -> undefined | non_neg_integer().
fetch_rate(#state{fetch_rate = Rate}) ->
    Rate.

-endif.

%% ------------------------------------------------------------------
%% Internal: try to serve the pending read
%% ------------------------------------------------------------------

try_serve(#state{pending = undefined} = State) ->
    {State, []};
try_serve(#state{pending = #pending{offset = Offset, bytes = Bytes}} = State) ->
    case try_read(State, Offset, Bytes) of
        {ok, Data, State1} ->
            State2 = note_hit(iolist_size(Data), State1#state{pending = undefined}),
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

%% The bytes are the pipeline's; what to do when it has none is this module's.
try_read(State, Offset, Bytes) ->
    case rabbitmq_stream_s3_read_pipeline:read(Offset, Bytes, pipeline(State)) of
        {ok, Data, Pipeline} ->
            {ok, Data, State#state{pipeline = Pipeline}};
        past_end ->
            %% Past the end of the current fragment. Transition.
            try_fragment_transition(State);
        await ->
            %% Chunk data is still streaming in. Wait for more, unless the
            %% fragment 404'd.
            case State of
                #state{current_not_found = true} -> {not_found_check_range, State};
                _ -> {await, State}
            end
    end.

try_fragment_transition(State0) ->
    case rabbitmq_stream_s3_read_pipeline:prefetch(pipeline(State0)) of
        {#fragment_ref{offset = NextOffset}, _Buffered} ->
            {State, Effects} = goto_next_fragment(State0),
            {next_fragment, NextOffset, Effects, State};
        not_found ->
            %% Next fragment 404. Need manifest range to decide.
            {not_found_check_range, State0};
        undefined ->
            try_peeked_transition(State0)
    end.

try_peeked_transition(#state{peeks = Peeks0, peek_tail = Tail0} = State0) ->
    {Peeks, Tail, _Attempted} = peek_next_fragment(State0, Peeks0, Tail0),
    State = State0#state{peeks = Peeks, peek_tail = Tail},
    case peek_head(Peeks, Tail) of
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
not_found_refresh(#state{peeks = Peeks0, peek_tail = Tail0} = State0) ->
    %% Path 1: the prefetched next fragment 404'd. Refresh past it, falling
    %% back to the current offset if the iterator is exhausted (the 404'd
    %% next was the last entry in the manifest).
    %%
    %% Through the memo, like every other look-ahead: asking the iterator
    %% directly here spent a synchronous group GET on an answer this reader has
    %% usually already paid for - and it is the answer that told it to prefetch
    %% the fragment that has just 404'd, so it is memoised by definition.
    {Peeks, Tail, _Attempted} = peek_next_fragment(State0, Peeks0, Tail0),
    State = State0#state{peeks = Peeks, peek_tail = Tail},
    case peek_head(Peeks, Tail) of
        {ok, #fragment_ref{offset = NotFoundOffset}, _Advanced} ->
            {State, [{refresh_iterator, NotFoundOffset}]};
        none ->
            {State, [{refresh_iterator, current_fragment_offset(State)}]};
        failed ->
            %% Probing the next entry hit a transient group fetch error. Retry
            %% rather than becoming local.
            retry_group_fetch(State)
    end.

%% Moves to the fragment being prefetched, resetting what this module tracks
%% about the fragment left behind. The pipeline decides what that costs in
%% cancelled requests (see its `advance/1`).
%%
%% `peek_tail` is left where it is. It says what follows the *last* of the
%% peeks, and popping the head does not move the last one, so the marker means
%% the same thing after the transition as before it.
goto_next_fragment(State0) ->
    {_NewOffset, Cancels, Pipeline} = rabbitmq_stream_s3_read_pipeline:advance(pipeline(State0)),
    {Iterator, Peeks} = advance_iterator(State0),
    State = State0#state{
        pipeline = Pipeline,
        iterator = Iterator,
        peeks = Peeks,
        current_not_found = false
    },
    {State, cancel_effects(Cancels)}.

cancel_effects(all) -> [{cancel_requests, all}];
cancel_effects(Ids) -> [{cancel_request, Id} || Id <- Ids].

%% The look-ahead already advanced the iterator past the entry being moved to,
%% and paid for the group fetch that took, so take its answer rather than
%% descending into the same group again.
%%
%% Only the head is consumed. The entries behind it were walked from the same
%% iterator and are still the fragments after the new current one, so keeping
%% them is what stops a transition costing back the reach the look-ahead just
%% bought - and what stops the descents being paid for twice.
advance_iterator(#state{peeks = [{_FragRef, Advanced} | Rest]}) ->
    {Advanced, Rest};
advance_iterator(#state{iterator = Iterator}) ->
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, _, It} -> {It, []};
        _ -> {Iterator, []}
    end.

%% A response that closed without delivering a byte is queued against the fault
%% clock by the pipeline; arming that clock is this module's half of it.
absorb_signals([], State) ->
    {State, []};
absorb_signals([empty_completion | Rest], State0) ->
    {State, Effects} = arm_retry(fault, State0),
    {State1, Effects1} = absorb_signals(Rest, State),
    {State1, Effects ++ Effects1}.

fail_range(Id, Kind, State0) ->
    case rabbitmq_stream_s3_read_pipeline:fail(Id, Kind, pipeline(State0)) of
        {ok, Pipeline} ->
            arm_retry(Kind, State0#state{pipeline = Pipeline});
        {dropped, Pipeline} ->
            %% A failed range that owed nothing was dropped rather than
            %% re-queued. No backoff and no retry timer: nothing failed to
            %% arrive. The freed depth slot may let a new range start.
            maybe_start_requests(State0#state{pipeline = Pipeline});
        stale ->
            {State0, []}
    end.

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
idle(Kind, #state{timers = Timers} = State) ->
    not is_map_key(Kind, Timers) andalso
        not rabbitmq_stream_s3_read_pipeline:waits_on(Kind, pipeline(State)).

inflight_count(State) ->
    rabbitmq_stream_s3_read_pipeline:inflight(pipeline(State)).

%% Bytes on the wire or queued for it, not yet in a buffer. Bounded for
%% throughput; see `has_room/1`.
committed(State) ->
    rabbitmq_stream_s3_read_pipeline:committed(pipeline(State)).

%% Bytes in a buffer the consumer has not read. Bounded for memory.
buffered(State) ->
    rabbitmq_stream_s3_read_pipeline:buffered(pipeline(State)).

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
%% touching the iterator, and `#state.peek_tail` is left `unknown` so the first
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
    %% `none` for the look-ahead's tail is what defers it: it reads as "the
    %% manifest ends here", so the frontier fills the current fragment and stops
    %% without walking the iterator. It is passed rather than stored, so the
    %% state's own look-ahead is untouched.
    {State1, _Peeks, _Tail, _Attempted, Effects} = extend_frontier(State, [], none, []),
    {State1, Effects}.

%% (Re-)issue ranges that are queued and not in flight. Their bytes are already
%% counted in `committed/1`, so they are not budget-gated - a range the reader
%% has committed to must be fetched, or the buffer never becomes contiguous
%% again and every read behind it stalls until the deadline. They do take a
%% slot, so the depth cap still applies; running before `extend_frontier/1`
%% keeps new ranges from taking the slots they are waiting for.
issue_ready(#state{cfg = #cfg{max_depth = MaxDepth}} = State) ->
    {Specs, Pipeline} = rabbitmq_stream_s3_read_pipeline:ready(MaxDepth, pipeline(State)),
    {State#state{pipeline = Pipeline}, [start_request_effect(Spec) || Spec <- Specs]}.

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
extend_frontier(#state{peeks = Peeks0, peek_tail = Tail0} = State) ->
    {State1, Peeks, Tail, Attempted, Effects} = extend_frontier(State, Peeks0, Tail0, []),
    arm_peek_retry(
        Tail, Attempted, State1#state{peeks = Peeks, peek_tail = Tail}, Effects
    ).

%% A group fetch that failed while looking ahead has to arm the retry itself.
%% Nothing else in this pass will: the ranges already queued are healthy, so no
%% `fail_range/3` runs. Without a timer nothing would pace the next attempt, and
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

extend_frontier(State, Peeks0, Tail0, Acc) ->
    extend_frontier(State, Peeks0, Tail0, false, Acc).

extend_frontier(State, Peeks0, Tail0, Attempted0, Acc) ->
    case has_room(State) of
        false ->
            {State, Peeks0, Tail0, Attempted0, lists:reverse(Acc)};
        true ->
            case next_range(State, Peeks0, Tail0) of
                {Peeks, Tail, Attempted, {FragRef, Range}} ->
                    {Spec, Pipeline} = rabbitmq_stream_s3_read_pipeline:push(
                        FragRef, Range, pipeline(State)
                    ),
                    extend_frontier(
                        State#state{pipeline = Pipeline},
                        Peeks,
                        Tail,
                        Attempted0 orelse Attempted,
                        [start_request_effect(Spec) | Acc]
                    );
                {Peeks, Tail, Attempted, none} ->
                    {State, Peeks, Tail, Attempted0 orelse Attempted, lists:reverse(Acc)}
            end
    end.

start_request_effect({Id, Key, Range, Fragment}) ->
    {start_request, Id, Key, Range, Fragment}.

%% Two budgets, not one. How hard to fetch and how much to hold ahead of the
%% consumer are different quantities answering different questions, so
%% `committed` is bounded for throughput and `buffered` for memory. Summing them
%% against a single ceiling would make a full buffer subtract directly from
%% concurrency: a consumer draining slower than the reader fetches would slow
%% the reader down, when a consumer that cannot keep up is a reason to stop
%% fetching *more*, once, at a memory bound, not a reason to fetch each range it
%% already owes less concurrently.
%%
%% Measured: at one configuration a drain-capped consumer held 18.0 requests in
%% flight where the same reader uncapped held 25.7.
%%
%% So `committed` is bounded for throughput and `buffered` for memory. The
%% reader keeps its full concurrency until the buffer is genuinely full and then
%% stops, rather than degrading in proportion to how far behind the consumer is.
%% The cost is that the two no longer share: worst-case memory is the sum of the
%% ceilings rather than one of them.
has_room(#state{cfg = #cfg{max_depth = MaxDepth}} = State) ->
    inflight_count(State) < MaxDepth andalso
        committed(State) < fetch_ceiling(State) andalso
        buffered(State) < buffer_ceiling(State).

%% How much the reader may have on the wire: what the concurrency target is
%% worth in bytes, floored at what the pending read needs.
%%
%% Flooring at what the pending read needs is the whole fix, and it is why both
%% ceilings carry the same floor: either one held below the pending read is the
%% same wedge, reached by a different route.
fetch_ceiling(#state{window = Window} = State) ->
    max(Window, pending_need(State)).

%% How much the reader may hold that the consumer has not read. Bounds memory
%% and nothing else: reaching it says the consumer is behind, which is a reason
%% to stop fetching ahead but not a reason to fetch what is already owed any
%% less concurrently.
buffer_ceiling(#state{cfg = #cfg{buffer_max = BufferMax}} = State) ->
    max(BufferMax, pending_need(State)).

%% What the pending read still needs from beyond the read position. Zero with no
%% read in hand, which cannot floor anything.
pending_need(#state{pending = #pending{offset = Offset, bytes = Bytes}} = State) ->
    ReadPos = rabbitmq_stream_s3_read_pipeline:read_position(pipeline(State)),
    Offset + Bytes - ReadPos;
pending_need(#state{}) ->
    0.

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
next_range(State, Peeks, Tail) ->
    FragRef = rabbitmq_stream_s3_read_pipeline:current_fragment(pipeline(State)),
    #fragment_ref{offset = Fragment, size = FragSize} = FragRef,
    Frontier = rabbitmq_stream_s3_read_pipeline:frontier(Fragment, pipeline(State)),
    case range_in_fragment(Frontier, FragSize, State) of
        {Start, End} -> {Peeks, Tail, false, {FragRef, {Start, End}}};
        none -> next_fragment_range(State, Peeks, Tail)
    end.

%% A next fragment known to be 404 is not looked ahead to: there is nothing to
%% prefetch and the transition decides what to do about it.
next_fragment_range(State, Peeks0, Tail0) ->
    case rabbitmq_stream_s3_read_pipeline:prefetch(pipeline(State)) of
        not_found ->
            {Peeks0, Tail0, false, none};
        _ ->
            spill(State, Peeks0, Tail0, Peeks0, false)
    end.

%% Walk the fragments already looked ahead to for one with room left, extending
%% the look-ahead by one when every one of them is spoken for.
%%
%% Walking past a full fragment rather than stopping at it is what lets the
%% window exceed one fragment: stopping would put the end of the frontier at the
%% first fully-spoken-for fragment, so reach at a fragment tail would be one
%% fragment however large the window is.
spill(State, Peeks, Tail, [], Attempted) ->
    %% Any fragment a further walk turns up is past the last one prefetched, so
    %% a horizon at all is a horizon this side of it: there is nothing to find.
    case rabbitmq_stream_s3_read_pipeline:prefetch_horizon(pipeline(State)) of
        unlimited ->
            case extend_peeks(State, Peeks, Tail) of
                {Peeks1, Tail1, true} when length(Peeks1) > length(Peeks) ->
                    spill(State, Peeks1, Tail1, lists:nthtail(length(Peeks), Peeks1), true);
                {Peeks1, Tail1, Attempted1} ->
                    {Peeks1, Tail1, Attempted orelse Attempted1, none}
            end;
        _Horizon ->
            {Peeks, Tail, Attempted, none}
    end;
spill(State, Peeks, Tail, [{FragRef, _Advanced} | Rest], Attempted) ->
    #fragment_ref{offset = Offset, size = Size} = FragRef,
    %% The look-ahead can be deeper than the horizon: a 404 arrives for a
    %% fragment the iterator was walked past long before, and the peeks on the
    %% far side of it are kept - `not_found_refresh/1` reads the 404'd offset off
    %% the head of them. They are not fetchable, so the walk stops here.
    case beyond_horizon(Offset, State) of
        true ->
            {Peeks, Tail, Attempted, none};
        false ->
            Frontier = rabbitmq_stream_s3_read_pipeline:frontier(Offset, pipeline(State)),
            case range_in_fragment(Frontier, Size, State) of
                {Start, End} -> {Peeks, Tail, Attempted, {FragRef, {Start, End}}};
                %% Walking past this fragment without issuing into it leaves it
                %% unseated in the pipeline's `nexts`, which is what would put
                %% the heads of `peeks` and `nexts` on different fragments -
                %% and `not_found_refresh/1` reads the 404'd offset off the
                %% first of them. It cannot happen here: `range_in_fragment/3`
                %% answers `none` for an unseated fragment only when its data
                %% region is empty, and `fragment_iterator:next/1` yields only
                %% fragment entries, descending into the group entries that are
                %% the size-0 case. An already-seated fragment answers `none`
                %% once it is fully spoken for, which is the case this walk is
                %% for.
                none -> spill(State, Peeks, Tail, Rest, Attempted)
            end
    end.

beyond_horizon(Offset, State) ->
    case rabbitmq_stream_s3_read_pipeline:prefetch_horizon(pipeline(State)) of
        unlimited -> false;
        Horizon -> Offset > Horizon
    end.

%% Walk the iterator one fragment further forward, memoising the entry and the
%% iterator advanced past it. Returns `{Peeks, Tail, Attempted}`.
%%
%% Nothing is walked past the lookahead cap, past the end of the manifest, or
%% while the fault clock is armed over a failed descent. Each attempt is a
%% synchronous group GET inside the core, so something has to pace them, and the
%% fault clock is what paces every other retry here. The `pool_busy` clock
%% cannot stand in: it fires for as long as the pool has no free connection,
%% which says nothing about whether the group object is fetchable.
%%
%% Whether to re-attempt is read off that clock rather than stored beside the
%% `failed` marker, so the two cannot drift apart: dropping the clock is what
%% licenses the next attempt, which means every site that disowns the clock
%% licenses one without having to remember to.
%%
%% `Attempted` says whether a fetch was actually made, which is what decides the
%% retry clock: a pass that never reached the look-ahead owes nothing, and arming
%% for it would keep alive the very clock that suppresses the next attempt.
extend_peeks(State, Peeks, Tail) ->
    case length(Peeks) >= max_lookahead(State) of
        true -> {Peeks, Tail, false};
        false -> extend_peeks_(State, Peeks, Tail)
    end.

extend_peeks_(#state{timers = Timers}, Peeks, failed) when is_map_key(fault, Timers) ->
    {Peeks, failed, false};
extend_peeks_(State, Peeks, Tail) when Tail =:= unknown; Tail =:= failed ->
    case rabbitmq_stream_s3_fragment_iterator:next(peek_iterator(State, Peeks)) of
        {ok, FragRef, Advanced} -> {Peeks ++ [{FragRef, Advanced}], unknown, true};
        end_of_manifest -> {Peeks, none, true};
        {error, {group_fetch_failed, _}} -> {Peeks, failed, true}
    end;
extend_peeks_(_State, Peeks, none) ->
    {Peeks, none, false}.

%% The iterator to walk forward from: the one advanced past the last fragment
%% already looked ahead to, or the reader's own when none has been.
peek_iterator(#state{iterator = Iterator}, []) ->
    Iterator;
peek_iterator(_State, Peeks) ->
    {_FragRef, Advanced} = lists:last(Peeks),
    Advanced.

%% Ensure at least one fragment has been looked ahead to, for the callers that
%% only ever need the nearest one. Returns `{Peeks, Tail, Attempted}`.
peek_next_fragment(_State, [_ | _] = Peeks, Tail) ->
    {Peeks, Tail, false};
peek_next_fragment(State, [], Tail) ->
    extend_peeks(State, [], Tail).

%% What the nearest look-ahead resolved to. Callers that ask about "the next
%% fragment" want the head, and the terminal marker only speaks for them when
%% there is no head.
peek_head([{FragRef, Advanced} | _], _Tail) -> {ok, FragRef, Advanced};
peek_head([], Tail) -> Tail.

max_lookahead(#state{cfg = #cfg{max_lookahead = MaxLookahead}}) ->
    MaxLookahead.

range_in_fragment(Frontier, FragSize, State) ->
    IdxStartPos = ?SEGMENT_HEADER_B + FragSize,
    case Frontier < IdxStartPos of
        true -> {Frontier, min(Frontier + request_size(State) - 1, IdxStartPos - 1)};
        false -> none
    end.

%% ------------------------------------------------------------------
%% Internal: configuration
%% ------------------------------------------------------------------

build_cfg(Opts) ->
    %% At least one byte per request. Zero makes `range_in_fragment/3` return
    %% `{Frontier, Frontier - 1}` - an inverted range, which is a `bytes=8-7`
    %% header S3 rejects and which counts nothing against the window, so the
    %% reader fills its whole depth with them and never gets a byte.
    RequestSize = max(1, maps:get(request_size, Opts, 4_194_304)),
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
        %% Defaulted to the window so that the bytes a reader may hold unread are
        %% bounded where they were before the budgets were split - what changes
        %% is that holding them no longer costs concurrency, not how many it may
        %% hold. Below one request it would stall a reader that has fetched a
        %% range the consumer has not reached yet.
        buffer_max = max(
            RequestSize,
            maps:get(buffer_max, Opts, max(RequestSize, maps:get(window_max, Opts, 33_554_432)))
        ),
        %% At least one request in flight. Zero leaves `has_room/1` false
        %% however far behind the consumer falls, so nothing is ever requested
        %% and every read waits out its whole deadline - three times over, with
        %% nothing in the log to say why.
        max_depth = max(1, maps:get(max_depth, Opts, 8)),
        %% Defaulted to the depth cap because a fragment is only ever looked
        %% ahead to in order to put a range in it, and no more ranges can be in
        %% flight than the depth allows - so at this value the backstop cannot
        %% bind before `has_room/1` does, which is the intent.
        max_lookahead = max(1, maps:get(max_lookahead, Opts, maps:get(max_depth, Opts, 8))),
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

%% ------------------------------------------------------------------
%% Internal: invariants (see `checked/2`)
%% ------------------------------------------------------------------

-ifdef(TEST).

%% Nothing may wait on a clock that is not running. A range put back by a
%% failure is released only when its kind's timer fires, so a clause that drops
%% the timer without releasing what waited on it leaves those ranges queued for
%% a round that will never come: `issue_ready/1` starts `ready` requests only,
%% and the pipeline reports the stranded one blocked, holding back every byte
%% queued behind it in its fragment.
%%
%% The look-ahead memo used to be a waiter here too, in its own field, and was
%% stranded exactly this way by `deadline_expired`. It is derived from the clock
%% now (see `peek_next_fragment/2`) rather than kept in step with it, which is
%% why it is not in this check: there is no longer a state for it to be in.
assert_clocks_have_waiters(#state{timers = Timers} = State, Event) ->
    Pipeline = pipeline(State),
    Waiting = [
        Kind
     || Kind <- [fault, pool_busy],
        rabbitmq_stream_s3_read_pipeline:queued_on(Kind, Pipeline)
    ],
    case [Kind || Kind <- Waiting, not is_map_key(Kind, Timers)] of
        [] -> ok;
        Stranded -> error({stranded_waiters, Stranded, Event, Timers})
    end.

%% A reader that owes a reply must be doing something about it: a range on the
%% wire or queued, a clock armed to put one back, or an effect handing the
%% problem to the shell. One that is doing none of those while the current
%% fragment still holds bytes it has never asked for has stopped for good - the
%% read waits out its deadline, and the retry behind it meets the same state,
%% because nothing about that state is going to change on its own.
%%
%% This is the shape of every wedge found in this module so far: a fetch ceiling
%% that could not admit the read in hand, a short response re-requested at a
%% range that could never flush, a stale prefetch holding window space nothing
%% could fetch into. Each was a different cause, and all of them looked like
%% this from here.
%%
%% "Bytes it has never asked for" is measured against the fragment's data region
%% directly, not by asking `extend_frontier/1` what it would issue. Two of those
%% three wedges were bugs *in* the issuance predicates, and a check that consults
%% `has_room/1` inherits whatever is wrong with it: asked whether it should have
%% fetched, a broken ceiling answers no and the check agrees the reader is
%% resting.
assert_not_wedged(
    #state{pending = #pending{}, timers = Timers, current_not_found = false} = State,
    Effects,
    Event
) when map_size(Timers) =:= 0 ->
    Pipeline = pipeline(State),
    case rabbitmq_stream_s3_read_pipeline:request_count(Pipeline) of
        0 ->
            #fragment_ref{offset = Fragment, size = FragSize} =
                rabbitmq_stream_s3_read_pipeline:current_fragment(Pipeline),
            Frontier = rabbitmq_stream_s3_read_pipeline:frontier(Fragment, Pipeline),
            Unfetched = Frontier < ?SEGMENT_HEADER_B + FragSize,
            case Unfetched andalso not handed_off(Effects) of
                false -> ok;
                true -> error({wedged, Event, State#state.pending})
            end;
        _ ->
            ok
    end;
assert_not_wedged(_State, _Effects, _Event) ->
    ok.

%% The read is no longer this state's problem: it has been answered, or the
%% shell has been asked to refresh the iterator, or the reader is stopping.
handed_off(Effects) ->
    lists:any(
        fun
            ({reply, _}) -> true;
            ({refresh_iterator, _}) -> true;
            ({fatal_error, _}) -> true;
            (stop) -> true;
            (_) -> false
        end,
        Effects
    ).

-endif.
