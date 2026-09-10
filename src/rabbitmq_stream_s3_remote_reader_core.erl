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
- `{observe, Kind}` - report a notable read-path event for metrics
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

Prefetch is sized by a byte budget partitioned between fetching and buffering,
and by a concurrency target that bounds the range GETs in flight and is what
sets bandwidth. See `room/1` and "concurrency control" below.

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
    %% Everything the reader may hold, buffered and in flight together. Half is
    %% what fetching may commit; the rest is the buffer's.
    max_memory :: pos_integer(),
    %% Most requests that may be in flight at once, across every fragment.
    max_depth :: pos_integer(),
    %% Most fragments the reader may look ahead to beyond the one it is reading.
    %%
    %% A backstop, not the working limit: what governs reach is the byte budget
    %% and the depth cap in `room/1`, and the look-ahead only ever extends when
    %% every fragment it already holds is spoken for. This bounds the walk for
    %% the cases those do not - a run of fragments with empty data regions
    %% consumes no request, so nothing else would stop it.
    max_lookahead :: pos_integer(),
    %% Whether `inflight_target` is searched for or pinned where it started.
    %% On unless the caller says otherwise, matching what the plugin ships (see
    %% `prefetch_auto_tune`), so a caller that says nothing gets the shipped
    %% behaviour. Note that the search only ever moves the target on a
    %% `tune_tick`, so a caller that never ticks runs at `inflight_initial`
    %% whatever this says.
    auto_tune :: boolean(),
    %% Where the search starts. Not the ceiling: `max_depth` is what the reader
    %% may never exceed, and starting at it would leave a tuner with nowhere to
    %% go and no way to discover it had overshot.
    inflight_initial :: pos_integer(),
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

    %% The pending read has already been counted as a miss, so `buffer_miss`
    %% counts reads that had to wait rather than the deliveries they waited
    %% through.
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
    %% S3 GET whenever an entry sits behind a group node, so answers are kept for
    %% as long as the iterator is the same one, and a transition promotes the
    %% head rather than descending again.
    %%
    %% More than one, because at a fragment's tail a single entry reaches only
    %% that fragment: a window larger than one fragment has nowhere else to go.
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
    %% This module has no clock, so time arrives as an input: the shell stamps
    %% each tick with the micros actually elapsed. That keeps `step/2` pure and
    %% lets a test drive a minute of tuning in virtual time.
    sample_bytes = 0 :: non_neg_integer(),
    %% Bytes per second over the last completed sample, or `undefined` before the
    %% first tick. Measured on what S3 delivered, since that is the quantity
    %% concurrency moves.
    fetch_rate :: undefined | non_neg_integer(),

    %% The same two quantities for the other end of the reader: bytes handed to
    %% the consumer, over the same sample.
    %%
    %% Equal to the fetch rate over any long run, so not a second reading of one
    %% quantity. What it answers is whether the serve rate moves when the fetch
    %% side is given more concurrency; one that does not says the ceiling is
    %% downstream of this module.
    served_sample = 0 :: non_neg_integer(),
    serve_rate :: undefined | non_neg_integer(),

    %% How many requests the reader is aiming to keep in flight, and the state
    %% of the search that sets it. See "concurrency control".
    inflight_target :: pos_integer(),
    %% `ramp` doubles while throughput answers, to reach the right order of
    %% magnitude in a few samples rather than one request at a time; `climb`
    %% steps by one and reverses when the rate falls off.
    tune_phase = ramp :: ramp | climb,
    probe_dir = up :: up | down,
    %% The rate the previous target produced, which is what a sample's rate is
    %% judged against.
    prev_rate :: undefined | non_neg_integer(),
    %% The best rate this reader has measured, and the target that produced it.
    %% An anchor, because comparing each sample only against the one before it
    %% cannot see a slow slide: past the peak the throughput curve is broad and
    %% shallow, so every step reads as noise while the total falls away.
    best_rate :: undefined | non_neg_integer(),
    best_target :: undefined | pos_integer(),
    %% Steps taken since the best was last beaten. Bounds how far the search
    %% wanders from the best point it knows: on a shallow slope nothing else
    %% turns it around and it walks to the ceiling.
    probes_since_best = 0 :: non_neg_integer(),

    %% Contention seen during the current sample: requests that never left the
    %% node for want of a pooled connection, or that S3 asked to slow down.
    %% Distinct from a buffer miss, which says the consumer waited and nothing
    %% about whether the fetch side has room for more.
    sample_contention = 0 :: non_neg_integer()
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

-doc """
Why a placement pass stopped issuing, which is the only thing that decides how
hard a reader fetches.

Four of these are budgets and say raise that budget; `reach` and `peek_failed`
say the reader ran out of fragments to ask for rather than out of room, which no
budget would fix. See `room/1` and "concurrency control".
""".
-type stall_reason() :: target | depth | fetch_budget | buffer | reach | peek_failed.

-type effect() ::
    {reply, read_result()}
    | {start_request, request_id(), rabbitmq_stream_s3:key(), {byte_offset(), byte_offset()},
        fragment_offset()}
    | {cancel_request, request_id()}
    | {cancel_requests, all}
    | {set_timer, backoff(), pos_integer()}
    | {cancel_timers, all}
    | {refresh_iterator, osiris:offset()}
    | {observe, observe_kind() | {stall, stall_reason()}}
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
    current_fragment_offset/1,
    inflight_target/1,
    fetch_rate/1,
    serve_rate/1,
    fetch_ceiling/1,
    memory_ceiling/1,
    committed/1,
    buffered/1
]).

-ifdef(TEST).
%% The ranges the core is still waiting on, in queue order. Tests describe what
%% S3 answers rather than which byte range the core happened to ask for, so
%% they use this to address a delivery to the right request.
-export([
    outstanding_ranges/1,
    read_position/1,
    load/1,
    request_id/3,
    tune/2
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
        inflight_target = initial_target(Cfg),
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
%% Aimed at one recurring defect: a derived fact stored as a field of its own,
%% which every clause must then remember to reset. That produces quiet
%% degradation rather than a crash - a reader that has stopped fetching, a clock
%% nobody will fire - which every safety property passes.
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
step_(State0, {tune_tick, ElapsedUs}) ->
    %% Close the sample. An elapsed of zero would divide by it, and says nothing
    %% either way, so it is left to accumulate into the next one - which is also
    %% what makes the tick safe to deliver early or twice.
    case ElapsedUs > 0 of
        true ->
            Rate = State0#state.sample_bytes * 1_000_000 div ElapsedUs,
            %% Closed over the same elapsed time as the fetch rate, so the two
            %% are comparable sample by sample rather than only on average.
            %% Nothing is tuned from it: it measures the consumer, which is not
            %% this reader's to control.
            ServeRate = State0#state.served_sample * 1_000_000 div ElapsedUs,
            State1 = tune(Rate, State0#state{
                sample_bytes = 0,
                fetch_rate = Rate,
                served_sample = 0,
                serve_rate = ServeRate
            }),
            %% A raised target does nothing until something issues against it,
            %% and the reader that most needs the extra concurrency is the one
            %% whose in-flight requests are all still streaming - so no delivery
            %% is due to drive a pass on its own.
            maybe_start_requests(State1#state{sample_contention = 0});
        false ->
            {State0, []}
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
    fail_range(Id, fault, note_contention(Reason, State0));
step_(State0, {request_error, Id, _Fragment, pool_busy}) ->
    %% Pool is growing — a connection becomes available once its TLS handshake
    %% completes (fast on same-region S3, but not instant). Use a mild backoff
    %% (25, 50, 100, 200, 400, 500, 500...) starting low to catch the connection
    %% as soon as it is ready, doubling up to a 500ms cap so we don't spin if the
    %% pool cannot grow (e.g. S3 unreachable).
    fail_range(Id, pool_busy, note_contention(pool_busy, State0));
step_(State0, {request_error, Id, _Fragment, pool_exhausted}) ->
    %% The same wait on the same clock - a connection still frees up by being
    %% checked in, and that happens in milliseconds - but a different reading of
    %% what it means. The pool is at `general_pool_max_size` with nothing left to
    %% open, so this reader is not waiting on growth it provoked; it is waiting
    %% on the other readers, and its own concurrency is part of what they are
    %% waiting on. See `note_contention/2`.
    fail_range(Id, pool_busy, note_contention(pool_exhausted, State0));
step_(State0, {request_error, _Id, _Fragment, Reason}) ->
    %% Non-retryable error (e.g. 403 AccessDenied). The report effect must
    %% precede `stop`, or nothing ties a stopped reader to a cause. Not gated on
    %% the request still being live: a 403 is about the reader's credentials,
    %% not one range.
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
    %% The buffer is kept: it holds a contiguous run of the current fragment
    %% that nothing in flight contributed to, so the retry is usually a read it
    %% can answer outright. Emptying it re-bases the fetch frontier at byte 0 of
    %% a fragment the consumer has already read through.
    %%
    %% Both clocks are disowned *and* cancelled. A timer left armed makes
    %% `arm_retry/2` a no-op for the next failure of that kind; a timer left
    %% running carries its own delay and can land inside a later backoff round,
    %% releasing that round's ranges early.
    %%
    %% The prefetched fragments go too: their bytes count against the buffer
    %% budget, so keeping them holds budget nothing is left to fetch into. The
    %% look-ahead memo stays - it is the truth about an iterator this event does
    %% not touch.
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
    %% The queue goes, but the iterator is left where it is: advancing it can
    %% cost a synchronous group GET for an answer nothing reads, since the reply
    %% hands the consumer to the local tier. The pending read is cleared with
    %% that reply, or a frame still in flight steps `try_serve/1` for a read
    %% already answered. The clocks go too: a `retry` already in the mailbox
    %% would land on the emptied state and re-request a fragment the consumer is
    %% on its way to reading locally.
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
                %% The refresh rebuilds the state, so the search starts over:
                %% what it had learned was about a fragment the reader has left.
                %% The target itself carries across - the ramp starts at one
                %% request because a new reader has no demand behind it, and
                %% this one has been serving a consumer all along.
                inflight_target = State0#state.inflight_target,
                %% The sample carries across for the same reason its elapsed time
                %% does: the shell stamps `sample_at` on the tick and nothing
                %% here can move it, so a rebuild that dropped the bytes would
                %% hand the next tick a part-sample's bytes over a whole sample's
                %% time and call the difference a slower reader. Both terms of
                %% the measurement describe the interval since the last tick,
                %% and a refresh part-way through it changes neither.
                sample_bytes = State0#state.sample_bytes,
                served_sample = State0#state.served_sample,
                sample_contention = State0#state.sample_contention,
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

%% Whether `Fragment` is the one being prefetched. The pipeline knows once bytes
%% have arrived for it; before that the look-ahead memo says which it is, and
%% that survives a current-fragment 404, which drops the prefetch's ranges
%% without changing what is being prefetched. Read off the memo rather than the
%% iterator, since resolving a peek can cost a synchronous group GET.
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

-doc """
How many requests the reader is currently aiming to keep in flight.

Published as a gauge by the shell. Read against `requests_in_flight` for what
the reader is achieving, it tells an operator whether a slow remote read is a
reader aiming low or a reader aiming high and not getting there - two different
problems that look identical from outside.
""".
-spec inflight_target(state()) -> pos_integer().
inflight_target(#state{inflight_target = Target}) ->
    Target.

-doc """
Bytes per second over the last completed sample, or `undefined` before the first
tick closes one.

This is the reading the concurrency search acts on, not a derived statistic:
every move the target makes is a comparison between two of these. Published so
that a target which has settled somewhere unexpected can be read against the
rates that put it there - a search that is working and a search misled by a
noisy sample produce the same target and are otherwise indistinguishable.
""".
-spec fetch_rate(state()) -> undefined | non_neg_integer().
fetch_rate(#state{fetch_rate = Rate}) ->
    Rate.

-doc """
Bytes per second handed to the consumer over the last completed sample, or
`undefined` before the first tick closes one.

Read against `fetch_rate/1`, which is measured over the same sample. They agree
over any long run, so what this is for is the comparison across operating
points: a serve rate that does not move when the fetch side is given more
concurrency says the ceiling is downstream of this reader, and no amount of
prefetch tuning will move it. Nothing on the fetch side can answer that.
""".
-spec serve_rate(state()) -> undefined | non_neg_integer().
serve_rate(#state{serve_rate = Rate}) ->
    Rate.

pipeline(#state{pipeline = Pipeline}) -> Pipeline.

-ifdef(TEST).
-spec outstanding_ranges(state()) -> [{fragment_offset(), byte_offset(), byte_offset()}].
outstanding_ranges(State) ->
    rabbitmq_stream_s3_read_pipeline:outstanding_ranges(pipeline(State)).

%% Where the consumer has read up to, so a test can carry on reading from
%% wherever an earlier phase left the reader rather than assuming a position.
-spec read_position(state()) -> byte_offset().
read_position(State) ->
    rabbitmq_stream_s3_read_pipeline:read_position(pipeline(State)).

%% What the two budgets and the depth cap bound, split the way `room/1`
%% bounds them: bytes on the wire, bytes held unread, and requests in flight.
-spec load(state()) -> {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
load(State) ->
    {committed(State), buffered(State), rabbitmq_stream_s3_read_pipeline:inflight(pipeline(State))}.

-endif.

%% ------------------------------------------------------------------
%% Internal: try to serve the pending read
%% ------------------------------------------------------------------

try_serve(#state{pending = undefined} = State) ->
    {State, []};
try_serve(#state{pending = #pending{offset = Offset, bytes = Bytes}} = State) ->
    case try_read(State, Offset, Bytes) of
        {ok, Data, State1} ->
            State2 = note_served(iolist_size(Data), State1#state{pending = undefined}),
            {State3, Effects} = maybe_start_requests(State2),
            {State3, [
                {reply, {ok, Data}},
                {observe, hit}
                | Effects
            ]};
        {next_fragment, NextOffset, CancelEffects, State1} ->
            State2 = State1#state{pending = undefined},
            {State3, Effects} = maybe_start_requests(State2),
            {State3,
                CancelEffects ++
                    [
                        {reply, {next_fragment, NextOffset}},
                        {observe, fragment_transition}
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
%% tier; the read deadline bounds the loop.
%%
%% The ranges in flight are left alone, except on the `iterator_refreshed` path
%% where the shell has already cancelled them. The failure is in advancing the
%% iterator, which says nothing about the fragment GETs already on the wire, and
%% cancelling one closes its pooled connection.
retry_group_fetch(State) ->
    arm_retry(fault, State).

%% ------------------------------------------------------------------
%% Internal: fragment navigation
%% ------------------------------------------------------------------

%% Which offset to refresh the iterator past after a 404. Two paths reach
%% `not_found_check_range` and they 404'd different fragments:
%%
%%  1. `try_fragment_transition` on `next = not_found`: the prefetched *next*
%%     fragment 404'd, and the iterator still points at it, so refresh past its
%%     own offset.
%%
%%  2. `try_read` on `current_not_found`: the *current* fragment 404'd, and the
%%     iterator was advanced past it at init, so it points at one that is still
%%     live. Refresh past the current fragment's offset or that live one is
%%     skipped (issue #173).
%%
%% The shell resumes at the first surviving fragment above the offset given.
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
%% The backoff grows once per armed round, not once per failed range: one fault
%% fails every pipelined range at once, and doubling per range would reach the
%% cap in a single round.
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
%% back to the minimum. Only a clock nothing waits on: with several ranges in
%% flight, S3 answering some while throttling others is what throttling looks
%% like from here, and resetting on any delivery hands back the delay the
%% failing ranges earned.
%%
%% A round the clock's own timer has just released counts as waiting on it,
%% which is what `#req.retried` records: at that instant no timer is armed and
%% nothing is queued in backoff.
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

%% Requests in flight that still owe bytes - what is occupying the wire, and so
%% what the concurrency target counts. See the pipeline's `inflight_owing/1`.
inflight_owing(State) ->
    rabbitmq_stream_s3_read_pipeline:inflight_owing(pipeline(State)).

%% Bytes on the wire or queued for it, not yet in a buffer. Bounded for
%% throughput; see `room/1`.
-spec committed(state()) -> non_neg_integer().
committed(State) ->
    rabbitmq_stream_s3_read_pipeline:committed(pipeline(State)).

%% Bytes in a buffer the consumer has not read. Bounded for memory.
-spec buffered(state()) -> non_neg_integer().
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
%% `init/5` or an iterator refresh. Unlike `maybe_start_requests/1` it does not
%% resolve the look-ahead: passing `none` for the tail holds the frontier inside
%% the current fragment without touching the iterator, and the first delivery
%% resolves it instead.
%%
%% `init/5` runs inside `gen_server:init/1` - the consumer's own process,
%% blocked until it returns - and resolving a peek can be a synchronous group
%% GET. The refresh path defers it for the same reason: the peek is one delivery
%% away either way.
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
%% in `committed/1`, so they are not budget-gated: a range the reader has
%% committed to must be fetched, or the buffer never becomes contiguous again.
%% They do take a slot, so the concurrency bound applies, and running before
%% `extend_frontier/1` keeps new ranges from taking the slots they wait for.
%%
%% That bound is the target as well as the depth cap, or a contention back-off
%% undoes itself when the fault timer releases every backed-off range at the
%% old concurrency. Deferring them is not a stall: the target is never below
%% one, and each delivery drives another pass.
issue_ready(#state{inflight_target = Target, cfg = #cfg{max_depth = MaxDepth}} = State) ->
    {Specs, Pipeline} = rabbitmq_stream_s3_read_pipeline:ready(
        Target, MaxDepth, pipeline(State)
    ),
    {State#state{pipeline = Pipeline}, [start_request_effect(Spec) || Spec <- Specs]}.

%% Append new ranges at the fetch frontier while the depth cap and the byte
%% budget allow, spilling into the prefetched next fragment once every byte of
%% the current one has been spoken for.
%%
%% Nothing is fetched while the current fragment is known to be 404. Retention
%% deleting it mid-read leaves buffered bytes below its index boundary, so the
%% frontier still points into an object that is gone, and each read those bytes
%% serve would fire a full `max_depth` of GETs at it. The refresh is driven by
%% the first read that cannot be served (see `not_found_refresh/1`).
extend_frontier(#state{current_not_found = true} = State) ->
    {State, []};
extend_frontier(#state{peeks = Peeks0, peek_tail = Tail0} = State) ->
    {State1, Peeks, Tail, Attempted, Stall, Effects} =
        extend_frontier(State, Peeks0, Tail0, false, []),
    %% Observed here rather than in the inner pass so the count is one per
    %% placement pass. `start_current_request/1` shares that pass but is not one:
    %% it defers the look-ahead deliberately, so it would report `reach` on a
    %% reader that is simply new.
    %%
    %% An idle reader is not stalled, it is finished, and the tick drives a pass
    %% five times a second whether or not there is anything to fetch for.
    %% Counted, those passes are most of the counts on a node holding attached
    %% readers nobody is reading from.
    arm_peek_retry(
        Tail,
        Attempted,
        State1#state{peeks = Peeks, peek_tail = Tail},
        Effects ++ observe_stall(State1, Stall)
    ).

%% A reader holding nothing, owing nothing and asked for nothing is finished,
%% not stalled - and the tick drives a pass five times a second regardless. A
%% full buffer is not this: that reader would fetch if it had room, which is
%% what `buffer` reports.
observe_stall(State, Stall) ->
    Idle =
        State#state.pending =:= undefined andalso
            committed(State) =:= 0 andalso
            buffered(State) =:= 0,
    case Idle of
        true -> [];
        false -> [{observe, {stall, Stall}}]
    end.

%% A group fetch that failed while looking ahead arms the retry itself: the
%% ranges already queued are healthy, so no `fail_range/3` runs, and the
%% look-ahead re-attempts precisely when no fault clock is armed.
%%
%% Armed on the attempt rather than on the memo's value, so every failed
%% re-attempt arms one. A pass that never reached the look-ahead must not arm:
%% it asked nothing of S3, and an armed clock is what suppresses the next
%% attempt.
arm_peek_retry(failed, true, State, Effects) ->
    {State1, RetryEffects} = arm_retry(fault, State),
    {State1, Effects ++ RetryEffects};
arm_peek_retry(_Peek, _Attempted, State, Effects) ->
    {State, Effects}.

extend_frontier(State, Peeks0, Tail0, Acc) ->
    {State1, Peeks, Tail, Attempted, _Stall, Effects} =
        extend_frontier(State, Peeks0, Tail0, false, Acc),
    {State1, Peeks, Tail, Attempted, Effects}.

%% Also returns why the pass stopped. Every pass ends on exactly one bound - a
%% budget, or having nothing left to ask for - so counting the reasons partitions
%% them, and the shares are readable as "this is what governs this reader".
extend_frontier(State, Peeks0, Tail0, Attempted0, Acc) ->
    case room(State) of
        {stall, Stall} ->
            {State, Peeks0, Tail0, Attempted0, Stall, lists:reverse(Acc)};
        ok ->
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
                    %% Room to fetch, but nothing to fetch: the reader is limited
                    %% by its reach rather than by any budget. A look-ahead that
                    %% failed is called out separately because it is transient -
                    %% a group GET that errored, retried on the fault clock -
                    %% where a plain `reach` is the manifest or the horizon, and
                    %% the two want opposite responses.
                    %% Only when this pass tried to extend the look-ahead. A
                    %% `failed` tail left by an earlier pass says nothing about
                    %% why this one found nothing: the walk may have stopped at
                    %% the horizon or the look-ahead cap, both of which are
                    %% `reach`. Same pair `arm_peek_retry/4` decides on.
                    Stall =
                        case {Tail, Attempted0 orelse Attempted} of
                            {failed, true} -> peek_failed;
                            _ -> reach
                        end,
                    {State, Peeks, Tail, Attempted0 orelse Attempted, Stall, lists:reverse(Acc)}
            end
    end.

start_request_effect({Id, Key, Range, Fragment}) ->
    {start_request, Id, Key, Range, Fragment}.

%% Whether another range may be issued, and if not, which bound stopped it. A
%% reader sitting at any of them looks the same from outside the process, so the
%% bound is reported as a counter.
%%
%% Returning the bound rather than a boolean keeps the order in one place: a
%% second function that named it would have to be kept in step with this one.
%% The cost is nothing, since a clause that returns a stall is reached where the
%% equivalent `andalso` term would have been false, and the bounds below it are
%% not evaluated either way.
%%
%% `inflight_owing` is the throughput gate - a request awaiting only its closing
%% frame is not using the wire - and `inflight` is the resource cap, since that
%% request still holds a pooled connection.
%%
%% Fetching never consults the buffer, which routinely holds more than its half
%% because a range is authorised without counting the committed bytes certain to
%% land in it. `memory_ceiling/1` bounds the two together and is the last word:
%% the only bound here that stops issuance outright.
-spec room(state()) -> ok | {stall, stall_reason()}.
room(#state{inflight_target = Target, cfg = #cfg{max_depth = MaxDepth}} = State) ->
    case inflight_owing(State) >= Target of
        true ->
            {stall, target};
        false ->
            case rabbitmq_stream_s3_read_pipeline:inflight(pipeline(State)) >= MaxDepth of
                true -> {stall, depth};
                false -> room_for_bytes(State)
            end
    end.

room_for_bytes(State) ->
    Committed = committed(State),
    case Committed >= fetch_ceiling(State) of
        true ->
            {stall, fetch_budget};
        false ->
            case buffered(State) + Committed >= memory_ceiling(State) of
                true -> {stall, buffer};
                false -> ok
            end
    end.

%% Both bounds are floored at what the pending read needs. A read of N bytes
%% cannot be served while fewer than N are outstanding, so a ceiling below N
%% wedges the reader for good - and the deadline is no escape, since the retry
%% refetches to the same ceiling. Reads are chunk sized, so a chunk larger than
%% the budget reaches it.
%%
%% Bytes the reader may have committed to fetching: what the concurrency target
%% is worth, capped at half `max_memory` so that the other half is always the
%% buffer's. Nothing here reads the buffer, so a full one cannot lock fetching
%% out. Every delivered byte moves from committed to buffered and stays there
%% until the consumer reads it, so a bound the two shared would do exactly that.
-spec fetch_ceiling(state()) -> non_neg_integer().
fetch_ceiling(
    #state{inflight_target = Target, cfg = #cfg{request_size = RequestSize, max_memory = MaxMemory}} =
        State
) ->
    max(min(Target * RequestSize, MaxMemory div 2), pending_need(State)).

%% Everything the reader may hold, buffered or in flight. The configured bound
%% itself, which the read in hand is the only thing that lifts.
-spec memory_ceiling(state()) -> non_neg_integer().
memory_ceiling(#state{cfg = #cfg{max_memory = MaxMemory}} = State) ->
    max(MaxMemory, pending_need(State)).

%% What the pending read still needs from beyond the read position. Zero with no
%% read in hand, which cannot floor anything.
pending_need(#state{pending = #pending{offset = Offset, bytes = Bytes}} = State) ->
    ReadPos = rabbitmq_stream_s3_read_pipeline:read_position(pipeline(State)),
    Offset + Bytes - ReadPos;
pending_need(#state{}) ->
    0.

%% The next range to request: the tail of the current fragment's data region,
%% or the head of a prefetched one once the current is fully spoken for. Every
%% range is clamped to its own fragment's data region, so a request can never
%% reach into the index region that follows it or past the end of the object.
%%
%% `Peeks` is threaded through the pass rather than resolved per range, because
%% resolving one can be a synchronous S3 GET: per range would spend `max_depth`
%% of them on the same object, blocking the reader while the read deadline
%% burns.
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
%% frontier exceed one fragment: stopping would put its end at the first
%% fully-spoken-for fragment, so reach at a fragment tail would be one fragment
%% however large the budget is.
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
                %% Walking past a fragment without issuing into it would leave
                %% `peeks` and the pipeline's `nexts` headed on different
                %% fragments, which `not_found_refresh/1` relies on. It cannot
                %% happen: `range_in_fragment/3` answers `none` for an unseated
                %% fragment only when its data region is empty, and the iterator
                %% yields only fragment entries.
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
%% synchronous group GET, so something must pace them, and the fault clock is
%% what paces every other retry here; `pool_busy` says nothing about whether a
%% group object is fetchable. Whether to re-attempt is read off that clock
%% rather than stored beside the `failed` marker, so the two cannot drift.
%%
%% `Attempted` says whether a fetch was made, which is what decides the retry
%% clock: a pass that never reached the look-ahead owes nothing.
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
    %% header S3 rejects and which counts nothing against the budget, so the
    %% reader fills its whole depth with them and never gets a byte.
    RequestSize = max(1, maps:get(request_size, Opts, 4_194_304)),
    #cfg{
        request_size = RequestSize,
        %% Never below two requests: fetching gets half of this, and a fetch
        %% ceiling under one range leaves nothing that can be issued.
        max_memory = max(2 * RequestSize, maps:get(max_memory, Opts, 67_108_864)),
        %% At least one request in flight. Zero has `room/1` refuse
        %% however far behind the consumer falls, so nothing is ever requested
        %% and every read waits out its whole deadline - three times over, with
        %% nothing in the log to say why.
        max_depth = max(1, maps:get(max_depth, Opts, 8)),
        %% The depth cap, because a fragment is only ever looked ahead to in
        %% order to put a range in it, and no more ranges can be in flight than
        %% the depth allows - so the backstop cannot bind before `room/1`
        %% does, which is the intent. Not exposed as a setting for the same reason,
        %% though a caller may still pass it.
        max_lookahead = max(1, maps:get(max_lookahead, Opts, maps:get(max_depth, Opts, 8))),
        auto_tune = maps:get(auto_tune, Opts, true),
        %% Defaulted to the ceiling, so a caller that says only how deep a reader
        %% may run gets exactly that depth and nothing to search from. The plugin
        %% passes it explicitly (`prefetch_inflight_initial`).
        inflight_initial = max(1, maps:get(inflight_initial, Opts, maps:get(max_depth, Opts, 8))),
        min_retry_delay_ms = maps:get(min_retry_delay_ms, Opts, 1_000),
        max_retry_delay_ms = maps:get(max_retry_delay_ms, Opts, 30_000)
    }.

%% Bytes per range request.
request_size(#state{cfg = #cfg{request_size = RequestSize}}) ->
    RequestSize.

%% Bytes handed to the consumer, accumulated into the sample the tick closes.
note_served(Bytes, #state{served_sample = Served} = State) ->
    State#state{served_sample = Served + Bytes}.

%% Only the first miss for a given pending read is counted. `try_serve/1` re-runs
%% on every delivery while a read waits, and `buffer_miss` counts reads that had
%% to wait rather than the deliveries they waited through.
note_miss(#state{missed_pending = true} = State) ->
    {State, []};
note_miss(State0) ->
    {State0#state{missed_pending = true}, [{observe, miss}]}.

%% ------------------------------------------------------------------
%% Internal: concurrency control
%%
%% One controlled variable, `inflight_target`: how many range GETs to keep on
%% the wire. Concurrency is what sets a remote reader's bandwidth, since a
%% single S3 connection transfers at roughly one rate whatever range size is
%% asked of it.
%%
%% Searched rather than configured, because the answer is not a property of the
%% configuration: the same reader against the same store wants different
%% concurrency with a cold connection pool and a warm one. Past the peak,
%% throughput falls away as requests queue rather than run, so this searches for
%% a maximum and treats the fall-off as the signal to come back down.
%% `#cfg.max_depth` is the ceiling, not the operating point.
%%
%% The search starts at one request so what a reader fetches stays proportional
%% to what its consumer has asked for: starting at the operating point would
%% have a consumer reading one message download the whole target's worth.
%% ------------------------------------------------------------------

%% A rate must beat the last one by this much to count as an improvement, in
%% percent. Below it the difference is sample noise, and acting on noise makes
%% the target wander instead of settle.
-define(TUNE_EPSILON_PCT, 5).

%% How far below the best rate seen the search will drift before returning to
%% the target that produced it. Wider than the epsilon, so ordinary sample noise
%% around the peak does not keep yanking it back, and narrow enough that a real
%% slide is caught before it has cost much throughput.
-define(TUNE_FALLBACK_PCT, 10).

%% Steps the search may take without beating its best before it goes back to
%% where that best was and tries the other direction. Small, because each step
%% past the peak is throughput spent to learn nothing.
-define(TUNE_PROBE_LIMIT, 3).

%% Contention is S3 asking the reader to slow down. Counted per sample and acted
%% on at the tick, so a burst of failures from one round costs one backoff
%% rather than one each.
%%
%% `pool_busy` is not counted. The pool grows reactively, so a checkout that
%% finds nothing free is what raising concurrency looks like while connections
%% open - the cost of growing, not evidence of having grown too far. It resolves
%% on its own, and if the concurrency does not pay, the sample rate says so.
%%
%% `pool_exhausted` is counted: at the ceiling there is nothing left to open, so
%% the wait does not resolve on its own, and a rate set by pool waiting reads as
%% flat - which `classify/2` answers by stepping further the way it was already
%% going. Only the pool can tell the two apart, see
%% `rabbitmq_stream_s3_api_aws_pool:saturation/1`.
note_contention(slow_down, #state{sample_contention = Count} = State) ->
    State#state{sample_contention = Count + 1};
note_contention(pool_exhausted, #state{sample_contention = Count} = State) ->
    State#state{sample_contention = Count + 1};
note_contention(_Reason, #state{} = State) ->
    State.

%% Where the search starts, and where a reader that does not search stays.
%%
%% One request with the search on: the ramp is what reaches the operating point,
%% so `inflight_initial` does not decide it and is not consulted. With the search
%% off nothing would ever move the target, so the configured value is both the
%% start and the whole life.
initial_target(#cfg{auto_tune = false, max_depth = MaxDepth, inflight_initial = Initial}) ->
    min(MaxDepth, Initial);
initial_target(#cfg{}) ->
    1.

tune(_Rate, #state{cfg = #cfg{auto_tune = false}} = State) ->
    State;
tune(_Rate, #state{sample_contention = Contention} = State) when Contention > 0 ->
    %% S3 asked the reader to slow down. Unlike a slower sample this is not a hint
    %% that the peak is behind us, it is the far side of it being reported
    %% directly, so it is worth more than one step: give back a quarter and
    %% resume the search from there.
    Target = State#state.inflight_target,
    retarget(Target - max(1, Target div 4), State#state{
        tune_phase = climb,
        probe_dir = down,
        prev_rate = undefined,
        best_rate = undefined,
        best_target = undefined,
        probes_since_best = 0
    });
tune(0, #state{} = State) ->
    %% A sample that delivered nothing says nothing about concurrency, so it is
    %% not a reading: a consumer that pauses, or one that has yet to issue its
    %% first read, is not evidence that the target is too high. Judged as one it
    %% would be, since a zero rate cannot improve on a zero rate - the ramp would
    %% read the pause as "doubling stopped paying", halve, and hand the climb a
    %% target it then has to walk back up a step at a time. The baseline is kept
    %% rather than cleared, so the next sample that does carry bytes is compared
    %% against the last one that did.
    State;
tune(Rate, #state{prev_rate = undefined} = State) ->
    %% Nothing to compare against yet - the first sample, or the one after a
    %% contention backoff. Take the reading and move on the next tick.
    State#state{prev_rate = Rate};
tune(
    Rate,
    #state{tune_phase = ramp, inflight_target = Target, prev_rate = Prev, cfg = Cfg} = State
) ->
    case improved(Rate, Prev) of
        false ->
            %% Doubling stopped paying. The peak is between here and half of
            %% here, so go back and look for it a step at a time. The reading
            %% goes with it: it belongs to a target the reader has left.
            retarget(max(1, Target div 2), State#state{
                tune_phase = climb, probe_dir = up, prev_rate = undefined
            });
        true when Target >= Cfg#cfg.max_depth ->
            %% Still paying, but there is nowhere left to double into. Hold at
            %% the ceiling and search from there rather than halving away from
            %% a rate that was still improving - the ceiling may well be the
            %% best place this reader can be, and the climb will step down soon
            %% enough if it is not.
            State#state{tune_phase = climb, probe_dir = up, prev_rate = Rate};
        true ->
            %% Double rather than step, so a reader reaches the right order of
            %% magnitude in a few samples instead of thirty.
            retarget(Target * 2, State#state{prev_rate = Rate})
    end;
tune(
    Rate,
    #state{
        tune_phase = climb,
        inflight_target = Target,
        probe_dir = Dir,
        best_rate = Best,
        best_target = BestTarget
    } = State
) ->
    if
        Best =/= undefined, Rate * 100 < Best * (100 - ?TUNE_FALLBACK_PCT) ->
            %% Well below the best this reader has managed. Go back to where that
            %% was rather than stepping towards it, and search again from there -
            %% the drift that got here was made of steps too small to notice, so
            %% unwinding it one at a time would take as long as it took to
            %% happen. The best is forgotten with the move: it belongs to
            %% conditions that may not hold any more, and keeping it would stop
            %% the reader ever settling anywhere else.
            retarget(BestTarget, State#state{
                prev_rate = undefined,
                best_rate = undefined,
                best_target = undefined,
                probes_since_best = 0,
                probe_dir = up
            });
        Best =:= undefined; Rate * 100 > Best * (100 + ?TUNE_EPSILON_PCT) ->
            %% A new best, by enough to be a reading rather than noise. Without
            %% that margin the best ratchets: any lucky sample at a higher target
            %% records one there, so the anchor climbs with the search instead of
            %% holding it, and nothing stops the target walking past the peak.
            retarget(step_target(Target, Dir), State#state{
                prev_rate = Rate,
                best_rate = Rate,
                best_target = Target,
                probes_since_best = 0
            });
        State#state.probes_since_best >= ?TUNE_PROBE_LIMIT ->
            %% Several steps without beating the best. Go back to it and try the
            %% other way, rather than carrying on into ground that has already
            %% failed to pay - a shallow slope reads as noise at every step, so
            %% nothing else here would turn the search around.
            retarget(BestTarget, State#state{
                prev_rate = undefined,
                probes_since_best = 0,
                probe_dir = flip(Dir)
            });
        true ->
            Probed = State#state.probes_since_best + 1,
            case classify(Rate, State#state.prev_rate) of
                worse ->
                    %% The last step was the wrong way. Turn around and take it
                    %% back.
                    Reversed = flip(Dir),
                    retarget(step_target(Target, Reversed), State#state{
                        probe_dir = Reversed, prev_rate = Rate, probes_since_best = Probed
                    });
                _ ->
                    %% Off the best but not far off it: a plateau, not a peak.
                    %% Keep probing the way we were. Holding still here is how
                    %% the search stops searching: a held target produces another
                    %% flat reading, which holds again, for ever.
                    retarget(step_target(Target, Dir), State#state{
                        prev_rate = Rate, probes_since_best = Probed
                    })
            end
    end.

%% Clamped to the configured ceiling, and never below one: a target of zero
%% has `room/1` refuse however far behind the consumer falls, so nothing
%% would ever be requested again.
retarget(Target, #state{cfg = #cfg{max_depth = MaxDepth}} = State) ->
    State#state{inflight_target = max(1, min(MaxDepth, Target))}.

%% Proportional, not one at a time. The ramp can leave the search a long way
%% from the peak - doubling overshoots by up to half of where it lands, and
%% against the ceiling by more - and a step of one would take a sample each to
%% walk back, so the reader would spend most of its run getting there.
step_target(Target, up) -> Target + max(1, Target div 8);
step_target(Target, down) -> Target - max(1, Target div 8).

flip(up) -> down;
flip(down) -> up.

improved(Rate, Prev) ->
    Rate * 100 > Prev * (100 + ?TUNE_EPSILON_PCT).

classify(Rate, Prev) ->
    case improved(Rate, Prev) of
        true ->
            better;
        false ->
            case Rate * 100 < Prev * (100 - ?TUNE_EPSILON_PCT) of
                true -> worse;
                false -> flat
            end
    end.

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
%% The look-ahead is not checked here: it is derived from the clock rather than
%% kept in step with it, so it has no state to be stranded in.
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
%% problem to the shell. One doing none of those while the current fragment
%% still holds bytes it has never asked for has stopped for good - the read
%% waits out its deadline and the retry behind it meets the same state.
%%
%% "Bytes it has never asked for" is measured against the fragment's data region
%% directly rather than by asking `extend_frontier/1` what it would issue: a
%% check that consults `room/1` inherits whatever is wrong with it, and a
%% broken ceiling would agree the reader is resting.
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
