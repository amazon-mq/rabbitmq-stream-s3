%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_remote_reader).
-moduledoc """
A gen_server process which reads stream data from the remote tier.

This process bridges async S3 responses and the synchronous log reader.
All decision logic (buffering, reassembly, prefetch sizing and concurrency,
retry, fragment transitions) lives in `rabbitmq_stream_s3_remote_reader_core`.
This module translates external events (gun messages, timer fires, gen_server
calls) into core events, feeds them to the core, and executes the resulting
effects.

## Effect execution

Effects returned by the core are executed in a loop. Some effects produce
immediate results that feed back into the core (e.g. `lookup_manifest_range`
resolves from the local manifest cache). The loop continues until no more
synchronous feedback is generated.
""".

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("include/rabbitmq_stream_s3.hrl").
-include("include/logging.hrl").

-behaviour(gen_server).

-record(cfg, {
    request_timeout_ms :: pos_integer(),
    pending_read_deadline_ms :: pos_integer()
}).

-define(C_BUFFER_HIT, 1).
-define(C_BUFFER_MISS, 2).
-define(C_FRAGMENT_TRANSITION, 3).
-define(C_REQUESTS_IN_FLIGHT, 4).
-define(C_READ_DURATION_MS, 5).
-define(C_READ, 6).
-define(C_TOTAL_REQUESTS, 7).
-define(C_FATAL_ERRORS, 8).
-define(COUNTER_KEY, {rabbitmq_stream_s3_remote_reader, counter}).
-define(COUNTERS, [
    {buffer_hit, ?C_BUFFER_HIT, counter, "Number of reads served from the buffer"},
    {buffer_miss, ?C_BUFFER_MISS, counter, "Number of reads that had to await async data"},
    {fragment_transition, ?C_FRAGMENT_TRANSITION, counter, "Number of fragment transitions"},
    {requests_in_flight, ?C_REQUESTS_IN_FLIGHT, gauge,
        "Current number of in-flight async requests"},
    {read_duration_ms, ?C_READ_DURATION_MS, counter, "Total milliseconds spent in read calls"},
    {read, ?C_READ, counter, "Number of read/4,5 calls"},
    {remote_reader_total_requests, ?C_TOTAL_REQUESTS, counter, "Number of S3 requests initiated"},
    {remote_reader_fatal_errors, ?C_FATAL_ERRORS, counter,
        "Number of remote readers stopped by a non-retryable S3 error"}
]).
%% Upper bucket boundaries follow the values the window can actually take. It
%% starts at prefetch_request_size (4 MiB), doubles on a miss up to
%% prefetch_window_max (32 MiB) and gives a request back at a time, so at the
%% default sizing it is always a multiple of 4 MiB in [4, 32] MiB: boundaries
%% spaced by the request size resolve every step it can make. The lower ones
%% cover a request size configured smaller than the default. The window never
%% exceeds its ceiling, so in normal operation +Inf stays empty.
-define(PREFETCH_WINDOW_BUCKETS, [
    262_144,
    1_048_576,
    2_097_152,
    4_194_304,
    8_388_608,
    12_582_912,
    16_777_216,
    20_971_520,
    25_165_824,
    29_360_128,
    33_554_432,
    infinity
]).

-type hint() :: chunk_boundary | within_chunk.
-export_type([hint/0]).

%% A read request from the log reader.
-record(read, {
    offset :: byte_offset(),
    bytes :: pos_integer(),
    hint :: hint()
}).

-record(state, {
    stream :: stream_id(),
    cfg :: #cfg{},
    core :: rabbitmq_stream_s3_remote_reader_core:state(),
    reader_ref :: reference(),
    %% Pending caller (at most one).
    from :: gen_server:from() | undefined,
    %% Timer that fires deadline_expired if the pending read is not served in time.
    deadline_timer :: reference() | undefined,
    %% Token tagging the current deadline timer's message. cancel_timer cannot
    %% remove an already-fired message, so a deadline_expired carrying a stale
    %% token (its read was served, or superseded by a newer read) is ignored.
    deadline_token :: reference() | undefined,
    %% Maps the backend's request id to the range it was issued for. Keyed by
    %% the backend id because that is what arriving frames carry; the core keys
    %% the same request by `{Fragment, RangeStart}`, which is what its events
    %% carry back.
    requests = #{} :: #{
        rabbitmq_stream_s3_api:async_req() => {
            osiris:offset(), byte_offset(), rabbitmq_stream_s3_api:async_state()
        }
    },
    %% Cancelled request refs (gun may still deliver frames).
    cancelled = #{} :: #{rabbitmq_stream_s3_api:async_req() => ok},
    %% The timer and token of the retry armed for each backoff kind. The handle
    %% is what `cancel_timers` cancels; the token is what identifies the round,
    %% and the kind is not enough on its own. cancel_timer cannot remove an
    %% already-queued message, so a timer that fired just before it was
    %% cancelled still arrives - and by then its kind can be armed again for a
    %% different round, because the batch that cancelled the timers arms it (an
    %% iterator refresh whose `start_request` effects fail does exactly that).
    %% Acting on the stale message would release the new round's ranges on a
    %% delay that round never earned.
    retry_timers = #{} :: #{
        rabbitmq_stream_s3_remote_reader_core:backoff() => {reference(), reference()}
    },
    %% Set to true when the core emits `stop`.
    stopping = false :: boolean()
}).

%% API
-export([
    start/1,
    stop/1,
    read/4,
    read/5,
    init_counters/0,
    prefetch_window_prometheus_format/0
]).

%% gen_server
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    format_status/1
]).

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

-spec init_counters() -> ok.
init_counters() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?COUNTER_KEY, Cnt),
    rabbitmq_stream_s3_histogram:new(?MODULE, ?PREFETCH_WINDOW_BUCKETS),
    ok.

-spec prefetch_window_prometheus_format() -> map().
prefetch_window_prometheus_format() ->
    {Buckets, Count, Sum} = rabbitmq_stream_s3_histogram:prometheus_format(
        ?MODULE, fun(X) -> X end, ?PREFETCH_WINDOW_BUCKETS
    ),
    #{
        prefetch_window_bytes => #{
            type => histogram,
            help => <<"Distribution of the remote reader's prefetch window in bytes">>,
            values => [{[], Buckets, Count, Sum}]
        }
    }.

start(Config) ->
    gen_server:start(?MODULE, Config, []).

%% Asynchronous, fire-and-forget stop. The reader also monitors its consumer
%% and stops on its own when the consumer exits; this reclaims it eagerly on a
%% become_local transition so it does not linger for the consumer's lifetime.
stop(Pid) ->
    gen_server:cast(Pid, stop).

%% The deadline a pending read is given before it is answered `{error, timeout}`.
%% Fixed, it silently caps how large a chunk the remote tier can serve: a read
%% cannot complete faster than the tier can deliver the bytes it asked for, so
%% past that cap every attempt expires. The log reader's retries do not rescue
%% it either - a retry resumes from what the core still has buffered, but the
%% bytes the read is waiting for are by definition the ones that did not arrive
%% in time, so each attempt meets the same wall and after
%% `?READ_RETRY_ATTEMPTS` the read fails for good. Chunks are one writer batch
%% and are bounded by message count rather than by bytes, and a chunk larger
%% than `fragment_target_size` simply becomes an oversized single-chunk
%% fragment, so the cap is reachable by ordinary publishing.
%%
%% So the budget scales with what the read needs fetched. `Offset` counts too,
%% as an upper bound on that rather than a tight one: a consumer that attaches
%% mid-fragment starts with nothing buffered, and a read is served as soon as
%% its bytes arrive, so an over-estimate costs headroom and nothing else.
-define(PENDING_READ_DEADLINE_MS, 40_000).
%% A deliberately pessimistic floor on tier throughput: 10 kB/ms is 10 MB/s,
%% against ~40 MB/s for a single connection and several pipelined. The margin
%% is for a slow tier; it still bounds how long a genuinely stuck read holds
%% its caller, since the term is linear in bytes actually asked for.
-define(DEADLINE_BYTES_PER_MS, 10_000).
%% The gen_server:call timeout must exceed the internal deadline so the deadline
%% always fires first and replies {error, timeout} to the caller. This avoids
%% overlapping reads (caller times out, new read arrives while from is still
%% set) which require unsafe buffer resets.
-define(READ_TIMEOUT_MARGIN_MS, 5_000).

read(Server, Offset, Bytes, Hint) ->
    Timeout = read_deadline_ms(Offset, Bytes, ?PENDING_READ_DEADLINE_MS) + ?READ_TIMEOUT_MARGIN_MS,
    read(Server, Offset, Bytes, Hint, Timeout).

%% The deadline for a read, and the basis for its caller's call timeout. Both
%% derive from this so the ordering between them holds at every read size.
read_deadline_ms(Offset, Bytes, BaseMs) ->
    ToFetch = max(0, Offset + Bytes - ?SEGMENT_HEADER_B),
    BaseMs + ToFetch div ?DEADLINE_BYTES_PER_MS.

read(Server, Offset, Bytes, Hint, Timeout) ->
    T0 = erlang:monotonic_time(),
    Result =
        try
            gen_server:call(Server, #read{offset = Offset, bytes = Bytes, hint = Hint}, Timeout)
        catch
            exit:{timeout, _} ->
                {error, timeout};
            exit:Reason ->
                %% The reader gen_server crashed or is already gone (noproc).
                %% Surface a clean error so the caller can restart it, instead
                %% of the exit propagating into and crashing the consumer
                %% connection (which may serve many other subscriptions).
                {error, {remote_reader_down, Reason}}
        end,
    Duration = rabbitmq_stream_s3_util:elapsed_ms(T0),
    counters:add(counter(), ?C_READ_DURATION_MS, Duration),
    counters:add(counter(), ?C_READ, 1),
    Result.

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

init(
    #{
        reader := Reader,
        stream := StreamId,
        location := #remote_location{
            position = Pos,
            fragment_ref = FragRef,
            iterator = Iterator
        }
    } = Config
) ->
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
    Opts = maps:get(opts, Config, #{}),
    Cfg = build_cfg(Opts),
    {Core0, Effects} = rabbitmq_stream_s3_remote_reader_core:init(
        StreamId, FragRef, Pos, Iterator, Opts
    ),
    State0 = #state{
        stream = StreamId,
        cfg = Cfg,
        core = Core0,
        reader_ref = erlang:monitor(process, Reader)
    },
    State = execute_effects(Effects, State0),
    {ok, State}.

handle_call(
    #read{offset = Offset, bytes = Bytes, hint = Hint},
    From,
    #state{cfg = #cfg{pending_read_deadline_ms = BaseDeadline}, core = Core0} = State0
) ->
    ?assertEqual(undefined, State0#state.from),
    {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        Core0, {read, Offset, Bytes, Hint}
    ),
    Token = make_ref(),
    Deadline = read_deadline_ms(Offset, Bytes, BaseDeadline),
    Timer = erlang:send_after(Deadline, self(), {deadline_expired, Token}),
    State1 = State0#state{
        core = Core1,
        from = From,
        deadline_timer = Timer,
        deadline_token = Token
    },
    State = execute_effects(Effects, State1),
    maybe_stop(State);
handle_call(Request, From, State) ->
    {stop, {unknown_call, From, Request}, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, _Pid, _Reason}, #state{reader_ref = MRef} = State) ->
    {stop, normal, State};
handle_info(
    {retry_requests, Kind, Token},
    #state{stream = StreamId, core = Core0, retry_timers = Timers} = State0
) ->
    case Timers of
        #{Kind := {_Timer, Token}} ->
            ?LOG_DEBUG(
                "remote_reader ~ts retry_requests firing for stream ~ts",
                [Kind, StreamId]
            ),
            {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(Core0, {retry, Kind}),
            State = execute_effects(
                Effects, State0#state{core = Core1, retry_timers = maps:remove(Kind, Timers)}
            ),
            maybe_stop(State);
        _ ->
            %% A retry timer that was cancelled after it had already fired (a
            %% read deadline expired, or the iterator was refreshed - both
            %% disown either backoff). cancel_timer cannot remove a queued
            %% message, so it is dropped here instead. The token is what makes
            %% that decidable: the same batch that cancels the timers can arm a
            %% fresh one for the same kind, so a guard on the kind alone would
            %% mistake the stale message for the new round's own and release
            %% that round's ranges before their delay had elapsed.
            {noreply, State0}
    end;
handle_info(
    {deadline_expired, Token},
    #state{deadline_token = Token, stream = StreamId, core = Core0} = State0
) ->
    ?LOG_DEBUG(
        "remote_reader deadline_expired for stream ~ts"
        " (requests_in_flight=~b)",
        [StreamId, map_size(State0#state.requests)]
    ),
    %% Cancel in-flight S3 requests so they don't feed stale data into the
    %% reset buffer after the core clears it.
    State1 = cancel_all_requests(State0),
    {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(Core0, deadline_expired),
    State = execute_effects(
        Effects, State1#state{core = Core1, deadline_timer = undefined, deadline_token = undefined}
    ),
    maybe_stop(State);
handle_info({deadline_expired, _StaleToken}, State) ->
    %% A deadline timer that fired after its read was served, or that belongs to
    %% a read since superseded by a newer one. cancel_timer cannot remove an
    %% already-queued message, so ignore the stale token rather than reset a
    %% newer read's buffer and cancel its in-flight requests.
    {noreply, State};
handle_info(Msg, #state{requests = Requests0, cancelled = Cancelled0} = State0) ->
    AsyncStates = #{Req => AsyncState || Req := {_, _, AsyncState} <- Requests0},
    case rabbitmq_stream_s3_api:match_async(Msg, AsyncStates, Cancelled0) of
        {ok, RequestId} ->
            handle_async_response(Msg, RequestId, State0);
        {cancelled, Ref, Final} ->
            Cancelled =
                case Final of
                    final -> maps:remove(Ref, Cancelled0);
                    more -> Cancelled0
                end,
            {noreply, State0#state{cancelled = Cancelled}};
        error ->
            ?LOG_DEBUG("remote_reader_shell received unexpected message: ~W", [Msg, 10]),
            {noreply, State0}
    end.

terminate(_Reason, State) ->
    _ = cancel_all_requests(State),
    ok.

format_status(#{state := #state{stream = StreamId, core = Core, from = From}} = Status) ->
    Status#{
        state := #{
            stream => StreamId,
            pending_caller => From =/= undefined,
            core => rabbitmq_stream_s3_remote_reader_core:pending(Core)
        }
    }.

%%----------------------------------------------------------------------------
%% Internal: async response handling
%%----------------------------------------------------------------------------

handle_async_response(
    Msg, RequestId, #state{requests = Requests0, cancelled = Cancelled0} = State0
) ->
    %% Find the range this request was issued for.
    #{RequestId := {FragOffset, RangeStart, AsyncState0}} = Requests0,
    case rabbitmq_stream_s3_api:handle_async(Msg, RequestId, AsyncState0) of
        ignore ->
            {noreply, State0};
        {continue, AsyncState} ->
            Requests = Requests0#{RequestId := {FragOffset, RangeStart, AsyncState}},
            {noreply, State0#state{requests = Requests}};
        {data, Data, Result} ->
            {Requests, DoneOrContinue} =
                case Result of
                    done ->
                        counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
                        {maps:remove(RequestId, Requests0), done};
                    AsyncState ->
                        {Requests0#{RequestId := {FragOffset, RangeStart, AsyncState}}, continue}
                end,
            step_and_execute(
                {data, FragOffset, RangeStart, Data, DoneOrContinue},
                State0#state{requests = Requests}
            );
        {done, ok} ->
            %% Empty 200 on range GET. Treat as transient error.
            counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
            Requests = maps:remove(RequestId, Requests0),
            step_and_execute(
                {request_error, FragOffset, RangeStart, timeout},
                State0#state{requests = Requests}
            );
        {done, {error, Reason}} ->
            counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
            Requests = maps:remove(RequestId, Requests0),
            step_and_execute(
                {request_error, FragOffset, RangeStart, Reason},
                State0#state{requests = Requests}
            );
        {done_cancel, {error, Reason}} ->
            counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
            Requests = maps:remove(RequestId, Requests0),
            Cancelled = Cancelled0#{RequestId => ok},
            step_and_execute(
                {request_error, FragOffset, RangeStart, Reason},
                State0#state{requests = Requests, cancelled = Cancelled}
            )
    end.

step_and_execute(Event, #state{core = Core0} = State0) ->
    {Core, Effects} = rabbitmq_stream_s3_remote_reader_core:step(Core0, Event),
    State = execute_effects(Effects, State0#state{core = Core}),
    maybe_stop(State).

%%----------------------------------------------------------------------------
%% Internal: effect execution loop
%%----------------------------------------------------------------------------

%% The connection pool hands out connections with a 100ms checkout timeout, so
%% executing a batch of start_request effects against a saturated pool would
%% block this process for 100ms per request while the caller's read deadline
%% burns. Once one checkout has come back `pool_busy` the rest of the batch is
%% reported to the core as busy without being attempted, capping the cost of a
%% saturated pool at one checkout timeout per batch.
execute_effects(Effects, State) ->
    {State1, _PoolBusy} = execute_effects(Effects, false, State),
    State1.

execute_effects([], PoolBusy, State) ->
    {State, PoolBusy};
execute_effects([Effect | Rest], PoolBusy0, State0) ->
    {State, PoolBusy} = execute_effect(Effect, PoolBusy0, State0),
    execute_effects(Rest, PoolBusy, State).

execute_effect(
    {reply, Result},
    PoolBusy,
    #state{stream = StreamId, from = From, deadline_timer = Timer} = State
) when
    From =/= undefined
->
    case Result of
        {ok, _} ->
            ok;
        {error, Reason} ->
            ?LOG_DEBUG(
                "remote_reader replying {error, ~p} for stream ~ts",
                [Reason, StreamId]
            );
        Other ->
            ?LOG_DEBUG(
                "remote_reader replying ~p for stream ~ts",
                [Other, StreamId]
            )
    end,
    cancel_deadline_timer(Timer),
    gen_server:reply(From, Result),
    {
        State#state{from = undefined, deadline_timer = undefined, deadline_token = undefined},
        PoolBusy
    };
execute_effect({reply, _Result}, PoolBusy, State) ->
    {State, PoolBusy};
execute_effect({observe, hit, Window}, PoolBusy, State) ->
    counters:add(counter(), ?C_BUFFER_HIT, 1),
    rabbitmq_stream_s3_histogram:observe(?MODULE, Window),
    {State, PoolBusy};
execute_effect({observe, miss, Window}, PoolBusy, State) ->
    counters:add(counter(), ?C_BUFFER_MISS, 1),
    rabbitmq_stream_s3_histogram:observe(?MODULE, Window),
    {State, PoolBusy};
execute_effect({observe, fragment_transition, Window}, PoolBusy, State) ->
    counters:add(counter(), ?C_FRAGMENT_TRANSITION, 1),
    rabbitmq_stream_s3_histogram:observe(?MODULE, Window),
    {State, PoolBusy};
execute_effect(
    {start_request, Key, {RangeStart, _} = Range, FragOffset},
    false,
    #state{
        stream = StreamId, cfg = #cfg{request_timeout_ms = Timeout}, requests = Requests0
    } = State
) ->
    case rabbitmq_stream_s3_api:get_range_async(Key, Range, #{timeout => Timeout}) of
        {ok, RequestId, AsyncState} ->
            counters:add(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
            counters:add(counter(), ?C_TOTAL_REQUESTS, 1),
            Requests = Requests0#{RequestId => {FragOffset, RangeStart, AsyncState}},
            {State#state{requests = Requests}, false};
        {error, pool_busy} ->
            ?LOG_DEBUG(
                "remote_reader start_request: pool_busy key=~ts frag=~b pos=~b",
                [Key, FragOffset, RangeStart]
            ),
            report_pool_busy(FragOffset, RangeStart, State);
        {error, Reason} ->
            %% The request never reached the pool: credentials could not be
            %% obtained or the region could not be resolved, so signing failed.
            %% Both are transient and node-local, and neither says anything
            %% about the object, so report it as a connection error and let the
            %% core put the range back with its usual backoff. Crashing here
            %% instead (this clause used to be absent) took the reader down on
            %% an IMDS blip and failed the consumer's read outright.
            ?LOG_WARNING(
                "remote_reader could not start a request for stream ~ts "
                "(key=~ts frag=~b pos=~b): ~0p",
                [StreamId, Key, FragOffset, RangeStart, Reason]
            ),
            report_request_error(FragOffset, RangeStart, connection_error, false, State)
    end;
execute_effect({start_request, _Key, {RangeStart, _}, FragOffset}, true, State) ->
    %% A checkout in this batch has already timed out; do not spend another
    %% timeout finding out the pool is still saturated.
    report_pool_busy(FragOffset, RangeStart, State);
execute_effect({cancel_request, Key}, PoolBusy, State) ->
    {cancel_request(Key, State), PoolBusy};
execute_effect({cancel_requests, all}, PoolBusy, State) ->
    {cancel_all_requests(State), PoolBusy};
execute_effect(
    {set_timer, Kind, DelayMs}, PoolBusy, #state{stream = StreamId, retry_timers = Timers} = State
) ->
    ?LOG_DEBUG(
        "remote_reader scheduling ~ts retry in ~bms for stream ~ts",
        [Kind, DelayMs, StreamId]
    ),
    Token = make_ref(),
    Timer = erlang:send_after(DelayMs, self(), {retry_requests, Kind, Token}),
    {State#state{retry_timers = Timers#{Kind => {Timer, Token}}}, PoolBusy};
execute_effect({cancel_timers, all}, PoolBusy, #state{retry_timers = Timers} = State) ->
    maps:foreach(fun(_Kind, {Timer, _Token}) -> cancel_retry_timer(Timer) end, Timers),
    {State#state{retry_timers = #{}}, PoolBusy};
execute_effect(
    {refresh_iterator, NotFoundOffset},
    PoolBusy,
    #state{stream = StreamId, core = Core0} = State0
) ->
    %% Cancel in-flight requests. The core will reinitialize at a new fragment.
    State1 = cancel_all_requests(State0),
    %% Synchronous local lookup. Rebuild iterator past the 404'd offset.
    Result = refresh_iterator(StreamId, NotFoundOffset),
    {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        Core0, {iterator_refreshed, Result}
    ),
    execute_effects(Effects, PoolBusy, State1#state{core = Core1});
execute_effect({fatal_error, Reason}, PoolBusy, #state{stream = StreamId} = State) ->
    counters:add(counter(), ?C_FATAL_ERRORS, 1),
    ?LOG_WARNING(
        "remote_reader for stream ~ts stopping on non-retryable S3 error: ~tp",
        [StreamId, Reason]
    ),
    {State, PoolBusy};
execute_effect(stop, PoolBusy, State) ->
    {State#state{stopping = true}, PoolBusy}.

report_pool_busy(FragOffset, RangeStart, State) ->
    report_request_error(FragOffset, RangeStart, pool_busy, true, State).

%% Tell the core that a range the shell was asked to start never got off the
%% ground. An error step can ask for requests to be started (a range that owed
%% nothing frees a depth slot for the next one), so the nested effects run with
%% the batch's busy flag carried through: a saturated pool passes `true`, which
%% turns any nested start into another report rather than another checkout.
%%
%% `PoolBusy` has to be what the shell actually saw. A start that failed before
%% the pool was reached (credentials, region) is not evidence the pool is busy,
%% and reporting a nested range as `pool_busy` on that path would grow
%% `pool_busy_delay` and park the range on a clock measuring something else.
%%
%% Recursion is bounded at one level either way. The nested range is in flight
%% by the time it is reported, so the core requeues it - `fail_req/4` takes the
%% `{ok, _}` branch, which only arms a timer - rather than freeing another slot.
%%
%% The flag the nested effects end on is what this returns, so it reaches the
%% rest of the outer batch. Only the nested run can raise it: a start that
%% failed before the pool was reached enters with `false`, and if one of the
%% requests it went on to start then found the pool saturated, discarding that
%% would leave the outer batch spending a 100ms checkout on each of its
%% remaining ranges - the cost the short-circuit exists to cap.
report_request_error(FragOffset, RangeStart, Reason, PoolBusy, #state{core = Core0} = State) ->
    {Core, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        Core0, {request_error, FragOffset, RangeStart, Reason}
    ),
    execute_effects(Effects, PoolBusy, State#state{core = Core}).

%% Rebuild the fragment iterator from the manifest cache, advancing past
%% the given offset (the fragment known to be 404).
refresh_iterator(StreamId, NotFoundOffset) ->
    %% A resolved row is never downgraded, so an unresolved (pending, or
    %% missing entirely) row mid-read means the row was released: the member
    %% is going down and this reader is about to die with it. end_of_manifest
    %% hands off to the local tier, whose own teardown handles the rest.
    Released = fun() ->
        ?LOG_DEBUG(
            "refresh_iterator for stream '~ts': not_found_offset=~b"
            " manifest unavailable, result=end_of_manifest",
            [StreamId, NotFoundOffset]
        ),
        end_of_manifest
    end,
    rabbitmq_stream_s3_manifest_replica:with_manifest(StreamId, #{
        resolved => fun(Manifest) -> refresh_iterator1(StreamId, NotFoundOffset, Manifest) end,
        pending => Released
    }).

refresh_iterator1(
    StreamId, NotFoundOffset, #manifest{first_offset = First, next_offset = Next} = Manifest
) ->
    GetGroupFun = rabbitmq_stream_s3_manifest:get_group_fun(StreamId),
    Iterator0 = rabbitmq_stream_s3_fragment_iterator:init(
        Manifest, NotFoundOffset, GetGroupFun
    ),
    %% Advance past the 404'd fragment.
    Iterator =
        case rabbitmq_stream_s3_fragment_iterator:next(Iterator0) of
            {ok, #fragment_ref{offset = O}, It} when O =< NotFoundOffset -> It;
            _ -> Iterator0
        end,
    %% Check if there's anything after.
    Result =
        case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
            {ok, #fragment_ref{offset = NextOff}, _} -> {iterator, NextOff};
            _ -> end_of_manifest
        end,
    ?LOG_DEBUG(
        "refresh_iterator for stream '~ts': not_found_offset=~b"
        " manifest_first=~b manifest_next=~b result=~p",
        [StreamId, NotFoundOffset, First, Next, Result]
    ),
    case Result of
        {iterator, _} -> Iterator;
        end_of_manifest -> end_of_manifest
    end.

maybe_stop(#state{stopping = true} = State) ->
    {stop, normal, State};
maybe_stop(State) ->
    {noreply, State}.

build_cfg(Opts) ->
    #cfg{
        request_timeout_ms = maps:get(request_timeout_ms, Opts, 15_000),
        pending_read_deadline_ms = maps:get(
            pending_read_deadline_ms, Opts, ?PENDING_READ_DEADLINE_MS
        )
    }.

cancel_all_requests(#state{requests = Requests, cancelled = Cancelled0} = State) ->
    maps:foreach(
        fun(ReqId, {_FragOffset, _RangeStart, AsyncState}) ->
            rabbitmq_stream_s3_api:cancel_async(ReqId, AsyncState)
        end,
        Requests
    ),
    counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, map_size(Requests)),
    NewCancelled = maps:merge(Cancelled0, #{ReqId => ok || ReqId := _ <- Requests}),
    State#state{requests = #{}, cancelled = NewCancelled}.

%% Abandon one range. The core drops requests individually when a fragment it
%% no longer reads still has ranges outstanding; frames for them may still
%% arrive, so the id is remembered as cancelled and matched away.
cancel_request({FragOffset, RangeStart}, #state{requests = Requests} = State) ->
    Matching = [
        ReqId
     || ReqId := {Frag, Start, _} <- Requests, Frag =:= FragOffset, Start =:= RangeStart
    ],
    lists:foldl(fun cancel_request_id/2, State, Matching).

cancel_request_id(ReqId, #state{requests = Requests, cancelled = Cancelled} = State) ->
    #{ReqId := {_, _, AsyncState}} = Requests,
    rabbitmq_stream_s3_api:cancel_async(ReqId, AsyncState),
    counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
    State#state{
        requests = maps:remove(ReqId, Requests),
        cancelled = Cancelled#{ReqId => ok}
    }.

cancel_retry_timer(Timer) ->
    _ = erlang:cancel_timer(Timer, [{async, true}, {info, false}]),
    ok.

cancel_deadline_timer(undefined) ->
    ok;
cancel_deadline_timer(Timer) ->
    _ = erlang:cancel_timer(Timer, [{async, true}, {info, false}]),
    ok.

counter() ->
    persistent_term:get(?COUNTER_KEY).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% A deadline_expired carrying a token that does not match the current read's
%% token (its read was already served, or it was superseded by a newer read)
%% must be ignored rather than reset a newer read's buffer and cancel its
%% in-flight requests. cancel_timer cannot remove an already-queued message.
stale_deadline_token_is_ignored_test() ->
    Stale = make_ref(),
    State = #state{deadline_token = make_ref()},
    ?assertEqual({noreply, State}, handle_info({deadline_expired, Stale}, State)),
    %% Also ignored when no read is pending (token undefined).
    State2 = #state{deadline_token = undefined},
    ?assertEqual({noreply, State2}, handle_info({deadline_expired, Stale}, State2)).

%% `cancel_timers` must leave no way for a cancelled retry to be acted on.
%% cancel_timer cannot remove a message that is already queued, so the timer map
%% is both the handle used to cancel and the record of which rounds may still
%% retry: a `retry_requests` whose token is not in it is stale.
cancel_timers_disarms_every_retry_timer_test() ->
    Token = make_ref(),
    Timer = erlang:send_after(60_000, self(), {retry_requests, fault, Token}),
    State = #state{retry_timers = #{fault => {Timer, Token}}},
    {State1, false} = execute_effect({cancel_timers, all}, false, State),
    ?assertEqual(#{}, State1#state.retry_timers),
    %% Even the message of a timer that had already fired is dropped. Acting on
    %% it would release the ranges of whatever backoff round is current, on a
    %% delay that round never earned.
    ?assertEqual({noreply, State1}, handle_info({retry_requests, fault, Token}, State1)).

%% Cancelling a kind's timer and arming a fresh one for it can happen in the
%% same effect batch: an iterator refresh emits `cancel_timers` followed by the
%% new fragment's `start_request` effects, and one of those failing arms the
%% kind again. A message from the cancelled timer then arrives with its kind
%% back in the map, so the kind alone cannot say whether it is stale - the token
%% can, and must, or the new round's ranges go back on the wire immediately.
stale_retry_token_is_ignored_while_the_kind_is_rearmed_test() ->
    Cancelled = make_ref(),
    State = #state{
        retry_timers = #{fault => {erlang:send_after(60_000, self(), ignored), make_ref()}}
    },
    ?assertEqual({noreply, State}, handle_info({retry_requests, fault, Cancelled}, State)),
    ?assert(is_map_key(fault, State#state.retry_timers)).

%% One kind's timer must not stand in for the other's: cancelling `fault` leaves
%% a live `pool_busy` timer that is still honoured.
stale_retry_is_ignored_per_kind_test() ->
    PoolBusy = {erlang:send_after(60_000, self(), ignored), make_ref()},
    State = #state{retry_timers = #{pool_busy => PoolBusy}},
    ?assertEqual({noreply, State}, handle_info({retry_requests, fault, make_ref()}, State)),
    ?assert(is_map_key(pool_busy, State#state.retry_timers)).

%% The histogram's boundaries have to follow the values the window can take, or
%% it reports resolution it cannot deliver. The window moves in whole requests
%% between `prefetch_request_size` and `prefetch_window_max`, so every step it
%% can make needs its own bucket - and none of them may land in +Inf, which is
%% for a window that has escaped its ceiling.
prefetch_window_buckets_resolve_every_window_step_test() ->
    RequestSize = rabbitmq_stream_s3_config:prefetch_request_size(),
    WindowMax = rabbitmq_stream_s3_config:prefetch_window_max(),
    Windows = lists:seq(RequestSize, WindowMax, RequestSize),
    Buckets = [bucket_of(W) || W <- Windows],
    ?assertEqual(length(Windows), length(lists:usort(Buckets))),
    ?assertNot(lists:member(infinity, Buckets)).

bucket_of(Value) ->
    hd([UB || UB <- ?PREFETCH_WINDOW_BUCKETS, UB =:= infinity orelse Value =< UB]).

%% Checking a connection out of a saturated pool costs a 100ms timeout, so once
%% one request in a batch has come back pool_busy the rest must be reported to
%% the core as busy without being attempted. Otherwise a reader pipelining N
%% requests blocks for N x 100ms with its caller's read deadline burning.
pool_busy_short_circuits_the_rest_of_a_batch_test() ->
    StreamId = <<"pool-busy-test">>,
    FragRef = #fragment_ref{offset = 0, uid = 1, size = 100_000},
    Manifest = #manifest{
        first_offset = 0,
        next_offset = 1,
        entries = ?ENTRY(0, 0, 0, ?MANIFEST_KIND_FRAGMENT, 100_000, 1)
    },
    Iterator = rabbitmq_stream_s3_fragment_iterator:init(
        Manifest, 0, fun(_) -> {error, not_found} end
    ),
    {Core0, _} = rabbitmq_stream_s3_remote_reader_core:init(
        StreamId, FragRef, ?SEGMENT_HEADER_B, Iterator, #{request_size => 1000, max_depth => 4}
    ),
    %% Reads that cannot be served grow the prefetch window until the reader
    %% pipelines; failing every range then leaves a batch for the retry to
    %% re-issue, which is when a saturated pool costs the most.
    Core1 = lists:foldl(
        fun(_, Acc) ->
            {Acc1, _} = rabbitmq_stream_s3_remote_reader_core:step(
                Acc, {read, ?SEGMENT_HEADER_B, 50_000, chunk_boundary}
            ),
            Acc1
        end,
        Core0,
        lists:seq(1, 3)
    ),
    Core2 = lists:foldl(
        fun({Fragment, Start, _End}, Acc) ->
            {Acc1, _} = rabbitmq_stream_s3_remote_reader_core:step(
                Acc, {request_error, Fragment, Start, slow_down}
            ),
            Acc1
        end,
        Core1,
        rabbitmq_stream_s3_remote_reader_core:outstanding_ranges(Core1)
    ),
    {Core, Effects} = rabbitmq_stream_s3_remote_reader_core:step(Core2, {retry, fault}),
    Starts = [E || {start_request, _, _, _} = E <- Effects],
    ?assert(length(Starts) > 1),
    State = #state{stream = StreamId, cfg = build_cfg(#{}), core = Core},
    %% Enter the batch as if a checkout had already timed out.
    {State1, true} = execute_effects(Starts, true, State),
    %% No request was issued, and the core has every range queued for retry.
    ?assertEqual(#{}, State1#state.requests),
    ?assertEqual(
        [{0, Start, End} || {start_request, _, {Start, End}, 0} <- Starts],
        rabbitmq_stream_s3_remote_reader_core:outstanding_ranges(State1#state.core)
    ).

%% A read cannot complete faster than the tier can deliver what it asked for, so
%% a fixed deadline caps the chunk size the remote tier can serve: past the cap
%% every attempt expires, and the log reader's retries do not rescue it - the
%% bytes a retry waits for are by definition the ones that did not arrive in
%% time, so each attempt meets the same wall.
read_deadline_scales_with_the_bytes_asked_for_test() ->
    Base = ?PENDING_READ_DEADLINE_MS,
    %% A chunk-header over-read is answered on the base budget.
    ?assertEqual(Base, read_deadline_ms(?SEGMENT_HEADER_B, 0, Base)),
    ?assert(read_deadline_ms(?SEGMENT_HEADER_B, 303, Base) < Base + 1_000),
    %% A 512 MiB chunk gets a budget it can actually be delivered in. At the
    %% pessimistic 10 MB/s floor that is ~51s of transfer on top of the base.
    Big = read_deadline_ms(?SEGMENT_HEADER_B, 512 * 1024 * 1024, Base),
    ?assert(Big > Base + 50_000),
    %% The offset counts too, as an upper bound on what the read needs fetched
    %% rather than a tight one: a consumer that attaches mid-fragment starts with
    %% nothing buffered, and an over-estimate costs headroom and nothing else.
    ?assert(
        read_deadline_ms(?SEGMENT_HEADER_B + 100_000_000, 1_000, Base) >
            read_deadline_ms(?SEGMENT_HEADER_B, 1_000, Base)
    ),
    %% Monotonic, so a larger read never gets a smaller budget.
    ?assert(read_deadline_ms(?SEGMENT_HEADER_B, 4_000_000, Base) >= Base).

%% The caller's call timeout must stay above the internal deadline at every read
%% size, or the caller times out first and leaves `from` set for an overlapping
%% read - which needs a buffer reset that is not safe to do underneath one.
call_timeout_exceeds_the_deadline_at_every_size_test() ->
    Base = ?PENDING_READ_DEADLINE_MS,
    lists:foreach(
        fun({Offset, Bytes}) ->
            Deadline = read_deadline_ms(Offset, Bytes, Base),
            CallTimeout = Deadline + ?READ_TIMEOUT_MARGIN_MS,
            ?assert(CallTimeout > Deadline)
        end,
        [
            {?SEGMENT_HEADER_B, 1},
            {?SEGMENT_HEADER_B, 303},
            {?SEGMENT_HEADER_B, 4 * 1024 * 1024},
            {?SEGMENT_HEADER_B, 512 * 1024 * 1024},
            {?SEGMENT_HEADER_B + 64 * 1024 * 1024, 512 * 1024 * 1024},
            {0, 0}
        ]
    ).

-endif.
