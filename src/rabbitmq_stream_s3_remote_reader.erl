%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_remote_reader).
-moduledoc """
A gen_server process which reads stream data from the remote tier.

This process bridges async S3 responses and the synchronous log reader.
All decision logic (buffering, retry, fragment transitions, AIMD) lives in
`rabbitmq_stream_s3_remote_reader_core`. This module translates external events (gun
messages, timer fires, gen_server calls) into core events, feeds them to the
core, and executes the resulting effects.

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
%% Upper bucket boundaries span the AIMD read-size range, whose ceiling is
%% read_size_max (64 MiB). The 16/32/64 MiB buckets resolve the high end where
%% the prefetch window spends most of its time under sustained reads; without
%% them every read above 8 MiB collapses into the +Inf bucket. Reads never
%% exceed the 64 MiB cap, so in normal operation +Inf stays empty.
-define(READ_SIZE_BUCKETS, [
    48,
    128,
    512,
    2_048,
    8_192,
    32_768,
    131_072,
    524_288,
    2_097_152,
    8_388_608,
    16_777_216,
    33_554_432,
    67_108_864,
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
    %% Maps fragment_offset -> {async_req, async_state}
    requests = #{} :: #{
        osiris:offset() => {
            rabbitmq_stream_s3_api:async_req(), rabbitmq_stream_s3_api:async_state()
        }
    },
    %% Cancelled request refs (gun may still deliver frames).
    cancelled = #{} :: #{rabbitmq_stream_s3_api:async_req() => ok},
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
    read_size_prometheus_format/0
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
    rabbitmq_stream_s3_histogram:new(?MODULE, ?READ_SIZE_BUCKETS),
    ok.

-spec read_size_prometheus_format() -> map().
read_size_prometheus_format() ->
    {Buckets, Count, Sum} = rabbitmq_stream_s3_histogram:prometheus_format(
        ?MODULE, fun(X) -> X end, ?READ_SIZE_BUCKETS
    ),
    #{
        read_size_bytes => #{
            type => histogram,
            help => <<"Distribution of remote tier read sizes in bytes">>,
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

%% The gen_server:call timeout must exceed PENDING_READ_DEADLINE_MS so the
%% internal deadline always fires first and replies {error, timeout} to the
%% caller. This avoids overlapping reads (caller times out, new read arrives
%% while from is still set) which require unsafe buffer resets.
-define(PENDING_READ_DEADLINE_MS, 40_000).
-define(SEND_FILE_READ_TIMEOUT_MS, 45_000).

read(Server, Offset, Bytes, Hint) ->
    read(Server, Offset, Bytes, Hint, ?SEND_FILE_READ_TIMEOUT_MS).

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
    #state{cfg = #cfg{pending_read_deadline_ms = Deadline}, core = Core0} = State0
) ->
    ?assertEqual(undefined, State0#state.from),
    {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        Core0, {read, Offset, Bytes, Hint}
    ),
    Token = make_ref(),
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
handle_info(retry_requests, #state{stream = StreamId, core = Core0} = State0) ->
    ?LOG_DEBUG(
        "remote_reader retry_requests firing for stream ~ts",
        [StreamId]
    ),
    {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(Core0, retry),
    State = execute_effects(Effects, State0#state{core = Core1}),
    maybe_stop(State);
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
    AsyncStates = #{Req => AsyncState || _ := {Req, AsyncState} <- Requests0},
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
    Msg, RequestId, #state{requests = Requests0, cancelled = Cancelled0, core = Core0} = State0
) ->
    %% Find which fragment this request belongs to.
    {FragOffset, {_, AsyncState0}} = find_request_by_id(RequestId, Requests0),
    case rabbitmq_stream_s3_api:handle_async(Msg, RequestId, AsyncState0) of
        ignore ->
            {noreply, State0};
        {continue, AsyncState} ->
            Requests = Requests0#{FragOffset := {RequestId, AsyncState}},
            {noreply, State0#state{requests = Requests}};
        {data, Data, Result} ->
            {Requests, DoneOrContinue} =
                case Result of
                    done ->
                        counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
                        {maps:remove(FragOffset, Requests0), done};
                    AsyncState ->
                        {Requests0#{FragOffset := {RequestId, AsyncState}}, continue}
                end,
            {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
                Core0, {data, RequestId, FragOffset, Data, DoneOrContinue}
            ),
            State = execute_effects(Effects, State0#state{core = Core1, requests = Requests}),
            maybe_stop(State);
        {done, ok} ->
            %% Empty 200 on range GET. Treat as transient error.
            counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
            Requests = maps:remove(FragOffset, Requests0),
            {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
                Core0, {request_error, RequestId, FragOffset, timeout}
            ),
            State = execute_effects(Effects, State0#state{core = Core1, requests = Requests}),
            maybe_stop(State);
        {done, {error, Reason}} ->
            counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
            Requests = maps:remove(FragOffset, Requests0),
            {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
                Core0, {request_error, RequestId, FragOffset, Reason}
            ),
            State = execute_effects(Effects, State0#state{core = Core1, requests = Requests}),
            maybe_stop(State);
        {done_cancel, {error, Reason}} ->
            counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
            Requests = maps:remove(FragOffset, Requests0),
            Cancelled = Cancelled0#{RequestId => ok},
            {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
                Core0, {request_error, RequestId, FragOffset, Reason}
            ),
            State = execute_effects(Effects, State0#state{
                core = Core1, requests = Requests, cancelled = Cancelled
            }),
            maybe_stop(State)
    end.

find_request_by_id(RequestId, Requests) ->
    maps:fold(
        fun
            (FragOff, {ReqId, _} = Val, undefined) when ReqId =:= RequestId ->
                {FragOff, Val};
            (_, _, Acc) ->
                Acc
        end,
        undefined,
        Requests
    ).

%%----------------------------------------------------------------------------
%% Internal: effect execution loop
%%----------------------------------------------------------------------------

execute_effects([], State) ->
    State;
execute_effects([Effect | Rest], State0) ->
    State = execute_effect(Effect, State0),
    execute_effects(Rest, State).

execute_effect(
    {reply, Result}, #state{stream = StreamId, from = From, deadline_timer = Timer} = State
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
    State#state{from = undefined, deadline_timer = undefined, deadline_token = undefined};
execute_effect({reply, _Result}, State) ->
    State;
execute_effect({observe, hit, ReadSize}, State) ->
    counters:add(counter(), ?C_BUFFER_HIT, 1),
    rabbitmq_stream_s3_histogram:observe(?MODULE, ReadSize),
    State;
execute_effect({observe, miss, ReadSize}, State) ->
    counters:add(counter(), ?C_BUFFER_MISS, 1),
    rabbitmq_stream_s3_histogram:observe(?MODULE, ReadSize),
    State;
execute_effect({observe, fragment_transition, ReadSize}, State) ->
    counters:add(counter(), ?C_FRAGMENT_TRANSITION, 1),
    rabbitmq_stream_s3_histogram:observe(?MODULE, ReadSize),
    State;
execute_effect(
    {start_request, Key, Range, FragOffset},
    #state{cfg = #cfg{request_timeout_ms = Timeout}, core = Core0, requests = Requests0} = State
) ->
    case rabbitmq_stream_s3_api:get_range_async(Key, Range, #{timeout => Timeout}) of
        {ok, RequestId, AsyncState} ->
            counters:add(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
            counters:add(counter(), ?C_TOTAL_REQUESTS, 1),
            Requests = Requests0#{FragOffset => {RequestId, AsyncState}},
            State#state{requests = Requests};
        {error, pool_busy} ->
            ?LOG_DEBUG(
                "remote_reader start_request: pool_busy key=~ts frag=~b",
                [Key, FragOffset]
            ),
            {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
                Core0, {request_error, make_ref(), FragOffset, pool_busy}
            ),
            execute_effects(Effects, State#state{core = Core1})
    end;
execute_effect({set_timer, DelayMs}, #state{stream = StreamId} = State) ->
    ?LOG_DEBUG(
        "remote_reader scheduling retry in ~bms for stream ~ts",
        [DelayMs, StreamId]
    ),
    erlang:send_after(DelayMs, self(), retry_requests),
    State;
execute_effect(
    {refresh_iterator, NotFoundOffset},
    #state{stream = StreamId, core = Core0} = State0
) ->
    %% Cancel in-flight requests. The core will reinitialize at a new fragment.
    State1 = cancel_all_requests(State0),
    %% Synchronous local lookup. Rebuild iterator past the 404'd offset.
    Result = refresh_iterator(StreamId, NotFoundOffset),
    {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        Core0, {iterator_refreshed, Result}
    ),
    execute_effects(Effects, State1#state{core = Core1});
execute_effect({fatal_error, Reason}, #state{stream = StreamId} = State) ->
    counters:add(counter(), ?C_FATAL_ERRORS, 1),
    ?LOG_WARNING(
        "remote_reader for stream ~ts stopping on non-retryable S3 error: ~tp",
        [StreamId, Reason]
    ),
    State;
execute_effect(stop, State) ->
    State#state{stopping = true}.

%% Rebuild the fragment iterator from the manifest cache, advancing past
%% the given offset (the fragment known to be 404).
refresh_iterator(StreamId, NotFoundOffset) ->
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{first_offset = First, next_offset = Next} = Manifest ->
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
            end;
        Missing when Missing =:= undefined; Missing =:= pending ->
            %% A resolved row is never downgraded, so mid-read this means the
            %% row was released: the member is going down and this reader is
            %% about to die with it. end_of_manifest hands off to the local
            %% tier, whose own teardown handles the rest.
            ?LOG_DEBUG(
                "refresh_iterator for stream '~ts': not_found_offset=~b"
                " manifest=~p result=end_of_manifest",
                [StreamId, NotFoundOffset, Missing]
            ),
            end_of_manifest
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
        fun(_FragOff, {ReqId, AsyncState}) ->
            rabbitmq_stream_s3_api:cancel_async(ReqId, AsyncState)
        end,
        Requests
    ),
    counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, map_size(Requests)),
    NewCancelled = maps:merge(Cancelled0, #{ReqId => ok || _ := {ReqId, _} <- Requests}),
    State#state{requests = #{}, cancelled = NewCancelled}.

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

-endif.
