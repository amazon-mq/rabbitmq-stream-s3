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
-include_lib("rabbit_common/include/rabbit.hrl").
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
-define(C_AWAIT_DURATION_MS, 5).
-define(C_AWAIT, 6).
-define(C_READ_DURATION_MS, 7).
-define(C_READ, 8).
-define(C_TOTAL_REQUESTS, 9).
-define(COUNTER_KEY, {rabbitmq_stream_s3_remote_reader, counter}).
-define(COUNTERS, [
    {buffer_hit, ?C_BUFFER_HIT, counter, "Number of reads served from the buffer"},
    {buffer_miss, ?C_BUFFER_MISS, counter, "Number of reads that had to await async data"},
    {fragment_transition, ?C_FRAGMENT_TRANSITION, counter, "Number of fragment transitions"},
    {requests_in_flight, ?C_REQUESTS_IN_FLIGHT, gauge,
        "Current number of in-flight async requests"},
    {await_duration_ms, ?C_AWAIT_DURATION_MS, counter,
        "Total milliseconds spent awaiting async data"},
    {await, ?C_AWAIT, counter, "Number of awaits"},
    {read_duration_ms, ?C_READ_DURATION_MS, counter, "Total milliseconds spent in read calls"},
    {read, ?C_READ, counter, "Number of read/4,5 calls"},
    {remote_reader_total_requests, ?C_TOTAL_REQUESTS, counter, "Number of S3 requests initiated"}
]).
-define(READ_SIZE_BUCKETS, [
    48, 128, 512, 2_048, 8_192, 32_768, 131_072, 524_288, 2_097_152, 8_388_608, infinity
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
    since :: integer() | undefined,
    %% Timer that fires deadline_expired if the pending read is not served in time.
    deadline_timer :: reference() | undefined,
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
    start_link/1,
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

start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

read(Server, Offset, Bytes, Hint) ->
    read(Server, Offset, Bytes, Hint, ?GEN_SERVER_CALL_TIMEOUT).

read(Server, Offset, Bytes, Hint, Timeout) ->
    T0 = erlang:monotonic_time(),
    Result = gen_server:call(Server, #read{offset = Offset, bytes = Bytes, hint = Hint}, Timeout),
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
    Timer = erlang:send_after(Deadline, self(), deadline_expired),
    State1 = State0#state{
        core = Core1,
        from = From,
        since = erlang:monotonic_time(),
        deadline_timer = Timer
    },
    State = execute_effects(Effects, State1),
    maybe_stop(State);
handle_call(Request, From, State) ->
    {stop, {unknown_call, From, Request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, _Pid, _Reason}, #state{reader_ref = MRef} = State) ->
    {stop, normal, State};
handle_info(retry_requests, #state{core = Core0} = State0) ->
    {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(Core0, retry),
    State = execute_effects(Effects, State0#state{core = Core1}),
    maybe_stop(State);
handle_info(deadline_expired, #state{from = undefined} = State) ->
    %% Timer fired after the read was already served. Ignore.
    {noreply, State};
handle_info(
    deadline_expired, #state{core = Core0, requests = Requests, cancelled = Cancelled0} = State0
) ->
    %% Cancel in-flight S3 requests so they don't feed stale data into the
    %% reset buffer after the core clears it.
    maps:foreach(
        fun(_FragOff, {ReqId, AsyncState}) ->
            rabbitmq_stream_s3_api:cancel_async(ReqId, AsyncState)
        end,
        Requests
    ),
    counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, map_size(Requests)),
    NewCancelled = maps:merge(Cancelled0, #{ReqId => ok || _ := {ReqId, _} <- Requests}),
    {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(Core0, deadline_expired),
    State = execute_effects(Effects, State0#state{
        core = Core1, requests = #{}, cancelled = NewCancelled, deadline_timer = undefined
    }),
    maybe_stop(State);
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

terminate(_Reason, #state{requests = Requests}) ->
    maps:foreach(
        fun(_FragOff, {ReqId, AsyncState}) ->
            rabbitmq_stream_s3_api:cancel_async(ReqId, AsyncState)
        end,
        Requests
    ),
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
    {reply, Result}, #state{from = From, since = Since, deadline_timer = Timer} = State
) when
    From =/= undefined
->
    Duration = rabbitmq_stream_s3_util:elapsed_ms(Since),
    counters:add(counter(), ?C_AWAIT_DURATION_MS, Duration),
    counters:add(counter(), ?C_AWAIT, 1),
    cancel_deadline_timer(Timer),
    gen_server:reply(From, Result),
    State#state{from = undefined, since = undefined, deadline_timer = undefined};
execute_effect({reply, _Result}, State) ->
    %% No pending caller (e.g. reply generated during init before any call).
    State;
execute_effect(
    {start_request, Key, Range, FragOffset},
    #state{cfg = #cfg{request_timeout_ms = Timeout}, requests = Requests0} = State
) ->
    case rabbitmq_stream_s3_api:get_range_async(Key, Range, #{timeout => Timeout}) of
        {ok, RequestId, AsyncState} ->
            counters:add(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
            counters:add(counter(), ?C_TOTAL_REQUESTS, 1),
            Requests = Requests0#{FragOffset => {RequestId, AsyncState}},
            State#state{requests = Requests};
        {error, pool_busy} ->
            %% Can't start now. The core will retry on the next event.
            State
    end;
execute_effect({set_timer, DelayMs}, State) ->
    erlang:send_after(DelayMs, self(), retry_requests),
    State;
execute_effect({lookup_manifest_range}, #state{stream = StreamId, core = Core0} = State) ->
    %% Synchronous local lookup — feed result back to core immediately.
    Range = rabbitmq_stream_s3_manifest_replica:get_range(StreamId),
    {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(Core0, {manifest_range, Range}),
    execute_effects(Effects, State#state{core = Core1});
execute_effect({refresh_iterator}, #state{stream = StreamId, core = Core0} = State) ->
    %% Synchronous local lookup — feed result back to core immediately.
    Result = refresh_iterator(StreamId, Core0),
    {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        Core0, {iterator_refreshed, Result}
    ),
    execute_effects(Effects, State#state{core = Core1});
execute_effect(
    {jump_to_oldest, FirstOffset},
    #state{stream = StreamId, core = Core0, requests = Requests, cancelled = Cancelled0} = State0
) ->
    %% Cancel in-flight requests, resolve the fragment from the manifest,
    %% and feed back to the core.
    maps:foreach(
        fun(_FragOff, {ReqId, AsyncState}) ->
            rabbitmq_stream_s3_api:cancel_async(ReqId, AsyncState)
        end,
        Requests
    ),
    counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, map_size(Requests)),
    NewCancelled = maps:merge(Cancelled0, #{ReqId => ok || _ := {ReqId, _} <- Requests}),
    State1 = State0#state{requests = #{}, cancelled = NewCancelled},
    case resolve_fragment_at(StreamId, FirstOffset) of
        {ok, FragRef, Iterator} ->
            {Core1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
                Core0, {jumped, FragRef, Iterator}
            ),
            execute_effects(Effects, State1#state{core = Core1});
        not_found ->
            %% Manifest doesn't have this offset. End of stream.
            case State1#state.from of
                undefined ->
                    State1;
                From ->
                    gen_server:reply(From, end_of_stream),
                    State1#state{from = undefined, since = undefined}
            end
    end;
execute_effect(stop, State) ->
    State#state{stopping = true}.

%% Rebuild the fragment iterator from the manifest cache.
refresh_iterator(StreamId, Core) ->
    FragOffset = rabbitmq_stream_s3_remote_reader_core:current_fragment_offset(Core),
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{} = Manifest ->
            GetGroupFun = rabbitmq_stream_s3_manifest:get_group_fun(StreamId),
            Iterator0 = rabbitmq_stream_s3_fragment_iterator:init(
                Manifest, FragOffset, GetGroupFun
            ),
            %% Advance past the current fragment.
            Iterator =
                case rabbitmq_stream_s3_fragment_iterator:next(Iterator0) of
                    {ok, _, It} -> It;
                    _ -> Iterator0
                end,
            %% Check if there's anything after.
            case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
                {ok, _, _} -> Iterator;
                _ -> end_of_manifest
            end;
        _ ->
            end_of_manifest
    end.

%% Look up a fragment ref at the given offset from the manifest cache.
resolve_fragment_at(StreamId, Offset) ->
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{} = Manifest ->
            GetGroupFun = rabbitmq_stream_s3_manifest:get_group_fun(StreamId),
            Iterator = rabbitmq_stream_s3_fragment_iterator:init(
                Manifest, Offset, GetGroupFun
            ),
            case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
                {ok, FragRef, _} -> {ok, FragRef, Iterator};
                _ -> not_found
            end;
        _ ->
            not_found
    end.

maybe_stop(#state{stopping = true} = State) ->
    {stop, normal, State};
maybe_stop(State) ->
    {noreply, State}.

build_cfg(Opts) ->
    #cfg{
        request_timeout_ms = maps:get(request_timeout_ms, Opts, 30_000),
        pending_read_deadline_ms = maps:get(pending_read_deadline_ms, Opts, 50_000)
    }.

cancel_deadline_timer(undefined) ->
    ok;
cancel_deadline_timer(Timer) ->
    _ = erlang:cancel_timer(Timer, [{async, true}, {info, false}]),
    ok.

counter() ->
    persistent_term:get(?COUNTER_KEY).
