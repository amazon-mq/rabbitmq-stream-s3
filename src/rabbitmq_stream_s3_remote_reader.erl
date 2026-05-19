%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_remote_reader).
-moduledoc """
A gen_server process which reads stream data from the remote tier.

Reads from the remote tier suffer from high latency, so this server pre-fetches
stream data aggressively. The pre-fetch amount is adjusted dynamically based on
the log reader's demand and S3's supply following a basic additive increase /
multiplicative decrease ([AIMD]) algorithm.

[AIMD]: https://en.wikipedia.org/wiki/Additive_increase/multiplicative_decrease
""".

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include("include/rabbitmq_stream_s3.hrl").
-include("include/logging.hrl").

-behaviour(gen_server).

%% 1 MiB
-define(MIN_READ_SIZE, 1048576).
%% 64 MiB
-define(MAX_READ_SIZE, 67108864).
%% 4 MiB
-define(INITIAL_READ_SIZE, 4194304).
%% Number of consecutive buffer hits before we increase read_size.
-define(HITS_TO_GROW, 8).
%% Additive increase step size (1 MiB in bytes).
-define(GROW_STEP, 1048576).
-define(MIN_RETRY_DELAY_MS, 1_000).
-define(MAX_RETRY_DELAY_MS, 30_000).
%% Cancel and retry an in-flight S3 request if no response arrives within
%% this window. Must be less than ?GEN_SERVER_CALL_TIMEOUT (60s) to allow
%% recovery before the caller times out.
-define(REQUEST_TIMEOUT_MS, 30_000).

-define(C_BUFFER_HIT, 1).
-define(C_BUFFER_MISS, 2).
-define(C_FRAGMENT_TRANSITION, 3).
-define(C_REQUESTS_IN_FLIGHT, 4).
-define(C_AWAIT_DURATION_MS, 5).
-define(C_AWAIT, 6).
-define(C_READ_DURATION_MS, 7).
-define(C_READ, 8).
-define(C_TOTAL_REQUESTS, 9).
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

%% API
-record(read, {
    offset :: byte_offset(),
    bytes :: pos_integer(),
    %% NOTE: unused since we reliably track fragment data size now.
    hint :: hint()
}).

%% A read request from read/4 or read/5 which could not be served immediately
%% from a pre-fetched buffer.
-record(pending_read, {
    read :: #read{},
    from :: gen_server:from(),
    %% erlang:monotonic_time/0 since the read started
    since :: integer()
}).

%% A request to the remote tier which is running in the background.
-record(request, {
    fragment :: osiris:offset(),
    %% In the future we could make parallel requests within the same fragment,
    %% so we may need to track the byte range. This is unused currently.
    range :: {byte_offset(), byte_offset()},
    state :: rabbitmq_stream_s3_api:async_state()
}).

-record(?MODULE, {
    stream :: stream_id(),
    read_size :: pos_integer(),
    read_size_min :: pos_integer(),
    read_size_max :: pos_integer(),
    %% Consecutive buffer hits since the last miss. Used for AIMD.
    hits_since_last_miss = 0 :: non_neg_integer(),
    %% Current retry delay in milliseconds. Doubles on each retry, resets on
    %% successful data arrival.
    retry_delay = ?MIN_RETRY_DELAY_MS :: pos_integer(),
    reader_ref :: reference(),

    %% Current fragment state.
    fragment_ref :: #fragment_ref{},
    key :: rabbitmq_stream_s3:key(),
    buffer = <<>> :: binary(),
    start_pos :: byte_offset(),
    current_pos :: byte_offset(),
    end_pos :: byte_offset(),

    %% Next fragment state (pre-fetched).
    next :: {#fragment_ref{}, binary()} | undefined | not_found,

    %% Fragment iterator for forward navigation.
    iterator :: rabbitmq_stream_s3_fragment_iterator:iterator(),

    %% All in-flight requests, keyed by async_req().
    requests = #{} :: #{rabbitmq_stream_s3_api:async_req() => #request{}},

    %% Refs of requests cancelled via cancel_requests/1. Gun may still deliver
    %% already-buffered frames for these refs after the cancel; tracking them
    %% here lets match_async/3 drop those messages silently without flooding
    %% the mailbox through the `error` branch.
    cancelled_requests = #{} :: #{rabbitmq_stream_s3_api:async_req() => ok},

    %% Pending read from the log reader process.
    pending_read :: #pending_read{} | undefined,
    current_not_found = false :: boolean()
}).

-type config() :: #{
    reader := pid(),
    stream := stream_id(),
    location := #remote_location{}
}.

-type hint() :: chunk_boundary | within_chunk.

-export_type([config/0, hint/0]).

-define(COUNTER_KEY, {?MODULE, counter}).
-define(READ_SIZE_BUCKETS, [
    48, 128, 512, 2_048, 8_192, 32_768, 131_072, 524_288, 2_097_152, 8_388_608, infinity
]).

-export([init_counters/0, read_size_prometheus_format/0]).

%% API
-export([
    read/4,
    read/5
]).

%% gen_server
-export([
    start_link/1,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    format_status/1,
    code_change/3
]).

%%----------------------------------------------------------------------------

-spec init_counters() -> ok.
init_counters() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?COUNTER_KEY, Cnt),
    rabbitmq_stream_s3_histogram:new(?MODULE, ?READ_SIZE_BUCKETS),
    ok.

-spec read(pid(), byte_offset(), pos_integer(), hint()) ->
    {ok, binary()}
    | {next_fragment, osiris:offset()}
    | {become_local, osiris:offset()}
    | end_of_stream.
read(Server, Offset, Bytes, Hint) ->
    read(Server, Offset, Bytes, Hint, ?GEN_SERVER_CALL_TIMEOUT).

-spec read(pid(), byte_offset(), pos_integer(), hint(), timeout()) ->
    {ok, binary()}
    | {next_fragment, osiris:offset()}
    | {become_local, osiris:offset()}
    | end_of_stream.
read(Server, Offset, Bytes, Hint, Timeout) ->
    T0 = erlang:monotonic_time(),
    Result = gen_server:call(Server, #read{offset = Offset, bytes = Bytes, hint = Hint}, Timeout),
    Duration = erlang:convert_time_unit(erlang:monotonic_time() - T0, native, millisecond),
    counters:add(counter(), ?C_READ_DURATION_MS, Duration),
    counters:add(counter(), ?C_READ, 1),
    Result.

%%----------------------------------------------------------------------------

start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

-spec init(config()) -> {ok, #?MODULE{}}.
init(#{
    reader := Reader,
    stream := StreamId,
    location := #remote_location{
        position = Pos,
        fragment_ref = #fragment_ref{offset = Fragment, uid = Uid} = FragRef,
        iterator = Iterator
    }
}) ->
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
    Key = rabbitmq_stream_s3:fragment_key(StreamId, Fragment, Uid),
    State0 = #?MODULE{
        stream = StreamId,
        read_size = ?INITIAL_READ_SIZE,
        read_size_min = ?MIN_READ_SIZE,
        read_size_max = ?MAX_READ_SIZE,
        reader_ref = erlang:monitor(process, Reader),
        start_pos = Pos,
        current_pos = Pos,
        end_pos = Pos,
        fragment_ref = FragRef,
        key = Key,
        iterator = Iterator
    },
    State = maybe_start_request(State0),
    {ok, State}.

handle_call(#read{} = Read, From, State0) ->
    %% This server is only used by the reader, so having two calls in-flight is
    %% impossible.
    ?assertEqual(undefined, State0#?MODULE.pending_read),
    case maybe_reply(Read, State0) of
        {noreply, State1} ->
            State = State1#?MODULE{
                pending_read = #pending_read{
                    read = Read,
                    from = From,
                    since = erlang:monotonic_time()
                }
            },
            {noreply, State};
        {retry, State1} ->
            erlang:send_after(?MIN_RETRY_DELAY_MS, self(), retry_requests),
            State = State1#?MODULE{
                pending_read = #pending_read{
                    read = Read,
                    from = From,
                    since = erlang:monotonic_time()
                }
            },
            {noreply, State};
        Reply ->
            Reply
    end;
handle_call(Request, From, State) ->
    {stop, {unknown_call, From, Request}, State}.

handle_cast(Message, State) ->
    ?LOG_DEBUG(?MODULE_STRING " received unexpected cast: ~W", [Message, 10]),
    {noreply, State}.

handle_info({'DOWN', MRef, process, _Pid, _Reason}, #?MODULE{reader_ref = MRef} = State) ->
    {stop, normal, State};
handle_info(retry_requests, State0) ->
    State = maybe_start_request(State0),
    maybe_reply_pending(State);
handle_info(
    Msg,
    #?MODULE{
        requests = Requests0,
        cancelled_requests = CancelledRequests0,
        retry_delay = RetryDelay0
    } = State0
) ->
    AsyncStates = #{Req => R#request.state || Req := R <- Requests0},
    case rabbitmq_stream_s3_api:match_async(Msg, AsyncStates, CancelledRequests0) of
        {ok, RequestId} ->
            #request{state = ReqState0} = Req0 = maps:get(RequestId, Requests0),
            case rabbitmq_stream_s3_api:handle_async(Msg, RequestId, ReqState0) of
                ignore ->
                    {noreply, State0};
                {continue, ReqState} ->
                    Requests = Requests0#{RequestId := Req0#request{state = ReqState}},
                    {noreply, State0#?MODULE{requests = Requests}};
                {data, Data, Result} ->
                    Requests =
                        case Result of
                            done ->
                                counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
                                maps:remove(RequestId, Requests0);
                            ReqState ->
                                Requests0#{RequestId := Req0#request{state = ReqState}}
                        end,
                    State1 = State0#?MODULE{
                        requests = Requests,
                        retry_delay = ?MIN_RETRY_DELAY_MS
                    },
                    State2 = add_data(Data, Req0, State1),
                    State3 = maybe_start_request(State2),
                    maybe_reply_pending(State3);
                {done, ok} ->
                    %% A 200 with no body on a range GET is abnormal.
                    %% Retry the request.
                    counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
                    ?LOG_WARNING("Received empty response for ~ts", [Req0#request.fragment]),
                    State1 = State0#?MODULE{requests = maps:remove(RequestId, Requests0)},
                    State2 = maybe_start_request(State1),
                    maybe_reply_pending(State2);
                {done, {error, Reason}} ->
                    counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
                    Requests = maps:remove(RequestId, Requests0),
                    State1 = State0#?MODULE{requests = Requests},
                    handle_request_error(Reason, Req0, RetryDelay0, State1);
                {done_cancel, {error, Reason}} ->
                    counters:sub(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
                    Requests = maps:remove(RequestId, Requests0),
                    CancelledRequests = CancelledRequests0#{RequestId => ok},
                    State1 = State0#?MODULE{
                        requests = Requests,
                        cancelled_requests = CancelledRequests
                    },
                    handle_request_error(Reason, Req0, RetryDelay0, State1)
            end;
        {cancelled, Ref, Final} ->
            %% Stale message for a request that called gun:cancel (e.g.
            %% after a timeout). Drop silently and remove the ref once
            %% its terminal frame arrives.
            CancelledRequests =
                case Final of
                    final -> maps:remove(Ref, CancelledRequests0);
                    more -> CancelledRequests0
                end,
            {noreply, State0#?MODULE{cancelled_requests = CancelledRequests}};
        error ->
            ?LOG_DEBUG(?MODULE_STRING " received unexpected message: ~W", [Msg, 10]),
            {noreply, State0}
    end;
handle_info(Msg, State) ->
    ?LOG_DEBUG(?MODULE_STRING " received unexpected message: ~W", [Msg, 10]),
    {noreply, State}.

handle_request_error(Reason, Req0, RetryDelay0, State) ->
    case {Reason, Req0#request.fragment =:= (State#?MODULE.fragment_ref)#fragment_ref.offset} of
        {not_found, true} ->
            maybe_reply_pending(State#?MODULE{
                current_not_found = true,
                retry_delay = ?MIN_RETRY_DELAY_MS
            });
        {not_found, false} ->
            maybe_reply_pending(State#?MODULE{
                next = not_found,
                retry_delay = ?MIN_RETRY_DELAY_MS
            });
        {Transient, _} when
            Transient =:= slow_down;
            Transient =:= internal_error
        ->
            erlang:send_after(RetryDelay0, self(), retry_requests),
            RetryDelay = min(RetryDelay0 * 2, ?MAX_RETRY_DELAY_MS),
            State1 = State#?MODULE{retry_delay = RetryDelay},
            maybe_reply_pending(State1);
        {Transient, _} when
            Transient =:= stream_error;
            Transient =:= connection_error;
            Transient =:= timeout
        ->
            maybe_reply_pending(State);
        _ ->
            {stop, {shutdown, Reason}, State}
    end.

terminate(_Reason, #?MODULE{requests = Requests}) ->
    cancel_requests(Requests),
    ok.

format_status(#{state := #?MODULE{} = State} = Status0) ->
    Status0#{state := format_state(State)}.

code_change(_, _, State) ->
    {ok, State}.

%%---------------------------------------------------------------------------

maybe_start_request(
    #?MODULE{
        read_size = ReadSize,
        end_pos = EndPos,
        current_pos = CurrentPos
    } = State
) when EndPos - CurrentPos > ReadSize ->
    %% The buffer has enough data. Check if we should pre-fetch the next fragment.
    maybe_start_next_request(State);
maybe_start_request(#?MODULE{} = State) ->
    State1 = maybe_start_current_request(State),
    maybe_start_next_request(State1).

maybe_start_current_request(
    #?MODULE{
        read_size = ReadSize,
        fragment_ref = #fragment_ref{offset = Fragment, size = FragSize},
        end_pos = EndPos,
        key = CurrentKey,
        requests = Requests0
    } = State0
) ->
    IdxStartPos = ?SEGMENT_HEADER_B + FragSize,
    case EndPos < IdxStartPos of
        true ->
            case has_request_for(Fragment, Requests0) of
                true ->
                    State0;
                false ->
                    Range = {EndPos, min(EndPos + ReadSize, IdxStartPos - 1)},
                    start_request(CurrentKey, Range, Fragment, State0)
            end;
        false ->
            State0
    end.

maybe_start_next_request(
    #?MODULE{
        stream = StreamId,
        read_size = ReadSize,
        fragment_ref = #fragment_ref{size = FragSize},
        end_pos = EndPos,
        next = undefined,
        iterator = Iterator,
        requests = Requests0
    } = State0
) when EndPos >= ?SEGMENT_HEADER_B + FragSize ->
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, #fragment_ref{offset = NextOffset, uid = NextUid}, _Iterator1} ->
            case has_request_for(NextOffset, Requests0) of
                true ->
                    State0;
                false ->
                    Key = rabbitmq_stream_s3:fragment_key(StreamId, NextOffset, NextUid),
                    Range = {?SEGMENT_HEADER_B, ?SEGMENT_HEADER_B + ReadSize},
                    start_request(Key, Range, NextOffset, State0)
            end;
        _ ->
            State0
    end;
maybe_start_next_request(State) ->
    State.

has_request_for(Fragment, Requests) ->
    maps:fold(
        fun(_, #request{fragment = F}, Acc) -> Acc orelse F =:= Fragment end,
        false,
        Requests
    ).

start_request(Key, Range, Fragment, #?MODULE{requests = Requests0} = State0) ->
    case rabbitmq_stream_s3_api:get_range_async(Key, Range, #{timeout => ?REQUEST_TIMEOUT_MS}) of
        {ok, RequestId, AsyncState} ->
            Request = #request{
                fragment = Fragment,
                range = Range,
                state = AsyncState
            },
            counters:add(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
            counters:add(counter(), ?C_TOTAL_REQUESTS, 1),
            State0#?MODULE{requests = Requests0#{RequestId => Request}};
        {error, pool_busy} ->
            State0
    end.

add_data(
    Data,
    #request{fragment = Fragment},
    #?MODULE{fragment_ref = #fragment_ref{offset = Fragment}} = State
) ->
    add_data_current(Data, State);
add_data(Data, #request{} = Req, #?MODULE{} = State) ->
    add_data_next(Data, Req, State).

add_data_current(
    Data,
    #?MODULE{
        start_pos = StartPos0,
        current_pos = CurrentPos,
        end_pos = EndPos0,
        buffer = Buffer0
    } = State0
) ->
    Buffer =
        case CurrentPos =:= StartPos0 of
            true ->
                <<Buffer0/binary, Data/binary>>;
            false ->
                <<
                    (binary:part(Buffer0, CurrentPos - StartPos0, EndPos0 - CurrentPos))/binary,
                    Data/binary
                >>
        end,
    State0#?MODULE{
        start_pos = CurrentPos,
        end_pos = EndPos0 + byte_size(Data),
        buffer = Buffer
    }.

add_data_next(
    Data, #request{fragment = NextOffset}, #?MODULE{next = Next0, iterator = Iterator} = State0
) ->
    Next =
        case Next0 of
            undefined ->
                %% First data for the next fragment. Look up its metadata from the iterator.
                case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
                    {ok, #fragment_ref{offset = NextOffset} = FragRef, _} ->
                        {FragRef, Data};
                    _ ->
                        {#fragment_ref{offset = NextOffset, uid = 0, size = 0}, Data}
                end;
            {#fragment_ref{offset = NextOffset} = FragRef, Buffer0} ->
                {FragRef, <<Buffer0/binary, Data/binary>>}
        end,
    State0#?MODULE{next = Next}.

maybe_reply_pending(#?MODULE{pending_read = undefined} = State) ->
    {noreply, State};
maybe_reply_pending(
    #?MODULE{
        pending_read = #pending_read{
            read = #read{} = Read,
            from = From,
            since = Since
        }
    } = State0
) ->
    case maybe_reply(Read, State0) of
        {reply, Reply, State1} ->
            Duration = erlang:convert_time_unit(
                erlang:monotonic_time() - Since,
                native,
                millisecond
            ),
            counters:add(counter(), ?C_AWAIT_DURATION_MS, Duration),
            counters:add(counter(), ?C_AWAIT, 1),
            gen_server:reply(From, Reply),
            State = State1#?MODULE{pending_read = undefined},
            {noreply, State};
        {stop, Reason, Reply, State1} ->
            gen_server:reply(From, Reply),
            State = State1#?MODULE{pending_read = undefined},
            {stop, Reason, State};
        {retry, State} ->
            %% No in-flight requests - the pool was busy when maybe_start_request
            %% was called. Schedule a retry so the read does not stall forever.
            erlang:send_after(?MIN_RETRY_DELAY_MS, self(), retry_requests),
            {noreply, State};
        {noreply, _} = NoReply ->
            NoReply
    end.

maybe_reply(#read{offset = Offset, bytes = Bytes, hint = _Hint}, #?MODULE{} = State0) ->
    case try_read(State0, Offset, Bytes) of
        {ok, State1, Data} ->
            rabbitmq_stream_s3_histogram:observe(?MODULE, byte_size(Data)),
            counters:add(counter(), ?C_BUFFER_HIT, 1),
            State2 = adjust_read_size(hit, State1),
            State = maybe_start_request(State2),
            {reply, {ok, Data}, State};
        {next_fragment, State1, NextOffset} ->
            counters:add(counter(), ?C_FRAGMENT_TRANSITION, 1),
            State = maybe_start_request(State1),
            {reply, {next_fragment, NextOffset}, State};
        {become_local, #?MODULE{fragment_ref = #fragment_ref{offset = Off}} = State} ->
            {stop, normal, {become_local, Off}, State};
        {end_of_stream, State} ->
            {reply, end_of_stream, State};
        {await, State1} ->
            counters:add(counter(), ?C_BUFFER_MISS, 1),
            State2 = adjust_read_size(miss, State1),
            State = maybe_start_request(State2),
            case State of
                #?MODULE{requests = Requests} when map_size(Requests) =:= 0 ->
                    {retry, State};
                _ ->
                    {noreply, State}
            end
    end.

%% AIMD read-ahead adjustment.
%% On miss: multiplicative decrease (halve), reset hit counter.
%% On hit: after HITS_TO_GROW consecutive hits, additive increase by GROW_STEP.
adjust_read_size(miss, #?MODULE{read_size = Current, read_size_min = Min} = State) ->
    State#?MODULE{
        read_size = max(Min, Current div 2),
        hits_since_last_miss = 0
    };
adjust_read_size(hit, #?MODULE{hits_since_last_miss = Hits} = State) when
    Hits + 1 < ?HITS_TO_GROW
->
    State#?MODULE{hits_since_last_miss = Hits + 1};
adjust_read_size(hit, #?MODULE{read_size = Current, read_size_max = Max} = State) ->
    State#?MODULE{
        read_size = min(Max, Current + ?GROW_STEP),
        hits_since_last_miss = 0
    }.

-spec try_read(#?MODULE{}, byte_offset(), NumBytes :: pos_integer()) ->
    {ok, #?MODULE{}, binary()}
    | {next_fragment, #?MODULE{}, osiris:offset()}
    | {become_local, #?MODULE{}}
    | {await, #?MODULE{}}
    | {end_of_stream, #?MODULE{}}.
try_read(
    #?MODULE{
        fragment_ref = #fragment_ref{offset = ThisFragment, size = FragSize},
        next = {#fragment_ref{offset = NextOffset}, _Buffer}
    } = State0,
    Offset,
    _Bytes
) when Offset >= ?SEGMENT_HEADER_B + FragSize ->
    ?LOG_DEBUG(
        ?MODULE_STRING " transitioning to next fragment (~20..0B->~20..0B)",
        [ThisFragment, NextOffset]
    ),
    State = goto_next_fragment(State0),
    {next_fragment, State, NextOffset};
try_read(
    #?MODULE{
        fragment_ref = #fragment_ref{size = FragSize},
        next = undefined,
        iterator = Iterator,
        requests = Requests
    } = State0,
    Offset,
    _Bytes
) when Offset >= ?SEGMENT_HEADER_B + FragSize ->
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, #fragment_ref{offset = NextOffset}, _Iterator1} ->
            case has_request_for(NextOffset, Requests) of
                true ->
                    {await, State0};
                false ->
                    {await, maybe_start_request(State0)}
            end;
        end_of_manifest ->
            %% Refresh the iterator from the manifest cache.
            case refresh_iterator(State0) of
                {ok, State1} ->
                    %% New entries available — try again.
                    {await, maybe_start_request(State1)};
                end_of_manifest ->
                    State = goto_next_fragment(State0),
                    {become_local, State}
            end;
        {error, _} ->
            State = goto_next_fragment(State0),
            {become_local, State}
    end;
try_read(
    #?MODULE{
        stream = StreamId,
        fragment_ref = #fragment_ref{size = FragSize},
        next = not_found
    } = State0,
    Offset,
    Bytes
) when Offset >= ?SEGMENT_HEADER_B + FragSize ->
    %% The request for the next fragment gave a 404.
    RemoteRange = rabbitmq_stream_s3_manifest_replica:get_range(StreamId),
    IdxStartPos = ?SEGMENT_HEADER_B + FragSize,
    case rabbitmq_stream_s3_fragment_iterator:next(State0#?MODULE.iterator) of
        {ok, #fragment_ref{offset = NextFragment}, _} ->
            ?LOG_DEBUG(
                "Next fragment (~20..0B) was not found (requested ~b + ~b, idx start ~b). Remote range for this stream is ~w",
                [NextFragment, Offset, Bytes, IdxStartPos, RemoteRange]
            ),
            case RemoteRange of
                {RemoteStartOffset, _EndOffset} when RemoteStartOffset >= NextFragment ->
                    %% Retention evicted the next fragment. Jump to oldest available.
                    jump_to_oldest(State0, RemoteStartOffset);
                {_StartOffset, EndOffset} when EndOffset =< NextFragment ->
                    State = goto_next_fragment(State0),
                    {become_local, State};
                {_StartOffset, _EndOffset} ->
                    State = goto_next_fragment(State0),
                    {await, State};
                empty ->
                    {end_of_stream, State0}
            end;
        _ ->
            case RemoteRange of
                empty ->
                    {end_of_stream, State0};
                _ ->
                    State = goto_next_fragment(State0),
                    {become_local, State}
            end
    end;
try_read(
    #?MODULE{fragment_ref = #fragment_ref{size = FragSize}} = State,
    Offset,
    _Bytes
) when Offset >= ?SEGMENT_HEADER_B + FragSize ->
    ?LOG_DEBUG("Requested ~b exceeds index ~b. State: ~0p", [
        Offset, ?SEGMENT_HEADER_B + FragSize, format_state(State)
    ]),
    erlang:error(unreachable);
try_read(#?MODULE{end_pos = EndPos} = State, Offset, Bytes) when Offset + Bytes > EndPos ->
    IdxStartPos = ?SEGMENT_HEADER_B + (State#?MODULE.fragment_ref)#fragment_ref.size,
    case EndPos >= IdxStartPos andalso Offset + ?CHUNK_HEADER_B =< EndPos of
        true ->
            %% The log-reader over-reads the chunk header to try to get the
            %% full filter. Cap the read at the index boundary.
            try_read(State, Offset, EndPos - Offset);
        false ->
            case State of
                #?MODULE{requests = Requests} when map_size(Requests) =/= 0 ->
                    {await, State};
                #?MODULE{current_not_found = true, stream = StreamId} ->
                    case rabbitmq_stream_s3_manifest_replica:get_range(StreamId) of
                        empty ->
                            {end_of_stream, State};
                        {FirstOffset, _} ->
                            jump_to_oldest(State, FirstOffset)
                    end;
                #?MODULE{read_size = ReadSize0} ->
                    Needed = Offset + Bytes - EndPos,
                    ReadSize = max(ReadSize0, Needed),
                    {await, maybe_start_current_request(State#?MODULE{read_size = ReadSize})}
            end
    end;
try_read(
    #?MODULE{
        fragment_ref = #fragment_ref{size = FragSize},
        start_pos = StartPos,
        current_pos = CurrentPos0,
        buffer = Buffer
    } = State0,
    Offset,
    Bytes0
) ->
    IdxStartPos = ?SEGMENT_HEADER_B + FragSize,
    %%% PRECONDITIONS
    ?assert(Offset >= StartPos),
    ?assert(Offset >= CurrentPos0),

    %% Cap the read at the index boundary.
    Bytes = min(Bytes0, IdxStartPos - Offset),

    Data = binary:part(Buffer, Offset - StartPos, Bytes),
    {ok, State0#?MODULE{current_pos = Offset}, Data}.

goto_next_fragment(
    #?MODULE{
        stream = StreamId,
        next = Next0,
        iterator = Iterator0
    } = State0
) ->
    case Next0 of
        {#fragment_ref{offset = NextOffset, uid = NextUid} = NextFragRef, Buffer} ->
            %% Advance the iterator past the entry we're transitioning to.
            Iterator =
                case rabbitmq_stream_s3_fragment_iterator:next(Iterator0) of
                    {ok, _, It} -> It;
                    _ -> Iterator0
                end,
            State0#?MODULE{
                start_pos = ?SEGMENT_HEADER_B,
                current_pos = ?SEGMENT_HEADER_B,
                end_pos = ?SEGMENT_HEADER_B + byte_size(Buffer),
                buffer = Buffer,
                fragment_ref = NextFragRef,
                key = rabbitmq_stream_s3:fragment_key(StreamId, NextOffset, NextUid),
                iterator = Iterator,
                next = undefined
            };
        _ ->
            %% No pre-fetched next fragment data. Advance iterator and reset.
            Iterator =
                case rabbitmq_stream_s3_fragment_iterator:next(Iterator0) of
                    {ok, _, It} -> It;
                    _ -> Iterator0
                end,
            State0#?MODULE{
                start_pos = ?SEGMENT_HEADER_B,
                current_pos = ?SEGMENT_HEADER_B,
                end_pos = ?SEGMENT_HEADER_B,
                buffer = <<>>,
                iterator = Iterator,
                next = undefined
            }
    end.

cancel_requests(Requests) ->
    maps:foreach(
        fun(RequestId, #request{state = ReqState}) ->
            rabbitmq_stream_s3_api:cancel_async(RequestId, ReqState)
        end,
        Requests
    ),
    counters:put(counter(), ?C_REQUESTS_IN_FLIGHT, 0).

jump_to_oldest(
    #?MODULE{stream = StreamId, requests = Requests, cancelled_requests = Cancelled0} = State0,
    FirstOffset
) ->
    cancel_requests(Requests),
    Cancelled = maps:merge(Cancelled0, #{Req => ok || Req := _ <- Requests}),
    %% Look up the fragment entry from the manifest to get UID and size.
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{entries = Entries} = Manifest when byte_size(Entries) >= ?ENTRY_B ->
            ?ENTRY(FirstOffset, _FTs, _LTs, ?MANIFEST_KIND_FRAGMENT, Size, Uid) =
                rabbitmq_stream_s3_array:at(0, ?ENTRY_B, Entries),
            GetGroupFun = rabbitmq_stream_s3_manifest:get_group_fun(StreamId),
            Iterator = rabbitmq_stream_s3_fragment_iterator:init(
                Manifest, FirstOffset, GetGroupFun
            ),
            %% Advance past the current entry.
            Iterator1 =
                case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
                    {ok, _, It} -> It;
                    _ -> Iterator
                end,
            Key = rabbitmq_stream_s3:fragment_key(StreamId, FirstOffset, Uid),
            ?LOG_WARNING(
                "Jumping to oldest available fragment ~20..0B",
                [FirstOffset]
            ),
            State = State0#?MODULE{
                fragment_ref = #fragment_ref{offset = FirstOffset, uid = Uid, size = Size},
                key = Key,
                buffer = <<>>,
                start_pos = ?SEGMENT_HEADER_B,
                current_pos = ?SEGMENT_HEADER_B,
                end_pos = ?SEGMENT_HEADER_B,
                next = undefined,
                current_not_found = false,
                requests = #{},
                cancelled_requests = Cancelled,
                iterator = Iterator1
            },
            {next_fragment, State, FirstOffset};
        _ ->
            {end_of_stream, State0}
    end.

refresh_iterator(
    #?MODULE{stream = StreamId, fragment_ref = #fragment_ref{offset = Fragment, size = FragSize}} =
        State
) ->
    NextOffset = Fragment + FragSize,
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{next_offset = ManifestNext} = Manifest when ManifestNext > NextOffset ->
            GetGroupFun = rabbitmq_stream_s3_manifest:get_group_fun(StreamId),
            Iterator = rabbitmq_stream_s3_fragment_iterator:init(Manifest, NextOffset, GetGroupFun),
            {ok, State#?MODULE{iterator = Iterator}};
        _ ->
            end_of_manifest
    end.

format_state(#?MODULE{
    stream = StreamId,
    read_size = ReadSize,
    read_size_min = ReadSizeMin,
    read_size_max = ReadSizeMax,
    hits_since_last_miss = Hits,
    buffer = Buffer,
    start_pos = StartPos,
    current_pos = CurrentPos,
    end_pos = EndPos,
    fragment_ref = #fragment_ref{offset = Fragment, uid = Uid, size = FragSize},
    next = Next0,
    requests = Requests,
    pending_read = PendingRead,
    current_not_found = CurrentNotFound
}) ->
    Next =
        case Next0 of
            {#fragment_ref{offset = NextOff}, Buf} ->
                {NextOff, <<(integer_to_binary(byte_size(Buf)))/binary, " bytes">>};
            _ ->
                Next0
        end,
    #{
        stream => StreamId,
        read_size => ReadSize,
        read_size_min => ReadSizeMin,
        read_size_max => ReadSizeMax,
        hits_since_last_miss => Hits,
        buffer => <<(integer_to_binary(byte_size(Buffer)))/binary, " bytes">>,
        start_pos => StartPos,
        current_pos => CurrentPos,
        end_pos => EndPos,
        fragment => Fragment,
        fragment_uid => Uid,
        fragment_size => FragSize,
        idx_start_pos => ?SEGMENT_HEADER_B + FragSize,
        next => Next,
        requests => #{F => R || _ := #request{fragment = F, range = R} <- Requests},
        pending_read => PendingRead =/= undefined,
        current_not_found => CurrentNotFound
    }.

counter() ->
    persistent_term:get(?COUNTER_KEY).

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
