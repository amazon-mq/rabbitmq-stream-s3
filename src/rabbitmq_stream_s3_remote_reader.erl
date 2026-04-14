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

-define(C_BUFFER_HIT, 1).
-define(C_BUFFER_MISS, 2).
-define(C_FRAGMENT_TRANSITION, 3).
-define(C_REQUESTS_IN_FLIGHT, 4).
-define(C_AWAIT_DURATION_MS, 5).
-define(C_AWAIT, 6).
-define(C_READ_DURATION_MS, 7).
-define(C_READ, 8).
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
    {read, ?C_READ, counter, "Number of read/4,5 calls"}
]).

%% API
-record(read, {
    offset :: byte_offset(),
    bytes :: pos_integer(),
    %% NOTE: unused since we reliably track #fragment_info.index_start_pos now.
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
    fragment :: osiris:offset(),
    key :: rabbitmq_stream_s3:key(),
    info :: #fragment_info{} | undefined,
    buffer = <<>> :: binary(),
    start_pos :: byte_offset(),
    current_pos :: byte_offset(),
    end_pos :: byte_offset(),

    %% Next fragment state (pre-fetched).
    next :: {#fragment_info{}, binary()} | undefined | not_found,

    %% All in-flight requests, keyed by async_req().
    requests = #{} :: #{rabbitmq_stream_s3_api:async_req() => #request{}},

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

-export([init_counters/0]).

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
        fragment = Fragment,
        position = Pos,
        fragment_info = Info
    }
}) ->
    Key = rabbitmq_stream_s3:fragment_key(StreamId, Fragment),
    State0 = #?MODULE{
        stream = StreamId,
        read_size = ?INITIAL_READ_SIZE,
        read_size_min = ?MIN_READ_SIZE,
        read_size_max = ?MAX_READ_SIZE,
        reader_ref = erlang:monitor(process, Reader),
        start_pos = Pos,
        current_pos = Pos,
        end_pos = Pos,
        fragment = Fragment,
        key = Key,
        info = Info
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
handle_info(Msg, #?MODULE{requests = Requests0, retry_delay = RetryDelay0} = State0) ->
    AsyncStates = #{Req => R#request.state || Req := R <- Requests0},
    case rabbitmq_stream_s3_api:match_async(Msg, AsyncStates) of
        {ok, RequestId} ->
            #request{state = ReqState0} = Req0 = maps:get(RequestId, Requests0),
            case rabbitmq_stream_s3_api:handle_async(Msg, RequestId, ReqState0) of
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
                    case {Reason, Req0#request.fragment =:= State1#?MODULE.fragment} of
                        {not_found, true} ->
                            maybe_reply_pending(State1#?MODULE{
                                current_not_found = true,
                                retry_delay = ?MIN_RETRY_DELAY_MS
                            });
                        {not_found, false} ->
                            maybe_reply_pending(State1#?MODULE{
                                next = not_found,
                                retry_delay = ?MIN_RETRY_DELAY_MS
                            });
                        {Transient, _} when
                            Transient =:= slow_down;
                            Transient =:= internal_error
                        ->
                            erlang:send_after(RetryDelay0, self(), retry_requests),
                            RetryDelay = min(RetryDelay0 * 2, ?MAX_RETRY_DELAY_MS),
                            State2 = State1#?MODULE{retry_delay = RetryDelay},
                            maybe_reply_pending(State2);
                        {Transient, _} when
                            Transient =:= stream_error;
                            Transient =:= connection_error
                        ->
                            maybe_reply_pending(State1);
                        _ ->
                            {stop, {shutdown, Reason}, State1}
                    end
            end;
        error ->
            ?LOG_DEBUG(?MODULE_STRING " received unexpected message: ~W", [Msg, 10]),
            {noreply, State0}
    end;
handle_info(retry_requests, State0) ->
    State = maybe_start_request(State0),
    maybe_reply_pending(State);
handle_info(Msg, State) ->
    ?LOG_DEBUG(?MODULE_STRING " received unexpected message: ~W", [Msg, 10]),
    {noreply, State}.

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
        fragment = Fragment,
        key = CurrentKey,
        %% Information about the current fragment isn't available - we need to
        %% request from the start of the fragment to read this from the
        %% header.
        info = undefined,
        requests = Requests0
    } = State0
) ->
    case has_request_for(Fragment, Requests0) of
        true ->
            State0;
        false ->
            Range = {0, ReadSize + ?FRAGMENT_HEADER_B},
            {ok, RequestId, AsyncState} = rabbitmq_stream_s3_api:get_range_async(CurrentKey, Range),
            Request = #request{fragment = Fragment, range = Range, state = AsyncState},
            counters:add(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
            State0#?MODULE{requests = Requests0#{RequestId => Request}}
    end;
maybe_start_current_request(
    #?MODULE{
        read_size = ReadSize,
        fragment = Fragment,
        end_pos = EndPos,
        key = CurrentKey,
        info = #fragment_info{index_start_pos = IdxStartPos},
        requests = Requests0
    } = State0
) when EndPos < IdxStartPos ->
    %% The current fragment hasn't been downloaded completely.
    %% TODO: is this reading too aggressively?
    case has_request_for(State0#?MODULE.fragment, Requests0) of
        true ->
            State0;
        false ->
            Range = {EndPos, min(EndPos + ReadSize, IdxStartPos - 1)},
            {ok, RequestId, AsyncState} = rabbitmq_stream_s3_api:get_range_async(CurrentKey, Range),
            Request = #request{fragment = Fragment, range = Range, state = AsyncState},
            counters:add(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
            State0#?MODULE{requests = Requests0#{RequestId => Request}}
    end;
maybe_start_current_request(State) ->
    State.

maybe_start_next_request(
    #?MODULE{
        stream = StreamId,
        read_size = ReadSize,
        info = #fragment_info{next_offset = NextOffset, index_start_pos = IdxStartPos},
        end_pos = EndPos,
        next = undefined,
        requests = Requests0
    } = State0
) when EndPos >= IdxStartPos ->
    case has_request_for(NextOffset, Requests0) of
        true ->
            State0;
        false ->
            Key = rabbitmq_stream_s3:fragment_key(StreamId, NextOffset),
            Range = {0, ReadSize + ?FRAGMENT_HEADER_B},
            {ok, RequestId, AsyncState} = rabbitmq_stream_s3_api:get_range_async(Key, Range),
            Request = #request{fragment = NextOffset, range = Range, state = AsyncState},
            counters:add(counter(), ?C_REQUESTS_IN_FLIGHT, 1),
            State0#?MODULE{requests = Requests0#{RequestId => Request}}
    end;
maybe_start_next_request(State) ->
    State.

has_request_for(Fragment, Requests) ->
    maps:fold(
        fun(_, #request{fragment = F}, Acc) -> Acc orelse F =:= Fragment end,
        false,
        Requests
    ).

add_data(Data, #request{fragment = Fragment}, #?MODULE{fragment = Fragment} = State) ->
    add_data_current(Data, State);
add_data(Data, #request{}, #?MODULE{} = State) ->
    add_data_next(Data, State).

add_data_current(Data, #?MODULE{info = undefined} = State0) ->
    ?assert(byte_size(Data) >= ?FRAGMENT_HEADER_B),
    {Info, Buffer} = rabbitmq_stream_s3_server:split_fragment_info(Data),
    State0#?MODULE{
        buffer = Buffer,
        end_pos = ?FRAGMENT_HEADER_B + byte_size(Buffer),
        info = Info
    };
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
                %% Fast-lane: appending like this creates less garbage than
                %% reallocating with binary:part/3 even if the partition covers
                %% the entire binary.
                <<Buffer0/binary, Data/binary>>;
            false ->
                %% Drop parts of the buffer that have already been served.
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

add_data_next(Data, #?MODULE{next = Next0} = State0) ->
    Next =
        case Next0 of
            undefined ->
                ?assert(byte_size(Data) >= ?FRAGMENT_HEADER_B),
                rabbitmq_stream_s3_server:split_fragment_info(Data);
            {Info, Buffer0} ->
                {Info, <<Buffer0/binary, Data/binary>>}
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
        {noreply, _} = NoReply ->
            NoReply
    end.

maybe_reply(#read{offset = Offset, bytes = Bytes, hint = Hint}, #?MODULE{} = State0) ->
    case try_read(State0, Offset, Bytes) of
        {ok, _State, ?IDX_HEADER(_)} when Hint =:= chunk_boundary ->
            %% This branch was important in the past but now that we track
            %% `#fragment_info.index_start_pos` reliably, `try_read/3` will
            %% never return this data. This branch is left temporarily to help
            %% with debugging.
            erlang:error(unreachable);
        {ok, State1, Data} ->
            counters:add(counter(), ?C_BUFFER_HIT, 1),
            State2 = adjust_read_size(hit, State1),
            State = maybe_start_request(State2),
            {reply, {ok, Data}, State};
        {next_fragment, State1, NextOffset} ->
            counters:add(counter(), ?C_FRAGMENT_TRANSITION, 1),
            State = maybe_start_request(State1),
            {reply, {next_fragment, NextOffset}, State};
        {become_local, #?MODULE{fragment = Off} = State} ->
            {stop, normal, {become_local, Off}, State};
        {end_of_stream, State} ->
            {reply, end_of_stream, State};
        {await, State1} ->
            counters:add(counter(), ?C_BUFFER_MISS, 1),
            State2 = adjust_read_size(miss, State1),
            State = maybe_start_request(State2),
            {noreply, State}
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
try_read(#?MODULE{info = undefined} = State, _Offset, _Bytes) ->
    %% The current fragment has not been read yet. The reader must wait until
    %% the request arrives.
    {await, State};
try_read(
    #?MODULE{
        fragment = ThisFragment,
        info = #fragment_info{index_start_pos = IdxStartPos},
        next = {#fragment_info{first_offset = NextOffset}, _Buffer}
    } = State0,
    Offset,
    _Bytes
) when Offset >= IdxStartPos ->
    ?LOG_DEBUG(
        ?MODULE_STRING " transitioning to next fragment (~20..0B->~20..0B)",
        [ThisFragment, NextOffset]
    ),
    State = goto_next_fragment(State0),
    {next_fragment, State, NextOffset};
try_read(
    #?MODULE{
        info = #fragment_info{next_offset = NextOffset, index_start_pos = IdxStartPos},
        next = undefined,
        requests = Requests
    } = State0,
    Offset,
    _Bytes
) when Offset >= IdxStartPos ->
    case has_request_for(NextOffset, Requests) of
        true ->
            %% The next fragment's info is being requested. Wait for its arrival.
            {await, State0};
        false ->
            %% No request in-flight for the next fragment. Start one.
            {await, maybe_start_request(State0)}
    end;
try_read(
    #?MODULE{
        stream = StreamId,
        info = #fragment_info{next_offset = NextFragment, index_start_pos = IdxStartPos},
        next = not_found
    } = State0,
    Offset,
    Bytes
) when Offset >= IdxStartPos ->
    %% The request for the next fragment gave a 404. The fragment doesn't exist
    %% or hasn't been uploaded yet.
    RemoteRange = rabbitmq_stream_s3_server:get_range(StreamId),
    ?LOG_DEBUG(
        "Next fragment (~20..0B) was not found (requested ~b + ~b, idx start ~b). Remote range for this stream is ~w",
        [
            NextFragment,
            Offset,
            Bytes,
            IdxStartPos,
            RemoteRange
        ]
    ),
    case RemoteRange of
        {RemoteStartOffset, _EndOffset} when RemoteStartOffset >= NextFragment ->
            %% re-set the state at this offset. Also take the min between this
            %% and the first local offset.
            erlang:error(unimplemented);
        {_StartOffset, EndOffset} when EndOffset < NextFragment ->
            State = goto_next_fragment(State0),
            {become_local, State};
        {_StartOffset, _EndOffset} ->
            %% TODO: next fragment has been uploaded. Re-request it.
            State = goto_next_fragment(State0),
            {await, State};
        empty ->
            %% TODO: become local?
            {end_of_stream, State0}
    end;
try_read(
    #?MODULE{info = #fragment_info{index_start_pos = IdxStartPos}} = State,
    Offset,
    _Bytes
) when Offset >= IdxStartPos ->
    ?LOG_DEBUG("Requested ~b exceeds index ~b. State: ~0p", [
        Offset, IdxStartPos, format_state(State)
    ]),
    erlang:error(unreachable);
try_read(#?MODULE{end_pos = EndPos} = State, Offset, Bytes) when Offset + Bytes > EndPos ->
    %% The current buffer does not contain enough data to fulfill the request.
    %% Wait until the current request is complete and then respond with the
    %% newly added data.
    case State of
        #?MODULE{requests = Requests} when map_size(Requests) =/= 0 ->
            %% The current fragment's info is being requested. Wait for its arrival.
            {await, State};
        #?MODULE{current_not_found = true, stream = StreamId, requests = Requests} ->
            %% The current fragment was deleted by retention while being read.
            %% Cancel all in-flight requests for the deleted fragment, then jump
            %% to the oldest fragment still available in the remote tier.
            cancel_requests(Requests),
            case rabbitmq_stream_s3_server:get_range(StreamId) of
                empty ->
                    {end_of_stream, State#?MODULE{requests = #{}}};
                {FirstOffset, _} ->
                    Key = rabbitmq_stream_s3:fragment_key(StreamId, FirstOffset),
                    ?LOG_WARNING(
                        "Fragment ~20..0B was deleted by retention while being read. "
                        "Jumping to oldest available fragment ~20..0B",
                        [State#?MODULE.fragment, FirstOffset]
                    ),
                    NewState = State#?MODULE{
                        fragment = FirstOffset,
                        key = Key,
                        info = undefined,
                        buffer = <<>>,
                        start_pos = ?FRAGMENT_HEADER_B,
                        current_pos = ?FRAGMENT_HEADER_B,
                        end_pos = ?FRAGMENT_HEADER_B,
                        next = undefined,
                        requests = #{},
                        current_not_found = false
                    },
                    {next_fragment, NewState, FirstOffset}
            end;
        #?MODULE{read_size = ReadSize0} ->
            %% The chunk is larger than the read-ahead. Bump read_size to
            %% cover the needed bytes and call maybe_start_current_request
            %% directly - bypassing the maybe_start_request guard that skips
            %% current requests when the buffer already exceeds read_size.
            Needed = Offset + Bytes - EndPos,
            ReadSize = max(ReadSize0, Needed),
            {await, maybe_start_current_request(State#?MODULE{read_size = ReadSize})}
    end;
try_read(
    #?MODULE{
        info = #fragment_info{index_start_pos = IdxStartPos},
        start_pos = StartPos,
        current_pos = CurrentPos0,
        buffer = Buffer
    } = State0,
    Offset,
    Bytes0
) ->
    %%% PRECONDITIONS
    %% We should never discard data from the buffer which the requester then
    %% attempts to read.
    ?assert(Offset >= StartPos),
    %% Reading never goes backwards. The requester may re-read within a chunk,
    %% but like a cursor, the read always goes forward.
    ?assert(Offset >= CurrentPos0),

    %% The log reader attempts to over-read the current chunk so that it can
    %% read large bloom filters. So we need to cap the amount of data returned
    %% to the start of the index in this fragment.
    Bytes = min(Bytes0, IdxStartPos - Offset),

    Data = binary:part(Buffer, Offset - StartPos, Bytes),
    {ok, State0#?MODULE{current_pos = Offset}, Data}.

goto_next_fragment(
    #?MODULE{
        stream = StreamId,
        info = #fragment_info{next_offset = NextFragment},
        next = Next0
    } = State0
) ->
    {Info, Buffer} =
        case Next0 of
            {I, _B} ->
                ?assertEqual(NextFragment, I#fragment_info.first_offset),
                Next0;
            _ ->
                {undefined, <<>>}
        end,
    State0#?MODULE{
        start_pos = ?FRAGMENT_HEADER_B,
        current_pos = ?FRAGMENT_HEADER_B,
        end_pos = ?FRAGMENT_HEADER_B + byte_size(Buffer),
        buffer = Buffer,
        fragment = NextFragment,
        key = rabbitmq_stream_s3:fragment_key(StreamId, NextFragment),
        info = Info,
        next = undefined
    }.

cancel_requests(Requests) ->
    maps:foreach(
        fun(RequestId, #request{state = ReqState}) ->
            rabbitmq_stream_s3_api:cancel_async(RequestId, ReqState)
        end,
        Requests
    ),
    counters:put(counter(), ?C_REQUESTS_IN_FLIGHT, 0).

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
    info = Info0,
    next = Next0,
    requests = Requests,
    pending_read = PendingRead,
    current_not_found = CurrentNotFound
}) ->
    Info =
        case Info0 of
            #fragment_info{} ->
                format_fragment_info(Info0);
            undefined ->
                undefined
        end,
    Next =
        case Next0 of
            {NextInfo, Buf} ->
                I = format_fragment_info(NextInfo),
                {I, <<(integer_to_binary(byte_size(Buf)))/binary, " bytes">>};
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
        info => Info,
        next => Next,
        requests => #{F => R || _ := #request{fragment = F, range = R} <- Requests},
        pending_read => PendingRead =/= undefined,
        current_not_found => CurrentNotFound
    }.

%% TODO: debug/helpers module for pretty-printers like this.
format_fragment_info(#fragment_info{
    first_offset = FirstOffset,
    next_offset = NextOffset,
    first_timestamp = FirstTs,
    last_timestamp = LastTs,
    seq_no = SeqNo,
    segment_offset = SegmentOffset,
    segment_start_pos = SegmentStartPos,
    size = Size,
    index_start_pos = IdxStartPos,
    roll_reason = RollReason
}) ->
    #{
        first_offset => FirstOffset,
        next_offset => NextOffset,
        first_timestamp => FirstTs,
        last_timestamp => LastTs,
        seq_no => SeqNo,
        segment_offset => SegmentOffset,
        segment_start_pos => SegmentStartPos,
        size => Size,
        index_start_pos => IdxStartPos,
        roll_reason => RollReason
    }.

counter() ->
    persistent_term:get(?COUNTER_KEY).
