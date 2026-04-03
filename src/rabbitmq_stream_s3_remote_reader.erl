%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_remote_reader).
-moduledoc """
A gen_server process which reads stream data from the remote tier.

Reads from the remote tier suffer from high latency, so this server pre-fetches
stream data aggressively.
""".

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include("include/rabbitmq_stream_s3.hrl").

-behaviour(gen_server).

-define(READAHEAD, "5MiB").

-record(?MODULE, {
    buffer = <<>> :: binary(),
    offset_start :: byte_offset() | undefined,
    offset_end :: byte_offset() | undefined,
    read_size :: pos_integer(),
    object :: binary(),
    index_start_pos :: byte_offset(),
    next_fragment_offset :: osiris:offset()
}).

%% API
-record(read, {
    offset :: byte_offset(),
    bytes :: pos_integer(),
    hint :: chunk_boundary | within_chunk
}).

-type config() :: #{
    reader := pid(),
    stream := stream_id(),
    location := #remote_location{}
}.

-export_type([config/0]).

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

read(Server, Offset, Bytes, Hint) ->
    read(Server, Offset, Bytes, Hint, ?GEN_SERVER_CALL_TIMEOUT).

read(Server, Offset, Bytes, Hint, Timeout) ->
    gen_server:call(Server, #read{offset = Offset, bytes = Bytes, hint = Hint}, Timeout).

%%----------------------------------------------------------------------------

start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

-spec init(config()) -> {ok, #?MODULE{}}.
init(#{
    reader := Reader,
    stream := StreamId,
    location := #remote_location{fragment = Fragment, fragment_info = Info0}
}) ->
    erlang:monitor(process, Reader),
    Key = rabbitmq_stream_s3:fragment_key(StreamId, Fragment),
    Info =
        case Info0 of
            #fragment_info{} ->
                {ok, Info0};
            undefined ->
                rabbitmq_stream_s3_server:get_fragment_info(Key)
        end,
    case Info of
        {ok, #fragment_info{index_start_pos = IdxStartPos, next_offset = NextOffset}} ->
            {ok, ReadSize} = rabbit_resource_monitor_misc:parse_information_unit(?READAHEAD),
            {ok, #?MODULE{
                object = Key,
                index_start_pos = IdxStartPos,
                read_size = ReadSize,
                next_fragment_offset = NextOffset
            }};
        {error, not_found} ->
            %% The fragment was deleted by retention before this reader could
            %% open it. Stop normally so the supervisor does not restart us.
            %% TODO: transition to the first available fragment.
            {stop, normal}
    end.

handle_call(#read{offset = Offset, bytes = Bytes, hint = Hint}, _From, State0) ->
    %% TODO: while reading, start a request for the next range of data when
    %% we near the end of the current section.
    case do_read(State0, Offset, Bytes) of
        {State, ?IDX_HEADER(_)} when Hint =:= chunk_boundary ->
            %% The reader has reached the section of the
            {reply, eof, State};
        {State, Data} ->
            {reply, {ok, Data}, State};
        eof ->
            {reply, eof, State0}
    end;
handle_call(Request, From, State) ->
    {stop, {unknown_call, From, Request}, State}.

handle_cast(close, State) ->
    {stop, normal, State};
handle_cast(Message, State) ->
    ?LOG_DEBUG(?MODULE_STRING " received unexpected cast: ~W", [Message, 10]),
    {noreply, State}.

handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) ->
    {stop, normal, State};
handle_info(Message, State) ->
    ?LOG_DEBUG(?MODULE_STRING " received unexpected message: ~W", [Message, 10]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

format_status(#{state := #?MODULE{buffer = Buffer} = State0} = Status0) ->
    %% Avoid formatting the buffer - it can be large.
    Size = <<(integer_to_binary(byte_size(Buffer)))/binary, " bytes">>,
    Status0#{state := State0#?MODULE{buffer = Size}}.

code_change(_, _, State) ->
    {ok, State}.

%%---------------------------------------------------------------------------

do_read(#?MODULE{index_start_pos = IdxStartPos}, Offset, _Bytes) when Offset >= IdxStartPos ->
    eof;
do_read(
    #?MODULE{
        object = Object,
        buffer = Buffer,
        read_size = ReadSize,
        offset_start = BufStart,
        offset_end = BufEnd
    } = State0,
    Offset,
    Bytes
) ->
    End = Offset + Bytes - 1,
    case (Offset >= BufStart) and (End =< BufEnd) of
        % Data is in buffer
        true ->
            OffsetInBuf = Offset - BufStart,
            {State0, binary:part(Buffer, OffsetInBuf, Bytes)};
        false ->
            ToRead = max(ReadSize, Bytes),
            {ok, NewBuffer} = rabbitmq_stream_s3_api:get_range(
                Object,
                {Offset, Offset + ToRead - 1}
            ),
            State = State0#?MODULE{
                buffer = NewBuffer,
                offset_start = Offset,
                offset_end = Offset + ToRead - 1
            },
            {State, binary:part(NewBuffer, 0, Bytes)}
    end.
