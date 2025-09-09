%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_log_reader).

-include_lib("kernel/include/logger.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-behaviour(osiris_log_reader).
-behaviour(gen_server).

-define(READAHEAD, "5MiB").
-define(READ_TIMEOUT, 10000).

-record(state, {
    connection_handle,
    buf,
    offset_start,
    offset_end,
    read_ahead,
    bucket,
    object,
    object_size
}).

-type state() :: {local, file:io_device()} | {remote, pid()}.

-export_type([state/0]).

%% osiris_log_reader
-export([open/1, pread/4, sendfile/5, close/1]).

%% gen_server
-export([
    start_link/2,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    format_status/1,
    code_change/3
]).

%%%===================================================================
%%% osiris_log_reader callbacks
%%%===================================================================

open(SegmentFile) ->
    case osiris_log_reader:open(SegmentFile) of
        {ok, Fd} ->
            {ok, {local, Fd}};
        {error, enoent} ->
            {ok, Bucket} = application:get_env(rabbitmq_stream_s3, bucket),
            Key = fragment_key(SegmentFile),
            case rabbitmq_stream_s3_log_reader_sup:add_child(Bucket, Key) of
                {ok, Pid} ->
                    {ok, {remote, Pid}};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

pread({local, Fd} = Reader, Offset, Bytes, Hint) ->
    case osiris_log_reader:pread(Fd, Offset, Bytes, Hint) of
        {ok, Data, _} ->
            {ok, Data, Reader};
        eof ->
            eof;
        {error, _} = Err ->
            Err
    end;
pread({remote, _} = Reader, Offset, Bytes, within) ->
    pread_remote(Reader, Offset, Bytes);
pread({remote, _} = Reader, Offset, Bytes, boundary) ->
    case pread_remote(Reader, Offset, Bytes) of
        %% This chunk boundary is not a chunk boundary at all - it's the start
        %% of the index.
        {ok, <<?REMOTE_IDX_MAGIC, _Vsn:32/unsigned, _/binary>>, _} ->
            eof;
        Result ->
            Result
    end.

pread_remote({remote, Remote} = Reader, Offset, Bytes) ->
    case gen_server:call(Remote, {pread, Offset, Bytes}, infinity) of
        {ok, Data} ->
            {ok, Data, Reader};
        eof ->
            eof;
        {error, _} = Err ->
            Err
    end.

sendfile(Transport, {local, Fd} = Reader, Socket, Offset, Bytes) ->
    case osiris_log_reader:sendfile(Transport, Fd, Socket, Offset, Bytes) of
        {ok, _} ->
            {ok, Reader};
        {error, _} = Err ->
            Err
    end;
sendfile(Transport, {remote, Remote} = Reader, Socket, Offset, Bytes) ->
    case gen_server:call(Remote, {pread, Offset, Bytes}, infinity) of
        {ok, Data} ->
            ok = send(Transport, Socket, Data),
            {ok, Reader};
        {error, _} = Err ->
            Err
    end.

close({local, Fd}) ->
    osiris_log_reader:close(Fd);
close({remote, Pid}) ->
    gen_server:cast(Pid, close).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

start_link(Bucket, Key) ->
    gen_server:start_link(?MODULE, {Bucket, Key}, []).

init({Bucket, Key}) ->
    {ok, Handle} = rabbitmq_aws:open_connection("s3", []),
    Size = rabbitmq_stream_s3_api:get_object_size(
        Handle,
        Bucket,
        Key,
        [{timeout, ?READ_TIMEOUT}]
    ),
    {ok, ReadAhead} =
        rabbit_resource_monitor_misc:parse_information_unit(?READAHEAD),
    {ok, #state{
        connection_handle = Handle,
        %% NOTE: the fragment file may not be read from the beginning (for
        %% example to skip the header) so we shouldn't eagerly pre-read from
        %% the beginning here.
        buf = <<>>,
        bucket = Bucket,
        object = Key,
        object_size = Size,
        read_ahead = ReadAhead
    }}.

handle_call({pread, Offset, Bytes}, _From, State0) ->
    %% TODO: while reading, start a request for the next range of data when
    %% we near the end of the current section.
    case do_pread(State0, Offset, Bytes) of
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

handle_info(Message, State) ->
    ?LOG_DEBUG(?MODULE_STRING " received unexpected message: ~W", [Message, 10]),
    {noreply, State}.

terminate(_Reason, #state{connection_handle = Handle}) ->
    ok = rabbitmq_aws:close_connection(Handle).

format_status(#{state := #state{buf = Buf} = State0} = Status0) ->
    %% Avoid formatting the buffer - it can be large.
    Size = lists:flatten(io_lib:format("~b bytes", [byte_size(Buf)])),
    Status0#{state := State0#state{buf = Size}}.

code_change(_, _, State) ->
    {ok, State}.

%%---------------------------------------------------------------------------
%% Helpers

fragment_key(File) ->
    [SegmentBasename, StreamName | _] = lists:reverse(filename:split(File)),
    Suffix = string:replace(SegmentBasename, ".segment", ".fragment", trailing),
    iolist_to_binary(["rabbitmq/stream/", StreamName, "/data/", Suffix]).

send(tcp, Socket, Data) ->
    gen_tcp:send(Socket, Data);
send(ssl, Socket, Data) ->
    ssl:send(Socket, Data).

do_pread(#state{object_size = Size}, Offset, _Bytes) when Offset >= Size ->
    eof;
do_pread(
    #state{
        connection_handle = Handle,
        bucket = Bucket,
        object = Object,
        buf = Buf,
        read_ahead = ReadAhead,
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
            {State0, binary:part(Buf, OffsetInBuf, Bytes)};
        false ->
            ToRead = max(ReadAhead, Bytes),
            {ok, NewBuf} = rabbitmq_stream_s3_api:get_object_with_range(
                Handle,
                Bucket,
                Object,
                {Offset, Offset + ToRead - 1},
                [{timeout, ?READ_TIMEOUT}]
            ),
            rabbitmq_stream_s3_counters:read_bytes(byte_size(NewBuf)),
            State = State0#state{
                buf = NewBuf,
                offset_start = Offset,
                offset_end = Offset + ToRead - 1
            },
            {State, binary:part(NewBuf, 0, Bytes)}
    end.
