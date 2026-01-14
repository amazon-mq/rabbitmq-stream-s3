%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_log_reader).

-include_lib("kernel/include/logger.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-behaviour(osiris_log_reader).
-behaviour(gen_server).

-define(READAHEAD, "5MiB").
-define(READ_TIMEOUT, 10000).

-record(remote, {
    pid :: pid(),
    dir :: directory(),
    transport :: tcp | ssl,
    next_offset :: osiris:offset(),
    shared :: atomics:atomics_ref(),
    fragment :: osiris:offset(),
    position :: byte_offset(),
    filter :: osiris_bloom:mstate() | undefined,
    chunk_selector :: all | user_data
}).

-record(?MODULE, {
    config :: osiris_log_reader:config(),
    mode :: #remote{} | osiris_log:state()
}).

-record(remote_iterator, {
    next_offset :: osiris:offset(),
    data :: binary()
}).

-record(state, {
    connection :: rabbitmq_aws:connection_handle(),
    buffer = <<>> :: binary(),
    offset_start :: byte_offset() | undefined,
    offset_end :: byte_offset() | undefined,
    read_size :: pos_integer(),
    bucket :: binary(),
    object :: binary(),
    object_size :: pos_integer()
}).

%% osiris_log_reader
-export([
    init_offset_reader/2,
    next_offset/1,
    committed_offset/1,
    close/1,
    send_file/3,
    chunk_iterator/3,
    iterator_next/1
]).

%% gen_server
-export([
    start_link/3,
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

init_offset_reader(OffsetSpec, #{dir := Dir0} = Config0) ->
    init_offset_reader0(OffsetSpec, Config0#{dir := list_to_binary(Dir0)}).

init_offset_reader0(first, #{reference := StreamId, dir := Dir, shared := Shared} = Config) ->
    LocalFirstOffset = osiris_log_shared:first_chunk_id(Shared),
    case rabbitmq_stream_s3_log_manifest:get_manifest(StreamId, Dir) of
        #manifest{first_offset = RemoteFirstOffset} when RemoteFirstOffset < LocalFirstOffset ->
            ?LOG_DEBUG(
                "Attaching remote reader at first offset ~b pos ~b for spec 'first'",
                [RemoteFirstOffset, ?SEGMENT_HEADER_B]
            ),
            init_remote_reader(RemoteFirstOffset, ?SEGMENT_HEADER_B, RemoteFirstOffset, Config);
        _ ->
            init_local_reader(first, Config)
    end;
init_offset_reader0(Offset, #{reference := StreamId, dir := Dir} = Config) when
    is_integer(Offset)
->
    ?LOG_DEBUG(?MODULE_STRING ":~ts/2 finding offset ~b", [?FUNCTION_NAME, Offset]),
    case init_local_reader({abs, Offset}, Config) of
        {ok, _} = Ok ->
            Ok;
        {error, {offset_out_of_range, {_First, LastLocalOffset}}} when Offset > LastLocalOffset ->
            ?LOG_DEBUG("requested offset ~b is higher than last local offset ~b", [
                Offset, LastLocalOffset
            ]),
            %% TODO: try the remote tier? Or just attach to next?
            init_local_reader(next, Config);
        {error, {offset_out_of_range, Range}} ->
            ?LOG_DEBUG("offset ~b is not local (local range ~w), trying the remote tier", [
                Offset, Range
            ]),
            case rabbitmq_stream_s3_log_manifest:get_manifest(StreamId, Dir) of
                undefined ->
                    init_local_reader(next, Config);
                #manifest{first_offset = FirstOffset} when Offset < FirstOffset ->
                    %% Emulate osiris_log's behavior: attach at the beginning
                    %% of the stream.
                    init_remote_reader(FirstOffset, ?SEGMENT_HEADER_B, FirstOffset, Config);
                #manifest{} = Manifest ->
                    {ok, ChunkId, Position, Fragment} = find_fragment_for_offset(
                        Offset, Manifest, Dir
                    ),
                    init_remote_reader(Fragment, Position, ChunkId, Config)
            end;
        {error, _} = Err ->
            Err
    end;
init_offset_reader0(OffsetSpec, Config) ->
    %% TODO: implement the other offset specs
    init_local_reader(OffsetSpec, Config).

next_offset(#?MODULE{mode = #remote{next_offset = NextOffset}}) ->
    NextOffset;
next_offset(#?MODULE{mode = Local}) ->
    osiris_log:next_offset(Local).

committed_offset(#?MODULE{mode = #remote{shared = Shared}}) ->
    osiris_log_shared:committed_chunk_id(Shared);
committed_offset(#?MODULE{mode = Local}) ->
    osiris_log:committed_offset(Local).

close(#?MODULE{mode = #remote{pid = Pid}}) ->
    ok = gen_server:cast(Pid, close);
close(#?MODULE{mode = Local}) ->
    ok = osiris_log:close(Local).

send_file(Socket, #?MODULE{mode = #remote{} = Remote0} = State0, Callback) ->
    case read_header(Remote0) of
        {ok,
            #{
                chunk_id := ChId,
                num_records := NumRecords,
                position := Position,
                next_position := NextPosition,
                header_data := HeaderData
            } = Header,
            #remote{
                pid = Pid,
                transport = Transport,
                chunk_selector = ChunkSelector
            } = Remote1} ->
            {ToSkip, ToSend} = select_amount_to_send(ChunkSelector, Header),
            DataPos = Position + ?CHUNK_HEADER_B + ToSkip,
            PrefixData = Callback(Header, ToSend + byte_size(HeaderData)),
            {ok, Data} = gen_server:call(Pid, {read, DataPos, ToSend, within_chunk}),
            case send(Transport, Socket, [PrefixData, HeaderData, Data]) of
                ok ->
                    Remote = Remote1#remote{
                        next_offset = ChId + NumRecords,
                        position = NextPosition
                    },
                    {ok, State0#?MODULE{mode = Remote}};
                {error, _} = Err ->
                    Err
            end;
        {local, Remote} ->
            case convert_remote_to_local(State0#?MODULE{mode = Remote}) of
                {ok, State} ->
                    send_file(Socket, State, Callback);
                {error, _} = Err ->
                    Err
            end;
        {end_of_stream, Remote} ->
            {end_of_stream, State0#?MODULE{mode = Remote}}
    end;
send_file(Socket, #?MODULE{mode = Local0} = State0, Callback) ->
    case osiris_log:send_file(Socket, Local0, Callback) of
        {ok, Local} ->
            State = State0#?MODULE{mode = Local},
            {ok, State};
        {end_of_stream, Local} ->
            State = State0#?MODULE{mode = Local},
            {end_of_stream, State};
        {error, _} = Err ->
            Err
    end.

chunk_iterator(#?MODULE{mode = #remote{} = Remote0} = State0, Credit, _PrevIter) ->
    case read_header(Remote0) of
        {ok,
            #{
                chunk_id := ChId,
                num_records := NumRecords,
                position := Position,
                next_position := NextPosition,
                filter_size := FilterSize,
                data_size := DataSize
            } = Header,
            #remote{pid = Pid} = Remote1} ->
            DataPos = Position + ?CHUNK_HEADER_B + FilterSize,
            {ok, Data} = gen_server:call(Pid, {read, DataPos, DataSize, within_chunk}),
            Iter = #remote_iterator{
                next_offset = ChId,
                data = Data
            },
            Remote = Remote1#remote{
                next_offset = ChId + NumRecords,
                position = NextPosition
            },
            State = State0#?MODULE{mode = Remote},
            {ok, Header, Iter, State};
        {local, Remote} ->
            case convert_remote_to_local(State0#?MODULE{mode = Remote}) of
                {ok, State} ->
                    chunk_iterator(State, Credit, undefined);
                {error, _} = Err ->
                    Err
            end;
        {end_of_stream, Remote} ->
            {end_of_stream, State0#?MODULE{mode = Remote}}
    end;
chunk_iterator(#?MODULE{mode = Local0} = State0, Credit, PrevIter) ->
    case osiris_log:chunk_iterator(Local0, Credit, PrevIter) of
        {ok, Header, Iter, Local} ->
            State = State0#?MODULE{mode = Local},
            {ok, Header, Iter, State};
        {end_of_stream, Local} ->
            State = State0#?MODULE{mode = Local},
            {end_of_stream, State};
        {error, _} = Err ->
            Err
    end.

iterator_next(#remote_iterator{next_offset = NextOffset0, data = Data0} = Iter0) ->
    case Data0 of
        ?REC_MATCH_SIMPLE(Len, Rem0) ->
            <<Record:Len/binary, Rem/binary>> = Rem0,
            Iter = Iter0#remote_iterator{next_offset = NextOffset0 + 1, data = Rem},
            {{NextOffset0, Record}, Iter};
        ?REC_MATCH_SUBBATCH(CompType, NumRecs, UncompressedLen, Len, Rem0) ->
            <<BatchData:Len/binary, Rem/binary>> = Rem0,
            Record = {batch, NumRecs, CompType, UncompressedLen, BatchData},
            Iter = Iter0#remote_iterator{next_offset = NextOffset0 + NumRecs, data = Rem},
            {{NextOffset0, Record}, Iter};
        <<>> ->
            end_of_chunk
    end;
iterator_next(Local) ->
    osiris_log:iterator_next(Local).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

start_link(Reader, Bucket, Key) ->
    gen_server:start_link(?MODULE, {Reader, Bucket, Key}, []).

init({Reader, Bucket, Key}) ->
    erlang:monitor(process, Reader),
    {ok, Connection} = rabbitmq_aws:open_connection("s3", []),
    {ok, Size} = rabbitmq_stream_s3_api:get_object_size(
        Connection,
        Bucket,
        Key,
        [{timeout, ?READ_TIMEOUT}]
    ),
    {ok, ReadSize} =
        rabbit_resource_monitor_misc:parse_information_unit(?READAHEAD),
    {ok, #state{
        connection = Connection,
        bucket = Bucket,
        object = Key,
        object_size = Size,
        read_size = ReadSize
    }}.

handle_call({read, Offset, Bytes, Hint}, _From, State0) ->
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

terminate(_Reason, #state{connection = Connection}) ->
    ok = rabbitmq_aws:close_connection(Connection).

format_status(#{state := #state{buffer = Buffer} = State0} = Status0) ->
    %% Avoid formatting the buffer - it can be large.
    Size = <<(integer_to_binary(byte_size(Buffer)))/binary, " bytes">>,
    Status0#{state := State0#state{buffer = Size}}.

code_change(_, _, State) ->
    {ok, State}.

%%---------------------------------------------------------------------------
%% Helpers

%% fragment_key(File) ->
%%     [SegmentBasename, StreamName | _] = lists:reverse(filename:split(File)),
%%     Suffix = string:replace(SegmentBasename, ".segment", ".fragment", trailing),
%%     iolist_to_binary(["rabbitmq/stream/", StreamName, "/data/", Suffix]).

send(tcp, Socket, Data) ->
    gen_tcp:send(Socket, Data);
send(ssl, Socket, Data) ->
    ssl:send(Socket, Data).

do_read(#state{object_size = Size}, Offset, _Bytes) when Offset >= Size ->
    %% TODO: store the segment data size in the prelude of a fragment so that
    %% we know eof accurately. This branch is never hit, currently, because we
    %% store the index at the end of the fragment.
    eof;
do_read(
    #state{
        connection = Connection,
        bucket = Bucket,
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
            {ok, NewBuffer} = rabbitmq_stream_s3_api:get_object_with_range(
                Connection,
                Bucket,
                Object,
                {Offset, Offset + ToRead - 1},
                [{timeout, ?READ_TIMEOUT}]
            ),
            rabbitmq_stream_s3_counters:read_bytes(byte_size(NewBuffer)),
            State = State0#state{
                buffer = NewBuffer,
                offset_start = Offset,
                offset_end = Offset + ToRead - 1
            },
            {State, binary:part(NewBuffer, 0, Bytes)}
    end.

%% This helper is mostly the same as osiris_log:read_header0/1. There are some
%% simplifications:
%% * Always over-read the chunk header so that we also read the chunk filter in
%%   a single read operation.
%% * Check the chunk selector within this helper. osiris_log does this in the
%%   callers, but we can always skip when the chunk selector doesn't match.
-spec read_header(#remote{}) ->
    {ok, osiris_log:header_map(), #remote{}}
    | {local, #remote{}}
    | {end_of_stream, #remote{}}.
read_header(#remote{shared = Shared, next_offset = NextOffset} = Remote) ->
    CanReadNext =
        osiris_log_shared:last_chunk_id(Shared) >= NextOffset andalso
            osiris_log_shared:committed_chunk_id(Shared) >= NextOffset,
    case CanReadNext of
        true ->
            read_header1(Remote);
        false ->
            {end_of_stream, Remote}
    end.

read_header1(
    #remote{
        pid = Pid0,
        dir = Dir,
        position = Position,
        next_offset = NextChId,
        shared = Shared
    } = Remote0
) ->
    %% Over-read the chunk header so that the filter (if it exists) is always
    %% included in the binary. Reading from the remote tier takes time, so
    %% over-reading is faster.
    %% TODO: make sure that over-reading is handled gracefully: as much of the
    %% binary should be returned as possible.
    case
        gen_server:call(
            Pid0,
            {read, Position, ?CHUNK_HEADER_B + ?MAX_FILTER_SIZE, chunk_boundary},
            infinity
        )
    of
        {ok, Header} ->
            read_header2(Remote0, Header);
        eof ->
            FirstChId = osiris_log_shared:first_chunk_id(Shared),
            LastChId = osiris_log_shared:last_chunk_id(Shared),
            case NextChId of
                FirstChId ->
                    {local, Remote0};
                _ when NextChId > LastChId ->
                    {end_of_stream, Remote0};
                _ ->
                    {ok, Bucket} = application:get_env(rabbitmq_stream_s3, bucket),
                    %% TODO: what if retention takes away fragments? We need to
                    %% jump ahead to the start of the log.
                    Key = rabbitmq_stream_s3_log_manifest:fragment_key(Dir, NextChId),
                    case rabbitmq_stream_s3_log_reader_sup:add_child(self(), Bucket, Key) of
                        {ok, Pid} ->
                            ok = gen_server:cast(Pid0, close),
                            Remote = Remote0#remote{
                                pid = Pid,
                                fragment = NextChId,
                                position = ?SEGMENT_HEADER_B
                            },
                            read_header1(Remote);
                        {error, not_found} ->
                            {end_of_stream, Remote0}
                    end
            end
    end.

read_header2(
    #remote{
        next_offset = NextChId0,
        position = Position0,
        chunk_selector = ChunkSelector,
        filter = Filter0
    } = Remote0,
    HeaderBin
) ->
    <<HeaderBin1:?CHUNK_HEADER_B/binary, _/binary>> = HeaderBin,
    {ok, Header} = osiris_log:parse_header(HeaderBin1, Position0),
    #{
        type := ChunkType,
        chunk_id := NextChId0,
        num_records := NumRecords,
        next_position := NextPosition,
        filter_size := FilterSize
    } = Header,
    case is_chunk_selected(ChunkSelector, ChunkType) of
        true ->
            ChunkFilter = binary:part(HeaderBin, ?CHUNK_HEADER_B, FilterSize),
            case is_bloom_match(ChunkFilter, Filter0) of
                true ->
                    {ok, Header, Remote0};
                false ->
                    %% skip and recurse
                    Remote = Remote0#remote{
                        next_offset = NextChId0 + NumRecords,
                        position = NextPosition
                    },
                    read_header(Remote)
            end;
        false ->
            %% skip and recurse
            Remote = Remote0#remote{
                next_offset = NextChId0 + NumRecords,
                position = NextPosition
            },
            read_header(Remote)
    end.

is_bloom_match(ChunkFilter, Filter0) ->
    case osiris_bloom:is_match(ChunkFilter, Filter0) of
        true ->
            true;
        false ->
            false;
        {retry_with, Filter} ->
            is_bloom_match(ChunkFilter, Filter)
    end.

is_chunk_selected(all, _ChunkType) ->
    true;
is_chunk_selected(user_data, ?CHNK_USER) ->
    true;
is_chunk_selected(_ChunkSelector, _ChunkType) ->
    false.

select_amount_to_send(user_data, #{
    type := ?CHNK_USER,
    filter_size := FilterSize,
    data_size := DataSize
}) ->
    {FilterSize, DataSize};
select_amount_to_send(_ChunkSelector, #{
    filter_size := FilterSize,
    data_size := DataSize,
    trailer_size := TrailerSize
}) ->
    {FilterSize, DataSize + TrailerSize}.

init_local_reader(OffsetSpec, Config) ->
    case osiris_log:init_offset_reader(OffsetSpec, Config) of
        {ok, Local} ->
            {ok, #?MODULE{config = Config, mode = Local}};
        {error, _} = Err ->
            Err
    end.

init_remote_reader(
    Fragment,
    Position,
    Offset,
    #{dir := Dir, options := Options, shared := Shared} = Config
) ->
    Filter =
        case Options of
            #{filter_spec := FilterSpec} ->
                osiris_bloom:init_matcher(FilterSpec);
            _ ->
                undefined
        end,
    {ok, Bucket} = application:get_env(rabbitmq_stream_s3, bucket),
    Key = rabbitmq_stream_s3_log_manifest:fragment_key(Dir, Fragment),
    case rabbitmq_stream_s3_log_reader_sup:add_child(self(), Bucket, Key) of
        {ok, Pid} ->
            Reader = #?MODULE{
                config = Config,
                mode = #remote{
                    pid = Pid,
                    dir = Dir,
                    transport = maps:get(transport, Options, tcp),
                    next_offset = Offset,
                    shared = Shared,
                    fragment = Fragment,
                    position = Position,
                    filter = Filter,
                    chunk_selector = maps:get(chunk_selector, Options, user_data)
                }
            },
            {ok, Reader};
        {error, _} = Err ->
            Err
    end.

convert_remote_to_local(#?MODULE{
    config = Config,
    mode = #remote{pid = Pid, next_offset = NextOffset}
}) ->
    ?LOG_DEBUG("Converting to local reader at offset ~b", [NextOffset]),
    ok = gen_server:cast(Pid, close),
    init_local_reader(first, Config).

%% TODO: make this generic for timestamps too.
-spec find_fragment_for_offset(
    osiris:offset(),
    #manifest{} | rabbitmq_stream_s3_binary_array:array(),
    directory()
) ->
    {ok, ChunkId :: osiris:offset(), byte_offset(), Fragment :: osiris:offset()}.
find_fragment_for_offset(Offset, #manifest{entries = Entries}, Dir) ->
    find_fragment_for_offset(Offset, Entries, Dir);
find_fragment_for_offset(Offset, Entries, Dir) ->
    RootIdx0 =
        rabbitmq_stream_s3_binary_array:partition_point(
            fun(?ENTRY(O, _T, _K, _S, _N, _)) -> Offset > O end,
            ?ENTRY_B,
            Entries
        ),
    RootIdx = saturating_decr(RootIdx0),
    ?ENTRY(EntryOffset, _, Kind, _, _, _) = rabbitmq_stream_s3_binary_array:at(
        RootIdx, ?ENTRY_B, Entries
    ),
    ?LOG_DEBUG("partition-point ~b for offset ~b is entry ~b", [
        RootIdx,
        Offset,
        EntryOffset
    ]),
    case Kind of
        ?MANIFEST_KIND_FRAGMENT ->
            ?LOG_DEBUG("Searching for offset ~b within fragment ~b", [Offset, EntryOffset]),
            %% TODO: pass in size now that we have it.
            {ok, #fragment_info{index_start_pos = IdxStartPos}} = rabbitmq_stream_s3_log_manifest:get_fragment_trailer(
                Dir, EntryOffset
            ),
            Index = index_data(Dir, EntryOffset, IdxStartPos),
            IndexIdx0 =
                rabbitmq_stream_s3_binary_array:partition_point(
                    fun(?INDEX_RECORD(O, _T, _P)) -> Offset >= O end,
                    ?INDEX_RECORD_B,
                    Index
                ),
            IndexIdx = saturating_decr(IndexIdx0),
            ?INDEX_RECORD(ChunkId, _, Pos) =
                rabbitmq_stream_s3_binary_array:at(IndexIdx, ?INDEX_RECORD_B, Index),
            ?LOG_DEBUG(
                "partition-point ~b for offset ~b is chunk id ~b, pos ~b", [
                    IndexIdx,
                    Offset,
                    ChunkId,
                    Pos
                ]
            ),
            ?LOG_DEBUG("Attaching to offset ~b, pos ~b, fragment ~b", [Offset, Pos, EntryOffset]),
            {ok, ChunkId, Pos, EntryOffset};
        _ ->
            %% Download the group and search recursively within that.
            ?LOG_DEBUG("Entry is not a fragment. Searching within group ~b kind ~b", [
                EntryOffset, Kind
            ]),
            <<
                _:4/binary,
                _:32,
                EntryOffset:64/unsigned,
                _:64,
                0:2/unsigned,
                _:70,
                GroupEntries/binary
            >> = get_group(Dir, Kind, EntryOffset),
            find_fragment_for_offset(Offset, GroupEntries, Dir)
    end.

saturating_decr(0) -> 0;
saturating_decr(N) -> N - 1.

index_data(Dir, FragmentOffset, StartPos) ->
    {ok, Bucket} = application:get_env(rabbitmq_stream_s3, bucket),
    {ok, Handle} = rabbitmq_aws:open_connection("s3"),
    Key = rabbitmq_stream_s3_log_manifest:fragment_key(Dir, FragmentOffset),
    ?LOG_DEBUG("Looking up key ~ts (~ts)", [Key, ?FUNCTION_NAME]),
    try
        {ok, Data} = rabbitmq_stream_s3_api:get_object_with_range(
            Handle, Bucket, Key, {StartPos, undefined}, [{timeout, ?READ_TIMEOUT}]
        ),
        binary:part(Data, 0, byte_size(Data) - ?FRAGMENT_TRAILER_B)
    after
        ok = rabbitmq_aws:close_connection(Handle)
    end.

get_group(Dir, Kind, GroupOffset) ->
    {ok, Bucket} = application:get_env(rabbitmq_stream_s3, bucket),
    {ok, Handle} = rabbitmq_aws:open_connection("s3"),
    Key = rabbitmq_stream_s3_log_manifest:group_key(Dir, Kind, GroupOffset),
    ?LOG_DEBUG("Looking up key ~ts (~ts)", [Key, ?FUNCTION_NAME]),
    try
        {ok, Data} = rabbitmq_stream_s3_api:get_object(
            Handle, Bucket, Key, [{timeout, ?READ_TIMEOUT}]
        ),
        <<
            _Magic:4/binary,
            _Vsn:32/unsigned,
            GroupOffset:64/unsigned,
            _FirstTimestamp:64/unsigned,
            0:2/unsigned,
            _TotalSize:70/unsigned,
            _GroupEntries/binary
        >> = Data,
        Data
    after
        ok = rabbitmq_aws:close_connection(Handle)
    end.
