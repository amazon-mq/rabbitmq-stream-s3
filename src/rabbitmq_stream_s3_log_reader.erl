%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_log_reader).
-moduledoc """
Functions for reading from either the remote or local tiers.

This is an implementation of the `osiris_log_reader` behaviour which resolves
offset specs to absolute positions and reads forwards through the stream data.
If the data can be found locally, this module delegates to `osiris_log`.
Otherwise this module spawns a gen_server which fetches data from the remote
tier.
""".

-include_lib("kernel/include/logger.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-behaviour(osiris_log_reader).
-behaviour(gen_server).

-define(READAHEAD, "5MiB").
-define(READ_TIMEOUT, 10000).

-record(remote, {
    pid :: pid(),
    stream :: stream_id(),
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
    connection :: rabbitmq_stream_s3_api:connection(),
    buffer = <<>> :: binary(),
    offset_start :: byte_offset() | undefined,
    offset_end :: byte_offset() | undefined,
    read_size :: pos_integer(),
    object :: binary(),
    segment_data_size :: pos_integer(),
    next_fragment_offset :: osiris:offset()
}).

%% osiris_log_reader
-export([
    init_offset_reader/2,
    next_offset/1,
    committed_chunk_id/1,
    committed_offset/1,
    close/1,
    send_file/3,
    chunk_iterator/3,
    iterator_next/1
]).

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

init_offset_reader(Tail, Config) when Tail =:= last orelse Tail =:= next ->
    init_local_reader(Tail, Config);
init_offset_reader(first, #{name := StreamId, shared := Shared} = Config) ->
    LocalFirstOffset = osiris_log_shared:first_chunk_id(Shared),
    case rabbitmq_stream_s3_server:get_manifest(StreamId) of
        #manifest{first_offset = RemoteFirstOffset} when RemoteFirstOffset < LocalFirstOffset ->
            ?LOG_DEBUG(
                "Attaching remote reader at first offset ~b pos ~b for spec 'first'",
                [RemoteFirstOffset, ?SEGMENT_HEADER_B]
            ),
            init_remote_reader(RemoteFirstOffset, ?SEGMENT_HEADER_B, RemoteFirstOffset, Config);
        _ ->
            init_local_reader(first, Config)
    end;
init_offset_reader(Offset, #{name := StreamId, shared := Shared} = Config) when
    is_integer(Offset)
->
    ?LOG_DEBUG(?MODULE_STRING ":~ts/2 finding offset ~b for stream '~ts'", [
        ?FUNCTION_NAME, Offset, StreamId
    ]),
    FirstChunkId = osiris_log_shared:first_chunk_id(Shared),
    case Offset >= FirstChunkId of
        true ->
            ?LOG_DEBUG(
                "Offset ~b is in the local tier of stream '~ts' (start ~b), using a local reader", [
                    Offset, StreamId, FirstChunkId
                ]
            ),
            init_local_reader(Offset, Config);
        false ->
            ?LOG_DEBUG(
                "Offset ~b of stream '~ts' is not local (start ~b), trying the remote tier", [
                    Offset, StreamId, FirstChunkId
                ]
            ),
            case rabbitmq_stream_s3_server:get_manifest(StreamId) of
                undefined ->
                    init_local_reader(next, Config);
                #manifest{first_offset = FirstOffset} when Offset < FirstOffset ->
                    %% Emulate osiris_log's behavior: attach at the beginning
                    %% of the stream.
                    init_remote_reader(FirstOffset, ?SEGMENT_HEADER_B, FirstOffset, Config);
                #manifest{entries = Entries} ->
                    {ok, ChunkId, Position, Fragment} = find_position(
                        {offset, Offset},
                        Entries,
                        StreamId
                    ),
                    init_remote_reader(Fragment, Position, ChunkId, Config)
            end
    end;
init_offset_reader({timestamp, Ts} = Spec, #{name := StreamId} = Config) ->
    ?LOG_DEBUG(?MODULE_STRING ":~ts/2 finding timestamp ~b for stream '~ts'", [
        ?FUNCTION_NAME, Ts, StreamId
    ]),
    %% We can't cheaply query the first timestamp from `osiris_log_shared`.
    %% Instead try the remote tier first.
    case rabbitmq_stream_s3_server:get_manifest(StreamId) of
        #manifest{first_offset = FirstOffset, first_timestamp = FirstTs} when Ts < FirstTs ->
            init_remote_reader(FirstOffset, ?SEGMENT_HEADER_B, FirstOffset, Config);
        #manifest{entries = Entries} ->
            case rabbitmq_stream_s3_array:last(?ENTRY_B, Entries) of
                ?ENTRY(_O, _FTs, LTs, _, _) when LTs >= Ts ->
                    {ok, ChunkId, Position, Fragment} = find_position(
                        {timestamp, Ts},
                        Entries,
                        StreamId
                    ),
                    init_remote_reader(Fragment, Position, ChunkId, Config);
                _ ->
                    init_local_reader(Spec, Config)
            end;
        undefined ->
            init_local_reader(Spec, Config)
    end;
init_offset_reader({abs, Offset} = Spec, #{name := StreamId} = Config) ->
    case init_local_reader(Spec, Config) of
        {ok, _} = Ok ->
            Ok;
        {error, {offset_out_of_range, empty}} = Err ->
            Err;
        {error, {offset_out_of_range, {_LocalFirst, LocalLast}}} = Err ->
            case rabbitmq_stream_s3_server:get_manifest(StreamId) of
                #manifest{first_offset = RemoteFirst, entries = Entries} ->
                    case RemoteFirst >= Offset of
                        true ->
                            {ok, ChunkId, Position, Fragment} = find_position(
                                {offset, Offset},
                                Entries,
                                StreamId
                            ),
                            init_remote_reader(Fragment, Position, ChunkId, Config);
                        false ->
                            {error, {offset_out_of_range, {RemoteFirst, LocalLast}}}
                    end;
                undefined ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

next_offset(#?MODULE{mode = #remote{next_offset = NextOffset}}) ->
    NextOffset;
next_offset(#?MODULE{mode = Local}) ->
    osiris_log:next_offset(Local).

committed_chunk_id(#?MODULE{mode = #remote{shared = Shared}}) ->
    osiris_log_shared:committed_chunk_id(Shared);
committed_chunk_id(#?MODULE{mode = Local}) ->
    osiris_log:committed_chunk_id(Local).

committed_offset(#?MODULE{mode = #remote{shared = Shared}}) ->
    osiris_log_shared:committed_offset(Shared);
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

start_link(Reader, Key) ->
    gen_server:start_link(?MODULE, {Reader, Key}, []).

init({Reader, Key}) ->
    erlang:monitor(process, Reader),
    {ok, Conn} = rabbitmq_stream_s3_api:open(),
    {ok, #fragment_info{size = SegmentDataSize, next_offset = NextOffset}} = rabbitmq_stream_s3_server:get_fragment_trailer(
        Key
    ),
    {ok, ReadSize} =
        rabbit_resource_monitor_misc:parse_information_unit(?READAHEAD),
    {ok, #state{
        connection = Conn,
        object = Key,
        segment_data_size = SegmentDataSize,
        read_size = ReadSize,
        next_fragment_offset = NextOffset
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
    ok = rabbitmq_stream_s3_api:close(Connection).

format_status(#{state := #state{buffer = Buffer} = State0} = Status0) ->
    %% Avoid formatting the buffer - it can be large.
    Size = <<(integer_to_binary(byte_size(Buffer)))/binary, " bytes">>,
    Status0#{state := State0#state{buffer = Size}}.

code_change(_, _, State) ->
    {ok, State}.

%%---------------------------------------------------------------------------
%% Helpers

send(tcp, Socket, Data) ->
    gen_tcp:send(Socket, Data);
send(ssl, Socket, Data) ->
    ssl:send(Socket, Data).

do_read(#state{segment_data_size = Size}, Offset, _Bytes) when Offset >= Size + ?SEGMENT_HEADER_B ->
    eof;
do_read(
    #state{
        connection = Connection,
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
                Connection,
                Object,
                {Offset, Offset + ToRead - 1}
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
        stream = StreamId,
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
                    %% TODO: what if retention takes away fragments? We need to
                    %% jump ahead to the start of the log.
                    Key = rabbitmq_stream_s3:fragment_key(StreamId, NextChId),
                    case rabbitmq_stream_s3_log_reader_sup:add_child(self(), Key) of
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
    #{name := StreamId, options := Options, shared := Shared} = Config
) ->
    Filter =
        case Options of
            #{filter_spec := FilterSpec} ->
                osiris_bloom:init_matcher(FilterSpec);
            _ ->
                undefined
        end,
    Key = rabbitmq_stream_s3:fragment_key(StreamId, Fragment),
    case rabbitmq_stream_s3_log_reader_sup:add_child(self(), Key) of
        {ok, Pid} ->
            Reader = #?MODULE{
                config = Config,
                mode = #remote{
                    pid = Pid,
                    stream = StreamId,
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

-spec find_position(
    {offset, osiris:offset()} | {timestamp, osiris:timestamp()},
    rabbitmq_stream_s3:entries(),
    stream_id()
) ->
    {ok, ChunkId :: osiris:offset(), byte_offset(), Fragment :: osiris:offset()}.
find_position(Spec, Entries, StreamId) ->
    Fragment = find_fragment(Entries, Spec, get_group_fun(StreamId)),
    find_position0(Spec, Fragment, StreamId).

-doc """
Finds the offset of the manifest which contains the requested offset or
timestamp.

This scans the entries array in logarithmic time. If the offset/timestamp
being searched for is within a group, the group will be fetched with `GetGroup`
and then searched recursively.
""".
-spec find_fragment(
    rabbitmq_stream_s3:entries(),
    {offset, osiris:offset()} | {timestamp, osiris:timestamp()},
    fun(
        (rabbitmq_stream_s3:uid(), rabbitmq_stream_s3:kind(), osiris:offset()) ->
            rabbitmq_stream_s3:entries()
    )
) -> Fragment :: osiris:offset().
find_fragment(Entries, Spec, GetGroup) ->
    PartitionPredicate =
        case Spec of
            {offset, Offset} ->
                fun(?ENTRY(O, _FTs, _LTs, _K, _)) -> Offset >= O end;
            {timestamp, Ts} ->
                fun(?ENTRY(_O, FTs, _LTs, _K, _)) -> Ts >= FTs end
        end,
    Idx0 = rabbitmq_stream_s3_array:partition_point(
        PartitionPredicate,
        ?ENTRY_B,
        Entries
    ),
    Idx = saturating_decr(Idx0),
    case rabbitmq_stream_s3_array:at(Idx, ?ENTRY_B, Entries) of
        ?FRAGMENT(EntryOffset, _FTs, _LTs, _Sq, _Sz, _) ->
            EntryOffset;
        ?GROUP(GroupOffset, _FTs, _LTs, Kind, Uid, _) ->
            %% Download the group and search recursively within that.
            ?LOG_DEBUG("Entry is not a fragment. Searching within group ~b kind ~b", [
                GroupOffset, Kind
            ]),
            GroupEntries = GetGroup(Uid, Kind, GroupOffset),
            find_fragment(GroupEntries, Spec, GetGroup)
    end.

find_position0(Spec, Fragment, StreamId) ->
    %% TODO: pass in size now that we have it.
    {ok, #fragment_info{index_start_pos = IdxStartPos}} = rabbitmq_stream_s3_server:get_fragment_trailer(
        StreamId,
        Fragment
    ),
    IndexData = index_data(StreamId, Fragment, IdxStartPos),
    {ChunkId, _, Pos} = find_index_position(IndexData, Spec),
    {ok, ChunkId, Pos, Fragment}.

find_index_position(IndexData, Spec) ->
    %% Osiris prefers different chunk boundaries for offset and timestamp
    %% lookups. If the requested offset is between two chunk IDs, Osiris
    %% resolves the offset as the earlier/lesser chunk ID. If the requested
    %% timestamp is between two chunk IDs, Osiris resolves the timestamp as
    %% the later/greater chunk ID.
    PartitionPredicate =
        case Spec of
            {offset, Offset} ->
                fun(?INDEX_RECORD(O, _T, _P)) -> Offset >= O end;
            {timestamp, Ts} ->
                fun(?INDEX_RECORD(_O, T, _P)) -> Ts > T end
        end,
    Idx0 =
        rabbitmq_stream_s3_array:partition_point(
            PartitionPredicate,
            ?INDEX_RECORD_B,
            IndexData
        ),
    ?INDEX_RECORD(ChunkId, ChunkTs, Pos) =
        case Spec of
            {offset, _} ->
                Idx = saturating_decr(Idx0),
                rabbitmq_stream_s3_array:at(Idx, ?INDEX_RECORD_B, IndexData);
            {timestamp, _} ->
                case rabbitmq_stream_s3_array:try_at(Idx0, ?INDEX_RECORD_B, IndexData) of
                    undefined ->
                        rabbitmq_stream_s3_array:last(?INDEX_RECORD_B, IndexData);
                    Record ->
                        Record
                end
        end,
    {ChunkId, ChunkTs, Pos}.

saturating_decr(0) -> 0;
saturating_decr(N) -> N - 1.

index_data(StreamId, FragmentOffset, StartPos) ->
    {ok, Conn} = rabbitmq_stream_s3_api:open(),
    Key = rabbitmq_stream_s3:fragment_key(StreamId, FragmentOffset),
    ?LOG_DEBUG("Looking up key ~ts (~ts)", [Key, ?FUNCTION_NAME]),
    try
        {ok, Data} = rabbitmq_stream_s3_api:get_range(
            Conn,
            Key,
            {StartPos, undefined},
            #{timeout => ?READ_TIMEOUT}
        ),
        binary:part(Data, 0, byte_size(Data) - ?FRAGMENT_TRAILER_B)
    after
        ok = rabbitmq_stream_s3_api:close(Conn)
    end.

get_group_fun(StreamId) ->
    fun(Uid, Kind, Offset) ->
        get_group(StreamId, Uid, Kind, Offset)
    end.

get_group(StreamId, Uid, Kind, GroupOffset) ->
    {ok, Conn} = rabbitmq_stream_s3_api:open(),
    Key = rabbitmq_stream_s3:group_key(StreamId, Uid, Kind, GroupOffset),
    ?LOG_DEBUG("Looking up key ~ts (~ts)", [Key, ?FUNCTION_NAME]),
    try
        {ok, Data} = rabbitmq_stream_s3_api:get(Conn, Key, #{timeout => ?READ_TIMEOUT}),
        <<
            _Magic:4/binary,
            _Vsn:32/unsigned,
            GroupOffset:64/unsigned,
            _FirstTimestamp:64/unsigned,
            0:2/unsigned,
            _TotalSize:70/unsigned,
            GroupEntries/binary
        >> = Data,
        GroupEntries
    after
        ok = rabbitmq_stream_s3_api:close(Conn)
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

find_fragment_test() ->
    Ts = erlang:system_time(millisecond),
    Size = 200,
    FragmentEntries = <<
        ?FRAGMENT(
            %% Fragments every 20 offsets, 0..=2000
            (N * 20),
            %% Timestamps between 2000ms ago and `Ts`
            (Ts - 2000 + N * 20),
            (Ts - 2000 + (N + 1) * 20),
            0,
            Size
        )
     || N <- lists:seq(0, 100)
    >>,

    FindFragment1 = fun(Spec) ->
        find_fragment(
            FragmentEntries,
            Spec,
            %% There are only fragments in this manifest.
            fun(_, _, _) -> erlang:error(unimplemented) end
        )
    end,
    %% Offsets:
    ?assertEqual(0, FindFragment1({offset, 0})),
    ?assertEqual(40, FindFragment1({offset, 50})),
    ?assertEqual(100, FindFragment1({offset, 100})),
    ?assertEqual(2_000, FindFragment1({offset, 10_000})),
    %% Timestamps:
    ?assertEqual(0, FindFragment1({timestamp, Ts - 2000})),
    %% When placed between two chunks timestamp search prefers the later chunk.
    %% But when searching in fragments we want to prefer the earlier fragment
    %% since it most likely contains the target timestamp.
    ?assertEqual(40, FindFragment1({timestamp, Ts - 2000 + 50})),
    ?assertEqual(100, FindFragment1({timestamp, Ts - 2000 + 100})),
    ?assertEqual(2_000, FindFragment1({timestamp, Ts - 2000 + 10_000})),

    %% Factor out those fragments into a group.
    NextFragmentEntries = <<
        ?FRAGMENT((N * 20), (Ts - 2000 + N * 20), (Ts - 2000 + (N + 1) * 20), 0, Size)
     || N <- lists:seq(101, 150)
    >>,
    GroupUid = rabbitmq_stream_s3:uid(),
    Entries = ?GROUP(
        0,
        (Ts - 2000),
        Ts,
        ?MANIFEST_KIND_GROUP,
        GroupUid,
        NextFragmentEntries
    ),
    GetGroup = fun(Uid, Kind, Offset) ->
        ?assertEqual(GroupUid, Uid),
        ?assertEqual(?MANIFEST_KIND_GROUP, Kind),
        ?assertEqual(0, Offset),
        FragmentEntries
    end,
    FindFragment2 = fun(Spec) ->
        find_fragment(Entries, Spec, GetGroup)
    end,
    %% The new fragments can be found normally.
    ?assertEqual(2040, FindFragment2({offset, 2050})),
    ?assertEqual(3000, FindFragment2({offset, 10_000})),
    %% The group's fragments can be found recursively.
    ?assertEqual(0, FindFragment2({offset, 0})),
    ?assertEqual(40, FindFragment2({offset, 50})),
    ?assertEqual(40, FindFragment2({timestamp, Ts - 2000 + 50})),

    ok.

find_index_position_test() ->
    Ts = erlang:system_time(millisecond),
    IndexData = <<?INDEX_RECORD((N * 20), (Ts - 60 * 20 + N * 20), N) || N <- lists:seq(0, 60)>>,
    FindPosition = fun(Spec) ->
        {ChunkId, _Ts, _Pos} = find_index_position(IndexData, Spec),
        ChunkId
    end,
    %% See `find_index_position/2`. Osiris prefers different chunk IDs for
    %% offsets compared to timestamps.
    ?assertEqual(0, FindPosition({offset, 0})),
    ?assertEqual(40, FindPosition({offset, 40})),
    ?assertEqual(40, FindPosition({offset, 50})),

    ?assertEqual(0, FindPosition({timestamp, Ts - 60 * 20})),
    ?assertEqual(40, FindPosition({timestamp, Ts - 60 * 20 + 40})),
    ?assertEqual(60 * 20, FindPosition({timestamp, Ts + 1000})),
    %% For timestamps, though, we prefer the later chunk ID. Chunk 60, not 40:
    ?assertEqual(60, FindPosition({timestamp, Ts - 60 * 20 + 50})),
    ok.

-endif.
