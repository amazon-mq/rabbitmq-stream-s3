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
-include("include/logging.hrl").

-define(DOMAIN, #{domain => ?RMQLOG_DOMAIN_STREAM_S3}).

-behaviour(osiris_log_reader).

%% OTP 27 dialyzer infers narrower success types for osiris_log:send_file/3
%% and osiris_log:chunk_iterator/3 than their specs declare, causing false
%% positives for the {offset_not_found, ...} branches and maybe_become_remote/2.
%% Remove these suppressions once OTP 28 is required.
-dialyzer({no_match, [send_file/3, chunk_iterator/3]}).
-dialyzer({no_unused, maybe_become_remote/2}).

-define(READ_TIMEOUT, 10000).
-define(SLOW_READ_THRESHOLD_MS, 10_000).

-define(C_REMOTE_INIT, 1).
-define(C_LOCAL_INIT, 2).
-define(C_REMOTE_CLOSE, 3).
-define(C_LOCAL_CLOSE, 4).
-define(C_RESOLVE_REMOTE_FIRST, 5).
-define(C_RESOLVE_REMOTE_NEXT, 6).
-define(C_RESOLVE_REMOTE_LAST, 7).
-define(C_RESOLVE_REMOTE_OFFSET, 8).
-define(C_RESOLVE_REMOTE_TIMESTAMP, 9).
-define(C_RESOLVE_LOCAL, 10).
-define(C_RESOLVE_DURATION_MS, 11).
-define(C_RESOLVE, 12).
-define(COUNTERS, [
    {remote_init, ?C_REMOTE_INIT, counter, "Readers initialized in remote mode"},
    {local_init, ?C_LOCAL_INIT, counter, "Readers initialized in local mode"},
    %% A become_local transition increments both remote_close and local_init.
    %% In the future we could support a become_remote transition (local_close +
    %% remote_init) for when local segments are deleted before the consumer
    %% finishes reading them.
    {remote_close, ?C_REMOTE_CLOSE, counter, "Remote readers closed"},
    {local_close, ?C_LOCAL_CLOSE, counter, "Local readers closed"},
    {resolve_remote_first, ?C_RESOLVE_REMOTE_FIRST, counter,
        "Offset specs resolved to remote tier (first)"},
    {resolve_remote_next, ?C_RESOLVE_REMOTE_NEXT, counter,
        "Offset specs resolved to remote tier (next)"},
    {resolve_remote_last, ?C_RESOLVE_REMOTE_LAST, counter,
        "Offset specs resolved to remote tier (last)"},
    {resolve_remote_offset, ?C_RESOLVE_REMOTE_OFFSET, counter,
        "Offset specs resolved to remote tier (absolute offset)"},
    {resolve_remote_timestamp, ?C_RESOLVE_REMOTE_TIMESTAMP, counter,
        "Offset specs resolved to remote tier (timestamp)"},
    {resolve_local, ?C_RESOLVE_LOCAL, counter, "Offset specs resolved to local tier"},
    {resolve_duration_ms, ?C_RESOLVE_DURATION_MS, counter,
        "Total milliseconds spent resolving offset specs"},
    {resolve, ?C_RESOLVE, counter, "Number of offset spec resolutions"}
]).
-define(COUNTER_KEY, {?MODULE, counter}).

-export([init_counters/0]).

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
    mode :: #remote{} | osiris_log:state(),
    verify_crc :: boolean()
}).

-record(remote_iterator, {
    next_offset :: osiris:offset(),
    data :: binary()
}).

%% osiris_log_reader
-export([
    resolve_offset_spec/2,
    init_offset_reader/2,
    next_offset/1,
    committed_chunk_id/1,
    committed_offset/1,
    close/1,
    send_file/3,
    chunk_iterator/3,
    iterator_next/1
]).

%% Debugging, testing.
-export([mode/1]).

-ifdef(TEST).
-export([find_fragment/3, find_index_position/2, resolve_remote_location/2]).
-endif.

%%%===================================================================
%%% Counters
%%%===================================================================

-spec init_counters() -> ok.
init_counters() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?COUNTER_KEY, Cnt),
    ok.

counter() ->
    persistent_term:get(?COUNTER_KEY).

record_resolve(T0, Tier, OffsetSpec) ->
    Cnt = counter(),
    Duration = erlang:convert_time_unit(erlang:monotonic_time() - T0, native, millisecond),
    counters:add(Cnt, ?C_RESOLVE_DURATION_MS, Duration),
    counters:add(Cnt, ?C_RESOLVE, 1),
    case Tier of
        local ->
            counters:add(Cnt, ?C_RESOLVE_LOCAL, 1);
        remote ->
            C =
                case OffsetSpec of
                    first -> ?C_RESOLVE_REMOTE_FIRST;
                    next -> ?C_RESOLVE_REMOTE_NEXT;
                    last -> ?C_RESOLVE_REMOTE_LAST;
                    {timestamp, _} -> ?C_RESOLVE_REMOTE_TIMESTAMP;
                    {abs, _} -> ?C_RESOLVE_REMOTE_OFFSET;
                    N when is_integer(N) -> ?C_RESOLVE_REMOTE_OFFSET
                end,
            counters:add(Cnt, C, 1)
    end.

%%%===================================================================
%%% osiris_log_reader callbacks
%%%===================================================================

resolve_offset_spec(OffsetSpec, Config) ->
    case resolve_remote_location(OffsetSpec, Config) of
        {ok, #remote_location{chunk_id = ChunkId}} ->
            {ok, ChunkId};
        {local, LocalSpec} ->
            osiris_log:resolve_offset_spec(LocalSpec, Config);
        {error, _} = Err ->
            Err
    end.

init_offset_reader(OffsetSpec, Config) ->
    T0 = erlang:monotonic_time(),
    case resolve_remote_location(OffsetSpec, Config) of
        {ok, Location} ->
            record_resolve(T0, remote, OffsetSpec),
            init_remote_reader(Location, Config);
        {local, LocalSpec} ->
            record_resolve(T0, local, OffsetSpec),
            init_local_reader(LocalSpec, Config);
        {error, _} = Err ->
            Err
    end.

resolve_remote_location(Spec, _Config) when Spec =:= last orelse Spec =:= next ->
    {local, Spec};
resolve_remote_location(first, #{name := StreamId, shared := Shared}) ->
    LocalFirstOffset = osiris_log_shared:first_chunk_id(Shared),
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{first_offset = RemoteFirstOffset} when RemoteFirstOffset < LocalFirstOffset ->
            ?LOG_DEBUG(
                "Attaching remote reader at first offset ~b for spec 'first'",
                [RemoteFirstOffset],
                ?DOMAIN
            ),
            resolve_first(StreamId, RemoteFirstOffset);
        _ ->
            {local, first}
    end;
resolve_remote_location(Offset, #{name := StreamId, shared := Shared}) when
    is_integer(Offset)
->
    ?LOG_DEBUG(
        ?MODULE_STRING ":~ts/2 finding offset ~b for stream '~ts'",
        [?FUNCTION_NAME, Offset, StreamId],
        ?DOMAIN
    ),
    FirstChunkId = osiris_log_shared:first_chunk_id(Shared),
    case Offset >= FirstChunkId of
        true ->
            ?LOG_DEBUG(
                "Offset ~b is in the local tier of stream '~ts' (start ~b), using a local reader",
                [Offset, StreamId, FirstChunkId],
                ?DOMAIN
            ),
            {local, Offset};
        false ->
            ?LOG_DEBUG(
                "Offset ~b of stream '~ts' is not local (start ~b), trying the remote tier",
                [
                    Offset, StreamId, FirstChunkId
                ],
                ?DOMAIN
            ),
            case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
                undefined ->
                    {local, next};
                #manifest{first_offset = FirstOffset} when Offset < FirstOffset ->
                    %% Emulate osiris_log's behavior: attach at the beginning
                    %% of the stream.
                    resolve_first(StreamId, FirstOffset);
                #manifest{} = Manifest ->
                    find_position({offset, Offset}, Manifest, StreamId)
            end
    end;
resolve_remote_location({timestamp, Ts} = Spec, #{name := StreamId}) ->
    ?LOG_DEBUG(
        ?MODULE_STRING ":~ts/2 finding timestamp ~b for stream '~ts'",
        [?FUNCTION_NAME, Ts, StreamId],
        ?DOMAIN
    ),
    %% We can't cheaply query the first timestamp from `osiris_log_shared`.
    %% Instead try the remote tier first.
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{first_offset = FirstOffset, first_timestamp = FirstTs} when Ts < FirstTs ->
            resolve_first(StreamId, FirstOffset);
        #manifest{entries = Entries} = Manifest ->
            case rabbitmq_stream_s3_array:last(?ENTRY_B, Entries) of
                ?ENTRY(_O, _FTs, LTs, _, _, _) when LTs >= Ts ->
                    find_position({timestamp, Ts}, Manifest, StreamId);
                _ ->
                    {local, Spec}
            end;
        undefined ->
            {local, Spec}
    end;
resolve_remote_location({abs, Offset}, Config) ->
    case total_range(Config) of
        {First, Last} when First =< Offset andalso Offset =< Last ->
            resolve_remote_location(Offset, Config);
        Range ->
            {error, {offset_out_of_range, Range}}
    end.

-doc "Finds the range of offsets in both local and remote tiers".
-spec total_range(osiris_log:config()) -> rabbitmq_stream_s3:range().
total_range(#{name := StreamId, shared := Shared}) ->
    case osiris_log_shared:first_chunk_id(Shared) of
        -1 ->
            empty;
        LocalFirst ->
            LocalLast = osiris_log_shared:committed_offset(Shared),
            case rabbitmq_stream_s3_manifest_replica:get_range(StreamId) of
                {RemoteFirst, RemoteNext} ->
                    {min(LocalFirst, RemoteFirst), max(LocalLast, RemoteNext - 1)};
                empty ->
                    {LocalFirst, LocalLast}
            end
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

%% The remote reader monitors the log reader process and stops when it exits,
%% so no explicit close is needed.
close(#?MODULE{mode = #remote{}}) ->
    counters:add(counter(), ?C_REMOTE_CLOSE, 1),
    ok;
close(#?MODULE{mode = Local}) ->
    counters:add(counter(), ?C_LOCAL_CLOSE, 1),
    ok = osiris_log:close(Local).

send_file(
    Socket,
    #?MODULE{config = Config, mode = #remote{} = Remote0, verify_crc = VerifyCrc} = State0,
    Callback
) ->
    case read_header(Remote0) of
        {ok,
            #{
                chunk_id := ChId,
                num_records := NumRecords,
                position := Position,
                next_position := NextPosition,
                header_data := HeaderData,
                crc := Crc,
                data_size := DataSize
            } = Header,
            #remote{
                pid = Pid,
                transport = Transport,
                chunk_selector = ChunkSelector
            } = Remote1} ->
            {ToSkip, ToSend} = select_amount_to_send(ChunkSelector, Header),
            DataPos = Position + ?CHUNK_HEADER_B + ToSkip,
            case read(Pid, DataPos, ToSend, within_chunk) of
                {ok, Data} ->
                    ok = maybe_validate_crc(ChId, Crc, Data, DataSize, VerifyCrc),
                    PrefixData = Callback(Header, ToSend + byte_size(HeaderData)),
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
                {next_fragment, Offset} ->
                    State = State0#?MODULE{
                        mode = Remote1#remote{
                            next_offset = Offset,
                            position = ?SEGMENT_HEADER_B
                        }
                    },
                    send_file(Socket, State, Callback);
                {become_local, _} ->
                    counters:add(counter(), ?C_REMOTE_CLOSE, 1),
                    case init_local_reader(Remote1#remote.next_offset, Config) of
                        {ok, State} ->
                            send_file(Socket, State, Callback);
                        {error, _} = Err ->
                            Err
                    end;
                end_of_stream ->
                    {end_of_stream, State0#?MODULE{mode = Remote1}}
            end;
        {become_local, _} ->
            counters:add(counter(), ?C_REMOTE_CLOSE, 1),
            case init_local_reader(Remote0#remote.next_offset, Config) of
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
            {ok, State0#?MODULE{mode = Local}};
        {offset_not_found, Local1} ->
            Offset = osiris_log:next_offset(Local1),
            case maybe_become_remote(Offset, State0#?MODULE{mode = Local1}) of
                {ok, State} ->
                    send_file(Socket, State, Callback);
                false ->
                    case osiris_log:open_next_segment(Local1) of
                        {ok, Local} ->
                            send_file(Socket, State0#?MODULE{mode = Local}, Callback);
                        not_found ->
                            {end_of_stream, State0#?MODULE{mode = Local1}}
                    end
            end;
        {end_of_stream, Local} ->
            {end_of_stream, State0#?MODULE{mode = Local}};
        {error, _} = Err ->
            Err
    end.

chunk_iterator(
    #?MODULE{config = Config, mode = #remote{} = Remote0, verify_crc = VerifyCrc} = State0,
    Credit,
    _PrevIter
) ->
    case read_header(Remote0) of
        {ok,
            #{
                chunk_id := ChId,
                num_records := NumRecords,
                position := Position,
                next_position := NextPosition,
                filter_size := FilterSize,
                data_size := DataSize,
                crc := Crc
            } = Header,
            #remote{pid = Pid} = Remote1} ->
            DataPos = Position + ?CHUNK_HEADER_B + FilterSize,
            case read(Pid, DataPos, DataSize, within_chunk) of
                {ok, Data} ->
                    ok = maybe_validate_crc(ChId, Crc, Data, DataSize, VerifyCrc),
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
                {next_fragment, Offset} ->
                    State = State0#?MODULE{
                        mode = Remote1#remote{
                            next_offset = Offset,
                            position = ?SEGMENT_HEADER_B
                        }
                    },
                    chunk_iterator(State, Credit, undefined);
                {become_local, _} ->
                    counters:add(counter(), ?C_REMOTE_CLOSE, 1),
                    case init_local_reader(Remote1#remote.next_offset, Config) of
                        {ok, State} ->
                            chunk_iterator(State, Credit, undefined);
                        {error, _} = Err ->
                            Err
                    end;
                end_of_stream ->
                    {end_of_stream, State0#?MODULE{mode = Remote1}}
            end;
        {become_local, _} ->
            counters:add(counter(), ?C_REMOTE_CLOSE, 1),
            case init_local_reader(Remote0#remote.next_offset, Config) of
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
            {ok, Header, Iter, State0#?MODULE{mode = Local}};
        {offset_not_found, Local1} ->
            Offset = osiris_log:next_offset(Local1),
            case maybe_become_remote(Offset, State0#?MODULE{mode = Local1}) of
                {ok, State} ->
                    chunk_iterator(State, Credit, undefined);
                false ->
                    case osiris_log:open_next_segment(Local1) of
                        {ok, Local} ->
                            chunk_iterator(State0#?MODULE{mode = Local}, Credit, undefined);
                        not_found ->
                            {end_of_stream, State0#?MODULE{mode = Local1}}
                    end
            end;
        {end_of_stream, Local} ->
            {end_of_stream, State0#?MODULE{mode = Local}};
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

%%---------------------------------------------------------------------------
%% Helpers

-spec mode(#?MODULE{}) -> local | remote.
mode(#?MODULE{mode = #remote{}}) -> remote;
mode(#?MODULE{}) -> local.

send(tcp, Socket, Data) ->
    gen_tcp:send(Socket, Data);
send(ssl, Socket, Data) ->
    ssl:send(Socket, Data).

%% This helper is mostly the same as osiris_log:read_header0/1. There are some
%% simplifications:
%% * Always over-read the chunk header so that we also read the chunk filter in
%%   a single read operation.
%% * Check the chunk selector within this helper. osiris_log does this in the
%%   callers, but we can always skip when the chunk selector doesn't match.
%% TODO: revise this comment as more moves into the server.
-spec read_header(#remote{}) ->
    {ok, osiris_log:header_map(), #remote{}}
    | {become_local, osiris:offset()}
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
        pid = Pid,
        position = Position
    } = Remote0
) ->
    %% Over-read the chunk header so that the filter (if it exists) is always
    %% included in the binary. Reading from the remote tier takes time, so
    %% over-reading is faster.
    case read(Pid, Position, ?CHUNK_HEADER_B + ?MAX_FILTER_SIZE, chunk_boundary) of
        {ok, Header} ->
            read_header2(Remote0, Header);
        {next_fragment, Offset} ->
            Remote = Remote0#remote{next_offset = Offset, position = ?SEGMENT_HEADER_B},
            read_header(Remote);
        {become_local, Offset} ->
            {become_local, Offset};
        end_of_stream ->
            {end_of_stream, Remote0}
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

%% Validates the CRC32 of chunk record data read from the remote tier.
%% In send_file, Data may include the trailer (DataSize + TrailerSize bytes);
%% the CRC only covers the first DataSize bytes.
maybe_validate_crc(_ChunkId, _Crc, _Data, _DataSize, false) ->
    ok;
maybe_validate_crc(ChunkId, Crc, Data, DataSize, true) ->
    RecordData = binary:part(Data, 0, DataSize),
    case erlang:crc32(RecordData) of
        Crc ->
            ok;
        Actual ->
            ?LOG_ERROR(
                "CRC validation failure reading chunk ~b from remote tier"
                " (expected=~b, actual=~b, size=~b)",
                [ChunkId, Crc, Actual, DataSize]
            ),
            exit({crc_validation_failure, {chunk_id, ChunkId}})
    end.

init_local_reader(OffsetSpec, Config) ->
    case osiris_log:init_offset_reader(OffsetSpec, Config) of
        {ok, Local} ->
            counters:add(counter(), ?C_LOCAL_INIT, 1),
            {ok, #?MODULE{
                config = Config,
                mode = Local,
                verify_crc = rabbitmq_stream_s3_config:verify_crc_on_read()
            }};
        {error, _} = Err ->
            Err
    end.

init_remote_reader(
    #remote_location{
        position = Position,
        chunk_id = Offset,
        fragment_ref = FragRef
    } = Location,
    #{name := StreamId, options := Options, shared := Shared} = Config
) ->
    Filter =
        case Options of
            #{filter_spec := FilterSpec} ->
                osiris_bloom:init_matcher(FilterSpec);
            _ ->
                undefined
        end,
    Conf = #{
        reader => self(),
        stream => StreamId,
        location => Location
    },
    case gen_server:start_link(rabbitmq_stream_s3_remote_reader, Conf, []) of
        {ok, Pid} ->
            counters:add(counter(), ?C_REMOTE_INIT, 1),
            Reader = #?MODULE{
                config = Config,
                verify_crc = rabbitmq_stream_s3_config:verify_crc_on_read(),
                mode = #remote{
                    pid = Pid,
                    stream = StreamId,
                    transport = maps:get(transport, Options, tcp),
                    next_offset = Offset,
                    shared = Shared,
                    fragment = FragRef#fragment_ref.offset,
                    position = Position,
                    filter = Filter,
                    chunk_selector = maps:get(chunk_selector, Options, user_data)
                }
            },
            {ok, Reader};
        {error, _} = Err ->
            Err
    end.

maybe_become_remote(Offset, #?MODULE{config = Config, mode = Local}) ->
    case resolve_remote_location(Offset, Config) of
        {ok, Location} ->
            case init_remote_reader(Location, Config) of
                {ok, RemoteState} ->
                    counters:add(counter(), ?C_LOCAL_CLOSE, 1),
                    osiris_log:close(Local),
                    {ok, RemoteState};
                {error, _} ->
                    false
            end;
        _ ->
            false
    end.

-spec find_position(
    {offset, osiris:offset()} | {timestamp, osiris:timestamp()},
    #manifest{},
    stream_id()
) -> {ok, #remote_location{}} | {error, any()}.
find_position(Spec, #manifest{} = Manifest, StreamId) ->
    GetGroupFun = rabbitmq_stream_s3_manifest:get_group_fun(StreamId),
    #fragment_ref{offset = FragmentOffset, uid = Uid, size = Size} =
        FragRef =
        find_fragment(Manifest#manifest.entries, Spec, GetGroupFun),
    %% Download the index from the fragment to find the exact chunk position.
    IdxStartPos = ?SEGMENT_HEADER_B + Size,
    IndexData = index_data(StreamId, FragmentOffset, Uid, IdxStartPos),
    {ChunkId, _, FragPos} = find_index_position(IndexData, Spec),
    %% Position within the fragment object (after the 8-byte header).
    Position = ?SEGMENT_HEADER_B + FragPos,
    %% Create an iterator positioned at this fragment for forward navigation.
    Iterator = rabbitmq_stream_s3_fragment_iterator:init(Manifest, FragmentOffset, GetGroupFun),
    %% Advance past the current entry so the iterator is ready for `next`.
    Iterator1 = advance_past_current(Iterator),
    {ok, #remote_location{
        chunk_id = ChunkId,
        position = Position,
        fragment_ref = FragRef,
        iterator = Iterator1
    }}.

-doc """
Finds the manifest entry which contains the requested offset or timestamp.

This scans the entries array in logarithmic time. If the offset/timestamp
being searched for is within a group, the group will be fetched with `GetGroup`
and then searched recursively.
""".
-spec find_fragment(
    rabbitmq_stream_s3:entries(),
    {offset, osiris:offset()} | {timestamp, osiris:timestamp()},
    fun((#group_ref{}) -> {ok, rabbitmq_stream_s3:entries()} | {error, any()})
) -> #fragment_ref{}.
find_fragment(Entries, Spec, GetGroup) ->
    PartitionPredicate =
        case Spec of
            {offset, Offset} ->
                fun(?ENTRY(O, _FTs, _LTs, _K, _Sz, _Uid, _)) -> Offset >= O end;
            {timestamp, Ts} ->
                fun(?ENTRY(_O, _FTs, LTs, _K, _Sz, _Uid, _)) -> Ts >= LTs end
        end,
    Idx0 = rabbitmq_stream_s3_array:partition_point(
        PartitionPredicate,
        ?ENTRY_B,
        Entries
    ),
    NumEntries = byte_size(Entries) div ?ENTRY_B,
    Idx =
        case Spec of
            {offset, _} -> saturating_decr(Idx0);
            {timestamp, _} -> min(Idx0, NumEntries - 1)
        end,
    case rabbitmq_stream_s3_array:at(Idx, ?ENTRY_B, Entries) of
        ?ENTRY(EntryOffset, _FTs, _LTs, ?MANIFEST_KIND_FRAGMENT, Size, Uid, _) ->
            #fragment_ref{offset = EntryOffset, uid = Uid, size = Size};
        ?ENTRY(GroupOffset, _FTs, _LTs, Kind, _Sz, Uid, _) ->
            %% Download the group and search recursively within that.
            ?LOG_DEBUG(
                "Entry is not a fragment. Searching within group ~b kind ~b",
                [GroupOffset, Kind],
                ?DOMAIN
            ),
            GroupRef = #group_ref{uid = Uid, kind = Kind, offset = GroupOffset},
            {ok, GroupEntries} = GetGroup(GroupRef),
            find_fragment(GroupEntries, Spec, GetGroup)
    end.

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

index_data(StreamId, FragmentOffset, Uid, IdxStartPos) ->
    Key = rabbitmq_stream_s3:fragment_key(StreamId, FragmentOffset, Uid),
    ?LOG_DEBUG("Looking up key ~ts (~ts)", [Key, ?FUNCTION_NAME], ?DOMAIN),
    {ok, Data} = rabbitmq_stream_s3_api:get_range(
        Key,
        {IdxStartPos, undefined},
        #{timeout => ?READ_TIMEOUT}
    ),
    Data.

%% Advance the iterator past the current entry (so next/1 returns the
%% entry *after* the one we resolved to).
advance_past_current(Iterator) ->
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, _, Iterator1} -> Iterator1;
        end_of_manifest -> Iterator;
        {error, _} -> Iterator
    end.

resolve_first(StreamId, FirstOffset) ->
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{entries = Entries} = Manifest when byte_size(Entries) >= ?ENTRY_B ->
            GetGroupFun = rabbitmq_stream_s3_manifest:get_group_fun(StreamId),
            Iterator = rabbitmq_stream_s3_fragment_iterator:init(
                Manifest, FirstOffset, GetGroupFun
            ),
            case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
                {ok, FragRef, Iterator1} ->
                    {ok, #remote_location{
                        chunk_id = FragRef#fragment_ref.offset,
                        position = ?SEGMENT_HEADER_B,
                        fragment_ref = FragRef,
                        iterator = Iterator1
                    }};
                _ ->
                    {local, first}
            end;
        _ ->
            {local, first}
    end.

-spec read(pid(), byte_offset(), pos_integer(), rabbitmq_stream_s3_remote_reader:hint()) ->
    {ok, binary()}
    | {next_fragment, osiris:offset()}
    | {become_local, osiris:offset()}
    | end_of_stream.
read(RemoteReader, Offset, Bytes, Hint) ->
    {Ms, Result} = timer:tc(
        rabbitmq_stream_s3_remote_reader,
        read,
        [RemoteReader, Offset, Bytes, Hint],
        millisecond
    ),
    case Ms > ?SLOW_READ_THRESHOLD_MS of
        true ->
            ?LOG_WARNING(
                "Slow remote tier read: ~bms (~b bytes at offset ~b)", [Ms, Bytes, Offset], ?DOMAIN
            ),
            ok;
        false ->
            ok
    end,
    Result.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

find_fragment_test() ->
    Ts = erlang:system_time(millisecond),
    Size = 200,
    FragmentEntries = <<
        ?ENTRY(
            (N * 20),
            (Ts - 2000 + N * 20),
            (Ts - 2000 + (N + 1) * 20),
            ?MANIFEST_KIND_FRAGMENT,
            Size,
            N
        )
     || N <- lists:seq(0, 100)
    >>,
    %% Factor out those fragments into a group.
    NextFragmentEntries = <<
        ?ENTRY(
            (N * 20),
            (Ts - 2000 + N * 20),
            (Ts - 2000 + (N + 1) * 20),
            ?MANIFEST_KIND_FRAGMENT,
            Size,
            N
        )
     || N <- lists:seq(101, 150)
    >>,
    GroupUid = rabbitmq_stream_s3:uid(),
    Entries = ?ENTRY(
        0,
        (Ts - 2000),
        Ts,
        ?MANIFEST_KIND_GROUP,
        0,
        GroupUid,
        NextFragmentEntries
    ),
    GetGroup = fun(#group_ref{uid = Uid, kind = Kind, offset = Offset}) ->
        ?assertEqual(GroupUid, Uid),
        ?assertEqual(?MANIFEST_KIND_GROUP, Kind),
        ?assertEqual(0, Offset),
        {ok, FragmentEntries}
    end,
    FindFragment2 = fun(Spec) ->
        #fragment_ref{offset = FoundOffset} = find_fragment(Entries, Spec, GetGroup),
        FoundOffset
    end,
    %% The new fragments can be found normally.
    ?assertEqual(2040, FindFragment2({offset, 2050})),
    ?assertEqual(3000, FindFragment2({offset, 10_000})),
    %% The group's fragments can be found recursively.
    ?assertEqual(0, FindFragment2({offset, 0})),
    ?assertEqual(40, FindFragment2({offset, 50})),
    ?assertEqual(40, FindFragment2({timestamp, Ts - 2000 + 50})),
    %% Offset and timestamp assertions on flat fragment lists are omitted here;
    %% they are covered by the property-based tests in unit_SUITE.
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
