%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_log_manifest).
-moduledoc """
Functions for interfacing with the local tier.

This is an implementation of the `osiris_log_manifest` behaviour and helper
functions related to recovering fragment information on boot. This module
doesn't contain any logic for uploading data. Instead it notifies
`rabbitmq_stream_s3_server` when new fragments are available (and other events)
and the server handles background uploads.
""".

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-behaviour(osiris_log_manifest).

-record(log_writer, {
    type :: writer | acceptor,
    stream :: stream_id(),
    %% Only defined for writers:
    fragment :: #fragment{} | undefined
}).

%% osiris_log_manifest
-export([
    init_manifest/2,
    overview/1,
    handle_event/2,
    close_manifest/1,
    delete/1
]).

-export([
    recover_fragments/2,
    find_fragments_in_range/3
]).

%%----------------------------------------------------------------------------

init_manifest(#{dir := Dir0, name := StreamId0} = Config0, acceptor) ->
    StreamId = iolist_to_binary(StreamId0),
    Dir = list_to_binary(Dir0),
    Config1 = Config0#{
        name := StreamId,
        dir := Dir,
        shared => osiris_log_shared:new(),
        counter => osiris_log:make_counter(Config0)
    },
    Config = Config1#{
        max_segment_size_bytes => ?MAX_SEGMENT_SIZE_BYTES,
        retention => []
    },
    ok = rabbitmq_stream_s3_server:init_acceptor(StreamId, Config1),
    {Config, #log_writer{type = acceptor, stream = StreamId}};
init_manifest(#{name := StreamId0, dir := Dir0} = Config0, writer) ->
    %% Binaries are more compact. Coerce name and dir.
    StreamId = iolist_to_binary(StreamId0),
    Dir = list_to_binary(Dir0),
    Config1 = Config0#{name := StreamId, dir := Dir},

    Config = Config0#{
        max_segment_size_bytes => ?MAX_SEGMENT_SIZE_BYTES,
        retention => []
    },
    ?LOG_DEBUG("Recovering available fragments for stream ~ts", [Dir]),
    %% Recover the current fragment information. To do this we scan through the
    %% most recent index file and find the last fragment. While performing this
    %% scan we also find the older fragments and notify the manifest server so
    %% that it may upload them if it hasn't already done so.
    {Fragment, Available} =
        case sorted_index_files_rev(Dir) of
            [] ->
                %% Empty stream, use default fragment.
                %% TODO: is offset zero always correct here?
                {#fragment{segment_offset = 0, seq_no = 0}, []};
            [LastIdxFile | _] ->
                %% TODO: handle LastIdxFile being empty / corrupted?
                %% `osiris_log:first_and_last_seginfos0/1` does but this part runs after
                %% the writer has cleaned up its log.
                recover_fragments(LastIdxFile)
        end,
    ok = rabbitmq_stream_s3_server:init_writer(StreamId, Config1, Available),
    Manifest = #log_writer{
        type = writer,
        stream = StreamId,
        fragment = Fragment
    },
    {Config, Manifest}.

handle_event(_Event, #log_writer{type = acceptor} = Manifest) ->
    Manifest;
handle_event(
    {segment_opened, RolledSegment, NewSegment},
    #log_writer{stream = StreamId, fragment = Fragment0} = Manifest0
) ->
    Fragment =
        %% Submit the fragment for the rolled segment (if there actually is one) to
        %% the manifest server for future upload.
        case RolledSegment of
            undefined ->
                %% The old fragment can be undefined when this event is emitted
                %% for the first segment in a stream or during initialization.
                %% In that case we recovered the fragment info during
                %% writer_manifest/1.
                Fragment0;
            _ ->
                case Fragment0 of
                    #fragment{first_offset = undefined} ->
                        %% The last fragment rolled at the same chunk as this
                        %% segment. Discard the empty in-progress fragment and
                        %% start a new one belonging to this segment.
                        ok;
                    _ ->
                        ok = rabbitmq_stream_s3_server:fragment_available(StreamId, Fragment0)
                end,
                #fragment{segment_offset = rabbitmq_stream_s3:segment_file_offset(NewSegment)}
        end,
    Manifest0#log_writer{fragment = Fragment};
handle_event(
    {chunk_written,
        #{id := ChId, timestamp := Ts, num := NumRecords, size := ChunkSize, pos := Pos}, Chunk},
    #log_writer{
        stream = StreamId,
        fragment =
            #fragment{
                segment_offset = SegmentOffset,
                segment_pos = SegmentPos,
                num_chunks = {StartNumChunks, NumChunks0},
                seq_no = SeqNo,
                size = Size0,
                checksum = Checksum0
            } = Fragment0
    } = Manifest0
) ->
    Fragment1 =
        case Fragment0 of
            #fragment{first_offset = undefined} ->
                ?assertEqual(Pos, SegmentPos),
                Fragment0#fragment{
                    first_offset = ChId,
                    first_timestamp = Ts
                };
            #fragment{} ->
                Fragment0
        end,
    Size = Size0 + ChunkSize,
    NumChunks = NumChunks0 + 1,
    Checksum = checksum(Checksum0, Chunk),
    Fragment2 = Fragment1#fragment{
        last_offset = ChId,
        next_offset = ChId + NumRecords,
        num_chunks = {StartNumChunks, NumChunks},
        size = Size,
        checksum = Checksum
    },
    Fragment =
        %% NOTE: in very high throughput scenarios, the writer can can batch
        %% together enough records to exceed the fragment size in a single
        %% chunk. A fragment cannot have zero chunks in it (it would be an
        %% empty file!) so we need to check that `NumChunks` is non-zero.
        case Size > ?MAX_FRAGMENT_SIZE_B andalso NumChunks0 > 0 of
            true ->
                ?assertNotEqual(undefined, Fragment2#fragment.first_offset),
                %% Roll over the fragment.
                ok = rabbitmq_stream_s3_server:fragment_available(StreamId, Fragment2),
                #fragment{
                    segment_offset = SegmentOffset,
                    segment_pos = SegmentPos + Size,
                    num_chunks = {StartNumChunks + NumChunks, 0},
                    seq_no = SeqNo + 1
                };
            false ->
                Fragment2
        end,
    Manifest0#log_writer{fragment = Fragment};
handle_event({retention_updated, Retention}, #log_writer{stream = StreamId} = Manifest) ->
    ok = rabbitmq_stream_s3_server:retention_updated(StreamId, Retention),
    Manifest.

overview(Dir) ->
    %% TODO: adjust the lower end of the range from the local overview to
    %% move up to the offset of the oldest segment which is not yet fully
    %% uploaded to the remote tier. (Also see the local retention function,
    %% it should be nearly the same calculation.) This could make replication
    %% to a new member quicker.
    _StreamId = iolist_to_binary(filename:basename(Dir)),
    osiris_log:overview(Dir).

close_manifest(#log_writer{}) ->
    %% TODO: unregister writers with the server.
    ok.

delete(_Config) ->
    %% TODO use the `dir` from config to delete the remote manifest and
    %% fragments.
    %% TODO the stream coordinator deletes individual replicas rather than
    %% issuing any "delete the whole stream," so we need to recognize when
    %% membership falls to zero in order to clean up from the remote tier.
    ok.

%%----------------------------------------------------------------------------

checksum(undefined, _) ->
    undefined;
checksum(Checksum, Data) ->
    erlang:crc32(Checksum, Data).

recover_fragments(IdxFile) ->
    recover_fragments(IdxFile, []).

recover_fragments(IdxFile, Acc) ->
    SegmentOffset = rabbitmq_stream_s3:index_file_offset(IdxFile),
    SegmentFile = iolist_to_binary(string:replace(IdxFile, <<".index">>, <<".segment">>, trailing)),
    %% TODO: we should be reading in smaller chunks with pread.
    {ok, <<_:?IDX_HEADER_B/binary, IdxArray/binary>>} = file:read_file(IdxFile),
    recover_fragments(
        ?MAX_FRAGMENT_SIZE_B,
        SegmentFile,
        SegmentOffset,
        0,
        0,
        Acc,
        IdxArray
    ).

recover_fragments(
    Threshold0,
    SegmentFile,
    SegmentOffset,
    SeqNo0,
    NumChunks0,
    Fragments0,
    IdxArray
) ->
    %% NOTE: all indexes in this function are chunk indexes. `FragmentBoundary`
    %% is a chunk index.
    FragmentBoundary = rabbitmq_stream_s3_array:partition_point(
        fun(<<_ChId:64, _Ts:64, _E:64, FilePos:32/unsigned, _ChT:8>>) ->
            Threshold0 > FilePos
        end,
        ?INDEX_RECORD_SIZE_B,
        IdxArray
    ),
    %% TODO: what if the partition point is the length? If there's no array
    %% left?
    <<FirstChId:64/unsigned, FirstTs:64/signed, _:64, StartFilePos:32/unsigned, _:8>> =
        rabbitmq_stream_s3_array:at(0, ?INDEX_RECORD_SIZE_B, IdxArray),
    ?LOG_DEBUG("Fragment boundary ~b (start size ~b)", [FragmentBoundary, StartFilePos]),
    case rabbitmq_stream_s3_array:try_at(FragmentBoundary, ?INDEX_RECORD_SIZE_B, IdxArray) of
        undefined ->
            <<LastChId:64/unsigned, _LastTs:64/signed, _:64, LastFilePos:32/unsigned, _:8>> =
                rabbitmq_stream_s3_array:last(?INDEX_RECORD_SIZE_B, IdxArray),
            Len = rabbitmq_stream_s3_array:len(?INDEX_RECORD_SIZE_B, IdxArray),

            %% Read the segment file to fill in the info we can't get from the
            %% index: the last chunk's size and the next offset.
            {ok, Fd} = file:open(SegmentFile, [read, raw, binary]),
            {ok, HeaderData} = file:pread(Fd, LastFilePos, ?CHUNK_HEADER_B),
            {ok, #{chunk_id := LastChId, num_records := NumRecords, next_position := NextFilePos}} =
                osiris_log:parse_header(HeaderData, LastFilePos),
            Fragment = #fragment{
                segment_offset = SegmentOffset,
                segment_pos = StartFilePos,
                num_chunks = {NumChunks0, Len},
                first_offset = FirstChId,
                first_timestamp = FirstTs,
                next_offset = LastChId + NumRecords,
                last_offset = LastChId,
                seq_no = SeqNo0,
                size = NextFilePos - StartFilePos,
                checksum = undefined
            },
            %% NOTE: `Fragments0` is naturally sorted descending by first offset.
            {Fragment, Fragments0};
        <<NextChId:64/unsigned, _NextTs:64/signed, _:64, NextFilePos:32/unsigned, _:8>> ->
            <<LastChId:64/unsigned, _LastTs:64/signed, _:64, _:32/unsigned, _:8>> =
                rabbitmq_stream_s3_array:at(
                    FragmentBoundary - 1, ?INDEX_RECORD_SIZE_B, IdxArray
                ),
            Fragment = #fragment{
                segment_offset = SegmentOffset,
                segment_pos = StartFilePos,
                num_chunks = {NumChunks0, FragmentBoundary},
                first_offset = FirstChId,
                first_timestamp = FirstTs,
                next_offset = NextChId,
                last_offset = LastChId,
                seq_no = SeqNo0,
                size = NextFilePos - StartFilePos,
                checksum = undefined
            },
            Threshold = NextFilePos + ?MAX_FRAGMENT_SIZE_B,
            SeqNo = SeqNo0 + 1,
            NumChunks = NumChunks0 + FragmentBoundary,
            Fragments = [Fragment | Fragments0],
            Rest = rabbitmq_stream_s3_array:slice(
                FragmentBoundary, ?INDEX_RECORD_SIZE_B, IdxArray
            ),
            recover_fragments(
                Threshold,
                SegmentFile,
                SegmentOffset,
                SeqNo,
                NumChunks,
                Fragments,
                Rest
            )
    end.

%% sorted_index_files(Dir) ->
%%     index_files(Dir, fun lists:sort/1).

sorted_index_files_rev(Dir) ->
    index_files(Dir, fun(Files) ->
        lists:sort(fun erlang:'>'/2, Files)
    end).

index_files(Dir, SortFun) ->
    SortFun([
        filename:join(Dir, F)
     || <<_:20/binary, ".index">> = F <- list_dir(Dir)
    ]).

list_dir(Dir) ->
    case prim_file:list_dir(Dir) of
        {error, enoent} ->
            [];
        {ok, Files} ->
            [list_to_binary(F) || F <- Files]
    end.

-spec find_fragments_in_range(directory() | [filename()], osiris:offset(), osiris:offset()) ->
    [#fragment{}].
find_fragments_in_range(Dir, From, To) when is_binary(Dir) ->
    [_ActiveIndex | IdxFiles] = sorted_index_files_rev(Dir),
    lists:foldl(
        fun(IdxFile, Acc0) ->
            {Last, Acc1} = recover_fragments(IdxFile, Acc0),
            [Last | Acc1]
        end,
        [],
        index_files_in_range(IdxFiles, From, To, [])
    ).

index_files_in_range([], _From, _To, Acc) ->
    Acc;
index_files_in_range([IdxFile | Rest], From, To, Acc) ->
    %% NOTE: the index file list is sorted descending by offset.
    FirstOffset = rabbitmq_stream_s3:index_file_offset(IdxFile),
    if
        FirstOffset > To ->
            index_files_in_range(Rest, From, To, Acc);
        FirstOffset =< From ->
            [IdxFile | Acc];
        true ->
            index_files_in_range(Rest, From, To, [IdxFile | Acc])
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

index_files_in_range_test() ->
    Files0 = [rabbitmq_stream_s3:offset_filename(O, <<"index">>) || O <- [100, 200, 300, 400, 500]],
    %% `index_files_in_range/4` expects the files sorted descending.
    Files = lists:reverse(Files0),
    GetRange = fun(From, To) ->
        [rabbitmq_stream_s3:index_file_offset(F) || F <- index_files_in_range(Files, From, To, [])]
    end,
    ?assertEqual([100], GetRange(125, 175)),
    ?assertEqual([100, 200], GetRange(125, 275)),
    ?assertEqual([100, 200], GetRange(100, 275)),
    ?assertEqual([400, 500], GetRange(400, 600)),
    ?assertEqual([500], GetRange(600, 700)),
    ?assertEqual([], GetRange(25, 75)),
    ok.

-endif.
