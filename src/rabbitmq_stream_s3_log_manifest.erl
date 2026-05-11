%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_log_manifest).
-moduledoc """
Helper functions for recovering fragment information from local index files.
""".

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-export([
    recover_fragments/1,
    recover_fragments/2,
    find_fragments_in_range/3
]).

%%----------------------------------------------------------------------------

%%----------------------------------------------------------------------------

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
    _Threshold0,
    _SegmentFile,
    SegmentOffset,
    SeqNo0,
    _NumChunks0,
    Fragments0,
    <<>>
) ->
    %% The index file exists but contains no records yet (e.g. the stream was
    %% created but no segment has been flushed). Treat as an empty fragment.
    {#fragment{segment_offset = SegmentOffset, seq_no = SeqNo0}, Fragments0};
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
            <<LastChId:64/unsigned, LastTs:64/signed, _:64, LastFilePos:32/unsigned, _:8>> =
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
                last_timestamp = LastTs,
                next_offset = LastChId + NumRecords,
                last_offset = LastChId,
                seq_no = SeqNo0,
                size = NextFilePos - StartFilePos,
                checksum = undefined
            },
            %% NOTE: `Fragments0` is naturally sorted descending by first offset.
            {Fragment, Fragments0};
        <<NextChId:64/unsigned, _NextTs:64/signed, _:64, NextFilePos:32/unsigned, _:8>> ->
            <<LastChId:64/unsigned, LastTs:64/signed, _:64, _:32/unsigned, _:8>> =
                rabbitmq_stream_s3_array:at(
                    FragmentBoundary - 1, ?INDEX_RECORD_SIZE_B, IdxArray
                ),
            Fragment = #fragment{
                segment_offset = SegmentOffset,
                segment_pos = StartFilePos,
                num_chunks = {NumChunks0, FragmentBoundary},
                first_offset = FirstChId,
                first_timestamp = FirstTs,
                last_timestamp = LastTs,
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
            {Last0, Acc1} = recover_fragments(IdxFile, Acc0),
            Last = Last0#fragment{roll_reason = segment_roll},
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
