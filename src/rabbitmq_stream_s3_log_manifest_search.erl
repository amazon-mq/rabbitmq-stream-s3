-module(rabbitmq_stream_s3_log_manifest_search).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-define(READ_TIMEOUT, 10000).

%% TODO: this module exists expressly so that I can 'c(?MODULE).' it in the
%% shell. Either merge this into rabbitmq_stream_s3_log_manifest or figure out a
%% proper place for it.

-export([
    position/3,
    recover_fragments/2,
    fragment_trailer/2,
    index_data/3,
    get_group/3
]).

-spec position(file:filename_all(), osiris:offset(), rabbitmq_stream_s3_log_manifest_entry:entries()) ->
    {ok, osiris:offset(), non_neg_integer(), filename:filename_all()}.
position(Dir, Offset, Entries) ->
    RootIdx0 =
        rabbitmq_stream_s3_binary_array:partition_point(
            fun(?ENTRY(O, _T, _K, _S, _N, _)) -> Offset > O end,
            ?ENTRY_B,
            Entries
        ),
    RootIdx = saturating_decr(RootIdx0),
    ?ENTRY(EntryOffset, _, Kind, _, _, _) = rabbitmq_stream_s3_binary_array:at(RootIdx, ?ENTRY_B, Entries),
    ?LOG_DEBUG("partition-point ~b for offset ~b is entry ~b", [
        RootIdx,
        Offset,
        EntryOffset
    ]),
    case Kind of
        ?MANIFEST_KIND_FRAGMENT ->
            ?LOG_DEBUG("Searching for offset ~b within fragment ~b", [Offset, EntryOffset]),
            %% TODO: pass in size now that we have it.
            {ok, #fragment_info{index_start_pos = IdxStartPos}} = fragment_trailer(
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
            File = filename:join(
                Dir, rabbitmq_stream_s3_log_manifest:make_file_name(EntryOffset, "fragment")
            ),
            ?LOG_DEBUG("Attaching to offset ~b, pos ~b, file ~ts", [Offset, Pos, File]),
            {ok, ChunkId, Pos, File};
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
            position(Dir, Offset, GroupEntries)
    end.

recover_fragments(Dir, Entries) ->
    %% Since the manifest is eventually consistent we need to follow the "next
    %% pointers" in the trailers of fragments to find the true last tiered
    %% fragment.
    ?ENTRY(LastManifestChId, _, _, _, _, _) = rabbitmq_stream_s3_binary_array:last(?ENTRY_B, Entries),
    {ok, #fragment_info{offset = LastManifestChId, next_offset = NextChId}} = fragment_trailer(
        Dir, LastManifestChId
    ),
    recover_fragments(Dir, NextChId, []).

recover_fragments(Dir, CurChId, Acc0) ->
    case fragment_trailer(Dir, CurChId) of
        {ok, #fragment_info{offset = CurChId, next_offset = NextChId} = Info} ->
            recover_fragments(Dir, NextChId, [Info | Acc0]);
        {error, not_found} ->
            {CurChId, lists:reverse(Acc0)}
    end.

saturating_decr(0) -> 0;
saturating_decr(N) -> N - 1.

fragment_trailer(Dir, FragmentOffset) ->
    {ok, Bucket} = application:get_env(rabbitmq_stream_s3, bucket),
    {ok, Handle} = rabbitmq_aws:open_connection("s3"),
    Key = rabbitmq_stream_s3_log_manifest:fragment_key(Dir, FragmentOffset),
    ?LOG_DEBUG("Looking up key ~ts (~ts)", [Key, ?FUNCTION_NAME]),
    try rabbitmq_stream_s3_api:get_object_with_range(
        Handle, Bucket, Key, -?FRAGMENT_TRAILER_B, [{timeout, ?READ_TIMEOUT}]
    ) of
        {ok, Data} ->
            {ok, rabbitmq_stream_s3_log_manifest:fragment_trailer_to_info(Data)};
        {error, _} = Err ->
            Err
    after
        ok = rabbitmq_aws:close_connection(Handle)
    end.

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
