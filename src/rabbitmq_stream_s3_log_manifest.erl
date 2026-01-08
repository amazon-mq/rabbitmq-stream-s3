%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_log_manifest).

%% TODO: this is just for testing.
-export([recover_fragments/1]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-define(SERVER, ?MODULE).

-behaviour(osiris_log_manifest).
-behaviour(gen_server).

-record(manifest_writer, {
    type :: writer | acceptor,
    %% Only defined for writers:
    writer_ref :: writer_ref() | undefined,
    fragment :: #fragment{} | undefined
}).

-record(?MODULE, {
    machine = rabbitmq_stream_s3_log_manifest_machine:new() :: rabbitmq_stream_s3_log_manifest_machine:state(),
    tasks = #{} :: #{pid() => effect()}
}).

%% records for the gen_server to handle:
-record(init_writer, {
    pid :: pid(),
    writer_ref :: writer_ref(),
    dir :: file:filename_all()
}).
-record(get_manifest, {dir :: file:filename_all()}).
-record(acceptor_overview, {dir :: file:filename_all()}).
-record(task_completed, {task_pid :: pid(), event :: event()}).

%% osiris_log_manifest
-export([
    writer_manifest/1,
    acceptor_manifest/2,
    overview/1,
    recover_tracking/3,
    handle_event/2,
    close_manifest/1,
    delete/1
]).

%% gen_server
-export([
    start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% Need to be exported for `erlang:apply/3`.
-export([start/0, format_osiris_event/1, execute_task/2]).

-export([get_manifest/1, get_fragment_trailer/2]).

%% Useful to search module.
-export([fragment_key/2, group_key/3, make_file_name/2, fragment_trailer_to_info/1]).

%% This server needs to be started by a boot step so that it is online before
%% the stream coordinator. Otherwise the stream coordinator will attempt to
%% recover replicas before this server is started and writer_manifest/1 will
%% fail a few times and look messy in the logs.

-rabbit_boot_step(
    {rabbitmq_stream_s3_log_manifest, [
        {description, "tiered storage S3 coordinator"},
        {mfa, {?MODULE, start, []}},
        {requires, pre_boot},
        {enables, core_initialized}
    ]}
).

start() ->
    {ok, _} = application:ensure_all_started(rabbitmq_aws),

    ok = setup_credentials_and_region(),

    ok = rabbitmq_stream_s3_counters:init(),
    rabbit_sup:start_child(?MODULE).

-spec get_manifest(file:filename_all()) -> #manifest{} | undefined.
get_manifest(Dir) ->
    gen_server:call(?SERVER, #get_manifest{dir = Dir}, infinity).

%%----------------------------------------------------------------------------

handle_event(_Event, #manifest_writer{type = acceptor} = Manifest) ->
    Manifest;
handle_event(
    {segment_opened, RolledSegment, NewSegment},
    #manifest_writer{writer_ref = WriterRef, fragment = Fragment0} = Manifest0
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
                        ok = gen_server:cast(
                            ?SERVER,
                            #fragment_available{writer_ref = WriterRef, fragment = Fragment0}
                        )
                end,
                #fragment{segment_offset = segment_file_offset(NewSegment)}
        end,
    Manifest0#manifest_writer{fragment = Fragment};
handle_event(
    {chunk_written, #{id := ChId, timestamp := Ts, num := NumRecords, size := ChunkSize}, Chunk},
    #manifest_writer{
        writer_ref = WriterRef,
        fragment =
            #fragment{
                segment_offset = SegmentOffset,
                segment_pos = SegmentPos0,
                num_chunks = {StartNumChunks, NumChunks0},
                seq_no = SeqNo0,
                size = Size0,
                checksum = Checksum0
            } = Fragment0
    } = Manifest0
) ->
    Fragment1 =
        case Fragment0 of
            #fragment{first_offset = undefined} ->
                Fragment0#fragment{
                    first_offset = ChId,
                    first_timestamp = Ts
                };
            #fragment{} ->
                Fragment0
        end,
    Size = Size0 + ChunkSize,
    NumChunks = NumChunks0 + 1,
    Fragment2 = Fragment1#fragment{
        last_offset = ChId,
        next_offset = ChId + NumRecords,
        num_chunks = {StartNumChunks, NumChunks},
        size = Size,
        checksum = checksum(Checksum0, Chunk)
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
                ok = gen_server:cast(
                    ?SERVER,
                    #fragment_available{writer_ref = WriterRef, fragment = Fragment2}
                ),
                #fragment{
                    segment_offset = SegmentOffset,
                    segment_pos = SegmentPos0 + Size,
                    num_chunks = {StartNumChunks + NumChunks, 0},
                    seq_no = SeqNo0 + 1
                };
            false ->
                Fragment2
        end,
    Manifest0#manifest_writer{fragment = Fragment};
handle_event({retention_updated, _Retention}, #manifest_writer{} = Manifest) ->
    %% TODO
    Manifest.

checksum(undefined, _) ->
    undefined;
checksum(Checksum, Data) ->
    erlang:crc32(Checksum, Data).

writer_manifest(#{dir := Dir, reference := Ref} = Config0) ->
    Config = Config0#{max_segment_size_bytes => ?MAX_SEGMENT_SIZE_BYTES},
    ok = gen_server:cast(?SERVER, #init_writer{pid = self(), writer_ref = Ref, dir = Dir}),
    ?LOG_DEBUG("Recovering available fragments for stream ~ts", [Dir]),
    %% Recover the current fragment information. To do this we scan through the
    %% most recent index file and find the last fragment. While performing this
    %% scan we also find the older fragments and notify the manifest server so
    %% that it may upload them if it hasn't already done so.
    Fragment =
        case sorted_index_files_rev(Dir) of
            [] ->
                %% Empty stream, use default fragment.
                %% TODO: is offset zero always correct here?
                #fragment{segment_offset = 0, seq_no = 0};
            [LastIdxFile | _] ->
                %% TODO: handle LastIdxFile being empty / corrupted?
                %% `osiris_log:first_and_last_seginfos0/1` does but this part runs after
                %% the writer has cleaned up its log.
                {Latest, Rest} = recover_fragments(LastIdxFile),
                %% TODO: what if other fragments from other segments haven't been
                %% uploaded yet? The manifest server should spawn a task to find
                %% and upload those fragments.
                _ = [
                    gen_server:cast(?SERVER, #fragment_available{writer_ref = Ref, fragment = F})
                 || F <- Rest
                ],
                Latest
        end,
    Manifest = #manifest_writer{
        type = writer,
        writer_ref = Ref,
        fragment = Fragment
    },
    {Manifest, Config}.

recover_fragments(IdxFile) ->
    ?LOG_DEBUG("Recovering fragments from index file ~ts", [IdxFile]),
    SegmentOffset = index_file_offset(IdxFile),
    %% TODO: we should be reading in smaller chunks with pread.
    {ok, <<_:?IDX_HEADER_B/binary, IdxArray/binary>>} = file:read_file(IdxFile),
    recover_fragments(
        ?MAX_FRAGMENT_SIZE_B,
        SegmentOffset,
        0,
        0,
        [],
        IdxArray
    ).

recover_fragments(
    Threshold0,
    SegmentOffset,
    SeqNo0,
    NumChunks0,
    Fragments0,
    IdxArray
) ->
    %% NOTE: all indexes in this function are chunk indexes. `FragmentBoundary`
    %% is a chunk index.
    FragmentBoundary = rabbitmq_stream_s3_binary_array:partition_point(
        fun(<<_ChId:64, _Ts:64, _E:64, FilePos:32/unsigned, _ChT:8>>) ->
            Threshold0 > FilePos
        end,
        ?INDEX_RECORD_SIZE_B,
        IdxArray
    ),
    %% TODO: what if the partition point is the length? If there's no array
    %% left?
    <<FirstChId:64/unsigned, FirstTs:64/signed, _:64, StartFilePos:32/unsigned, _:8>> =
        rabbitmq_stream_s3_binary_array:at(0, ?INDEX_RECORD_SIZE_B, IdxArray),
    ?LOG_DEBUG("Fragment boundary ~b (start size ~b)", [FragmentBoundary, StartFilePos]),
    case rabbitmq_stream_s3_binary_array:try_at(FragmentBoundary, ?INDEX_RECORD_SIZE_B, IdxArray) of
        undefined ->
            <<LastChId:64/unsigned, _LastTs:64/signed, _:64, _:32/unsigned, _:8>> =
                rabbitmq_stream_s3_binary_array:last(?INDEX_RECORD_SIZE_B, IdxArray),
            Len = rabbitmq_stream_s3_binary_array:len(?INDEX_RECORD_SIZE_B, IdxArray),
            Fragment = #fragment{
                segment_offset = SegmentOffset,
                segment_pos = StartFilePos,
                num_chunks = {NumChunks0, Len},
                first_offset = FirstChId,
                first_timestamp = FirstTs,
                %% next_offset and size are filled in with the info from active_segment.
                next_offset = undefined,
                last_offset = LastChId,
                seq_no = SeqNo0,
                checksum = undefined
            },
            {Fragment, lists:reverse(Fragments0)};
        <<NextChId:64/unsigned, _NextTs:64/signed, _:64, NextFilePos:32/unsigned, _:8>> ->
            <<LastChId:64/unsigned, _LastTs:64/signed, _:64, _:32/unsigned, _:8>> =
                rabbitmq_stream_s3_binary_array:at(
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
            Rest = rabbitmq_stream_s3_binary_array:slice(
                FragmentBoundary, ?INDEX_RECORD_SIZE_B, IdxArray
            ),
            recover_fragments(Threshold, SegmentOffset, SeqNo, NumChunks, Fragments, Rest)
    end.

overview(Dir) ->
    LocalOverview = osiris_log:overview(Dir),
    ?LOG_DEBUG("Local overview: ~w", [LocalOverview]),
    case LocalOverview of
        #{range := empty} ->
            %% If the stream is empty there's nothing to do.
            %% TODO: could a stream be entirely uploaded to the remote tier?
            LocalOverview;
        #{range := {LocalFrom, LocalTo}} ->
            ?LOG_DEBUG("local range ~w", [{LocalFrom, LocalTo}]),
            Info = gen_server:call(?SERVER, #acceptor_overview{dir = Dir}, infinity),
            maps:merge(LocalOverview, Info)
    end.

recover_tracking(Trk0, SegmentFile, #manifest_writer{}) ->
    %% TODO: we must check if the segment file is sparse. If it is, we need to
    %% recover tracking from the remote tier. See how this function is defined
    %% in osiris_log: we need to read through the chunks and use
    %% `osiris_tracking:init/2` on snapshot-type chunks and
    %% `osiris_tracking:append_trailer/3` on tracking delta chunks or any
    %% user chunks with trailers. We should be able to reuse the gen_server
    %% from this module to perform the necessary reads.
    osiris_log:recover_tracking(Trk0, SegmentFile, undefined).

acceptor_manifest(Overview0, #{dir := Dir, epoch := Epoch} = Config0) ->
    ?LOG_DEBUG("acceptor got remote overview: ~w", [Overview0]),
    Config = Config0#{max_segment_size_bytes => ?MAX_SEGMENT_SIZE_BYTES},
    case list_dir(Dir) of
        [] ->
            case Overview0 of
                #{last_tiered_fragment_offset := LTFO, last_tiered_segment_offset := LTSO} ->
                    create_sparse_segment(Dir, Epoch, LTSO, LTFO),
                    ok;
                _ ->
                    ok
            end,
            Manifest = #manifest_writer{type = acceptor},
            {Manifest, Config};
        _ ->
            exit(replica_local_log_has_data)
    end.

close_manifest(#manifest_writer{}) ->
    %% TODO: unregister writers with the server.
    ok.

delete(_Config) ->
    %% TODO use the `dir` from config to delete the remote manifest and
    %% fragments.
    %% TODO the stream coordinator deletes individual replicas rather than
    %% issuing any "delete the whole stream," so we need to recognize when
    %% membership falls to zero in order to clean up from the remote tier.
    ok.

%%---------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {ok, #?MODULE{}}.

handle_call(#get_manifest{dir = Dir}, From, State) ->
    %% TODO: this is too simplistic. Reading from the root manifest should be
    %% done within the server. And then the server should give a spec to
    %% readers to find anything within branches.
    Event = #manifest_requested{dir = Dir, requester = From},
    evolve_event(Event, State);
handle_call(#acceptor_overview{dir = Dir}, _From, #?MODULE{machine = MacState} = State) ->
    Manifest = rabbitmq_stream_s3_log_manifest_machine:get_manifest(Dir, MacState),
    {reply, acceptor_overview(Manifest), State};
handle_call(Request, From, State) ->
    ?LOG_INFO(?MODULE_STRING " received unexpected call from ~p: ~W", [From, Request, 10]),
    {noreply, State}.

handle_cast(#init_writer{pid = Pid, writer_ref = WriterRef, dir = Dir}, State) ->
    Event = #writer_spawned{pid = Pid, writer_ref = WriterRef, dir = Dir},
    evolve_event(Event, State);
handle_cast(
    #task_completed{task_pid = TaskPid, event = Event},
    #?MODULE{tasks = Tasks0} = State0
) ->
    %% assertion
    #{TaskPid := _Effect} = Tasks0,
    Tasks = maps:remove(TaskPid, Tasks0),
    evolve_event(Event, State0#?MODULE{tasks = Tasks});
handle_cast(#fragment_available{} = Event, State) ->
    evolve_event(Event, State);
handle_cast(Message, State) ->
    ?LOG_DEBUG(?MODULE_STRING " received unexpected cast: ~W", [Message, 10]),
    {noreply, State}.

handle_info({osiris_offset, WriterRef, Offset}, State) ->
    Event = #commit_offset_increased{writer_ref = WriterRef, offset = Offset},
    evolve_event(Event, State);
handle_info({'DOWN', _MRef, process, Pid, Reason}, #?MODULE{tasks = Tasks0} = State0) ->
    case maps:take(Pid, Tasks0) of
        {_Effect, Tasks} ->
            ?LOG_INFO("Task ~p down with reason ~w. Tasks: ~W", [
                Pid, Reason, Tasks0, 10
            ]),
            %% TODO: retry. We have the effect and can attempt it again.
            {noreply, State0#?MODULE{tasks = Tasks}};
        error ->
            {noreply, State0}
    end;
handle_info(Message, State) ->
    ?LOG_DEBUG(
        ?MODULE_STRING " received unexpected message: ~W",
        [Message, 10]
    ),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%---------------------------------------------------------------------------

format_osiris_event(Event) ->
    Event.

%% Copied from osiris (but removed the flatten).
make_file_name(N, Suff) ->
    iolist_to_binary(io_lib:format("~20..0B.~s", [N, Suff])).

manifest_key(Dir) ->
    manifest_key(Dir, <<"manifest">>).

manifest_key(Dir, Filename) ->
    StreamName = filename:basename(Dir),
    iolist_to_binary([<<"rabbitmq/stream/">>, StreamName, <<"/metadata/">>, Filename]).

group_key(Dir, Kind, Offset) ->
    manifest_key(Dir, make_file_name(Offset, group_extension(Kind))).

stream_data_key(Dir, File) ->
    StreamName = filename:basename(Dir),
    iolist_to_binary([<<"rabbitmq/stream/">>, StreamName, <<"/data/">>, File]).

fragment_key(Dir, Offset) ->
    stream_data_key(Dir, make_file_name(Offset, "fragment")).

segment_file_offset(File) ->
    <<Digits:20/binary, ".segment">> = iolist_to_binary(filename:basename(File)),
    binary_to_integer(Digits).

index_file_offset(File) ->
    <<Digits:20/binary, ".index">> = iolist_to_binary(filename:basename(File)),
    binary_to_integer(Digits).

-spec evolve_event(event(), #?MODULE{}) -> {noreply, #?MODULE{}}.
evolve_event(Event, #?MODULE{machine = MacState0} = State0) ->
    {MacState, Effects} = rabbitmq_stream_s3_log_manifest_machine:apply(
        metadata(), Event, MacState0
    ),
    State = lists:foldl(fun apply_effect/2, State0#?MODULE{machine = MacState}, Effects),
    {noreply, State}.

-spec apply_effect(effect(), #?MODULE{}) -> #?MODULE{}.
apply_effect(#reply{to = To, response = Response}, State) ->
    ok = gen_server:reply(To, Response),
    State;
apply_effect(#register_offset_listener{writer_pid = Pid, offset = Offset}, State) ->
    ok = osiris:register_offset_listener(Pid, Offset, {?MODULE, format_osiris_event, []}),
    State;
apply_effect(#upload_fragment{} = Event, State) ->
    spawn_task(Event, State);
apply_effect(#upload_manifest{} = Event, State) ->
    spawn_task(Event, State);
apply_effect(#rebalance_manifest{} = Event, State) ->
    spawn_task(Event, State);
apply_effect(#resolve_manifest{} = Event, State) ->
    spawn_task(Event, State).

spawn_task(Effect, #?MODULE{tasks = Tasks0} = State0) ->
    %% NOTE: use of `erlang:self/0` is intentional here. If we casted to the
    %% server's registered name instead, a restarted manifest server after a
    %% crash could get nonsensical task events from old incarnations.
    {TaskPid, _MRef} = spawn_monitor(?MODULE, execute_task, [Effect, self()]),
    State0#?MODULE{tasks = Tasks0#{TaskPid => Effect}}.

execute_task(Effect, ManifestServer) ->
    Event = execute_task(Effect),
    gen_server:cast(ManifestServer, #task_completed{task_pid = self(), event = Event}).

execute_task(#upload_fragment{
    writer_ref = WriterRef,
    dir = Dir,
    fragment =
        #fragment{
            segment_offset = SegmentOffset,
            segment_pos = SegmentPos,
            first_offset = FragmentOffset,
            first_timestamp = Ts,
            next_offset = NextOffset,
            checksum = Checksum0,
            num_chunks = {IdxStart, IdxLen},
            seq_no = SeqNo,
            size = Size
        } = Fragment
}) ->
    Timeout = application:get_env(rabbitmq_stream_s3, segment_upload_timeout, 45_000),
    {ok, Bucket} = application:get_env(rabbitmq_stream_s3, bucket),
    {ok, Handle} = rabbitmq_aws:open_connection("s3"),
    FragmentFilename = make_file_name(FragmentOffset, "fragment"),
    SegmentFilename = make_file_name(SegmentOffset, "segment"),
    IndexFilename = make_file_name(SegmentOffset, "index"),
    Key = fragment_key(Dir, FragmentOffset),
    ?LOG_INFO(
        "Starting upload of ~ts (~b of ~ts, next offset ~b, in ~ts): ~w", [
            FragmentFilename, SeqNo, SegmentFilename, NextOffset, Dir, Fragment
        ]
    ),

    try
        {UploadMSec, {UploadSize, FragmentInfo}} = timer:tc(
            fun() ->
                {ok, SegFd} = file:open(filename:join(Dir, SegmentFilename), [read, raw, binary]),
                {ok, IdxFd} = file:open(filename:join(Dir, IndexFilename), [read, raw, binary]),

                {ok, SegData} = file:pread(SegFd, SegmentPos, Size),
                {ok, IdxData0} = file:pread(
                    IdxFd,
                    ?IDX_HEADER_B + (IdxStart * ?INDEX_RECORD_SIZE_B),
                    IdxLen * ?INDEX_RECORD_SIZE_B
                ),
                %% Convert from osiris index style to fragment index. We can
                %% drop epoch since it's not necessary after commit. TODO: right?
                %% TODO: this is pretty messy. Use the INDEX_RECORD macro.
                IdxData = <<
                    <<
                        IdxChId:64/unsigned,
                        IdxTs:64/signed,
                        (SegmentFilePos - SegmentPos + ?SEGMENT_HEADER_B):32/unsigned
                    >>
                 || <<
                        IdxChId:64/unsigned,
                        IdxTs:64/signed,
                        _Epoch:64/unsigned,
                        SegmentFilePos:32/unsigned,
                        _ChType:8/unsigned
                    >> <= IdxData0
                >>,
                Trailer = ?FRAGMENT_TRAILER(
                    FragmentOffset,
                    Ts,
                    NextOffset,
                    SeqNo,
                    Size,
                    IdxStart,
                    SegmentPos,
                    (?SEGMENT_HEADER_B + Size + ?IDX_HEADER_B),
                    (byte_size(IdxData))
                ),
                Data = [?SEGMENT_HEADER, SegData, ?IDX_HEADER, IdxData, Trailer],
                Checksum =
                    case Checksum0 of
                        undefined ->
                            erlang:crc32(Data);
                        _ ->
                            erlang:crc32(Checksum0, [?IDX_HEADER, IdxData, Trailer])
                    end,
                %% TODO: should be able to upload this in chunks. Gun should
                %% support that.
                ok = rabbitmq_stream_s3_api:put_object(
                    Handle,
                    Bucket,
                    Key,
                    Data,
                    [
                        {payload_hash, "UNSIGNED-PAYLOAD"},
                        {crc32, Checksum},
                        {timeout, Timeout}
                    ]
                ),
                {iolist_size(Data), fragment_trailer_to_info(Trailer)}
            end,
            millisecond
        ),
        ?LOG_INFO("Uploaded ~ts of ~ts in ~b msec (~b bytes)", [
            FragmentFilename, SegmentFilename, UploadMSec, UploadSize
        ]),
        %% TODO: update counters for fragments.
        rabbitmq_stream_s3_counters:segment_uploaded(UploadSize),

        #fragment_uploaded{writer_ref = WriterRef, info = FragmentInfo}
    after
        ok = rabbitmq_aws:close_connection(Handle)
    end;
execute_task(#rebalance_manifest{
    dir = Dir,
    kind = GroupKind,
    size = GroupSize,
    new_group = GroupEntries,
    rebalanced = RebalancedEntries,
    manifest = Manifest0
}) ->
    {ok, Bucket} = application:get_env(rabbitmq_stream_s3, bucket),
    Ext = group_extension(GroupKind),
    ?ENTRY(GroupOffset, Ts, _, _, _, _) = GroupEntries,
    Key = manifest_key(Dir, make_file_name(GroupOffset, group_extension(GroupKind))),
    Data = [
        group_header(GroupKind),
        <<GroupOffset:64/unsigned, Ts:64/signed, 0:2/signed, GroupSize:70/unsigned>>,
        GroupEntries
    ],

    {ok, Handle} = rabbitmq_aws:open_connection("s3"),
    try
        ?LOG_INFO("rebalancing: adding a ~ts to the manifest for '~tp'", [Ext, Dir]),
        {UploadMsec, ok} = timer:tc(
            fun() ->
                ok = rabbitmq_stream_s3_api:put_object(Handle, Bucket, Key, Data)
            end,
            millisecond
        ),
        DataSize = iolist_size(Data),
        ?LOG_INFO("Uploaded ~ts for '~tp' in ~b msec (~b bytes)", [
            Ext, Dir, UploadMsec, DataSize
        ]),
        %% TODO: counters per group kind.
        %% rabbitmq_stream_s3_counters:manifest_written(Size),
        ok
    after
        ok = rabbitmq_aws:close_connection(Handle)
    end,
    Manifest = Manifest0#manifest{entries = RebalancedEntries},
    #manifest_rebalanced{dir = Dir, manifest = Manifest};
execute_task(#upload_manifest{
    dir = Dir,
    manifest = #manifest{
        first_offset = Offset, first_timestamp = Ts, total_size = Size, entries = Entries
    }
}) ->
    {ok, Bucket} = application:get_env(rabbitmq_stream_s3, bucket),
    {ok, Handle} = rabbitmq_aws:open_connection("s3"),
    Key = manifest_key(Dir),
    Data = [?MANIFEST(Offset, Ts, Size, <<>>), Entries],

    try
        ?LOG_INFO("Uploading manifest for '~tp'", [Dir]),
        {UploadMsec, ok} = timer:tc(
            fun() ->
                ok = rabbitmq_stream_s3_api:put_object(Handle, Bucket, Key, Data)
            end,
            millisecond
        ),
        ManifestSize = iolist_size(Data),
        ?LOG_INFO("Uploaded manifest for '~tp' in ~b msec (~b bytes)", [
            Dir, UploadMsec, ManifestSize
        ]),
        rabbitmq_stream_s3_counters:manifest_written(ManifestSize),
        ok
    after
        ok = rabbitmq_aws:close_connection(Handle)
    end,
    #manifest_uploaded{dir = Dir};
execute_task(#resolve_manifest{dir = Dir}) ->
    {ok, Bucket} = application:get_env(rabbitmq_stream_s3, bucket),
    {ok, Handle} = rabbitmq_aws:open_connection("s3"),
    Key = manifest_key(Dir),
    Manifest =
        try rabbitmq_stream_s3_api:get_object(Handle, Bucket, Key) of
            {ok, ?MANIFEST(FirstOffset, FirstTimestamp, TotalSize, Entries) = Data} ->
                rabbitmq_stream_s3_counters:manifest_read(byte_size(Data)),
                #manifest{
                    first_offset = FirstOffset,
                    first_timestamp = FirstTimestamp,
                    total_size = TotalSize,
                    entries = Entries
                };
            {error, not_found} ->
                undefined;
            {error, _} = Err ->
                exit(Err)
        after
            ok = rabbitmq_aws:close_connection(Handle)
        end,
    resolve_manifest_tail(Dir, Manifest).

resolve_manifest_tail(Dir, #manifest{entries = <<>>} = Manifest0) ->
    #manifest_resolved{dir = Dir, manifest = Manifest0};
resolve_manifest_tail(Dir, undefined) ->
    #manifest_resolved{dir = Dir, manifest = undefined};
resolve_manifest_tail(Dir, #manifest{entries = Entries} = Manifest) ->
    ?ENTRY(Offset, _, _, _, _, _) = rabbitmq_stream_s3_binary_array:last(?ENTRY_B, Entries),
    {ok, #fragment_info{offset = Offset, next_offset = NextOffset}} = get_fragment_trailer(
        Dir,
        Offset
    ),
    resolve_manifest_tail(NextOffset, Dir, Manifest).

resolve_manifest_tail(
    NextOffset0,
    Dir,
    #manifest{entries = Entries0, total_size = TotalSize0} = Manifest0
) ->
    case get_fragment_trailer(Dir, NextOffset0) of
        {ok, #fragment_info{
            offset = NextOffset0,
            next_offset = NextOffset,
            timestamp = Ts,
            size = Size,
            seq_no = SeqNo
        }} ->
            Entries =
                <<Entries0/binary,
                    (?ENTRY(NextOffset0, Ts, ?MANIFEST_KIND_FRAGMENT, Size, SeqNo, <<>>))/binary>>,
            Manifest = Manifest0#manifest{entries = Entries, total_size = TotalSize0 + Size},
            resolve_manifest_tail(NextOffset, Dir, Manifest);
        {error, not_found} ->
            #manifest_resolved{dir = Dir, manifest = Manifest0}
    end.

group_extension(?MANIFEST_KIND_GROUP) -> "group";
group_extension(?MANIFEST_KIND_KILO_GROUP) -> "kgroup";
group_extension(?MANIFEST_KIND_MEGA_GROUP) -> "mgroup".

group_header(?MANIFEST_KIND_GROUP) ->
    <<?MANIFEST_GROUP_MAGIC, ?MANIFEST_GROUP_VERSION:32/unsigned>>;
group_header(?MANIFEST_KIND_KILO_GROUP) ->
    <<?MANIFEST_KILO_GROUP_MAGIC, ?MANIFEST_KILO_GROUP_VERSION:32/unsigned>>;
group_header(?MANIFEST_KIND_MEGA_GROUP) ->
    <<?MANIFEST_MEGA_GROUP_MAGIC, ?MANIFEST_MEGA_GROUP_VERSION:32/unsigned>>.

get_fragment_trailer(Dir, FragmentOffset) ->
    {ok, Bucket} = application:get_env(rabbitmq_stream_s3, bucket),
    {ok, Handle} = rabbitmq_aws:open_connection("s3"),
    Key = rabbitmq_stream_s3_log_manifest:fragment_key(Dir, FragmentOffset),
    ?LOG_DEBUG("Looking up key ~ts (~ts)", [Key, ?FUNCTION_NAME]),
    try
        rabbitmq_stream_s3_api:get_object_with_range(
            Handle, Bucket, Key, -?FRAGMENT_TRAILER_B, []
        )
    of
        {ok, Data} ->
            {ok, rabbitmq_stream_s3_log_manifest:fragment_trailer_to_info(Data)};
        {error, _} = Err ->
            Err
    after
        ok = rabbitmq_aws:close_connection(Handle)
    end.

-spec fragment_trailer_to_info(binary()) -> #fragment_info{}.
fragment_trailer_to_info(
    ?FRAGMENT_TRAILER(
        Offset,
        Ts,
        NextOffset,
        SeqNo,
        Size,
        NumChunksInSegment,
        SegmentStartPos,
        IdxStartPos,
        IdxSize
    )
) ->
    #fragment_info{
        offset = Offset,
        timestamp = Ts,
        next_offset = NextOffset,
        seq_no = SeqNo,
        num_chunks_in_segment = NumChunksInSegment,
        segment_start_pos = SegmentStartPos,
        size = Size,
        index_start_pos = IdxStartPos,
        index_size = IdxSize
    }.

acceptor_overview(undefined) ->
    #{};
acceptor_overview(#manifest{entries = <<>>}) ->
    %% I suppose that this can be empty when the entire stream expires.
    %% TODO: delete the manifest from the remote tier then instead? Err we
    %% probably need to keep the last offset.
    #{};
acceptor_overview(#manifest{entries = Entries}) ->
    SegmentStart = rabbitmq_stream_s3_binary_array:rfind(
        fun(?ENTRY(_O, _T, _K, _S, SeqNo, _)) ->
            SeqNo =:= 0
        end,
        ?ENTRY_B,
        Entries
    ),
    %% NOTE: this cannot be `undefined` because we will only delete entire
    %% segments via retention.
    ?assert(is_integer(SegmentStart)),
    ?ENTRY(LTSO, _, _, _, _, _) = rabbitmq_stream_s3_binary_array:at(
        SegmentStart, ?ENTRY_B, Entries
    ),
    ?ENTRY(LTFO, _, _, _, _, _) = rabbitmq_stream_s3_binary_array:last(?ENTRY_B, Entries),
    %% Hmm. So with eventual consistency. The uploaded fragments can outrun
    %% the segment. So we should be prepared for this to change.
    #{
        last_tiered_segment_offset => LTSO,
        last_tiered_fragment_offset => LTFO
    }.

create_sparse_segment(Dir, Epoch, LTSO, LTFO) ->
    ok = filelib:ensure_dir(Dir),
    case file:make_dir(Dir) of
        ok ->
            ok;
        {error, eexist} ->
            ok;
        Err ->
            throw(Err)
    end,

    %% TODO: what if LTSO and LTFO are the same? Probably doesn't make much
    %% difference in our strategy actually.
    SegmentFile = filename:join(Dir, make_file_name(LTSO, "segment")),
    IndexFile = filename:join(Dir, make_file_name(LTSO, "index")),
    ?LOG_DEBUG("Creating sparse fragment up to last-tiered-fragment ~b in ~ts", [LTFO, SegmentFile]),

    {ok, Bucket} = application:get_env(rabbitmq_stream_s3, bucket),
    StartFragmentKey = fragment_key(Dir, LTSO),
    LastFragmentKey = fragment_key(Dir, LTFO),

    {ok, SegFd} = file:open(SegmentFile, [read, write, raw, binary]),
    {ok, IdxFd} = file:open(IndexFile, [read, write, raw, binary]),
    {ok, Handle} = rabbitmq_aws:open_connection("s3"),
    try
        %% A sparse segment needs a few things to be valid.
        %% * First: the log header and the first chunk's header. We don't
        %%   need the first chunk, just its header.
        {ok, FirstChunkHeader} = rabbitmq_stream_s3_api:get_object_with_range(
            Handle,
            Bucket,
            StartFragmentKey,
            {?SEGMENT_HEADER_B, ?SEGMENT_HEADER_B + ?CHUNK_HEADER_B}
        ),
        ?LOG_DEBUG("Writing segment header with first chunk ~w", [FirstChunkHeader]),
        ok = file:write(SegFd, [?SEGMENT_HEADER, FirstChunkHeader]),

        {ok, TrailingFragmentData} = rabbitmq_stream_s3_api:get_object_with_range(
            Handle,
            Bucket,
            LastFragmentKey,
            -(?INDEX_RECORD_B + ?FRAGMENT_TRAILER_B)
        ),
        ?LOG_DEBUG("Read trailing fragment data ~w", [TrailingFragmentData]),
        <<LastIdxRecord:?INDEX_RECORD_B/binary, FragmentTrailer:?FRAGMENT_TRAILER_B/binary>> =
            TrailingFragmentData,
        #fragment_info{
            segment_start_pos = SegmentStartPos,
            num_chunks_in_segment = NumChunksInSegment,
            size = FragmentDataSize,
            index_size = IdxSize
        } = fragment_trailer_to_info(FragmentTrailer),
        %% * Second: we need to know where to seek in order to make the
        %%   file sparse.
        ?INDEX_RECORD(LastIdxOffs, LastIdxTs, FragmentFilePos) = LastIdxRecord,
        SegmentFilePos = SegmentStartPos + FragmentFilePos,
        ?LOG_DEBUG("Moving seg fd to ~w", [SegmentFilePos]),
        {ok, _} = file:position(SegFd, SegmentFilePos),
        %% * Third: we need the last index record at the correct position.
        NumChunksInFragment = (IdxSize div ?INDEX_RECORD_B),
        LastIdxRecordPos =
            ?IDX_HEADER_B +
                %% `-1` because we're writing the last index record.
                ?INDEX_RECORD_SIZE_B * (NumChunksInSegment + NumChunksInFragment - 1),
        ?LOG_DEBUG("Moving idx fd to ~w", [LastIdxRecordPos]),
        {ok, _} = file:position(IdxFd, LastIdxRecordPos),
        ok = file:write(IdxFd, <<
            LastIdxOffs:64/unsigned,
            LastIdxTs:64/signed,
            Epoch:64/unsigned,
            SegmentFilePos:32/unsigned,
            %% Faked. TODO: I'm pretty sure this is unused in osiris.
            %% Should we add it (.e. by adding it to the trailer?). We
            %% could store the epoch in the trailer too. No need to store
            %% all this info per-chunk in the remote tier.
            %% Also it's in the chunk header so we could parse that (though
            %% it's nice that we don't currently).
            (_ChType = 0):8/unsigned
        >>),
        %% * Fourth: we need to put the last chunk into the segment file.
        {ok, LastChunkData} = rabbitmq_stream_s3_api:get_object_with_range(
            Handle,
            Bucket,
            LastFragmentKey,
            {FragmentFilePos, FragmentDataSize + ?SEGMENT_HEADER_B}
        ),
        ?LOG_DEBUG("Read ~b bytes (~b - ~b) of last chunk data: ~W", [
            byte_size(LastChunkData), FragmentFilePos, FragmentDataSize, LastChunkData, 10
        ]),
        ok = file:write(SegFd, LastChunkData)
    after
        ok = rabbitmq_aws:close_connection(Handle),
        ok = file:close(SegFd),
        ok = file:close(IdxFd)
    end.

list_dir(Dir) ->
    case prim_file:list_dir(Dir) of
        {error, enoent} ->
            [];
        {ok, Files} ->
            [list_to_binary(F) || F <- Files]
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

setup_credentials_and_region() ->
    AccessKey = application:get_env(rabbitmq_stream_s3, aws_access_key, undefined),
    SecretKey = application:get_env(rabbitmq_stream_s3, aws_secret_key, undefined),
    Region = application:get_env(rabbitmq_stream_s3, aws_region, undefined),

    ok = maybe_setup_credentials(AccessKey, SecretKey),
    ok = maybe_setup_region(Region).

maybe_setup_credentials(undefined, _) -> ok;
maybe_setup_credentials(_, undefined) -> ok;
maybe_setup_credentials(AccessKey, SecretKey) -> rabbitmq_aws:set_credentials(AccessKey, SecretKey).

maybe_setup_region(undefined) -> ok;
maybe_setup_region(Region) -> rabbitmq_aws:set_region(Region).

-spec metadata() -> rabbitmq_stream_s3_log_manifest_machine:metadata().
metadata() ->
    #{time => erlang:monotonic_time()}.
