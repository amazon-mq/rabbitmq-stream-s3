%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_log_manifest).

%% TODO: this is just for testing.
-export([recover_fragments/1]).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-define(SERVER, ?MODULE).
-define(LAST_TIERED_OFFSET_TABLE, rabbitmq_stream_s3_log_manifest_last_tiered_offset).

-behaviour(osiris_log_manifest).
-behaviour(gen_server).

-record(log_writer, {
    type :: writer | acceptor,
    stream :: stream_id(),
    %% Only defined for writers:
    fragment :: #fragment{} | undefined
}).

-record(?MODULE, {
    machine = rabbitmq_stream_s3_log_manifest_machine:new() :: rabbitmq_stream_s3_log_manifest_machine:state(),
    tasks = #{} :: #{pid() => effect()},
    %% A lookup from stream reference to ID. This is used to determine the
    %% stream ID associated with an offset notification `{osiris_offset,
    %% Reference, osiris:offset()}` sent by the writer (because we used
    %% `osiris:register_offset_listener/3`).
    references = #{} :: #{stream_reference() => stream_id()}
}).

%% records for the gen_server to handle:
-record(init_writer, {
    pid :: pid(),
    stream :: stream_id(),
    reference :: stream_reference(),
    dir :: directory(),
    %% Corresponds to the `replica_nodes` key passed in `osiris:config()` for
    %% writers.
    replica_nodes :: [node()],
    retention :: [osiris:retention_spec()]
}).
-record(init_acceptor, {
    pid :: pid(),
    writer_pid :: pid(),
    stream :: stream_id(),
    reference :: stream_reference(),
    dir :: directory()
}).
-record(get_manifest, {stream :: stream_id()}).
-record(task_completed, {task_pid :: pid(), event :: event() | ok}).

%% osiris_log_manifest
-export([
    init_manifest/2,
    overview/1,
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

-export([get_manifest/1, get_fragment_trailer/1, get_fragment_trailer/2]).

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
    ok = rabbitmq_stream_s3_api_aws:init(),

    ok = rabbitmq_stream_s3_counters:init(),
    rabbit_sup:start_child(?MODULE).

-spec get_manifest(stream_id()) -> #manifest{} | undefined.
get_manifest(StreamId) ->
    gen_server:call(?SERVER, #get_manifest{stream = StreamId}, infinity).

%%----------------------------------------------------------------------------

handle_event(_Event, #log_writer{type = acceptor} = Manifest) ->
    Manifest;
handle_event(
    {segment_opened, RolledSegment, NewSegment},
    #log_writer{stream = Stream, fragment = Fragment0} = Manifest0
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
                            #fragment_available{stream = Stream, fragment = Fragment0}
                        )
                end,
                #fragment{segment_offset = segment_file_offset(NewSegment)}
        end,
    Manifest0#log_writer{fragment = Fragment};
handle_event(
    {chunk_written, #{id := ChId, timestamp := Ts, num := NumRecords, size := ChunkSize}, Chunk},
    #log_writer{
        stream = Stream,
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
                    #fragment_available{stream = Stream, fragment = Fragment2}
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
    Manifest0#log_writer{fragment = Fragment};
handle_event({retention_updated, Retention}, #log_writer{stream = StreamId} = Manifest) ->
    ok = gen_server:cast(?SERVER, #retention_updated{
        stream = StreamId,
        retention = Retention
    }),
    Manifest.

checksum(undefined, _) ->
    undefined;
checksum(Checksum, Data) ->
    erlang:crc32(Checksum, Data).

init_manifest(
    #{
        dir := Dir0,
        name := StreamId0,
        reference := Reference,
        leader_pid := LeaderPid
    } = Config0,
    acceptor
) ->
    StreamId = iolist_to_binary(StreamId0),
    Dir = list_to_binary(Dir0),
    Config = Config0#{
        max_segment_size_bytes => ?MAX_SEGMENT_SIZE_BYTES,
        retention => [{'fun', local_retention_fun(StreamId)}]
    },
    ok = gen_server:cast(?SERVER, #init_acceptor{
        pid = self(),
        writer_pid = LeaderPid,
        stream = StreamId,
        reference = Reference,
        dir = Dir
    }),
    {Config, #log_writer{type = acceptor, stream = StreamId}};
init_manifest(
    #{
        dir := Dir0,
        name := StreamId0,
        reference := Reference,
        replica_nodes := ReplicaNodes
    } = Config0,
    writer
) ->
    StreamId = iolist_to_binary(StreamId0),
    Dir = list_to_binary(Dir0),
    %% TODO: apply original retention settings to the remote tier.
    _ = maps:get(retention, Config0, []),
    Config = Config0#{
        max_segment_size_bytes => ?MAX_SEGMENT_SIZE_BYTES,
        retention => [{'fun', local_retention_fun(StreamId)}]
    },
    ok = gen_server:cast(?SERVER, #init_writer{
        pid = self(),
        stream = StreamId,
        reference = Reference,
        dir = Dir,
        replica_nodes = ReplicaNodes,
        retention = maps:get(retention, Config0, [])
    }),
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
                    gen_server:cast(?SERVER, #fragment_available{
                        stream = StreamId,
                        fragment = F
                    })
                 || F <- Rest
                ],
                Latest
        end,
    Manifest = #log_writer{
        type = writer,
        stream = StreamId,
        fragment = Fragment
    },
    {Config, Manifest}.

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

%%---------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    %% Table keyed by stream ID. This is used for local retention: segments
    %% with all offsets less than or equal to the last tiered offset can be
    %% deleted by retention since they are stored in the remote tier.
    _ = ets:new(?LAST_TIERED_OFFSET_TABLE, [named_table]),
    set_tick_timer(),
    {ok, #?MODULE{}}.

handle_call(#get_manifest{stream = StreamId}, From, State) ->
    %% TODO: this is too simplistic. Reading from the root manifest should be
    %% done within the server. And then the server should give a spec to
    %% readers to find anything within branches.
    Event = #manifest_requested{stream = StreamId, requester = From},
    {noreply, evolve_event(Event, State)};
handle_call(Request, From, State) ->
    ?LOG_INFO(?MODULE_STRING " received unexpected call from ~p: ~W", [From, Request, 10]),
    {noreply, State}.

handle_cast(
    #init_writer{
        pid = Pid,
        stream = StreamId,
        reference = Reference,
        dir = Dir,
        replica_nodes = ReplicaNodes,
        retention = Retention
    },
    #?MODULE{references = References0} = State0
) ->
    State1 = State0#?MODULE{references = References0#{Reference => StreamId}},
    Event = #writer_spawned{
        pid = Pid,
        stream = StreamId,
        dir = Dir,
        replica_nodes = ReplicaNodes,
        retention = Retention
    },
    {noreply, evolve_event(Event, State1)};
handle_cast(
    #init_acceptor{writer_pid = WriterPid, stream = StreamId, reference = Reference},
    #?MODULE{references = References0} = State0
) ->
    ok = gen_server:cast({?SERVER, node(WriterPid)}, #manifest_requested{
        stream = StreamId,
        requester = self()
    }),
    State1 = State0#?MODULE{references = References0#{Reference => StreamId}},
    {noreply, evolve_event(#acceptor_spawned{stream = StreamId}, State1)};
handle_cast(#manifest_requested{} = Event, State) ->
    {noreply, evolve_event(Event, State)};
handle_cast(
    #task_completed{task_pid = TaskPid, event = Event},
    #?MODULE{tasks = Tasks0} = State0
) ->
    %% assertion
    #{TaskPid := _Effect} = Tasks0,
    Tasks = maps:remove(TaskPid, Tasks0),
    State1 = State0#?MODULE{tasks = Tasks},
    State =
        case Event of
            ok ->
                State1;
            _ ->
                evolve_event(Event, State1)
        end,
    {noreply, State};
handle_cast(#fragment_available{} = Event, State) ->
    {noreply, evolve_event(Event, State)};
handle_cast(#retention_updated{} = Event, State) ->
    {noreply, evolve_event(Event, State)};
handle_cast(Message, State) ->
    ?LOG_DEBUG(?MODULE_STRING " received unexpected cast: ~W", [Message, 10]),
    {noreply, State}.

handle_info({osiris_offset, Reference, Offset}, #?MODULE{references = References} = State) ->
    case References of
        #{Reference := StreamId} ->
            Event = #commit_offset_increased{stream = StreamId, offset = Offset},
            {noreply, evolve_event(Event, State)};
        _ ->
            {noreply, State}
    end;
handle_info(#set_last_tiered_offset{} = Effect, State) ->
    {noreply, apply_effect(Effect, State)};
handle_info(#manifest_resolved{} = Event, State) ->
    {noreply, evolve_event(Event, State)};
handle_info(#fragments_applied{} = Event, State) ->
    {noreply, evolve_event(Event, State)};
handle_info(tick_timeout, State0) ->
    State = evolve_event(#tick{}, State0),
    ok = set_tick_timer(),
    {noreply, State};
handle_info({'DOWN', _MRef, process, Pid, Reason}, #?MODULE{tasks = Tasks0} = State0) ->
    case maps:take(Pid, Tasks0) of
        {Effect, Tasks} ->
            ?LOG_INFO("Task ~0p (~p) down with reason ~0p. Tasks: ~0P", [
                Effect, Pid, Reason, Tasks0, 10
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

-spec make_file_name(osiris:offset(), Suffix :: binary()) -> filename().
make_file_name(Offset, Suffix) when is_integer(Offset) andalso is_binary(Suffix) ->
    <<(iolist_to_binary(io_lib:format("~20..0B", [Offset])))/binary, ".", Suffix/binary>>.

-spec manifest_key(stream_id()) -> binary().
manifest_key(StreamId) when is_binary(StreamId) ->
    manifest_key(StreamId, <<"manifest">>).

-spec manifest_key(stream_id(), filename()) -> binary().
manifest_key(StreamId, Filename) when is_binary(StreamId) andalso is_binary(Filename) ->
    <<"/rabbitmq/stream/", StreamId/binary, "/metadata/", Filename/binary>>.

-spec group_key(stream_id(), rabbitmq_stream_s3_log_manifest_entry:kind(), osiris:offset()) ->
    binary().
group_key(StreamId, Kind, Offset) ->
    manifest_key(StreamId, make_file_name(Offset, group_extension(Kind))).

-spec stream_data_key(stream_id(), filename()) -> binary().
stream_data_key(StreamId, Filename) when is_binary(StreamId) andalso is_binary(Filename) ->
    <<"/rabbitmq/stream/", StreamId/binary, "/data/", Filename/binary>>.

-spec fragment_key(stream_id(), osiris:offset()) -> binary().
fragment_key(StreamId, Offset) when is_binary(StreamId) andalso is_integer(Offset) ->
    stream_data_key(StreamId, make_file_name(Offset, <<"fragment">>)).

-spec segment_file_offset(file:filename_all()) -> osiris:offset().
segment_file_offset(Filename) ->
    filename_offset(filename:basename(Filename, <<".segment">>)).

-spec index_file_offset(file:filename_all()) -> osiris:offset().
index_file_offset(Filename) ->
    filename_offset(filename:basename(Filename, <<".index">>)).

-spec filename_offset(file:filename_all()) -> osiris:offset().
filename_offset(Basename) when is_binary(Basename) ->
    binary_to_integer(Basename);
filename_offset(Basename) when is_list(Basename) ->
    list_to_integer(Basename).

-spec evolve_event(event(), #?MODULE{}) -> #?MODULE{}.
evolve_event(Event, #?MODULE{machine = MacState0} = State0) ->
    {MacState, Effects} = rabbitmq_stream_s3_log_manifest_machine:apply(
        metadata(), Event, MacState0
    ),
    lists:foldl(fun apply_effect/2, State0#?MODULE{machine = MacState}, Effects).

-spec apply_effect(effect(), #?MODULE{}) -> #?MODULE{}.
apply_effect(#reply{to = To, response = Response}, State) ->
    ok = gen_server:reply(To, Response),
    State;
apply_effect(#send{to = Pid, message = Message, options = Options}, State) ->
    _ = erlang:send(Pid, Message, Options),
    State;
apply_effect(#register_offset_listener{writer_pid = Pid, offset = Offset}, State) ->
    ok = osiris:register_offset_listener(Pid, Offset, {?MODULE, format_osiris_event, []}),
    State;
apply_effect(#set_last_tiered_offset{stream = StreamId, offset = Offset}, State) ->
    _ = ets:update_element(
        ?LAST_TIERED_OFFSET_TABLE,
        StreamId,
        {2, Offset},
        {StreamId, Offset}
    ),
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
    stream = StreamId,
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
    {ok, Conn} = rabbitmq_stream_s3_api:open(),
    FragmentFilename = make_file_name(FragmentOffset, <<"fragment">>),
    SegmentFilename = make_file_name(SegmentOffset, <<"segment">>),
    IndexFilename = make_file_name(SegmentOffset, <<"index">>),
    Key = fragment_key(StreamId, FragmentOffset),
    ?LOG_DEBUG(
        "Starting upload of ~ts (~b of ~ts, next offset ~b of ~ts): ~w", [
            FragmentFilename, SeqNo, SegmentFilename, NextOffset, StreamId, Fragment
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
                ok = rabbitmq_stream_s3_api:put(
                    Conn,
                    Key,
                    Data,
                    #{
                        unsigned_payload => true,
                        crc32 => Checksum,
                        timeout => Timeout
                    }
                ),
                {iolist_size(Data), fragment_trailer_to_info(Trailer)}
            end,
            millisecond
        ),
        ?LOG_DEBUG("Uploaded ~ts of ~ts in ~b msec (~b bytes)", [
            FragmentFilename, SegmentFilename, UploadMSec, UploadSize
        ]),
        %% TODO: update counters for fragments.
        rabbitmq_stream_s3_counters:segment_uploaded(UploadSize),

        #fragment_uploaded{stream = StreamId, info = FragmentInfo}
    after
        ok = rabbitmq_stream_s3_api:close(Conn)
    end;
execute_task(#rebalance_manifest{
    stream = StreamId,
    kind = GroupKind,
    size = GroupSize,
    new_group = GroupEntries,
    rebalanced = RebalancedEntries,
    manifest = Manifest0
}) ->
    Ext = group_extension(GroupKind),
    ?ENTRY(GroupOffset, Ts, _, _, _, _) = GroupEntries,
    Key = manifest_key(StreamId, make_file_name(GroupOffset, group_extension(GroupKind))),
    Data = [
        group_header(GroupKind),
        <<GroupOffset:64/unsigned, Ts:64/signed, 0:2/signed, GroupSize:70/unsigned>>,
        GroupEntries
    ],

    {ok, Conn} = rabbitmq_stream_s3_api:open(),
    try
        ?LOG_DEBUG("rebalancing: adding a ~ts to the manifest for '~tp'", [Ext, StreamId]),
        {UploadMsec, ok} = timer:tc(
            fun() ->
                ok = rabbitmq_stream_s3_api:put(Conn, Key, Data)
            end,
            millisecond
        ),
        DataSize = iolist_size(Data),
        ?LOG_DEBUG("Uploaded ~ts for '~tp' in ~b msec (~b bytes)", [
            Ext, StreamId, UploadMsec, DataSize
        ]),
        %% TODO: counters per group kind.
        %% rabbitmq_stream_s3_counters:manifest_written(Size),
        ok
    after
        ok = rabbitmq_stream_s3_api:close(Conn)
    end,
    Manifest = Manifest0#manifest{entries = RebalancedEntries},
    #manifest_rebalanced{stream = StreamId, manifest = Manifest};
execute_task(#upload_manifest{
    stream = StreamId,
    manifest = #manifest{
        first_offset = Offset,
        first_timestamp = Ts,
        next_offset = NextOffset,
        total_size = Size,
        entries = Entries
    }
}) ->
    {ok, Conn} = rabbitmq_stream_s3_api:open(),
    Key = manifest_key(StreamId),
    Data = [?MANIFEST(Offset, Ts, NextOffset, Size, <<>>), Entries],

    try
        ?LOG_DEBUG("Uploading manifest for '~tp'", [StreamId]),
        {UploadMsec, ok} = timer:tc(
            fun() ->
                ok = rabbitmq_stream_s3_api:put(Conn, Key, Data)
            end,
            millisecond
        ),
        ManifestSize = iolist_size(Data),
        ?LOG_DEBUG("Uploaded manifest for '~tp' in ~b msec (~b bytes)", [
            StreamId, UploadMsec, ManifestSize
        ]),
        rabbitmq_stream_s3_counters:manifest_written(ManifestSize),
        ok
    after
        ok = rabbitmq_stream_s3_api:close(Conn)
    end,
    #manifest_uploaded{stream = StreamId};
execute_task(#resolve_manifest{stream = StreamId}) ->
    {ok, Conn} = rabbitmq_stream_s3_api:open(),
    Key = manifest_key(StreamId),
    Manifest0 =
        try rabbitmq_stream_s3_api:get(Conn, Key) of
            {ok, ?MANIFEST(FirstOffset, FirstTimestamp, NextOffset, TotalSize, Entries) = Data} ->
                rabbitmq_stream_s3_counters:manifest_read(byte_size(Data)),
                #manifest{
                    first_offset = FirstOffset,
                    first_timestamp = FirstTimestamp,
                    next_offset = NextOffset,
                    total_size = TotalSize,
                    entries = Entries
                };
            {error, not_found} ->
                #manifest{};
            {error, _} = Err ->
                exit(Err)
        after
            ok = rabbitmq_stream_s3_api:close(Conn)
        end,
    #manifest_resolved{
        stream = StreamId,
        manifest = resolve_manifest_tail(StreamId, Manifest0)
    }.

resolve_manifest_tail(
    StreamId,
    #manifest{
        next_offset = NextOffset0,
        entries = Entries0,
        total_size = TotalSize0
    } = Manifest0
) ->
    case get_fragment_trailer(StreamId, NextOffset0) of
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
            Manifest = Manifest0#manifest{
                next_offset = NextOffset,
                total_size = TotalSize0 + Size,
                entries = Entries
            },
            resolve_manifest_tail(StreamId, Manifest);
        {error, not_found} ->
            Manifest0
    end.

group_extension(?MANIFEST_KIND_GROUP) -> <<"group">>;
group_extension(?MANIFEST_KIND_KILO_GROUP) -> <<"kgroup">>;
group_extension(?MANIFEST_KIND_MEGA_GROUP) -> <<"mgroup">>.

group_header(?MANIFEST_KIND_GROUP) ->
    <<?MANIFEST_GROUP_MAGIC, ?MANIFEST_GROUP_VERSION:32/unsigned>>;
group_header(?MANIFEST_KIND_KILO_GROUP) ->
    <<?MANIFEST_KILO_GROUP_MAGIC, ?MANIFEST_KILO_GROUP_VERSION:32/unsigned>>;
group_header(?MANIFEST_KIND_MEGA_GROUP) ->
    <<?MANIFEST_MEGA_GROUP_MAGIC, ?MANIFEST_MEGA_GROUP_VERSION:32/unsigned>>.

get_fragment_trailer(StreamId, FragmentOffset) ->
    get_fragment_trailer(fragment_key(StreamId, FragmentOffset)).

get_fragment_trailer(Key) ->
    {ok, Conn} = rabbitmq_stream_s3_api:open(),
    ?LOG_DEBUG("Looking up key ~ts (~ts)", [Key, ?FUNCTION_NAME]),
    try rabbitmq_stream_s3_api:get_range(Conn, Key, -?FRAGMENT_TRAILER_B) of
        {ok, Data} ->
            {ok, fragment_trailer_to_info(Data)};
        {error, _} = Err ->
            Err
    after
        ok = rabbitmq_stream_s3_api:close(Conn)
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

set_tick_timer() ->
    Timeout = application:get_env(rabbitmq_stream_s3, tick_timeout_milliseconds, 5000),
    _ = erlang:send_after(Timeout, self(), tick_timeout),
    ok.

-spec metadata() -> rabbitmq_stream_s3_log_manifest_machine:metadata().
metadata() ->
    #{time => erlang:system_time(millisecond)}.

-spec local_retention_fun(stream_id()) -> osiris:retention_fun().
local_retention_fun(Stream) ->
    fun(IdxFiles) ->
        try ets:lookup_element(?LAST_TIERED_OFFSET_TABLE, Stream, 2) of
            LastTieredOffset ->
                eval_local_retention(IdxFiles, LastTieredOffset)
        catch
            error:badarg ->
                {[], IdxFiles}
        end
    end.

-spec eval_local_retention(IdxFiles :: [filename()], osiris:offset()) ->
    {ToDelete :: [filename()], ToKeep :: [filename(), ...]}.
eval_local_retention(IdxFiles, LastTieredOffset) ->
    %% Always keep the current active segment no matter what the last tiered
    %% offset is.
    eval_local_retention(lists:reverse(IdxFiles), LastTieredOffset, [], []).

eval_local_retention([], _LastTieredOffset, ToDelete, ToKeep) ->
    %% Always keep the current active segment no matter what the last tiered
    %% offset is.
    {lists:reverse(ToDelete), ToKeep};
eval_local_retention([IdxFile | Rest], LastTieredOffset, ToDelete, ToKeep) ->
    Offset = binary_to_integer(filename:basename(IdxFile, <<".index">>)),
    case Offset > LastTieredOffset of
        true ->
            eval_local_retention(Rest, LastTieredOffset, ToDelete, [IdxFile | ToKeep]);
        false ->
            {lists:reverse(Rest), [IdxFile | ToKeep]}
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

eval_local_retention_test() ->
    IdxFiles = [
        <<"/data/00000000000000000000.index">>,
        <<"/data/00000000000000000100.index">>,
        <<"/data/00000000000000000200.index">>,
        <<"/data/00000000000000000300.index">>,
        <<"/data/00000000000000000400.index">>
    ],
    ?assertEqual(
        {
            [
                <<"/data/00000000000000000000.index">>,
                <<"/data/00000000000000000100.index">>
            ],
            [
                <<"/data/00000000000000000200.index">>,
                <<"/data/00000000000000000300.index">>,
                <<"/data/00000000000000000400.index">>
            ]
        },
        eval_local_retention(IdxFiles, 250)
    ),
    %% Always keep the current segment:
    ?assertEqual(
        {
            [
                <<"/data/00000000000000000000.index">>,
                <<"/data/00000000000000000100.index">>,
                <<"/data/00000000000000000200.index">>,
                <<"/data/00000000000000000300.index">>
            ],
            [
                <<"/data/00000000000000000400.index">>
            ]
        },
        eval_local_retention(IdxFiles, 450)
    ),
    ?assertEqual(
        {
            [
                <<"/data/00000000000000000000.index">>,
                <<"/data/00000000000000000100.index">>,
                <<"/data/00000000000000000200.index">>
            ],
            [
                <<"/data/00000000000000000300.index">>,
                <<"/data/00000000000000000400.index">>
            ]
        },
        eval_local_retention(IdxFiles, 300)
    ),
    ?assertEqual(
        {
            [],
            [
                <<"/data/00000000000000000000.index">>,
                <<"/data/00000000000000000100.index">>,
                <<"/data/00000000000000000200.index">>,
                <<"/data/00000000000000000300.index">>,
                <<"/data/00000000000000000400.index">>
            ]
        },
        eval_local_retention(IdxFiles, 0)
    ),
    ok.

-endif.
