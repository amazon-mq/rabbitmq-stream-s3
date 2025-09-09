%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_log_manifest).

%% TODO: this is just for testing.
-export([recover_fragments/1]).

-include_lib("osiris/src/osiris.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-define(SERVER, ?MODULE).

-behaviour(osiris_log_manifest).
-behaviour(gen_server).

-define(LOG_HEADER_HASH, erlang:crc32(?LOG_HEADER)).

-record(fragment, {
    segment_offset :: osiris:offset(),
    segment_pos = ?LOG_HEADER_SIZE :: pos_integer(),
    %% Number of chunks in prior fragments and number in current fragment.
    num_chunks = {0, 0} :: {non_neg_integer(), non_neg_integer()},
    first_offset :: osiris:offset() | undefined,
    first_timestamp :: osiris:timestamp() | undefined,
    last_offset :: osiris:offset() | undefined,
    next_offset :: osiris:offset() | undefined,
    %% Zero-based increasing integer for sequence number within the segment.
    seq_no = 0 :: non_neg_integer(),
    %% NOTE: header size is not included.
    size = 0 :: non_neg_integer(),
    %% TODO: do checksum during upload if undefined.
    checksum = ?LOG_HEADER_HASH :: checksum() | undefined
}).

-record(writer, {
    %% Pid of the osiris_writer process. Used to attach offset listeners.
    pid :: pid(),
    %% Local dir of the log.
    dir :: file:filename_all(),
    %% Current commit offset (updated by offset listener notifications) known
    %% to the manifest - this can lag behind the actual commit offset.
    commit_offset = -1 :: osiris:offset(),
    %% List of segments in ascending offset order which have been rolled and
    %% are awaiting upload.
    uncommitted_fragments = [] :: [#fragment{}],
    %% Fragments which are currently being uploaded and their monitor ref.
    uploading_fragments = #{} :: #{osiris:offset() => reference()},
    %% List of fragments in ascending offset order which have been uploaded
    %% successfully but have not yet been applied to the manifest.
    uploaded_fragments = [] :: [#fragment_info{}],
    %% The next offset that should be uploaded.
    %% All offsets under this have been tiered without any "holes" in the
    %% remote log.
    next_tiered_offset :: osiris:offset() | undefined
}).

-record(manifest_writer, {
    type :: writer | acceptor,
    local :: osiris_log:manifest(),
    %% Only defined for writers:
    writer_ref :: writer_ref() | undefined,
    fragment :: #fragment{} | undefined
}).

-record(manifest, {
    first_offset :: osiris:offset(),
    first_timestamp :: osiris:timestamp(),
    total_size :: non_neg_integer(),
    entries :: binary()
}).

%% NOTE: Pending is reversed.
-type upload_status() :: {uploading, Pending :: [#fragment{}]} | {last_uploaded, non_neg_integer()}.

-record(?MODULE, {
    writers = #{} :: #{writer_ref() => #writer{}},
    manifests = #{} :: #{
        file:filename_all() =>
            {#manifest{}, upload_status()} | undefined | {pending, reference(), [gen_server:from()]}
    },
    tasks = #{} :: #{reference() => task()}
}).

%% Set by `rabbit_stream_queue:make_stream_conf/1'.
-type writer_ref() :: rabbit_amqqueue:name().

-type task() ::
    {manifest, file:filename_all()} | {fragment, writer_ref(), #fragment{}}.

-type checksum() :: non_neg_integer().

%% osiris_log_manifest
-export([
    writer_manifest/1,
    acceptor_manifest/2,
    overview/1,
    find_data_reader_position/2,
    find_offset_reader_position/2,
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

-export([start/0, format_osiris_event/1]).

%% Useful to search module.
-export([fragment_key/2, group_key/3, make_file_name/2, fragment_trailer_to_info/1]).

%% This server needs to be started by a boot step so that it is online before
%% the stream coordinator. Otherwise the stream coordinator will attempt to
%% recover replicas before this server is started and init_manifest/2 will
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
    ok = rabbitmq_stream_s3_counters:init(),
    rabbit_sup:start_child(?MODULE).

%%----------------------------------------------------------------------------

handle_event(Event, #manifest_writer{local = Local0, type = acceptor} = Manifest0) ->
    Manifest0#manifest_writer{local = osiris_log:handle_event(Event, Local0)};
handle_event(
    {segment_opened, RolledSegment, NewSegment} = Event,
    #manifest_writer{local = Local0, writer_ref = WriterRef, fragment = Fragment0} = Manifest0
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
                            {fragment, WriterRef, Fragment0}
                        )
                end,
                #fragment{segment_offset = segment_file_offset(NewSegment)}
        end,
    Manifest0#manifest_writer{
        local = osiris_log:handle_event(Event, Local0),
        fragment = Fragment
    };
handle_event(
    {chunk_written, #chunk_info{id = ChId, timestamp = Ts, num = NumRecords, size = ChunkSize},
        Chunk} = Event,
    #manifest_writer{
        local = Local0,
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
                ok = gen_server:cast(?SERVER, {fragment, WriterRef, Fragment2}),
                #fragment{
                    segment_offset = SegmentOffset,
                    segment_pos = SegmentPos0 + Size,
                    num_chunks = {StartNumChunks + NumChunks, 0},
                    seq_no = SeqNo0 + 1
                };
            false ->
                Fragment2
        end,
    Manifest0#manifest_writer{
        local = osiris_log:handle_event(Event, Local0),
        fragment = Fragment
    };
handle_event({retention_updated, _Retention}, #manifest_writer{} = Manifest) ->
    %% TODO
    Manifest.

checksum(undefined, _) ->
    undefined;
checksum(Checksum, Data) ->
    erlang:crc32(Checksum, Data).

writer_manifest(#{dir := Dir, reference := Ref} = Config0) ->
    %% TODO: upon init the writer needs to figure out what was uploaded to S3
    %% unsuccessfully given their local log.
    Config = Config0#{max_segment_size_bytes := ?MAX_SEGMENT_SIZE_BYTES},
    {LocalInfo, _, Local} = osiris_log:writer_manifest(Config),
    %% We only need two keys:
    Remote = gen_server:call(?SERVER, {init_writer, Ref, Dir}),
    ?LOG_DEBUG("Recovering stream ~ts", [Dir]),
    Fragment = recover_fragment(Dir, Ref, LocalInfo, Remote),
    Manifest = #manifest_writer{
        type = writer,
        local = Local,
        writer_ref = Ref,
        fragment = Fragment
    },
    {merge_writer_info(LocalInfo, Remote), Config, Manifest}.

%% TODO: it doesn't matter that the manifest itself is undefined. We can
%% upload segments when creating a stream and then the manifest could fail
%% to be written. So we need to attempt to find 00000...000.fragment anyways.
recover_fragment(_Dir, _Ref, #{num_segments := 0}, undefined) ->
    ?LOG_DEBUG("Fully empty stream"),
    %% Empty stream, nothing to recover.
    #fragment{segment_offset = 0, seq_no = 0};
recover_fragment(_Dir, _Ref, #{num_segments := 0}, _) ->
    ?LOG_DEBUG("Empty local stream but remote has stuff"),
    %% TODO: can we recover from this as a writer?
    exit({todo, ?FUNCTION_NAME, no_local_data});
recover_fragment(
    _Dir,
    _Ref,
    #{
        active_segment := #{
            size := Size,
            chunks := NumChunks,
            first := #chunk_info{id = FirstChId, timestamp = FirstTs},
            last := #chunk_info{id = LastChId, num = Num}
        }
    },
    undefined
) when Size < ?MAX_FRAGMENT_SIZE_B ->
    ?LOG_DEBUG("Local active segment is smaller than a fragment"),
    %% This first segment is smaller than a fragment. Use this part of the
    %% segment as-is. We can avoid reading the index file at all in this case.
    F = #fragment{
        segment_offset = FirstChId,
        num_chunks = {0, NumChunks},
        first_offset = FirstChId,
        first_timestamp = FirstTs,
        last_offset = LastChId,
        next_offset = LastChId + Num,
        seq_no = 0,
        size = Size - ?LOG_HEADER_SIZE,
        checksum = undefined
    },
    ?LOG_DEBUG("Recovered fragment ~w", [F]),
    F;
recover_fragment(
    _Dir,
    Ref,
    #{
        active_segment := #{
            file := File, size := SegmentSize, last := #chunk_info{id = LastChId, num = Num}
        }
    },
    undefined
) ->
    ?LOG_DEBUG("Recovering fragments from active segment (not published)"),
    NextOffset = LastChId + Num,
    %% The manifest was never successfully written. Queue fragments for upload
    %% now and figure out the currently active fragment.
    {Fragment0, Fragments} = recover_fragments(File),
    _ = [ok = gen_server:cast(?SERVER, {fragment, Ref, F}) || F <- Fragments],
    %% next_offset and size are filled in with the info from active_segment.
    Size = SegmentSize - Fragment0#fragment.segment_pos,
    F = Fragment0#fragment{next_offset = NextOffset, size = Size},
    ?LOG_DEBUG("Recovered active fragment ~w, existing fragments ~w", [F, Fragments]),
    F;
recover_fragment(
    Dir,
    Ref,
    #{active_segment := #{last := #chunk_info{id = LastChId}}},
    #manifest{entries = Entries}
) ->
    ?LOG_DEBUG("There are local segments and remote fragments"),
    {LastTieredFragmentChId, PendingInfos} = rabbitmq_stream_s3_log_manifest_search:recover_fragments(
        Dir, Entries
    ),
    %% Assert that the last tiered offset is less than last chunk ID here.
    %% If it isn't we might be able to recover by creating a segment out of
    %% fragments?
    ?LOG_DEBUG("Local segment last is ~b and last fragment is ~b, recovered pending: ~w", [
        LastChId, LastTieredFragmentChId, PendingInfos
    ]),
    ?assert(LastTieredFragmentChId < LastChId),
    %% TODO: the manifest server needs to ignore fragment infos which it knows
    %% are already tiered.
    _ = [ok = gen_server:cast(?SERVER, {fragment_uploaded, Ref, I}) || I <- PendingInfos],
    exit({todo, ?FUNCTION_NAME, the_hard_case}).

recover_fragments(File) ->
    ?LOG_DEBUG("Recoving fragments from segment file ~ts", [File]),
    SegmentOffset = segment_file_offset(File),
    IdxFile = iolist_to_binary(string:replace(File, ".segment", ".index", trailing)),
    %% TODO: we should be reading in smaller chunks with pread.
    {ok, <<_:?IDX_HEADER_SIZE/binary, IdxArray/binary>>} = file:read_file(IdxFile),
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
    FragmentBoundary = rabbitmq_stream_s3_binary_array:partition_point(
        fun(<<_ChId:64, _Ts:64, _E:64, FilePos:32/unsigned, _ChT:8>>) ->
            %% ?LOG_DEBUG("testing ~b > ~b", [Threshold0, FilePos]),
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

merge_writer_info(#{num_segments := 0}, undefined) ->
    %% Totally empty stream.
    #{num_segments => 0};
merge_writer_info(#{num_segments := 0}, _) ->
    %% TODO: this should not really be possible except theoretically. Think
    %% about if we want to allow this case.
    exit(empty_writer_with_content_in_remote_tier);
merge_writer_info(Local, undefined) ->
    Local;
merge_writer_info(
    #{num_segments := NLocalSegs, active_segment := ActiveSegment},
    #manifest{first_offset = FirstOffset, first_timestamp = FirstTimestamp}
) ->
    %% TODO: this is simple. Think about edge cases. I'm assuming that the
    %% remote tier starts further back that the local log (or at the same
    %% point).
    #{
        %% Dummy value. Screw it, it's only used for counters. We could make
        %% our own counters if we really wanted.
        num_segments => NLocalSegs,
        first_offset => FirstOffset,
        first_timestamp => FirstTimestamp,
        active_segment => ActiveSegment
    }.

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
            Info = gen_server:call(?SERVER, {acceptor_overview, Dir}, infinity),
            maps:merge(LocalOverview, Info)
    end.

acceptor_manifest(Overview0, #{dir := Dir, epoch := Epoch} = Config0) ->
    ?LOG_DEBUG("acceptor got remote overview: ~w", [Overview0]),
    Config = Config0#{max_segment_size_bytes := ?MAX_SEGMENT_SIZE_BYTES},
    case list_dir(Dir) of
        [] ->
            Overview =
                case Overview0 of
                    #{last_tiered_fragment_offset := LTFO, last_tiered_segment_offset := LTSO} ->
                        NextOffset = create_sparse_segment(Dir, Epoch, LTSO, LTFO),
                        Overview0#{epoch_offsets := [{Epoch, NextOffset}]};
                    _ ->
                        Overview0
                end,
            {LocalInfo, _, Local} = osiris_log:acceptor_manifest(Overview, Config),
            Manifest = #manifest_writer{
                type = acceptor,
                local = Local
            },
            {LocalInfo, Config, Manifest};
        _ ->
            exit(replica_local_log_has_data)
    end.

find_data_reader_position(TailInfo, Config) ->
    %% Data readers are only used for replication. With tiered storage the only
    %% point of replication is to ensure durability with minimal latency. Once
    %% in the remote tier there's no point in replicating the data anymore as
    %% the remote tier is assumed to be durable.
    osiris_log:find_data_reader_position(TailInfo, Config).

find_offset_reader_position(first, #{dir := Dir} = Config) ->
    %% For offset-spec `first` we always need to check the remote tier. Only
    %% if it is empty can we serve `first` from the local tier.
    case gen_server:call(?SERVER, {init_reader, Dir}) of
        undefined ->
            osiris_log:find_offset_reader_position(first, Config);
        #manifest{first_offset = FirstOffset} ->
            %% `first` will always be a segment boundary. So we can figure out
            %% the first offset directly. It's the first chunk.
            Pos = ?LOG_HEADER_SIZE,
            File = filename:join(Dir, make_file_name(FirstOffset, "segment")),
            ?LOG_DEBUG(
                "Attaching to remote tier at offset ~b (byte ~b) for spec 'first' in ~ts",
                [FirstOffset, Pos, File]
            ),
            {ok, FirstOffset, Pos, File}
    end;
find_offset_reader_position(Offset, #{dir := Dir} = Config) when is_integer(Offset) ->
    ?LOG_DEBUG(?MODULE_STRING ":~ts/2 finding offset ~b", [?FUNCTION_NAME, Offset]),
    case osiris_log:find_offset_reader_position({abs, Offset}, Config) of
        {ok, _, _, _} = FoundLocally ->
            FoundLocally;
        {error, {offset_out_of_range, {_First, LastLocalOffset}}} when Offset > LastLocalOffset ->
            ?LOG_DEBUG("requested offset ~b is higher than last local offset ~b", [
                Offset, LastLocalOffset
            ]),
            %% TODO: try the remote tier? Or just attach to next?
            exit({todo, offset_find_higher_than_local_tier});
        {error, {offset_out_of_range, Range}} ->
            ?LOG_DEBUG("offset ~b is not local (local range ~w), trying the remote tier", [
                Offset, Range
            ]),
            case gen_server:call(?SERVER, {init_reader, Dir}) of
                undefined ->
                    %% TODO: No stream? Just attach to next? I guess?
                    osiris_log:find_offset_reader_position(next, Config);
                #manifest{first_offset = FirstOffset} when Offset < FirstOffset ->
                    %% NOTE: emulates osiris's offset behavior.
                    %% `first` will always be a segment boundary. So we can figure out
                    %% the first offset directly. It's the first chunk.
                    Pos = ?LOG_HEADER_SIZE,
                    File = filename:join(Dir, make_file_name(FirstOffset, "segment")),
                    ?LOG_DEBUG(
                        "Attaching to remote tier at offset ~b (byte ~b) for spec ~b in ~ts",
                        [FirstOffset, Pos, Offset, File]
                    ),
                    {ok, FirstOffset, Pos, File};
                #manifest{entries = Entries} ->
                    rabbitmq_stream_s3_log_manifest_search:position(Dir, Offset, Entries)
            end;
        {error, _} = Err ->
            Err
    end;
find_offset_reader_position(OffsetSpec, Config) ->
    ?LOG_DEBUG("~ts with offset spec ~w", [?FUNCTION_NAME, OffsetSpec]),
    %% TODO: support all offset specs. For some offset specs we might be able
    %% to skip reading the manifest (i.e. tail reads).
    case osiris_log:find_offset_reader_position(OffsetSpec, Config) of
        {ok, _, _, _} = FoundLocally ->
            FoundLocally;
        {error, _} = Err ->
            ?LOG_DEBUG("~ts error: ~w", [?FUNCTION_NAME, Err]),
            Err
    end.

close_manifest(#manifest_writer{}) ->
    %% TODO: unregister writers with the server.
    ok.

delete(_Config) ->
    %% TODO use the `dir` from config to delete the remote manifest and
    %% fragments.
    ok.

%%---------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    ok = application:ensure_started(rabbitmq_aws),
    %% {ok, AccessKey} = application:get_env(rabbitmq_stream_s3, aws_access_key),
    %% {ok, SecretKey} = application:get_env(rabbitmq_stream_s3, aws_secret_key),
    %% {ok, Region} = application:get_env(rabbitmq_stream_s3, aws_region),
    %% ok = rabbitmq_aws:set_credentials(AccessKey, SecretKey),
    %% ok = rabbitmq_aws:set_region(Region),

    %% TODO: temporary while we test larger segments.
    application:set_env(
        osiris,
        max_segment_size_chunks,
        %% 256_000 by default

        %% 2^20
        1_048_576
    ),

    {ok, #?MODULE{}}.

handle_call(
    {init_writer, WriterRef, Dir},
    {Pid, _Tag} = From,
    #?MODULE{writers = Writers0} = State0
) ->
    ok = register_offset_listener(Pid, -1),
    State = State0#?MODULE{
        writers = Writers0#{
            WriterRef => #writer{pid = Pid, dir = Dir}
        }
    },
    async_get_manifest(Dir, From, State);
handle_call({init_reader, Dir}, From, State) ->
    %% TODO: this is too simplistic. Reading from the root manifest should be
    %% done within the server. And then the server should give a spec to
    %% readers to find anything within branches.
    async_get_manifest(Dir, From, State);
handle_call({acceptor_overview, Dir}, _From, #?MODULE{manifests = Manifests} = State) ->
    Overview =
        case Manifests of
            #{Dir := {Manifest, _}} ->
                acceptor_overview(Manifest);
            _ ->
                #{}
        end,
    {reply, Overview, State};
handle_call(Request, _From, State) ->
    {stop, {unhandled_call, Request}, State}.

async_get_manifest(
    Dir, ReplyTo, #?MODULE{manifests = Manifests0, tasks = Tasks0} = State0
) ->
    case Manifests0 of
        #{Dir := {pending, Task, Replies0}} ->
            %% The manifest is being downloaded. Add this process to
            %% the set of waiting callers.
            Manifests = Manifests0#{
                Dir := {pending, Task, [ReplyTo | Replies0]}
            },
            State = State0#?MODULE{manifests = Manifests},
            {noreply, State};
        #{Dir := {#manifest{} = Manifest, _UploadStatus}} ->
            %% The manifest was already downloaded. Use the cached
            %% value.
            {reply, Manifest, State0};
        _ ->
            %% Otherwise kick off a task for the download.
            {_Pid, MRef} = spawn_monitor(fun() ->
                download_manifest(Dir)
            end),
            Manifests = Manifests0#{Dir => {pending, MRef, [ReplyTo]}},
            State = State0#?MODULE{
                manifests = Manifests,
                tasks = Tasks0#{MRef => {manifest, Dir}}
            },
            {noreply, State}
    end.

handle_cast(
    {manifest, Dir, Manifest},
    #?MODULE{manifests = Manifests0, tasks = Tasks0} = State0
) ->
    case Manifests0 of
        #{Dir := {pending, MRef, Replies}} ->
            true = erlang:demonitor(MRef),
            lists:foreach(
                fun(Caller) ->
                    gen_server:reply(Caller, Manifest)
                end,
                Replies
            ),
            UploadStatus = {last_uploaded, 0},
            State = State0#?MODULE{
                manifests = Manifests0#{Dir := {Manifest, UploadStatus}},
                tasks = maps:remove(MRef, Tasks0)
            },
            {noreply, State};
        _ ->
            {noreply, State0}
    end;
handle_cast(
    {rebalanced_manifest, Dir, Manifest0},
    #?MODULE{manifests = Manifests0, tasks = Tasks0} = State0
) ->
    case Manifests0 of
        #{Dir := {_Manifest0, UploadStatus0}} ->
            %% assertion
            {uploading, Pending0} = UploadStatus0,
            %% Pending is stored reversed for quick prepends.
            Pending = lists:reverse(Pending0),
            %% Force an update of the manifest when rebalancing.
            UploadStatus1 = {last_uploaded, infinity},
            {Manifest, UploadStatus, Tasks} =
                apply_infos(Pending, Manifest0, UploadStatus1, Tasks0, Dir),
            State = State0#?MODULE{
                manifests = Manifests0#{Dir := {Manifest, UploadStatus}},
                tasks = Tasks
            },
            {noreply, State};
        _ ->
            ?LOG_ERROR("This shouldn't happen (rebalancing)! ~p ~w", [Dir, Manifest0]),
            {noreply, State0}
    end;
handle_cast(
    {manifest_uploaded, Dir},
    #?MODULE{manifests = Manifests0, tasks = Tasks0} = State0
) ->
    case Manifests0 of
        #{Dir := {Manifest0, UploadStatus0}} ->
            %% assertion
            {uploading, Pending0} = UploadStatus0,
            %% Pending is stored reversed for quick prepends.
            Pending = lists:reverse(Pending0),
            {Manifest, UploadStatus, Tasks} =
                apply_infos(Pending, Manifest0, {last_uploaded, 0}, Tasks0, Dir),
            State = State0#?MODULE{
                manifests = Manifests0#{Dir := {Manifest, UploadStatus}},
                tasks = Tasks
            },
            {noreply, State};
        _ ->
            ?LOG_ERROR("This shouldn't happen (after upload)! ~p", [Dir]),
            {noreply, State0}
    end;
handle_cast(
    {fragment, WriterRef, Fragment},
    #?MODULE{writers = Writers0} = State0
) ->
    case Writers0 of
        #{WriterRef := #writer{uncommitted_fragments = Fragments0} = Writer0} ->
            Fragments = [Fragment | Fragments0],
            Writer = Writer0#writer{uncommitted_fragments = Fragments},
            State = State0#?MODULE{writers = Writers0#{WriterRef := Writer}},
            {noreply, State};
        _ ->
            {noreply, State0}
    end;
handle_cast(
    {fragment_uploaded, WriterRef, #fragment_info{offset = Offset} = Info},
    #?MODULE{writers = Writers0, manifests = Manifests0, tasks = Tasks0} = State0
) ->
    case Writers0 of
        #{
            WriterRef := #writer{
                dir = Dir,
                uploading_fragments = Uploading0,
                uploaded_fragments = Uploaded0,
                next_tiered_offset = NTO0
            } = Writer0
        } ->
            %% The fragment might've been uploaded by an old incarnation of
            %% the writer before shutting down, so we might not have the
            %% fragment in state here.
            {Tasks2, Uploading} =
                case Uploading0 of
                    #{Offset := MRef} ->
                        true = erlang:demonitor(MRef),
                        Tasks1 = maps:remove(MRef, Tasks0),
                        Uploading1 = maps:remove(Offset, Uploading0),
                        {Tasks1, Uploading1};
                    _ ->
                        {Tasks0, Uploading0}
                end,
            %% Fragments could possibly be uploaded out of order. Only add the
            %% uploaded fragments to the manifest once there are no "holes"
            %% remaining in the sequence of fragments.
            %% TODO: this list is always sorted and is built one fragment at a
            %% time. Write a little helper to insert at the right position in
            %% linear time.
            Uploaded1 = sort_infos([Info | Uploaded0]),
            {NTO, Pending, Finished} = split_uploaded_infos(NTO0, Uploaded1, []),
            #{Dir := {Manifest0, UploadStatus0}} = Manifests0,
            {Manifest, UploadStatus, Tasks} = apply_infos(
                Finished, Manifest0, UploadStatus0, Tasks2, Dir
            ),
            Writer = Writer0#writer{
                uploading_fragments = Uploading,
                uploaded_fragments = Pending,
                next_tiered_offset = NTO
            },
            State = State0#?MODULE{
                writers = Writers0#{WriterRef := Writer},
                manifests = Manifests0#{Dir := {Manifest, UploadStatus}},
                tasks = Tasks
            },
            {noreply, State};
        _ ->
            {noreply, State0}
    end;
handle_cast(Message, State) ->
    ?LOG_DEBUG(?MODULE_STRING " received unexpected cast: ~W", [Message, 10]),
    {noreply, State}.

handle_info(
    {osiris_offset, WriterRef, CommitOffset},
    #?MODULE{writers = Writers0, tasks = Tasks0} = State0
) ->
    case Writers0 of
        #{
            WriterRef := #writer{
                pid = Pid,
                dir = Dir,
                uncommitted_fragments = Uncommitted0,
                uploading_fragments = Uploading0
            } = Writer0
        } ->
            {Committed, Uncommitted} = lists:splitwith(
                fun(#fragment{last_offset = LastOffset}) ->
                    LastOffset < CommitOffset
                end,
                Uncommitted0
            ),
            {Uploading, Tasks} = lists:foldl(
                fun(#fragment{first_offset = Offset} = Fragment, {Uploading1, Tasks1}) ->
                    {_Pid, MRef} = spawn_monitor(fun() ->
                        upload_fragment(WriterRef, Dir, Fragment)
                    end),
                    Uploading2 = Uploading1#{Offset => MRef},
                    Tasks2 = Tasks1#{MRef => {fragment, WriterRef, Fragment}},
                    {Uploading2, Tasks2}
                end,
                {Uploading0, Tasks0},
                Committed
            ),

            ok = register_offset_listener(Pid, CommitOffset + 1),
            Writer = Writer0#writer{
                commit_offset = CommitOffset,
                uncommitted_fragments = Uncommitted,
                uploading_fragments = Uploading
            },
            Writers = Writers0#{WriterRef := Writer},
            State = State0#?MODULE{writers = Writers, tasks = Tasks},
            {noreply, State};
        _ ->
            {noreply, State0}
    end;
handle_info({'DOWN', MRef, process, Pid, Reason}, #?MODULE{tasks = Tasks0} = State0) ->
    State = State0#?MODULE{tasks = maps:remove(MRef, Tasks0)},
    case Reason of
        normal ->
            {noreply, State};
        _ ->
            ?LOG_INFO("Task ~w (~w) down with reason ~w. Tasks: ~W", [
                Pid, MRef, Reason, Tasks0, 10
            ]),
            %% TODO... retry failed tasks?
            {noreply, State}
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

register_offset_listener(WriterPid, Offset) ->
    osiris:register_offset_listener(
        WriterPid, Offset, {?MODULE, format_osiris_event, []}
    ).

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

split_uploaded_infos(
    NextTieredOffset,
    [#fragment_info{offset = FirstOffset, next_offset = NextOffset} = Info | Rest],
    Acc
) when
    NextTieredOffset =:= undefined orelse FirstOffset =:= NextTieredOffset
->
    split_uploaded_infos(NextOffset, Rest, [Info | Acc]);
split_uploaded_infos(NextTieredOffset, PendingUploaded, Acc) ->
    {NextTieredOffset, PendingUploaded, lists:reverse(Acc)}.

%% Sort fragment trailers by offset, ascending.
sort_infos(Infos) when is_list(Infos) ->
    lists:sort(
        fun(#fragment_info{offset = OffsetA}, #fragment_info{offset = OffsetB}) ->
            OffsetA =< OffsetB
        end,
        Infos
    ).

upload_fragment(
    WriterRef,
    Dir,
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
) ->
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
        {UploadMSec, {UploadSize, Trailer}} = timer:tc(
            fun() ->
                {ok, SegFd} = file:open(filename:join(Dir, SegmentFilename), [read, raw, binary]),
                {ok, IdxFd} = file:open(filename:join(Dir, IndexFilename), [read, raw, binary]),

                {ok, SegData} = file:pread(SegFd, SegmentPos, Size),
                {ok, IdxData0} = file:pread(
                    IdxFd,
                    ?IDX_HEADER_SIZE + (IdxStart * ?INDEX_RECORD_SIZE_B),
                    IdxLen * ?INDEX_RECORD_SIZE_B
                ),
                %% Convert from osiris index style to fragment index. We can
                %% drop epoch since it's not necessary after commit. TODO: right?
                %% TODO: this is pretty messy. Use the INDEX_RECORD macro.
                IdxData = <<
                    <<
                        IdxChId:64/unsigned,
                        IdxTs:64/signed,
                        (SegmentFilePos - SegmentPos + ?LOG_HEADER_SIZE):32/unsigned
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
                    (?LOG_HEADER_SIZE + Size + ?IDX_HEADER_SIZE),
                    (byte_size(IdxData))
                ),
                Data = [?LOG_HEADER, SegData, ?IDX_HEADER, IdxData, Trailer],
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

        ok = gen_server:cast(?SERVER, {fragment_uploaded, WriterRef, Trailer})
    after
        ok = rabbitmq_aws:close_connection(Handle)
    end.

-doc """
Apply successfully uploaded fragments to their stream's manifest.

This function also evaluates whether the manifest should be rebalanced and/or
uploaded to the remote tier.
""".
apply_infos(
    [], #manifest{entries = Entries} = Manifest, {last_uploaded, _} = UploadStatus0, Tasks0, Dir
) when
    ?ENTRIES_LEN(Entries) >= 2 * ?MANIFEST_BRANCHING_FACTOR
->
    %% The manifest is loaded. Try to rebalance away a group. TODO see if we
    %% can improve this "load factor." It's pretty simple at the moment.
    case rabbitmq_stream_s3_log_manifest_entry:rebalance(Entries) of
        undefined ->
            ?LOG_DEBUG("Manifest is loaded but rebalancing is not possible.", []),
            {Manifest, UploadStatus0, Tasks0};
        {GroupKind, GroupSize, Group, Rebalanced} ->
            ?LOG_DEBUG("Compacting away ~b kind ~b's from entries of byte size ~b", [
                ?MANIFEST_BRANCHING_FACTOR, GroupKind, byte_size(Entries)
            ]),
            {_, MRef} = spawn_monitor(fun() ->
                rebalance_manifest(Dir, GroupKind, GroupSize, Group, Rebalanced, Manifest)
            end),
            Tasks = Tasks0#{MRef => {rebalance_manifest, Dir}},
            {Manifest, {uploading, []}, Tasks}
    end;
apply_infos([], Manifest, {last_uploaded, NumUpdates}, Tasks0, Dir) when
    NumUpdates >= ?FRAGMENT_UPLOADS_PER_MANIFEST_UPDATE
->
    %% Updates have been debounced but there have been enough that now it is
    %% time to perform the upload.
    case NumUpdates of
        infinity ->
            ?LOG_DEBUG("Forcing upload of manifest");
        _ when is_integer(NumUpdates) ->
            ?LOG_DEBUG("Uploading manifest because there have been ~b updates since last upload", [
                NumUpdates
            ])
    end,
    {_, MRef} = spawn_monitor(fun() -> upload_manifest(Dir, Manifest) end),
    Tasks = Tasks0#{MRef => {rebalance_manifest, Dir}},
    {Manifest, {uploading, []}, Tasks};
apply_infos([], Manifest, UploadStatus, Tasks, _Dir) ->
    %% The manifest is currently being uploaded, or there are no updates
    %% necessary. Skip the upload.
    ?LOG_DEBUG("Skipping upload of manifest with status ~w", [UploadStatus]),
    {Manifest, UploadStatus, Tasks};
apply_infos(
    [#fragment_info{offset = Offset, timestamp = Ts, seq_no = SeqNo, size = Size} | Rest],
    undefined,
    UploadStatus0,
    Tasks,
    Dir
) ->
    ?assertEqual({last_uploaded, 0}, UploadStatus0),
    %% The very first fragment in the manifest. Create a new manifest.
    Manifest = #manifest{
        first_offset = Offset,
        first_timestamp = Ts,
        total_size = Size,
        entries = ?ENTRY(Offset, Ts, ?MANIFEST_KIND_FRAGMENT, Size, SeqNo, <<>>)
    },
    %% And force its upload.
    UploadStatus = {last_uploaded, infinity},
    apply_infos(Rest, Manifest, UploadStatus, Tasks, Dir);
apply_infos([Fragment | Rest], Manifest, {uploading, Pending0}, Tasks, Dir) ->
    %% The manifest is currently being uploaded. Queue the fragment for later
    %% application once the current upload completes.
    apply_infos(Rest, Manifest, {uploading, [Fragment | Pending0]}, Tasks, Dir);
apply_infos(
    [#fragment_info{offset = Offset, timestamp = Ts, seq_no = SeqNo, size = Size} | Rest],
    #manifest{total_size = TotalSize0, entries = Entries0} = Manifest0,
    {last_uploaded, NumUpdates0},
    Tasks,
    Dir
) ->
    %% Common case: the manifest exists. Append the fragment to the entries.
    Manifest = Manifest0#manifest{
        total_size = TotalSize0 + Size,
        entries =
            <<Entries0/binary,
                ?ENTRY(Offset, Ts, ?MANIFEST_KIND_FRAGMENT, Size, SeqNo, <<>>)/binary>>
    },
    apply_infos(Rest, Manifest, {last_uploaded, NumUpdates0 + 1}, Tasks, Dir).

rebalance_manifest(Dir, GroupKind, GroupSize, GroupEntries, RebalancedEntries, Manifest0) ->
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

    ok = gen_server:cast(?SERVER, {rebalanced_manifest, Dir, Manifest}).

upload_manifest(Dir, #manifest{
    first_offset = Offset, first_timestamp = Ts, total_size = Size, entries = Entries
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

        ok = gen_server:cast(?SERVER, {manifest_uploaded, Dir})
    after
        ok = rabbitmq_aws:close_connection(Handle)
    end.

download_manifest(Dir) ->
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
    ok = gen_server:cast(?SERVER, {manifest, Dir, Manifest}).

group_extension(?MANIFEST_KIND_GROUP) -> "group";
group_extension(?MANIFEST_KIND_KILO_GROUP) -> "kgroup";
group_extension(?MANIFEST_KIND_MEGA_GROUP) -> "mgroup".

group_header(?MANIFEST_KIND_GROUP) ->
    <<?MANIFEST_GROUP_MAGIC, ?MANIFEST_GROUP_VERSION:32/unsigned>>;
group_header(?MANIFEST_KIND_KILO_GROUP) ->
    <<?MANIFEST_KILO_GROUP_MAGIC, ?MANIFEST_KILO_GROUP_VERSION:32/unsigned>>;
group_header(?MANIFEST_KIND_MEGA_GROUP) ->
    <<?MANIFEST_MEGA_GROUP_MAGIC, ?MANIFEST_MEGA_GROUP_VERSION:32/unsigned>>.

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
            {?LOG_HEADER_SIZE, ?LOG_HEADER_SIZE + ?HEADER_SIZE_B}
        ),
        ?LOG_DEBUG("Writing segment header with first chunk ~w", [FirstChunkHeader]),
        ok = file:write(SegFd, [?LOG_HEADER, FirstChunkHeader]),

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
            next_offset = NextOffset,
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
            ?IDX_HEADER_SIZE +
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
            {FragmentFilePos, FragmentDataSize + ?LOG_HEADER_SIZE}
        ),
        ?LOG_DEBUG("Read ~b bytes (~b - ~b) of last chunk data: ~W", [
            byte_size(LastChunkData), FragmentFilePos, FragmentDataSize, LastChunkData, 10
        ]),
        ok = file:write(SegFd, LastChunkData),
        NextOffset
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
