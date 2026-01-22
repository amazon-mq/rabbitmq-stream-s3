%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_log_manifest_machine_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-import(rabbit_ct_helpers, [get_config/2]).

-define(MAC, rabbitmq_stream_s3_log_manifest_machine).
-define(META(), #{time => ?LINE}).

all() ->
    [
        spawn_writer,
        simultaneous_manifest_requests,
        spawn_writer_after_readers,
        out_of_order_fragment_uploads,
        recover_uploaded_fragments,
        manifest_replication,
        retention
    ].

%%----------------------------------------------------------------------------

spawn_writer(Config) ->
    Dir = directory(Config),
    Mac0 = ?MAC:new(),
    Pid = self(),
    StreamId = erlang:make_ref(),
    Event1 = #writer_spawned{
        stream = StreamId,
        pid = Pid,
        dir = Dir
    },
    {Mac1, Effects1} = ?MAC:apply(?META(), Event1, Mac0),
    ?assertEqual(
        [
            #resolve_manifest{stream = StreamId},
            #register_offset_listener{writer_pid = Pid, offset = -1}
        ],
        Effects1
    ),
    Event2 = #manifest_resolved{stream = Dir, manifest = #manifest{}},
    {_Mac2, Effects2} = ?MAC:apply(?META(), Event2, Mac1),
    ?assertEqual([], Effects2),
    ok.

simultaneous_manifest_requests(Config) ->
    %% While a writer is starting up an offset reader could start up too, for
    %% example when using `stream-perf-test` on single-node RabbitMQ. The
    %% download of the manifest should be shared between any processes which
    %% request it without duplicate downloads effects.

    Dir = directory(Config),
    WriterPid = self(),
    StreamId = erlang:make_ref(),
    Event1 = #writer_spawned{
        stream = StreamId,
        pid = WriterPid,
        dir = Dir
    },

    %% Simulate two offset readers starting while the manifest is being
    %% downloaded.
    Pid1 = spawn(fun() -> ok end),
    From1 = {Pid1, erlang:make_ref()},
    Event2 = #manifest_requested{stream = StreamId, requester = From1},
    Pid2 = spawn(fun() -> ok end),
    From2 = {Pid2, erlang:make_ref()},
    Event3 = #manifest_requested{stream = StreamId, requester = From2},

    {Mac1, Effects1} = handle_events(?META(), [Event1, Event2, Event3], ?MAC:new()),
    ?assertMatch(
        [
            #resolve_manifest{stream = StreamId},
            #register_offset_listener{}
        ],
        Effects1
    ),

    Event4 = #manifest_resolved{stream = StreamId, manifest = #manifest{}},
    {_Mac2, Effects2} = ?MAC:apply(?META(), Event4, Mac1),
    ?assertMatch(
        [
            #reply{to = From1, response = #manifest{}},
            #reply{to = From2, response = #manifest{}},
            #set_range{next = 0}
        ],
        Effects2
    ),
    ok.

spawn_writer_after_readers(Config) ->
    %% A writer could hypothetically start up after readers have requested the
    %% manifest. The manifest should only be resolved once.

    Dir = directory(Config),
    WriterPid = self(),
    StreamId = erlang:make_ref(),
    Pid1 = spawn(fun() -> ok end),
    From1 = {Pid1, erlang:make_ref()},
    ManifestRequested1 = #manifest_requested{stream = StreamId, requester = From1},
    Pid2 = spawn(fun() -> ok end),
    From2 = {Pid2, erlang:make_ref()},
    ManifestRequested2 = #manifest_requested{stream = StreamId, requester = From2},
    {Mac1, Effects1} = handle_events(
        ?META(),
        [ManifestRequested1, ManifestRequested2],
        ?MAC:new()
    ),
    ?assertEqual([#resolve_manifest{stream = StreamId}], Effects1),

    WriterSpawned = #writer_spawned{
        stream = StreamId,
        pid = WriterPid,
        dir = Dir
    },
    {Mac2, Effects2} = ?MAC:apply(?META(), WriterSpawned, Mac1),
    ?assertEqual([#register_offset_listener{writer_pid = WriterPid, offset = -1}], Effects2),

    ManifestResolved = #manifest_resolved{stream = StreamId, manifest = #manifest{}},
    {_Mac3, Effects3} = ?MAC:apply(?META(), ManifestResolved, Mac2),
    ?assertMatch(
        [
            #reply{to = From1, response = #manifest{}},
            #reply{to = From2, response = #manifest{}},
            #set_range{next = 0}
        ],
        Effects3
    ),

    ok.

out_of_order_fragment_uploads(Config) ->
    {Mac0, StreamId} = setup_writer(Config, ?MAC:new(#{debounce_modifications => 3})),
    Fragments = [fragment(From, To) || {From, To} <- [{0, 19}, {20, 39}, {40, 59}]],
    FragmentsAvailable = [#fragment_available{stream = StreamId, fragment = F} || F <- Fragments],
    {Mac1, Effects1} = handle_events(?META(), FragmentsAvailable, Mac0),
    ?assertEqual([], Effects1),
    Event1 = #commit_offset_increased{stream = StreamId, offset = 60},
    {Mac2, Effects2} = ?MAC:apply(?META(), Event1, Mac1),
    ?assertMatch(
        [
            #upload_fragment{stream = StreamId, fragment = #fragment{first_offset = 0}},
            #upload_fragment{stream = StreamId, fragment = #fragment{first_offset = 20}},
            #upload_fragment{stream = StreamId, fragment = #fragment{first_offset = 40}},
            #register_offset_listener{}
        ],
        Effects2
    ),
    [Up1, Up2, Up3] = [
        #fragment_uploaded{stream = StreamId, info = fragment_to_info(F)}
     || F <- Fragments
    ],
    {Mac3, Effects3} = handle_events(?META(), [Up2, Up3], Mac2),
    %% The manifest won't be updated until a run of fragments have been uploaded:
    %% no holes are allowed in the manifest.
    ?assertEqual([], Effects3),
    %% Once the first fragment has finished uploading then the manifest is
    %% updated for all fragments.
    {_Mac4, Effects4} = ?MAC:apply(?META(), Up1, Mac3),
    ?assertMatch(
        [
            #set_range{next = 60},
            #upload_manifest{
                stream = StreamId,
                manifest = #manifest{first_offset = 0, next_offset = 60}
            }
        ],
        Effects4
    ),
    ok.

recover_uploaded_fragments(Config) ->
    %% The writer uploads fragments but the updated manifest is not yet
    %% uploaded. When restarting, the writer resolve the full manifest and
    %% upload it.
    {Mac0, StreamId} = setup_writer(Config, ?MAC:new(#{debounce_modifications => 1})),
    [F1, F2, _F3] = Fragments = [fragment(From, To) || {From, To} <- [{0, 19}, {20, 39}, {40, 59}]],
    FragmentsAvailable = [#fragment_available{stream = StreamId, fragment = F} || F <- Fragments],
    {Mac1, Effects1} = handle_events(?META(), FragmentsAvailable, Mac0),
    ?assertEqual([], Effects1),
    COI1 = #commit_offset_increased{stream = StreamId, offset = 40},
    {Mac2, Effects2} = ?MAC:apply(?META(), COI1, Mac1),
    ?assertMatch(
        [
            #upload_fragment{stream = StreamId, fragment = #fragment{first_offset = 0}},
            #upload_fragment{stream = StreamId, fragment = #fragment{first_offset = 20}},
            #register_offset_listener{}
        ],
        Effects2
    ),
    [Up1, _Up2, Up3] = [
        #fragment_uploaded{stream = StreamId, info = fragment_to_info(F)}
     || F <- Fragments
    ],
    %% Say that 1 and 2 are uploaded but before the reboot the manifest server
    %% only sees 1 complete its upload.
    {_Mac3, Effects3} = ?MAC:apply(?META(), Up1, Mac2),
    ?assertMatch(
        [
            #set_range{next = 20},
            #upload_manifest{
                manifest = #manifest{
                    first_offset = 0,
                    total_size = 200,
                    next_offset = 20
                }
            }
        ],
        Effects3
    ),

    %% --- Say that the writer reboots now ---
    %% During recovery it will notify the manifest server of the available
    %% fragments in the current segment. The manifest server will resolve
    %% what has been uploaded (fragments 1 and 2 now) and

    Pid = self(),
    Dir = directory(Config),
    WriterSpawned = #writer_spawned{
        stream = StreamId,
        pid = Pid,
        dir = Dir
    },
    {Mac4, Effects4} = ?MAC:apply(?META(), WriterSpawned, ?MAC:new()),
    ?assertMatch([#resolve_manifest{stream = StreamId}, #register_offset_listener{}], Effects4),
    %% The existing local fragments are sent to the manifest server as
    %% available.
    {Mac5, Effects5} = handle_events(?META(), FragmentsAvailable, Mac4),
    ?assertEqual([], Effects5),
    %% Before the manifest is resolved, the commit offset increases.
    COI2 = #commit_offset_increased{stream = StreamId, offset = 60},
    {Mac6, Effects6} = ?MAC:apply(?META(), COI2, Mac5),
    ?assertMatch([#register_offset_listener{}], Effects6),
    ManifestResolved = #manifest_resolved{
        stream = StreamId,
        manifest = fragments_to_manifest([F1, F2])
    },
    {Mac7, Effects7} = ?MAC:apply(?META(), ManifestResolved, Mac6),
    %% The last fragment is now eligible for upload since the commit offset
    %% increased.
    ?assertMatch(
        [
            #upload_fragment{stream = StreamId, fragment = #fragment{first_offset = 40}},
            #set_range{next = 40}
        ],
        Effects7
    ),
    {_Mac8, Effects8} = ?MAC:apply(?META(), Up3, Mac7),
    ?assertMatch(
        [
            #set_range{next = 60},
            #upload_manifest{manifest = #manifest{first_offset = 0, next_offset = 60}}
        ],
        Effects8
    ),
    ok.

manifest_replication(Config) ->
    Dir = directory(Config),
    WriterPid = self(),
    ReplicaPid = spawn(fun() -> ok end),
    ReplicaNode = 'rabbit@replica',
    StreamId = erlang:make_ref(),
    WriterSpawned = #writer_spawned{
        stream = StreamId,
        pid = WriterPid,
        dir = Dir,
        replica_nodes = [ReplicaNode]
    },
    Writer0 = ?MAC:new(#{debounce_modifications => 3}),
    {Writer1, Effects1} = ?MAC:apply(?META(), WriterSpawned, Writer0),
    ?assertMatch([#resolve_manifest{}, #register_offset_listener{}], Effects1),
    %% Replica manifest server requests the manifest from the writer when the
    %% acceptor initializes.
    ManifestRequested = #manifest_requested{stream = StreamId, requester = ReplicaPid},
    {Writer2, Effects2} = ?MAC:apply(?META(), ManifestRequested, Writer1),
    ?assertEqual([], Effects2),
    %% Once the manifest is resolved, the writer will forward it to the
    %% replica which requested it.
    ManifestResolved = #manifest_resolved{stream = StreamId, manifest = #manifest{}},
    {Writer3, Effects3} = ?MAC:apply(?META(), ManifestResolved, Writer2),
    ?assertMatch(
        [
            #send{to = ReplicaPid, message = ManifestResolved},
            #set_range{next = 0}
        ],
        Effects3
    ),
    AcceptorSpawned = #acceptor_spawned{stream = StreamId},
    {Replica1, Effects4} = handle_events(?META(), [AcceptorSpawned, ManifestResolved], ?MAC:new()),
    ?assertMatch([#set_range{next = 0}], Effects4),
    %% Now when fragments are fully uploaded and applied to the manifest, the
    %% acceptor will get notifications that fragments were applied.
    Fragments = [fragment(From, To) || {From, To} <- [{0, 19}, {20, 39}, {40, 59}]],
    FragmentsAvailable = [#fragment_available{stream = StreamId, fragment = F} || F <- Fragments],
    CommitOffsetIncreased = #commit_offset_increased{stream = StreamId, offset = 60},
    {Writer4, Effects5} = handle_events(
        ?META(), FragmentsAvailable ++ [CommitOffsetIncreased], Writer3
    ),
    ?assertMatch(
        [#upload_fragment{}, #upload_fragment{}, #upload_fragment{}, #register_offset_listener{}],
        Effects5
    ),
    Infos = [fragment_to_info(F) || F <- Fragments],
    Uploaded0 = [#fragment_uploaded{stream = StreamId, info = I} || I <- Infos],
    Uploaded = lists:reverse(Uploaded0),
    {Writer5, Effects6} = handle_events(?META(), Uploaded, Writer4),
    FragmentsApplied = #fragments_applied{
        stream = StreamId,
        first_offset = (hd(Fragments))#fragment.first_offset,
        first_timestamp = (hd(Fragments))#fragment.first_timestamp,
        fragments = Infos
    },
    ?assertMatch(
        [
            #send{to = {_, ReplicaNode}, message = FragmentsApplied},
            #set_range{next = 60},
            #upload_manifest{}
        ],
        Effects6
    ),
    {_Writer6, Effects7} = ?MAC:apply(?META(), #manifest_uploaded{stream = StreamId}, Writer5),
    ?assertEqual([], Effects7),
    {_Replica2, Effects8} = ?MAC:apply(?META(), FragmentsApplied, Replica1),
    ?assertMatch([#set_range{next = 60}], Effects8),
    ok.

retention(Config) ->
    Dir = directory(Config),
    Pid = self(),
    ReplicaNode = 'rabbit@replica',
    StreamId = erlang:make_ref(),
    WriterSpawned = #writer_spawned{
        stream = StreamId,
        pid = Pid,
        dir = Dir,
        replica_nodes = [ReplicaNode],
        retention = [{max_bytes, 700}]
    },
    Writer0 = ?MAC:new(#{debounce_modifications => 1}),
    {Writer1, _Effects1} = ?MAC:apply(?META(), WriterSpawned, Writer0),

    Ts = erlang:system_time(millisecond),
    Entries = <<
        ?ENTRY((N * 20), (Ts - 100 + N * 20), ?MANIFEST_KIND_FRAGMENT, 200, N, <<>>)
     || N <- lists:seq(0, 4)
    >>,
    Manifest = #manifest{
        first_offset = 0,
        first_timestamp = Ts - 100,
        next_offset = 120,
        total_size = 1000,
        entries = Entries
    },
    ManifestResolved = #manifest_resolved{stream = StreamId, manifest = Manifest},
    {Writer2, Effects2} = ?MAC:apply(?META(), ManifestResolved, Writer1),
    ?assertMatch([#set_range{first = 0, next = 120}], Effects2),

    AcceptorSpawned = #acceptor_spawned{stream = StreamId},
    {Replica1, REffects1} = handle_events(?META(), [AcceptorSpawned, ManifestResolved], ?MAC:new()),
    ?assertMatch([#set_range{first = 0, next = 120}], REffects1),

    FragmentsApplied1 = #fragments_applied{
        stream = StreamId,
        first_offset = 40,
        first_timestamp = Ts - 100 + 40,
        fragments = []
    },
    {Writer3, Effects3} = ?MAC:apply(?META(), #tick{}, Writer2),
    ?assertMatch(
        [
            #send{to = {_, ReplicaNode}, message = FragmentsApplied1},
            #set_range{first = 40, next = 120},
            #upload_manifest{manifest = #manifest{first_offset = 40, total_size = 600}},
            #delete_fragments{offsets = [0, 20]}
        ],
        Effects3
    ),
    {Writer4, Effects4} = ?MAC:apply(?META(), #manifest_uploaded{stream = StreamId}, Writer3),
    ?assertMatch([], Effects4),

    {Replica2, REffects2} = ?MAC:apply(?META(), FragmentsApplied1, Replica1),
    ?assertMatch([#set_range{first = 40, next = 120}], REffects2),

    RetentionUpdated = #retention_updated{stream = StreamId, retention = [{max_bytes, 500}]},
    {Writer5, Effects5} = ?MAC:apply(?META(), RetentionUpdated, Writer4),
    FragmentsApplied2 = #fragments_applied{
        stream = StreamId,
        first_offset = 60,
        first_timestamp = Ts - 100 + 60,
        fragments = []
    },
    ?assertMatch(
        [
            #send{to = {_, ReplicaNode}, message = FragmentsApplied2},
            #set_range{first = 60, next = 120},
            #upload_manifest{manifest = #manifest{first_offset = 60, total_size = 400}},
            #delete_fragments{offsets = [40]}
        ],
        Effects5
    ),
    {Writer6, Effects6} = ?MAC:apply(?META(), #manifest_uploaded{stream = StreamId}, Writer5),
    ?assertMatch([], Effects6),

    {Replica3, REffects3} = ?MAC:apply(?META(), FragmentsApplied2, Replica2),
    ?assertMatch([#set_range{first = 60, next = 120}], REffects3),

    %% Publish another fragment which will exceed the max-bytes setting.
    %% That fragment should be added to the manifest and the oldest fragment
    %% currently in the manifest (offset 80) should be deleted by retention.
    %% Both the new fragment and fragment deletion should be reflected in the
    %% same upload of the manifest object.
    Fragment = fragment(120, 139),
    {Writer7, Effects7} = handle_events(
        ?META(),
        [
            #fragment_available{stream = StreamId, fragment = Fragment},
            #commit_offset_increased{stream = StreamId, offset = 150}
        ],
        Writer6
    ),
    ?assertMatch(
        [
            #upload_fragment{fragment = #fragment{first_offset = 120}},
            #register_offset_listener{}
        ],
        Effects7
    ),
    Info = fragment_to_info(Fragment),
    FragmentUploaded = #fragment_uploaded{stream = StreamId, info = Info},
    {Writer8, Effects8} = ?MAC:apply(?META(), FragmentUploaded, Writer7),
    FragmentsApplied3 = #fragments_applied{
        stream = StreamId,
        first_offset = 80,
        first_timestamp = Ts - 100 + 80,
        fragments = [Info]
    },
    ?assertMatch(
        [
            #send{to = {_, ReplicaNode}, message = FragmentsApplied3},
            #set_range{first = 80, next = 140},
            #upload_manifest{
                manifest = #manifest{first_offset = 80, next_offset = 140, total_size = 400}
            },
            #delete_fragments{offsets = [60]}
        ],
        Effects8
    ),
    {_Writer9, Effects9} = ?MAC:apply(?META(), #manifest_uploaded{stream = StreamId}, Writer8),
    ?assertMatch([], Effects9),

    {_Replica4, REffects4} = ?MAC:apply(?META(), FragmentsApplied3, Replica3),
    ?assertMatch([#set_range{first = 80, next = 140}], REffects4),
    ok.

%%----------------------------------------------------------------------------

setup_writer(Config) ->
    setup_writer(Config, ?MAC:new()).

setup_writer(Config, Mac0) ->
    Dir = directory(Config),
    Pid = self(),
    StreamId = erlang:make_ref(),
    Event1 = #writer_spawned{
        stream = StreamId,
        pid = Pid,
        dir = Dir
    },
    Event2 = #manifest_resolved{stream = StreamId, manifest = #manifest{}},
    {Mac, _} = handle_events(?META(), [Event1, Event2], Mac0),
    {Mac, StreamId}.

handle_events(Meta, Events, Mac) ->
    handle_events(Meta, Events, Mac, []).

handle_events(_Meta, [], Mac, Acc) ->
    Effects = lists:flatten(lists:reverse(Acc)),
    {Mac, Effects};
handle_events(Meta, [Event | Rest], Mac0, Acc) ->
    {Mac, Effects} = ?MAC:apply(Meta, Event, Mac0),
    handle_events(Meta, Rest, Mac, [Effects | Acc]).

fragment(Offset, LastOffset) ->
    #fragment{
        segment_offset = Offset,
        first_offset = Offset,
        first_timestamp = erlang:system_time(millisecond),
        last_offset = LastOffset,
        next_offset = LastOffset + 1,
        size = 200
    }.

fragment_to_info(#fragment{
    first_offset = O,
    first_timestamp = T,
    next_offset = N,
    seq_no = Seq,
    size = Size
}) ->
    #fragment_info{
        offset = O,
        timestamp = T,
        next_offset = N,
        seq_no = Seq,
        size = Size
    }.

fragments_to_manifest([
    #fragment{
        first_offset = Offset,
        next_offset = NextOffset,
        first_timestamp = Ts,
        size = Size,
        seq_no = SeqNo
    }
    | Rest
]) ->
    fragments_to_manifest(Rest, #manifest{
        first_offset = Offset,
        next_offset = NextOffset,
        first_timestamp = Ts,
        total_size = Size,
        entries = ?ENTRY(Offset, Ts, ?MANIFEST_KIND_FRAGMENT, Size, SeqNo, <<>>)
    }).

fragments_to_manifest([], Manifest) ->
    Manifest;
fragments_to_manifest(
    [
        #fragment{
            first_offset = Offset,
            next_offset = NextOffset,
            first_timestamp = Ts,
            size = Size,
            seq_no = SeqNo
        }
        | Rest
    ],
    #manifest{total_size = TotalSize0, entries = Entries0} = Manifest0
) ->
    Manifest = Manifest0#manifest{
        next_offset = NextOffset,
        total_size = TotalSize0 + Size,
        entries =
            <<Entries0/binary,
                (?ENTRY(Offset, Ts, ?MANIFEST_KIND_FRAGMENT, Size, SeqNo, <<>>))/binary>>
    },
    fragments_to_manifest(Rest, Manifest).

directory(Config) ->
    list_to_binary(get_config(Config, priv_dir)).
