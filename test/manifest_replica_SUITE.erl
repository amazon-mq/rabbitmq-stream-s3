%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(manifest_replica_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbitmq_stream_s3/include/rabbitmq_stream_s3.hrl").

-import(rabbitmq_stream_s3_test_helpers, [build_manifest/1]).

all() ->
    [
        unknown_stream,
        put_and_get,
        apply_append_edit,
        apply_retention_edit,
        range_updates_on_edit,
        sequenced_edits_applied_in_order,
        gap_triggers_resync,
        rapid_edits_with_gap_then_resync,
        stale_edit_after_resync_ignored,
        retention_and_append_in_same_broadcast,
        sync_live_manifest_then_broadcast_double_applies
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_TC, Config) ->
    {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
    unlink(Pid),
    [{cache_pid, Pid} | Config].

end_per_testcase(_TC, Config) ->
    Pid = ?config(cache_pid, Config),
    gen_server:stop(Pid),
    Config.

%% ------------------------------------------------------------------
%% Tests
%% ------------------------------------------------------------------

unknown_stream(_Config) ->
    ?assertEqual(undefined, rabbitmq_stream_s3_manifest_replica:get_manifest(<<"unknown">>)),
    ?assertEqual(empty, rabbitmq_stream_s3_manifest_replica:get_range(<<"unknown">>)).

put_and_get(_Config) ->
    StreamId = <<"stream-1">>,
    {Manifest, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000}},
        {fragment, #{offset => 50, size => 2000}}
    ]),
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest),
    ?assertEqual(Manifest, rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId)),
    ?assertEqual({0, 51}, rabbitmq_stream_s3_manifest_replica:get_range(StreamId)).

apply_append_edit(_Config) ->
    StreamId = <<"stream-2">>,
    {Manifest0, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000}}
    ]),
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest0),

    %% Append a new fragment via edit.
    NewEntry = ?ENTRY(50, 500, 600, ?MANIFEST_KIND_FRAGMENT, 2000, 42),
    Edit = #edit{
        first_offset = 0,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = 51,
        size = 2000,
        entries = NewEntry,
        pos = byte_size(Manifest0#manifest.entries),
        len = 0
    },
    ok = rabbitmq_stream_s3_manifest_replica:apply_edit(StreamId, Edit),

    Manifest1 = rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId),
    ?assertEqual(51, Manifest1#manifest.next_offset),
    ?assertEqual(3000, Manifest1#manifest.total_size),
    ?assertEqual({0, 51}, rabbitmq_stream_s3_manifest_replica:get_range(StreamId)).

apply_retention_edit(_Config) ->
    StreamId = <<"stream-3">>,
    {Manifest0, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000, uid => 1}},
        {fragment, #{offset => 50, size => 2000, uid => 2}},
        {fragment, #{offset => 100, size => 3000, uid => 3}}
    ]),
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest0),

    %% Retention removes the first entry.
    Edit = #edit{
        first_offset = 50,
        first_timestamp = 500,
        first_last_timestamp = 510,
        next_offset = undefined,
        size = -1000,
        entries = <<>>,
        pos = 0,
        len = ?ENTRY_B
    },
    ok = rabbitmq_stream_s3_manifest_replica:apply_edit(StreamId, Edit),

    Manifest1 = rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId),
    ?assertEqual(50, Manifest1#manifest.first_offset),
    ?assertEqual(5000, Manifest1#manifest.total_size),
    ?assertEqual({50, 101}, rabbitmq_stream_s3_manifest_replica:get_range(StreamId)).

range_updates_on_edit(_Config) ->
    StreamId = <<"stream-4">>,
    {Manifest0, _} = build_manifest([
        {fragment, #{offset => 100, size => 5000}}
    ]),
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest0),
    ?assertEqual({100, 101}, rabbitmq_stream_s3_manifest_replica:get_range(StreamId)),

    %% Append another fragment.
    NewEntry = ?ENTRY(200, 2000, 2100, ?MANIFEST_KIND_FRAGMENT, 6000, 99),
    Edit = #edit{
        first_offset = 100,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = 201,
        size = 6000,
        entries = NewEntry,
        pos = byte_size(Manifest0#manifest.entries),
        len = 0
    },
    ok = rabbitmq_stream_s3_manifest_replica:apply_edit(StreamId, Edit),
    ?assertEqual({100, 201}, rabbitmq_stream_s3_manifest_replica:get_range(StreamId)).

sequenced_edits_applied_in_order(_Config) ->
    StreamId = <<"stream-seq">>,
    {Manifest0, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000}}
    ]),
    %% Sync establishes the baseline: seq=0, epoch=1.
    ok = rabbitmq_stream_s3_manifest_replica:sync(StreamId, 0, 1, Manifest0),
    ?assertEqual(Manifest0, rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId)),

    %% Edit at seq=1 is applied.
    Edit1 = #edit{
        first_offset = 0,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = 51,
        size = 2000,
        entries = ?ENTRY(50, 500, 600, ?MANIFEST_KIND_FRAGMENT, 2000, 42),
        pos = byte_size(Manifest0#manifest.entries),
        len = 0
    },
    ok = rabbitmq_stream_s3_manifest_replica:apply_edits(StreamId, [Edit1], 1, 1),
    ?assertEqual(
        51, (rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId))#manifest.next_offset
    ),

    %% Edit at seq=2 is applied.
    Edit2 = #edit{
        first_offset = 0,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = 101,
        size = 3000,
        entries = ?ENTRY(100, 1000, 1100, ?MANIFEST_KIND_FRAGMENT, 3000, 43),
        pos = byte_size(Manifest0#manifest.entries) + ?ENTRY_B,
        len = 0
    },
    ok = rabbitmq_stream_s3_manifest_replica:apply_edits(StreamId, [Edit2], 2, 1),
    ?assertEqual(
        101, (rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId))#manifest.next_offset
    ).

gap_triggers_resync(_Config) ->
    StreamId = <<"stream-gap">>,
    {Manifest0, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000}}
    ]),

    %% Register a fake writer in the registry so the resync request has a target.
    rabbitmq_stream_s3_registry:init(),
    Self = self(),
    WriterPid = spawn_link(fun() -> fake_writer(Self) end),
    yes = rabbitmq_stream_s3_registry:register_name({StreamId, node()}, WriterPid),

    %% Sync establishes baseline: seq=0, epoch=1.
    ok = rabbitmq_stream_s3_manifest_replica:sync(StreamId, 0, 1, Manifest0),

    %% Send edit with seq=2 (gap: expected 1). Should trigger resync.
    Edit = #edit{
        first_offset = 0,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = 51,
        size = 2000,
        entries = ?ENTRY(50, 500, 600, ?MANIFEST_KIND_FRAGMENT, 2000, 42),
        pos = byte_size(Manifest0#manifest.entries),
        len = 0
    },
    {error, gap} = rabbitmq_stream_s3_manifest_replica:apply_edits(StreamId, [Edit], 2, 1),

    %% The edit was NOT applied (gap detected).
    ?assertEqual(Manifest0, rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId)),

    %% The fake writer received a resync request.
    receive
        {resync_received, Node} -> ?assertEqual(node(), Node)
    after 1000 ->
        ct:fail("resync request not received by writer")
    end,

    rabbitmq_stream_s3_registry:unregister_name({StreamId, node()}),
    unlink(WriterPid),
    exit(WriterPid, kill).

rapid_edits_with_gap_then_resync(_Config) ->
    StreamId = <<"stream-rapid">>,
    {Manifest0, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000}}
    ]),

    rabbitmq_stream_s3_registry:init(),
    Self = self(),
    WriterPid = spawn_link(fun() -> fake_writer_loop(Self) end),
    yes = rabbitmq_stream_s3_registry:register_name({StreamId, node()}, WriterPid),

    %% Sync at seq=0, epoch=1.
    ok = rabbitmq_stream_s3_manifest_replica:sync(StreamId, 0, 1, Manifest0),

    %% Edit seq=1 applies.
    Edit1 = append_edit(Manifest0, 50, 1000, 51, 2000, 42),
    ok = rabbitmq_stream_s3_manifest_replica:apply_edits(StreamId, [Edit1], 1, 1),
    M1 = rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId),
    ?assertEqual(51, M1#manifest.next_offset),

    %% Edit seq=2 applies.
    Edit2 = append_edit(M1, 100, 1000, 101, 3000, 43),
    ok = rabbitmq_stream_s3_manifest_replica:apply_edits(StreamId, [Edit2], 2, 1),
    M2 = rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId),
    ?assertEqual(101, M2#manifest.next_offset),

    %% Edit seq=4 (gap: expected 3). Triggers resync.
    Edit4 = append_edit(M2, 200, 1000, 201, 4000, 44),
    {error, gap} = rabbitmq_stream_s3_manifest_replica:apply_edits(StreamId, [Edit4], 4, 1),
    receive
        {resync_received, _} -> ok
    after 1000 -> ct:fail("no resync")
    end,

    %% Re-sync with a fresh manifest at seq=5.
    {ManifestNew, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000}},
        {fragment, #{offset => 50, size => 2000}},
        {fragment, #{offset => 100, size => 3000}},
        {fragment, #{offset => 150, size => 4000}},
        {fragment, #{offset => 200, size => 5000}}
    ]),
    ok = rabbitmq_stream_s3_manifest_replica:sync(StreamId, 5, 1, ManifestNew),

    %% Subsequent edit at seq=6 applies normally.
    Edit6 = append_edit(ManifestNew, 300, 1000, 301, 6000, 45),
    ok = rabbitmq_stream_s3_manifest_replica:apply_edits(StreamId, [Edit6], 6, 1),
    M3 = rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId),
    ?assertEqual(301, M3#manifest.next_offset),

    rabbitmq_stream_s3_registry:unregister_name({StreamId, node()}),
    unlink(WriterPid),
    exit(WriterPid, kill).

stale_edit_after_resync_ignored(_Config) ->
    StreamId = <<"stream-stale">>,
    {Manifest0, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000}}
    ]),

    rabbitmq_stream_s3_registry:init(),
    Self = self(),
    WriterPid = spawn_link(fun() -> fake_writer_loop(Self) end),
    yes = rabbitmq_stream_s3_registry:register_name({StreamId, node()}, WriterPid),

    %% Sync at seq=0, epoch=1.
    ok = rabbitmq_stream_s3_manifest_replica:sync(StreamId, 0, 1, Manifest0),

    %% Edit seq=1 applies.
    Edit1 = append_edit(Manifest0, 50, 1000, 51, 2000, 42),
    ok = rabbitmq_stream_s3_manifest_replica:apply_edits(StreamId, [Edit1], 1, 1),

    %% Re-sync at seq=10, epoch=2 (new writer after election).
    {ManifestNew, _} = build_manifest([
        {fragment, #{offset => 0, size => 5000}},
        {fragment, #{offset => 100, size => 6000}}
    ]),
    ok = rabbitmq_stream_s3_manifest_replica:sync(StreamId, 10, 2, ManifestNew),

    %% Stale edit from old epoch (seq=2, epoch=1) arrives after re-sync.
    %% Should be rejected (gap/epoch mismatch).
    StaleEdit = append_edit(Manifest0, 100, 1000, 101, 3000, 43),
    {error, gap} = rabbitmq_stream_s3_manifest_replica:apply_edits(
        StreamId, [StaleEdit], 2, 1
    ),
    receive
        {resync_received, _} -> ok
    after 1000 -> ct:fail("no resync")
    end,

    %% Manifest unchanged (stale edit was ignored).
    ?assertEqual(ManifestNew, rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId)),

    rabbitmq_stream_s3_registry:unregister_name({StreamId, node()}),
    unlink(WriterPid),
    exit(WriterPid, kill).

retention_and_append_in_same_broadcast(_Config) ->
    StreamId = <<"stream-mixed">>,
    {Manifest0, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000, uid => 1}},
        {fragment, #{offset => 50, size => 2000, uid => 2}},
        {fragment, #{offset => 100, size => 3000, uid => 3}}
    ]),
    ok = rabbitmq_stream_s3_manifest_replica:sync(StreamId, 0, 1, Manifest0),

    %% A single broadcast containing: append (new fragment) then retention (remove first).
    AppendEdit = #edit{
        first_offset = 0,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = 201,
        size = 4000,
        entries = ?ENTRY(200, 2000, 2100, ?MANIFEST_KIND_FRAGMENT, 4000, 99),
        pos = byte_size(Manifest0#manifest.entries),
        len = 0
    },
    RetentionEdit = #edit{
        first_offset = 50,
        first_timestamp = 500,
        first_last_timestamp = 510,
        next_offset = undefined,
        size = -1000,
        entries = <<>>,
        pos = 0,
        len = ?ENTRY_B
    },
    %% Apply both edits in one sequenced call (append first, then retention).
    ok = rabbitmq_stream_s3_manifest_replica:apply_edits(
        StreamId, [AppendEdit, RetentionEdit], 1, 1
    ),

    M = rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId),
    %% First offset advanced (retention removed offset 0 entry).
    ?assertEqual(50, M#manifest.first_offset),
    %% Next offset advanced (append added offset 200 entry).
    ?assertEqual(201, M#manifest.next_offset),
    %% 3 entries remain: offsets 50, 100, 200.
    ?assertEqual(3 * ?ENTRY_B, byte_size(M#manifest.entries)),
    %% Total size: original 6000 + 4000 (append) - 1000 (retention) = 9000.
    ?assertEqual(9000, M#manifest.total_size).

%% Demonstrates *why* the writer must sync the persisted manifest, not the live
%% one. There is no deterministic writer-side regression test for the fix
%% itself: the consistency requirement couples a shell value (broadcast_seq)
%% with a core value (last_persisted_manifest), and expressing it as a pure test
%% would require moving the broadcast sequence into the core (a core/shell
%% contract change deferred to a later commit). This test instead pins the
%% replica-side consequence so the catastrophe is documented where the
%% corruption happens.
%%
%% The writer's broadcast_seq only advances when a {broadcast, _} effect runs,
%% so it lags any edit already applied to the live manifest but not yet
%% broadcast. The buggy writer sent the *live* manifest (which already contains
%% that edit) tagged with the *lagging* seq. The replica caches an edit it was
%% never told about, then the deferred broadcast re-delivers the same edit
%% in-sequence and the replica applies it a second time. No gap is ever observed
%% so no self-healing resync fires: the divergence is silent and permanent.
sync_live_manifest_then_broadcast_double_applies(_Config) ->
    {Manifest0, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000}}
    ]),
    %% An append edit for a second fragment at offset 50. This is the edit that
    %% has been applied to the writer's live manifest but is still queued in
    %% edits_since_persist, not yet broadcast.
    Edit = #edit{
        first_offset = 0,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = 51,
        size = 2000,
        entries = ?ENTRY(50, 500, 600, ?MANIFEST_KIND_FRAGMENT, 2000, 42),
        pos = byte_size(Manifest0#manifest.entries),
        len = 0
    },
    %% The live manifest the buggy writer holds: persisted manifest + Edit.
    LiveManifest = rabbitmq_stream_s3_manifest:apply_edit(Edit, Manifest0),
    ?assertEqual(2 * ?ENTRY_B, byte_size(LiveManifest#manifest.entries)),
    ?assertEqual(3000, LiveManifest#manifest.total_size),

    %% Correct (fixed) writer: sync the *persisted* manifest at seq 0, then the
    %% broadcast delivers Edit at seq 1. The replica converges on LiveManifest.
    Good = <<"stream-c1-good">>,
    ok = rabbitmq_stream_s3_manifest_replica:sync(Good, 0, 1, Manifest0),
    ok = rabbitmq_stream_s3_manifest_replica:apply_edits(Good, [Edit], 1, 1),
    ?assertEqual(LiveManifest, rabbitmq_stream_s3_manifest_replica:get_manifest(Good)),

    %% Buggy writer: sync the *live* manifest (already contains Edit) at the
    %% lagging seq 0, then the deferred broadcast delivers Edit again at seq 1.
    Bad = <<"stream-c1-bad">>,
    ok = rabbitmq_stream_s3_manifest_replica:sync(Bad, 0, 1, LiveManifest),
    %% In-sequence (1 = 0 + 1), so no gap is detected and Edit is applied a
    %% second time onto a manifest that already reflects it.
    ok = rabbitmq_stream_s3_manifest_replica:apply_edits(Bad, [Edit], 1, 1),
    Corrupt = rabbitmq_stream_s3_manifest_replica:get_manifest(Bad),

    %% The double-apply corrupts the replica. The offset-50 fragment is
    %% duplicated in the ordered entries array (a consumer binary-searching it
    %% gets wrong/duplicate results) and total_size is double-counted.
    ?assertEqual(3 * ?ENTRY_B, byte_size(Corrupt#manifest.entries)),
    ?assertEqual(5000, Corrupt#manifest.total_size),
    ?assertNotEqual(LiveManifest, Corrupt).

fake_writer(TestPid) ->
    receive
        {'$gen_cast', {resync, Node}} ->
            TestPid ! {resync_received, Node}
    end.

fake_writer_loop(TestPid) ->
    receive
        {'$gen_cast', {resync, Node}} ->
            TestPid ! {resync_received, Node},
            fake_writer_loop(TestPid)
    end.

%% Build an append edit for a new fragment at Offset with given Size.
append_edit(Manifest, Offset, Ts, NextOffset, Size, Uid) ->
    LastTs = Ts + 100,
    #edit{
        first_offset = Manifest#manifest.first_offset,
        first_timestamp = Manifest#manifest.first_timestamp,
        first_last_timestamp = Manifest#manifest.first_last_timestamp,
        next_offset = NextOffset,
        size = Size,
        entries = ?ENTRY(Offset, Ts, LastTs, ?MANIFEST_KIND_FRAGMENT, Size, Uid),
        pos = byte_size(Manifest#manifest.entries),
        len = 0
    }.
