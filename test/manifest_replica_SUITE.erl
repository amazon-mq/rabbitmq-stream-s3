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
        sync_live_manifest_then_broadcast_double_applies,
        manifest_reset_requires_resync,
        cached_epoch_reflects_writer_epoch,
        retention_evaluated_floors_first_offset_and_timestamp,
        member_down_releases_state,
        forget_releases_writer_row,
        reregister_repoints_monitor,
        sync_without_context_dropped,
        register_after_dropped_sync_requests_resync,
        apply_edits_without_context_dropped,
        register_after_dropped_edits_requests_resync,
        register_replica_context_marks_pending,
        mark_pending_does_not_downgrade_resolved
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

%% osiris has no terminate/delete hook, so manifest_replica monitors the member
%% that registered a context and releases the context, sequence, and cached row
%% when it goes down. This is how per-node replica state is reclaimed when a
%% replica moves off the node or the stream is deleted.
member_down_releases_state(_Config) ->
    Replica = rabbitmq_stream_s3_manifest_replica,
    Stream = <<"down-stream">>,
    Shared = atomics:new(1, []),
    Counter = counters:new(5, []),
    {Manifest, _} = build_manifest([{fragment, #{offset => 0, size => 1000}}]),
    Member = spawn(fun() ->
        receive
            stop -> ok
        end
    end),

    ok = Replica:register_replica_context(Stream, Member, <<"/tmp/s">>, Shared, Counter),
    ok = Replica:sync(Stream, 0, 1, Manifest),
    ?assert(Replica:is_context_registered(Stream)),
    ?assertNotEqual(undefined, Replica:get_manifest(Stream)),

    %% The member dies: its DOWN releases context, sequence, and cached row.
    exit(Member, kill),
    ok = await(fun() -> not Replica:is_context_registered(Stream) end),
    ?assertEqual(undefined, Replica:get_manifest(Stream)).

%% The writer node's cached row is written by put_manifest and has no member to
%% monitor; the replica reader releases it explicitly via forget/1 on teardown.
forget_releases_writer_row(_Config) ->
    Replica = rabbitmq_stream_s3_manifest_replica,
    Stream = <<"writer-stream">>,
    {Manifest, _} = build_manifest([{fragment, #{offset => 0, size => 1000}}]),

    ok = Replica:put_manifest(Stream, Manifest, 1),
    ?assertNotEqual(undefined, Replica:get_manifest(Stream)),
    ok = Replica:forget(Stream),
    ?assertEqual(undefined, Replica:get_manifest(Stream)).

%% A member restart re-registers the context. The old incarnation's monitor must
%% be dropped so its later DOWN cannot evict the new context.
reregister_repoints_monitor(_Config) ->
    Replica = rabbitmq_stream_s3_manifest_replica,
    Stream = <<"restart-stream">>,
    Shared = atomics:new(1, []),
    Counter = counters:new(5, []),
    Old = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    New = spawn(fun() ->
        receive
            stop -> ok
        end
    end),

    ok = Replica:register_replica_context(Stream, Old, <<"/tmp/s">>, Shared, Counter),
    ok = Replica:register_replica_context(Stream, New, <<"/tmp/s">>, Shared, Counter),

    %% The old member dies. Its monitor was dropped on re-registration, so this
    %% must not release the context now owned by the new member.
    OwnRef = monitor(process, Old),
    exit(Old, kill),
    receive
        {'DOWN', OwnRef, process, Old, _} -> ok
    after 2000 -> ct:fail(old_member_did_not_exit)
    end,
    %% A synchronous call is processed after any message already enqueued in
    %% manifest_replica; the context must still be registered.
    ?assert(Replica:is_context_registered(Stream)),

    %% The new member dies: now the context is released.
    exit(New, kill),
    ok = await(fun() -> not Replica:is_context_registered(Stream) end).

%% register_replica_context marks the row pending before the member finishes
%% starting: a reader attaching in the window before a sync or resolution lands
%% must fail closed rather than see the row as absent.
register_replica_context_marks_pending(_Config) ->
    Replica = rabbitmq_stream_s3_manifest_replica,
    Stream = <<"pending-on-register-stream">>,
    Shared = atomics:new(1, []),
    Counter = counters:new(5, []),
    Member = spawn(fun() ->
        receive
            stop -> ok
        end
    end),

    ?assertEqual(undefined, Replica:get_manifest(Stream)),
    ok = Replica:register_replica_context(Stream, Member, <<"/tmp/s">>, Shared, Counter),
    ?assertEqual(pending, Replica:get_manifest(Stream)).

%% The pending marker is insert-if-absent: it must never downgrade a row that
%% has already resolved, whether the caller racing behind the resolution is a
%% repeated register_replica_context (reconciliation re-registering a replica
%% after manifest_replica itself restarts) or a direct mark_pending call (the
%% writer path).
mark_pending_does_not_downgrade_resolved(_Config) ->
    Replica = rabbitmq_stream_s3_manifest_replica,
    Stream = <<"resolved-then-pending-stream">>,
    Shared = atomics:new(1, []),
    Counter = counters:new(5, []),
    Member = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    {Manifest, _} = build_manifest([{fragment, #{offset => 0, size => 1000}}]),

    ok = Replica:register_replica_context(Stream, Member, <<"/tmp/s">>, Shared, Counter),
    ok = Replica:sync(Stream, 0, 1, Manifest),
    ?assertEqual(Manifest, Replica:get_manifest(Stream)),

    ok = Replica:register_replica_context(Stream, Member, <<"/tmp/s">>, Shared, Counter),
    ?assertEqual(Manifest, Replica:get_manifest(Stream)),

    ok = Replica:mark_pending(Stream),
    ?assertEqual(Manifest, Replica:get_manifest(Stream)).

%% A sync for a stream with no registered reader context is dropped rather than
%% caching a row that no member monitor would ever reclaim (the pre-registration
%% / post-DOWN strand). get_manifest stays undefined: nothing was cached.
sync_without_context_dropped(_Config) ->
    Replica = rabbitmq_stream_s3_manifest_replica,
    Stream = <<"no-context-stream">>,
    {Manifest, _} = build_manifest([{fragment, #{offset => 0, size => 1000}}]),
    ok = Replica:sync(Stream, 0, 1, Manifest),
    ?assertEqual(undefined, Replica:get_manifest(Stream)).

%% Registering a context for a stream whose sync was dropped requests a re-sync
%% from the writer the dropped sync came from, so a sync that raced ahead of
%% registration is recovered rather than leaving the cache empty.
register_after_dropped_sync_requests_resync(_Config) ->
    Replica = rabbitmq_stream_s3_manifest_replica,
    Stream = <<"resync-on-register-stream">>,
    {Manifest, _} = build_manifest([{fragment, #{offset => 0, size => 1000}}]),

    %% A fake writer registered under {Stream, node()} receives the resync cast.
    rabbitmq_stream_s3_registry:init(),
    Self = self(),
    WriterPid = spawn_link(fun() -> fake_writer(Self) end),
    yes = rabbitmq_stream_s3_registry:register_name({Stream, node()}, WriterPid),

    %% The sync arrives before any context: it is dropped and its writer node is
    %% remembered.
    ok = Replica:sync(Stream, 0, 1, Manifest),
    ?assertEqual(undefined, Replica:get_manifest(Stream)),

    %% Registering the context now requests a resync from that writer.
    Member = register_context(Stream),
    receive
        {resync_received, Node} -> ?assertEqual(node(), Node)
    after 1000 -> ct:fail("resync request not received after context registration")
    end,

    rabbitmq_stream_s3_registry:unregister_name({Stream, node()}),
    unlink(WriterPid),
    exit(WriterPid, kill),
    exit(Member, kill).

%% Edits for a stream with no registered reader context are dropped the same
%% way a sync is: applying them would advance seqs with no cached row for any
%% member monitor to ever reclaim. get_manifest stays undefined: nothing was
%% cached, and the sequence was not advanced.
apply_edits_without_context_dropped(_Config) ->
    Replica = rabbitmq_stream_s3_manifest_replica,
    Stream = <<"no-context-edits-stream">>,
    Edit = #edit{
        first_offset = 0,
        first_timestamp = 1000,
        first_last_timestamp = 1100,
        next_offset = 1,
        size = 1000,
        entries = ?ENTRY(0, 1000, 1100, ?MANIFEST_KIND_FRAGMENT, 1000, 1),
        pos = 0,
        len = 0
    },
    ?assertEqual({error, no_context}, Replica:apply_edits(Stream, [Edit], 1, 1)),
    ?assertEqual(undefined, Replica:get_manifest(Stream)).

%% Registering a context for a stream whose edits were dropped requests a
%% re-sync from the writer the dropped edits came from, the same recovery path
%% used for a dropped sync (register_after_dropped_sync_requests_resync).
register_after_dropped_edits_requests_resync(_Config) ->
    Replica = rabbitmq_stream_s3_manifest_replica,
    Stream = <<"resync-on-register-edits-stream">>,
    Edit = #edit{
        first_offset = 0,
        first_timestamp = 1000,
        first_last_timestamp = 1100,
        next_offset = 1,
        size = 1000,
        entries = ?ENTRY(0, 1000, 1100, ?MANIFEST_KIND_FRAGMENT, 1000, 1),
        pos = 0,
        len = 0
    },

    %% A fake writer registered under {Stream, node()} receives the resync cast.
    rabbitmq_stream_s3_registry:init(),
    Self = self(),
    WriterPid = spawn_link(fun() -> fake_writer(Self) end),
    yes = rabbitmq_stream_s3_registry:register_name({Stream, node()}, WriterPid),

    %% The edit arrives before any context: it is dropped and its writer node
    %% is remembered.
    ?assertEqual({error, no_context}, Replica:apply_edits(Stream, [Edit], 1, 1)),
    ?assertEqual(undefined, Replica:get_manifest(Stream)),

    %% Registering the context now requests a resync from that writer.
    Member = register_context(Stream),
    receive
        {resync_received, Node} -> ?assertEqual(node(), Node)
    after 1000 -> ct:fail("resync request not received after context registration")
    end,

    rabbitmq_stream_s3_registry:unregister_name({Stream, node()}),
    unlink(WriterPid),
    exit(WriterPid, kill),
    exit(Member, kill).

%% Register a live reader context so syncs for the stream are accepted (a sync
%% for a stream with no context is dropped). Returns the member pid; it is left
%% alive for the duration of the test.
register_context(Stream) ->
    Shared = atomics:new(1, []),
    Counter = counters:new(5, []),
    Member = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    ok = rabbitmq_stream_s3_manifest_replica:register_replica_context(
        Stream, Member, <<"/tmp/s">>, Shared, Counter
    ),
    Member.

await(Fun) ->
    await(Fun, 2000).

await(_Fun, Remaining) when Remaining =< 0 ->
    {error, timeout};
await(Fun, Remaining) ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(20),
            await(Fun, Remaining - 20)
    end.

unknown_stream(_Config) ->
    ?assertEqual(undefined, rabbitmq_stream_s3_manifest_replica:get_manifest(<<"unknown">>)),
    ?assertEqual(empty, rabbitmq_stream_s3_manifest_replica:get_range(<<"unknown">>)).

%% The cache records the writer epoch that produced each manifest, so GC can tell
%% whether this node reflects the committed reset or lags it. put_manifest/3 and
%% sync stamp the epoch; put_manifest/2 leaves it undefined.
cached_epoch_reflects_writer_epoch(_Config) ->
    Replica = rabbitmq_stream_s3_manifest_replica,
    StreamId = <<"epoch-stream">>,
    _ = register_context(StreamId),
    {Manifest, _} = build_manifest([{fragment, #{offset => 0, size => 1000}}]),

    %% put_manifest/3 (the writer's local update) records its epoch.
    ok = Replica:put_manifest(StreamId, Manifest, 7),
    ?assertEqual({Manifest, 7}, Replica:get_manifest_and_epoch(StreamId)),

    %% A sync at a higher epoch advances the cached epoch.
    {Manifest2, _} = build_manifest([{fragment, #{offset => 0, size => 2000}}]),
    ok = Replica:sync(StreamId, 0, 9, Manifest2),
    ?assertMatch({_, 9}, Replica:get_manifest_and_epoch(StreamId)),

    %% put_manifest/2 stores no epoch: the cached epoch is undefined.
    LegacyStream = <<"epoch-stream-legacy">>,
    ok = Replica:put_manifest(LegacyStream, Manifest),
    ?assertEqual({Manifest, undefined}, Replica:get_manifest_and_epoch(LegacyStream)),

    %% Unknown stream resolves to undefined, like get_manifest/1.
    ?assertEqual(undefined, Replica:get_manifest_and_epoch(<<"unknown">>)).

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
    _ = register_context(StreamId),
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
    _ = register_context(StreamId),
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
    _ = register_context(StreamId),
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
    _ = register_context(StreamId),
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
    _ = register_context(StreamId),
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
%% in-sequence and the replica applies it a second time. No gap is ever
%% observed, so the sequence guard does not fire. apply_edit/2's
%% append-position assertion is the backstop: it rejects the duplicate (the
%% replica's array already contains the edit) and requests a resync, so the
%% silent permanent divergence this characterized is now caught. The test pins
%% both the historical shape and that backstop.
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
    _ = register_context(Good),
    ok = rabbitmq_stream_s3_manifest_replica:sync(Good, 0, 1, Manifest0),
    ok = rabbitmq_stream_s3_manifest_replica:apply_edits(Good, [Edit], 1, 1),
    ?assertEqual(LiveManifest, rabbitmq_stream_s3_manifest_replica:get_manifest(Good)),

    %% Buggy writer: sync the *live* manifest (already contains Edit) at the
    %% lagging seq 0, then the deferred broadcast re-delivers Edit at seq 1.
    %% The duplicate is in-sequence, so the seq guard does not catch it - but
    %% apply_edit/2's append-position assertion does: the replica's array
    %% already contains Edit, so the append no longer lands at the array end.
    %% The replica rejects the edit, leaves its manifest untouched, and requests
    %% a resync rather than silently double-applying (the old corruption this
    %% test used to characterize).
    rabbitmq_stream_s3_registry:init(),
    Self = self(),
    WriterPid = spawn_link(fun() -> fake_writer(Self) end),
    Bad = <<"stream-c1-bad">>,
    _ = register_context(Bad),
    yes = rabbitmq_stream_s3_registry:register_name({Bad, node()}, WriterPid),
    ok = rabbitmq_stream_s3_manifest_replica:sync(Bad, 0, 1, LiveManifest),
    ?assertEqual(
        {error, apply_failed},
        rabbitmq_stream_s3_manifest_replica:apply_edits(Bad, [Edit], 1, 1)
    ),
    %% No corruption: the cached manifest is exactly the one that was synced;
    %% the duplicate fragment was not spliced in or double-counted.
    ?assertEqual(LiveManifest, rabbitmq_stream_s3_manifest_replica:get_manifest(Bad)),
    receive
        {resync_received, Node} -> ?assertEqual(node(), Node)
    after 1000 -> ct:fail("resync request not received by writer")
    end,
    rabbitmq_stream_s3_registry:unregister_name({Bad, node()}),
    unlink(WriterPid),
    exit(WriterPid, kill).

%% Characterization of the manifest-reset propagation bug. Demonstrates *why* a
%% writer that resets its manifest (local-log-ahead recovery: it discards the
%% remote manifest and restarts at the local floor) must propagate the reset to
%% replicas with a full sync, not just locally. Like the live-manifest case,
%% there is no deterministic writer-side regression test here: triggering the
%% reset on a writer while a replica is attached needs peer-node lifecycle
%% orchestration in the integration suite (tracked as coverage debt). This test
%% pins the replica-side consequence at the boundary where corruption happens.
%%
%% A reset leaves broadcast_seq unchanged, so the writer's next edit arrives
%% in-sequence relative to the replica's pre-reset seq. If the reset was not
%% synced, the replica still holds the old manifest. apply_edit/2's
%% append-position assertion is the backstop: the post-reset edit is built
%% against the fresh manifest, so it no longer matches the stale array and is
%% rejected, triggering a resync instead of splicing onto a manifest the writer
%% has already thrown away.
manifest_reset_requires_resync(_Config) ->
    {OldManifest, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000}},
        {fragment, #{offset => 50, size => 2000}}
    ]),
    %% The fresh manifest the writer installs at the local floor (offset 100):
    %% empty, first_offset = next_offset = 100.
    FreshManifest = #manifest{first_offset = 100, next_offset = 100},
    %% The first edit after the reset: a fragment uploaded at the new floor.
    PostResetEdit = #edit{
        first_offset = 100,
        first_timestamp = 1000,
        first_last_timestamp = 1100,
        next_offset = 150,
        size = 3000,
        entries = ?ENTRY(100, 1000, 1100, ?MANIFEST_KIND_FRAGMENT, 3000, 7),
        pos = 0,
        len = 0
    },

    %% Buggy writer: resets locally but never syncs the replica. The replica
    %% keeps OldManifest at seq 0; the post-reset edit arrives in-sequence at
    %% seq 1. The seq guard does not catch it, but apply_edit/2's
    %% append-position assertion does: the post-reset append is built against
    %% the fresh (empty) manifest, so Pos no longer matches the stale array.
    %% The replica rejects the edit, keeps OldManifest, and requests a resync
    %% (which would then deliver the fresh manifest) rather than splicing onto
    %% the discarded manifest (the old corruption this test used to
    %% characterize).
    rabbitmq_stream_s3_registry:init(),
    Self = self(),
    WriterPid = spawn_link(fun() -> fake_writer(Self) end),
    Bad = <<"stream-reset-bad">>,
    _ = register_context(Bad),
    yes = rabbitmq_stream_s3_registry:register_name({Bad, node()}, WriterPid),
    ok = rabbitmq_stream_s3_manifest_replica:sync(Bad, 0, 1, OldManifest),
    ?assertEqual(
        {error, apply_failed},
        rabbitmq_stream_s3_manifest_replica:apply_edits(Bad, [PostResetEdit], 1, 1)
    ),
    ?assertEqual(OldManifest, rabbitmq_stream_s3_manifest_replica:get_manifest(Bad)),
    receive
        {resync_received, RNode} -> ?assertEqual(node(), RNode)
    after 1000 -> ct:fail("resync request not received by writer")
    end,
    rabbitmq_stream_s3_registry:unregister_name({Bad, node()}),
    unlink(WriterPid),
    exit(WriterPid, kill),

    %% Fixed writer: propagates the reset with a full sync (at the unchanged
    %% broadcast_seq) before the next edit. The replica drops OldManifest,
    %% adopts FreshManifest, then applies the edit onto it.
    Good = <<"stream-reset-good">>,
    _ = register_context(Good),
    ok = rabbitmq_stream_s3_manifest_replica:sync(Good, 0, 1, OldManifest),
    ok = rabbitmq_stream_s3_manifest_replica:sync(Good, 0, 1, FreshManifest),
    ok = rabbitmq_stream_s3_manifest_replica:apply_edits(Good, [PostResetEdit], 1, 1),
    Converged = rabbitmq_stream_s3_manifest_replica:get_manifest(Good),
    %% Only the post-reset fragment, first_offset advanced to the new floor.
    ?assertEqual(?ENTRY_B, byte_size(Converged#manifest.entries)),
    ?assertEqual(100, Converged#manifest.first_offset),
    ?assertEqual(150, Converged#manifest.next_offset),
    ?assertEqual(3000, Converged#manifest.total_size),
    ?assertNotEqual(OldManifest, Converged).

%% After local retention runs, osiris sets the first_offset and first_timestamp
%% counters from the local tier's oldest surviving segment. on_retention_evaluated/2
%% must floor both to the remote tier whenever the manifest holds older data, so
%% the management UI reports the true oldest message rather than letting the
%% "first timestamp" march forward as local segments are deleted. Both counters
%% are taken as the min so a smaller local value (an empty/lagging remote tier)
%% is never clobbered.
retention_evaluated_floors_first_offset_and_timestamp(_Config) ->
    Hooks = rabbitmq_stream_s3_hooks,
    Replica = rabbitmq_stream_s3_manifest_replica,

    %% Remote tier holds older data than the local tier: the oldest message is
    %% the manifest's first_offset/first_timestamp, not the local segment's.
    Remote = <<"stream-floor-remote">>,
    {Manifest, _} = build_manifest([
        {fragment, #{offset => 50, first_ts => 500, size => 1000}}
    ]),
    ?assertEqual(50, Manifest#manifest.first_offset),
    ?assertEqual(500, Manifest#manifest.first_timestamp),
    ok = Replica:put_manifest(Remote, Manifest),
    Cnt = counters:new(5, []),
    counters:put(Cnt, ?C_OSIRIS_LOG_FIRST_OFFSET, 200),
    counters:put(Cnt, ?C_OSIRIS_LOG_FIRST_TIMESTAMP, 2000),
    ok = Hooks:on_retention_evaluated(Cnt, #{name => Remote}),
    ?assertEqual(50, counters:get(Cnt, ?C_OSIRIS_LOG_FIRST_OFFSET)),
    ?assertEqual(500, counters:get(Cnt, ?C_OSIRIS_LOG_FIRST_TIMESTAMP)),

    %% Empty remote tier (manifest with no entries, carrying the -1
    %% first_timestamp sentinel): the counters must be left untouched so the
    %% sentinel never reaches the UI.
    Empty = <<"stream-floor-empty">>,
    ok = Replica:put_manifest(Empty, #manifest{}),
    CntEmpty = counters:new(5, []),
    counters:put(CntEmpty, ?C_OSIRIS_LOG_FIRST_OFFSET, 200),
    counters:put(CntEmpty, ?C_OSIRIS_LOG_FIRST_TIMESTAMP, 2000),
    ok = Hooks:on_retention_evaluated(CntEmpty, #{name => Empty}),
    ?assertEqual(200, counters:get(CntEmpty, ?C_OSIRIS_LOG_FIRST_OFFSET)),
    ?assertEqual(2000, counters:get(CntEmpty, ?C_OSIRIS_LOG_FIRST_TIMESTAMP)),

    %% Local tier is the oldest (remote lags behind it): min keeps the smaller
    %% local values, it never raises them to the remote tier.
    Lagging = <<"stream-floor-lagging">>,
    {LagManifest, _} = build_manifest([
        {fragment, #{offset => 50, first_ts => 500, size => 1000}}
    ]),
    ok = Replica:put_manifest(Lagging, LagManifest),
    CntLocal = counters:new(5, []),
    counters:put(CntLocal, ?C_OSIRIS_LOG_FIRST_OFFSET, 10),
    counters:put(CntLocal, ?C_OSIRIS_LOG_FIRST_TIMESTAMP, 100),
    ok = Hooks:on_retention_evaluated(CntLocal, #{name => Lagging}),
    ?assertEqual(10, counters:get(CntLocal, ?C_OSIRIS_LOG_FIRST_OFFSET)),
    ?assertEqual(100, counters:get(CntLocal, ?C_OSIRIS_LOG_FIRST_TIMESTAMP)).

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
