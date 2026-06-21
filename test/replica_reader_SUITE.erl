%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(replica_reader_SUITE).
-moduledoc """
Integration tests for the remote replica reader.

## Barrier techniques

The write flow is asynchronous both in local and remote tiers. Writes are casts
to the `osiris_writer`, and remote replication is a completely background
process. To avoid `timer:sleep/1`, this suite uses _barrier_ techniques: calls
that block the test process until the desired state has been reached.

1. `osiris_writer`: `flush_writer/1` (a local helper wrapping
   `query_replication_state/1`) is a `gen_batch_server:call` that returns
   only after all prior casts (writes) have been processed and records
   have been written to disk.

2. `rabbitmq_stream_s3_replica_reader`: `await_offset/2` blocks the caller
   until the reader has visited the given offset. This means that the offset
   is less than or equal to the `#manifest.next_offset`. This ensures that
   fragments exist on the remote tier, the manifest cache is updated and that
   retention may have been kicked off.

Retention is also asynchronous (a cast to the retention server) but we don't
have a way to form a barrier. To assert the results of retention we use the
`?awaitMatch` macro which is popular for asynchronous assertions in RabbitMQ.
""".

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("rabbitmq_stream_s3/include/rabbitmq_stream_s3.hrl").
-include_lib("rabbit/include/rabbit_khepri.hrl").

-import(rabbitmq_stream_s3_test_helpers, [
    start_writer/2,
    start_writer/3,
    start_cluster/3,
    start_cluster/4,
    start_replica_reader/3,
    flush_writer/1,
    seed_log/2,
    await_offset/2,
    list_fragment_offsets/1,
    list_segment_offsets/1,
    list_segment_offsets/2,
    get_range/1,
    get_range/2
]).

suite() ->
    [{ct_hooks, [rabbitmq_stream_s3_cth]}].

all() ->
    [
        {group, single_node},
        {group, with_replica}
    ].

groups() ->
    [
        {single_node, [], [
            registry_lifecycle,
            uploads_fragments,
            fragment_spans_segment_boundary,
            range_advances_monotonically,
            retention_reclaims_uploaded_segments,
            message_count_reflects_remote_tier,
            resumes_after_restart,
            lost_transfer_result_recovered_by_deadline,
            large_record_cuts_immediately,
            seed_log_uploads_deterministic,
            local_ahead_discards_manifest,
            remote_tier_ahead_discards_manifest,
            reconcile_reattaches_orphaned_writer,
            reconcile_reseeds_writer_cache_after_restart,
            stream_deletion_cleans_remote_tier,
            stream_deletion_during_active_upload,
            persist_not_found_stops_reader,
            discover_attaches_to_existing_writer,
            on_init_writer_tolerates_already_started,
            two_layer_supervision_structure,
            duplicate_start_child_no_orphan_supervisor,
            remote_retention_deletes_fragments,
            remote_retention_on_update,
            remote_retention_survives_multiple_persist_cycles,
            uploads_rebalance_into_group,
            remote_retention_deletes_within_group,
            remote_retention_deletes_all_fragments,
            old_manifest_roots_deleted,
            attach_to_stream_with_prior_retention,
            stale_retention_result_is_ignored,
            stale_persist_result_is_ignored,
            stale_group_upload_result_is_ignored
        ]},
        {with_replica, [], [
            replication_happy_path,
            replication_survives_reader_restart,
            reconcile_recovers_replica_after_cache_restart
        ]}
    ].

init_per_suite(Config) ->
    %% Ensure this node is distributed (required for peer nodes).
    case node() of
        nonode@nohost ->
            {ok, _} = net_kernel:start([ct_writer, shortnames]);
        _ ->
            ok
    end,
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(single_node, Config) ->
    Config;
init_per_group(with_replica, Config) ->
    {Peer, ReplicaNode} = rabbitmq_stream_s3_cth:setup_peer(Config),
    [{peer, Peer}, {replica_node, ReplicaNode} | Config].

end_per_group(single_node, Config) ->
    Config;
end_per_group(with_replica, Config) ->
    peer:stop(?config(peer, Config)),
    Config.

init_per_testcase(TestCase, Config) ->
    StreamId = <<"__", (atom_to_binary(TestCase))/binary, "_1">>,
    WriterCfg = #{
        name => binary_to_list(StreamId),
        epoch => 1,
        replica_nodes => [],
        leader_node => node(),
        reference => StreamId,
        options => #{},
        max_segment_size_bytes => 10_000_000
    },
    [{stream_id, StreamId}, {writer_cfg, WriterCfg} | Config].

end_per_testcase(_TestCase, Config) ->
    %% Stop the writer if still running (cascades to replica reader).
    WriterCfg = ?config(writer_cfg, Config),
    catch osiris_writer:stop(WriterCfg),
    %% Clean up any per-test app env overrides.
    application:unset_env(rabbitmq_stream_s3, fragment_target_size),
    application:unset_env(rabbitmq_stream_s3, persist_threshold),
    application:unset_env(rabbitmq_stream_s3, transfer_deadline_ms),
    Config.

%% ------------------------------------------------------------------
%% Tests
%% ------------------------------------------------------------------

registry_lifecycle(Config) ->
    StreamId = ?config(stream_id, Config),
    WriterCfg = ?config(writer_cfg, Config),

    _ = start_writer(Config, #{}),

    %% Verify it registered.
    ?assertMatch(
        Pid when is_pid(Pid),
        rabbitmq_stream_s3_registry:whereis_name({StreamId, node()})
    ),

    %% Stop the writer and verify the replica reader unregisters.
    ok = osiris_writer:stop(WriterCfg),
    ?awaitMatch(
        undefined,
        rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
        1000
    ).

on_init_writer_tolerates_already_started(Config) ->
    %% Regression. The writer init hook starts the replica reader,
    %% which is registered by {StreamId, node()} rather than by writer pid. If
    %% a reader already exists for the stream on this node (discover/0 ran
    %% first, or a prior incarnation has not terminated), start_child returns
    %% {error, {already_started, _}}. The hook must tolerate that instead of a
    %% badmatch crashing osiris_log:init/2.
    StreamId = ?config(stream_id, Config),
    Writer = start_writer(Config, #{}),
    %% The hook already started and registered the reader.
    ?assertMatch(
        Pid when is_pid(Pid),
        rabbitmq_stream_s3_registry:whereis_name({StreamId, node()})
    ),
    #{shared := Shared, dir := Dir} = gen_batch_server:call(Writer, get_reader_context),
    Counter = osiris_counters:fetch({osiris_writer, StreamId}),
    HookConfig = (?config(writer_cfg, Config))#{
        shared => Shared,
        dir => Dir,
        counter => Counter,
        remote_config => #{persist_threshold => 1}
    },
    %% Re-invoke the writer init hook against the already-running reader.
    %% Before this fix it exited with {badmatch, {error, {already_started, _}}}.
    Result = rabbitmq_stream_s3_hooks:on_init(writer, Writer, HookConfig),
    ?assertMatch(#{retention := _}, Result).

two_layer_supervision_structure(Config) ->
    %% The replica reader is supervised in two layers: the factory
    %% (rabbitmq_stream_s3_replica_reader_sup) has one per-stream supervisor
    %% (rabbitmq_stream_s3_stream_sup) child per stream, and the reader worker
    %% lives under that per-stream supervisor. This isolates each stream's
    %% restart budget.
    StreamId = ?config(stream_id, Config),
    _ = start_writer(Config, #{}),
    ReaderPid = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
    ?assert(is_pid(ReaderPid)),

    %% The factory's direct child is a supervisor, not the reader worker.
    FactoryChildren = supervisor:which_children(rabbitmq_stream_s3_replica_reader_sup),
    ?assertMatch([{_, _, supervisor, [rabbitmq_stream_s3_stream_sup]}], FactoryChildren),
    [{_, StreamSupPid, supervisor, _}] = FactoryChildren,
    ?assert(is_pid(StreamSupPid)),
    ?assertNotEqual(ReaderPid, StreamSupPid),

    %% The reader worker lives one level down, under the per-stream supervisor.
    StreamSupChildren = supervisor:which_children(StreamSupPid),
    ?assertMatch(
        [{rabbitmq_stream_s3_replica_reader, ReaderPid, worker, _}],
        StreamSupChildren
    ).

duplicate_start_child_no_orphan_supervisor(Config) ->
    %% A second start_child for a stream that already has a registered reader
    %% returns {error, {already_started, Pid}} with the existing reader pid,
    %% and does not leave an orphaned per-stream supervisor under the factory.
    StreamId = ?config(stream_id, Config),
    Writer = start_writer(Config, #{}),
    ReaderPid = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
    ?assert(is_pid(ReaderPid)),
    [_] = supervisor:which_children(rabbitmq_stream_s3_replica_reader_sup),

    %% Build a config equivalent to what the writer hook would pass, and try to
    %% start a second reader for the same stream.
    #{shared := Shared, dir := Dir} = gen_batch_server:call(Writer, get_reader_context),
    Counter = osiris_counters:fetch({osiris_writer, StreamId}),
    DupConfig = #{
        stream => StreamId,
        writer_pid => Writer,
        dir => iolist_to_binary(Dir),
        shared => Shared,
        counter => Counter,
        reference => StreamId,
        epoch => 1,
        persist_threshold => 1
    },
    ?assertEqual(
        {error, {already_started, ReaderPid}},
        rabbitmq_stream_s3_replica_reader_sup:start_child(DupConfig)
    ),

    %% The factory still has exactly one per-stream supervisor: the duplicate
    %% start did not leak an orphan.
    ?assertMatch([_], supervisor:which_children(rabbitmq_stream_s3_replica_reader_sup)),
    %% The original reader is untouched and still registered.
    ?assertEqual(ReaderPid, rabbitmq_stream_s3_registry:whereis_name({StreamId, node()})).

uploads_fragments(Config) ->
    StreamId = ?config(stream_id, Config),

    Writer = start_writer(Config, #{fragment_target_size => 500}),

    %% Write enough data for multiple fragments.
    lists:foreach(
        fun(_) -> osiris_writer:write(Writer, <<"payload that is reasonably sized">>) end,
        lists:seq(1, 200)
    ),
    flush_writer(Writer),

    %% Barrier: wait for replica reader to upload past all written data.
    ok = await_offset(StreamId, 199),

    %% Multiple fragments produced.
    Fragments = list_fragment_offsets(Config),
    ?assert(length(Fragments) >= 2),

    %% Range starts at 0 and covers all data.
    {0, NextOffset} = get_range(Config),
    ?assert(NextOffset >= 200).

fragment_spans_segment_boundary(Config) ->
    StreamId = ?config(stream_id, Config),

    %% 500-byte segments, 1500-byte fragment target.
    %% Each record is 300 bytes. With flush_writer between writes,
    %% each write becomes its own chunk. A chunk + header exceeds 500
    %% bytes so each segment holds exactly one chunk. The fragment
    %% target requires at least 3 chunks, therefore at least 3 segments.
    Record = binary:copy(<<"x">>, 300),
    Writer = start_writer(Config, #{max_segment_size_bytes => 500}, #{
        fragment_target_size => 1500
    }),

    [
        begin
            osiris_writer:write(Writer, Record),
            flush_writer(Writer)
        end
     || _ <- lists:seq(1, 12)
    ],

    ok = await_offset(StreamId, 8),

    %% With one chunk per segment and a 1500-byte fragment target,
    %% each fragment must span multiple segments. Two fragments
    %% confirms the reader crossed segment boundaries.
    Fragments = list_fragment_offsets(Config),
    ?assert(length(Fragments) >= 2),
    [0, Second | _] = Fragments,
    ?assert(Second >= 2).

range_advances_monotonically(Config) ->
    StreamId = ?config(stream_id, Config),

    Writer = start_writer(Config, #{fragment_target_size => 500}),

    %% Write in batches, barrier after each, verify range advances.
    lists:foreach(
        fun(_) -> osiris_writer:write(Writer, <<"monotonic range test data!!">>) end,
        lists:seq(1, 100)
    ),
    ok = await_offset(StreamId, 50),
    {0, N1} = get_range(Config),
    ?assert(N1 >= 50),

    lists:foreach(
        fun(_) -> osiris_writer:write(Writer, <<"monotonic range test data!!">>) end,
        lists:seq(1, 100)
    ),
    ok = await_offset(StreamId, 150),
    {0, N2} = get_range(Config),
    ?assert(N2 > N1).

retention_reclaims_uploaded_segments(Config) ->
    StreamId = ?config(stream_id, Config),

    Writer = start_writer(Config, #{max_segment_size_bytes => 5000}, #{
        fragment_target_size => 10000
    }),

    %% Write enough to produce a fragment and advance the range.
    Record = binary:copy(<<"x">>, 50),
    lists:foreach(
        fun(_) -> osiris_writer:write(Writer, Record) end,
        lists:seq(1, 501)
    ),
    %% Barrier: ensure all messages are written.
    flush_writer(Writer),
    Segments = list_segment_offsets(Config),
    ?assert(length(Segments) > 1),

    %% Replica reader barrier: await fragment upload.
    CurrentSegment = lists:last(Segments),
    await_offset(StreamId, CurrentSegment),
    Fragments = list_fragment_offsets(Config),
    ?assert(length(Fragments) > 1),

    %% Retention will eventually reclaim everything but the current segment.
    ?awaitMatch([_], list_segment_offsets(Config), 1_000).

message_count_reflects_remote_tier(Config) ->
    StreamId = ?config(stream_id, Config),

    _Writer = start_writer(Config, #{max_segment_size_bytes => 5000}, #{
        fragment_target_size => 10000
    }),

    %% Write enough to produce multiple segments and fragments.
    Record = binary:copy(<<"x">>, 50),
    lists:foreach(
        fun(_) -> osiris_writer:write(_Writer, Record) end,
        lists:seq(1, 501)
    ),
    flush_writer(_Writer),
    ?assert(length(list_segment_offsets(Config)) > 1),

    %% Wait for uploads to advance past the first segment.
    CurrentSegment = lists:last(list_segment_offsets(Config)),
    await_offset(StreamId, CurrentSegment),

    %% Wait for local retention to trim uploaded segments.
    ?awaitMatch([_], list_segment_offsets(Config), 1_000),

    %% After local retention, only one segment remains. Its offset is the
    %% local tier's first offset and must be > 0 (earlier segments were
    %% trimmed). The manifest covers from offset 0, so the counter should
    %% report 0 (the manifest's first_offset), not the local segment offset.
    [LocalFirst] = list_segment_offsets(Config),
    ?assert(LocalFirst > 0),

    #manifest{first_offset = ManifestFirst} =
        rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId),

    %% The counter must report the manifest's first_offset (the overall
    %% stream first), not the local segment's first offset. This is what
    %% rabbit_osiris_metrics reads for the management UI message count.
    ?awaitMatch(
        #{first_offset := FO} when FO =:= ManifestFirst,
        osiris_counters:overview({osiris_writer, StreamId}),
        1_000
    ),
    ?assert(ManifestFirst < LocalFirst).

resumes_after_restart(Config) ->
    StreamId = ?config(stream_id, Config),
    WriterCfg = ?config(writer_cfg, Config),

    Writer1 = start_writer(Config, #{fragment_target_size => 500}),

    lists:foreach(
        fun(_) -> osiris_writer:write(Writer1, <<"first generation data">>) end,
        lists:seq(1, 100)
    ),
    ok = await_offset(StreamId, 50),
    {0, RangeAfterFirst} = get_range(Config),
    FragmentsAfterFirst = length(list_fragment_offsets(Config)),

    %% Stop and restart immediately, without waiting for cleanup.
    ok = osiris_writer:stop(WriterCfg),
    Writer2 = start_writer(Config, #{fragment_target_size => 500}),

    lists:foreach(
        fun(_) -> osiris_writer:write(Writer2, <<"second generation data">>) end,
        lists:seq(1, 100)
    ),
    ok = await_offset(StreamId, RangeAfterFirst + 50),

    %% Range continued from where it was, not from 0.
    {0, RangeAfterSecond} = get_range(Config),
    ?assert(RangeAfterSecond > RangeAfterFirst),

    %% No duplicate fragments (new fragments were appended, not re-uploaded).
    ?assert(length(list_fragment_offsets(Config)) > FragmentsAfterFirst).

large_record_cuts_immediately(Config) ->
    StreamId = ?config(stream_id, Config),

    %% Fragment target is 1000 bytes. A single 2000-byte record exceeds it.
    Record = binary:copy(<<"L">>, 2000),
    Writer = start_writer(Config, #{}, #{fragment_target_size => 1000}),

    osiris_writer:write(Writer, Record),
    flush_writer(Writer),

    %% One record, one chunk, one fragment.
    ok = await_offset(StreamId, 1),
    ?assertEqual([0], list_fragment_offsets(Config)).

%% Regression for the governor-transfer-liveness issue: a submitted transfer
%% whose result never comes back (governor crash dropping a queued submission,
%% an externally killed upload task, or a lost message) must not pin the
%% in-flight queue head forever. The reader arms a per-transfer deadline and,
%% on expiry, resubmits under the same reference so the pipeline stays live.
%%
%% The test stands a controllable process in for the governor. It drops the
%% first submission of each reference (simulating a lost result) and serves the
%% resubmission normally. Without the reader-side deadline, await_offset would
%% hang on the dropped first submission.
lost_transfer_result_recovered_by_deadline(Config) ->
    StreamId = ?config(stream_id, Config),
    %% Short deadline so the resubmit happens quickly. Correctness does not
    %% depend on the value (a spurious early resubmit is safe); this only keeps
    %% the test fast.
    application:set_env(rabbitmq_stream_s3, transfer_deadline_ms, 300),

    Self = self(),
    ok = supervisor:terminate_child(rabbitmq_stream_s3_sup, rabbitmq_stream_s3_governor),
    Interceptor = spawn(fun() -> governor_interceptor(Self, #{}) end),
    true = register(rabbitmq_stream_s3_governor, Interceptor),

    try
        %% One 2000-byte record with a 1000-byte target cuts exactly one
        %% fragment, hence exactly one transfer reference.
        Writer = start_writer(Config, #{}, #{fragment_target_size => 1000}),
        osiris_writer:write(Writer, binary:copy(<<"L">>, 2000)),
        flush_writer(Writer),

        %% Returns only because the deadline fired and the resubmission was
        %% served. Generous timeout: 300ms deadline + a fast FS upload.
        ok = rabbitmq_stream_s3_test_helpers:await_offset(StreamId, 1, 5000),
        ?assertEqual([0], list_fragment_offsets(Config)),

        %% Confirm the same reference was submitted twice: dropped, then served.
        receive
            {interceptor_resubmitted, _Ref} -> ok
        after 5000 ->
            ct:fail("governor interceptor never received a resubmission")
        end
    after
        catch unregister(rabbitmq_stream_s3_governor),
        catch exit(Interceptor, kill),
        %% Restore the real governor for subsequent tests.
        {ok, _} = supervisor:restart_child(
            rabbitmq_stream_s3_sup, rabbitmq_stream_s3_governor
        )
    end.

%% Stand-in for the governor process. Speaks the same cast protocol
%% (gen_server:cast wraps the request in {'$gen_cast', _}). Drops the first
%% submission of each reference (no reply, simulating a lost transfer_result)
%% and serves every later submission of a reference it has already seen by
%% running the upload closure and replying, exactly as the governor does.
governor_interceptor(Test, Seen) ->
    receive
        {'$gen_cast', {submit, Fun, _Size, ReplyTo, Ref}} ->
            case maps:is_key(Ref, Seen) of
                false ->
                    governor_interceptor(Test, Seen#{Ref => dropped});
                true ->
                    _ = spawn(fun() ->
                        Result =
                            try
                                Fun()
                            catch
                                C:R -> {error, {C, R}}
                            end,
                        ReplyTo ! {transfer_result, Ref, Result}
                    end),
                    Test ! {interceptor_resubmitted, Ref},
                    governor_interceptor(Test, Seen)
            end;
        _Other ->
            governor_interceptor(Test, Seen)
    end.

seed_log_uploads_deterministic(Config) ->
    %% Seed: 2 segments, 3 chunks each, 200 bytes payload per chunk.
    %% Fragment target: 500 bytes.
    %% Chunks 0+1+2 = 600 bytes >= 500 -> cut after chunk 2 (fragment at offset 0).
    %% Chunks 3+4+5 = 600 bytes >= 500 -> cut after chunk 5 (fragment at offset 3).
    %% Result: exactly 2 fragments at offsets [0, 3].
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [
            {chunk, #{size => 200}},
            {chunk, #{size => 200}},
            {chunk, #{size => 200}}
        ]},
        {segment, [
            {chunk, #{size => 200}},
            {chunk, #{size => 200}},
            {chunk, #{size => 200}}
        ]}
    ]),

    ?assertEqual(6, NextOffset),

    Writer = start_writer(Config, #{}, #{fragment_target_size => 500}),
    flush_writer(Writer),

    ok = await_offset(Config, NextOffset),

    ?assertEqual([0, 3], list_fragment_offsets(Config)),
    {0, N} = get_range(Config),
    ?assertEqual(6, N).

local_ahead_discards_manifest(Config) ->
    StreamId = ?config(stream_id, Config),

    %% Phase 1: seed, upload, confirm fragments exist.
    #{next_offset := N1} = seed_log(Config, [
        {segment, [{chunk, #{records => 10, size => 600}}]},
        {segment, [{chunk, #{records => 10, size => 600}}]}
    ]),
    Writer = start_writer(
        Config, #{max_segment_size_bytes => 5000, retention => [{max_bytes, 10000}]}, #{
            fragment_target_size => 500
        }
    ),
    flush_writer(Writer),
    await_offset(Config, N1),
    OldFragments = list_fragment_offsets(Config),
    ?assert(length(OldFragments) > 0),

    %% Phase 2: stop replica reader, write until retention deletes old segments.
    ReaderPid = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
    ok = rabbitmq_stream_s3_replica_reader_sup:stop_child(ReaderPid),
    ?assertEqual(undefined, rabbitmq_stream_s3_registry:whereis_name({StreamId, node()})),

    Record = binary:copy(<<"x">>, 500),
    lists:foreach(
        fun(_) ->
            osiris_writer:write(Writer, Record),
            flush_writer(Writer)
        end,
        lists:seq(1, 50)
    ),
    %% Wait for retention to reclaim the seeded segments (offsets 0 and 10).
    ?awaitMatch(
        [S | _] when S > 10,
        list_segment_offsets(Config),
        1000
    ),

    %% Phase 3: restart replica reader. It should discard old manifest and upload new data.
    _ = start_replica_reader(Writer, Config, #{fragment_target_size => 500}),

    %% Wait for the new replica reader to upload.
    [FirstLocal | _] = list_segment_offsets(Config),
    await_offset(Config, FirstLocal + 10),

    %% Old fragments should be gone (deleted asynchronously by the replica reader).
    ?awaitMatch(
        [],
        [F || F <- OldFragments, lists:member(F, list_fragment_offsets(Config))],
        1000
    ),
    %% New fragments exist starting at or after the local first offset.
    NewFragments = list_fragment_offsets(Config),
    ?assert(length(NewFragments) > 0),
    ?assert(hd(NewFragments) >= FirstLocal).

remote_tier_ahead_discards_manifest(Config) ->
    StreamId = ?config(stream_id, Config),

    %% Phase 1: seed a local log, upload, so the manifest reaches next_offset N.
    #{next_offset := N} = seed_log(Config, [
        {segment, [{chunk, #{records => 10, size => 600}}]},
        {segment, [{chunk, #{records => 10, size => 600}}]}
    ]),
    Writer = start_writer(Config, #{}, #{fragment_target_size => 500}),
    flush_writer(Writer),
    await_offset(Config, N),

    %% Phase 2: stop the reader and forge a manifest whose next_offset is far
    %% beyond the local log's last offset, as a leader election or a
    %% data-directory loss would leave it (the remote tier ahead of local).
    %% Keep the committed revision so the reader trusts this cached manifest on
    %% resolve.
    ReaderPid = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
    ok = rabbitmq_stream_s3_replica_reader_sup:stop_child(ReaderPid),
    Manifest = rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId),
    {ok, #{revision := Rev}} = rabbitmq_stream_s3_db:get(StreamId),
    Ahead = Manifest#manifest{next_offset = N + 1000, revision = Rev},
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Ahead),

    %% Phase 3: restart the reader. It must discard the remote manifest and
    %% restart from the local log's first offset, then re-tier the local log
    %% back up to N - rather than wedging in a 1s retry_resolve loop with
    %% next_offset pinned at N+1000 (the bug). Recovery resets next_offset below
    %% the forged value and re-tiering brings it back to exactly N.
    _ = start_replica_reader(Writer, Config, #{fragment_target_size => 500}),
    ?awaitMatch(
        #manifest{next_offset = N},
        rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId),
        5000
    ).

reconcile_reattaches_orphaned_writer(Config) ->
    %% The writer-restart race (and a parked reader) leaves a live writer with no
    %% replica reader and nothing to start one. Periodic reconciliation must
    %% re-attach a reader, and must not disturb an already-attached one.
    StreamId = ?config(stream_id, Config),
    _Writer = start_writer(Config, #{}),
    Pid1 = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
    ?assert(is_pid(Pid1)),

    %% Orphan the writer: stop its reader while the writer stays alive.
    ok = rabbitmq_stream_s3_replica_reader_sup:stop_child(Pid1),
    ?awaitMatch(
        undefined,
        rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
        1000
    ),

    %% Reconciliation re-attaches a fresh reader to the still-live writer.
    ok = rabbitmq_stream_s3_hooks:reconcile(),
    Pid2 = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
    ?assert(is_pid(Pid2)),
    ?assertNotEqual(Pid1, Pid2),

    %% Idempotent: a second tick leaves the healthy reader untouched.
    ok = rabbitmq_stream_s3_hooks:reconcile(),
    ?assertEqual(Pid2, rabbitmq_stream_s3_registry:whereis_name({StreamId, node()})).

reconcile_reseeds_writer_cache_after_restart(Config) ->
    %% On the writer node the manifest cache is filled by each persist. If the
    %% node's manifest_replica restarts and the stream is then idle (no further
    %% persist), the cache stays empty and consumers on this node resolve the
    %% remote tier as absent, silently skipping remote data. Writer-side
    %% reconciliation must re-seed the local cache from the reader's manifest.
    StreamId = ?config(stream_id, Config),
    Cache = rabbitmq_stream_s3_manifest_replica,
    Writer = start_writer(Config, #{fragment_target_size => 1000}),
    Record = binary:copy(<<"R">>, 300),
    [osiris:write(Writer, undefined, I, Record) || I <- lists:seq(1, 50)],
    %% Await the full offset so the persist pipeline is quiesced: an in-flight
    %% persist would otherwise re-seed the cache right after we clear it and the
    %% stream would not be genuinely idle.
    ok = await_offset(StreamId, 50),
    %% The writer node's local cache holds the stream's manifest.
    ?awaitMatch(M when M =/= undefined, Cache:get_manifest(StreamId), 2000),

    %% Restart the manifest_replica: the owned-ETS cache is lost. The stream is
    %% idle, so no persist refills it.
    OldPid = whereis(Cache),
    true = exit(OldPid, kill),
    ?awaitMatch(P when is_pid(P) andalso P =/= OldPid, whereis(Cache), 2000),
    ?assertEqual(undefined, Cache:get_manifest(StreamId)),

    %% Writer-side reconcile re-seeds the local cache from the reader's manifest.
    ?awaitMatch(
        M when M =/= undefined,
        begin
            ok = rabbitmq_stream_s3_hooks:reconcile(),
            Cache:get_manifest(StreamId)
        end,
        5000
    ).

stream_deletion_cleans_remote_tier(Config) ->
    StreamId = ?config(stream_id, Config),
    WriterCfg = ?config(writer_cfg, Config),

    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]}
    ]),
    Writer = start_writer(Config, #{}, #{fragment_target_size => 500}),
    flush_writer(Writer),
    await_offset(Config, NextOffset),
    ?assert(length(list_fragment_offsets(Config)) > 0),

    %% Stop the writer (cascades to replica reader).
    osiris_writer:stop(WriterCfg),

    %% Delete remote tier objects.
    ok = rabbitmq_stream_s3_reaper:delete_stream(StreamId),
    ?awaitMatch([], list_fragment_offsets(Config), 1000),

    %% Delete local tier.
    Dir = filename:join(
        application:get_env(osiris, data_dir, "/tmp/osiris"),
        maps:get(name, WriterCfg)
    ),
    osiris_log:delete_directory(WriterCfg),
    ?assertNot(filelib:is_dir(Dir)).

stream_deletion_during_active_upload(Config) ->
    StreamId = ?config(stream_id, Config),
    WriterCfg = ?config(writer_cfg, Config),

    %% Use a large fragment target so the replica reader accumulates data
    %% but hasn't finished uploading when we kill the writer.
    Writer = start_writer(Config, #{}, #{fragment_target_size => 100_000}),

    %% Write enough data to trigger at least one fragment cut and have
    %% the governor task in flight.
    Record = binary:copy(<<"D">>, 500),
    lists:foreach(
        fun(_) ->
            osiris_writer:write(Writer, Record),
            flush_writer(Writer)
        end,
        lists:seq(1, 300)
    ),

    %% Verify the replica reader is alive and has work in progress.
    ReaderPid = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
    ?assert(is_pid(ReaderPid)),
    ReaderMon = monitor(process, ReaderPid),

    %% Stop the writer while uploads may be in flight.
    ok = osiris_writer:stop(WriterCfg),

    %% The replica reader should terminate cleanly (normal exit).
    receive
        {'DOWN', ReaderMon, process, ReaderPid, Reason} ->
            ?assertEqual(normal, Reason)
    after 5000 ->
        ct:fail("replica reader did not stop after writer down")
    end,

    %% Registry is cleaned up.
    ?assertEqual(undefined, rabbitmq_stream_s3_registry:whereis_name({StreamId, node()})).

persist_not_found_stops_reader(Config) ->
    %% When the stream's Khepri metadata node is deleted (as the keep-while
    %% condition does when the queue is removed), an in-flight persist's db:put
    %% returns not_found. For an established stream (expected revision > 0) the
    %% reader must stop cleanly rather than retry forever or resurrect the
    %% deleted stream.
    StreamId = ?config(stream_id, Config),
    Writer = start_writer(Config, #{}, #{fragment_target_size => 500}),
    Record = binary:copy(<<"D">>, 600),
    %% First persist: establishes the metadata node at a revision > 0.
    osiris_writer:write(Writer, Record),
    flush_writer(Writer),
    await_offset(Config, 1),
    ReaderPid = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
    ?assert(is_pid(ReaderPid)),
    ?assertMatch({ok, #{revision := R}} when R > 0, rabbitmq_stream_s3_db:get(StreamId)),
    %% Delete the metadata node out from under the reader.
    ok = khepri:delete(
        rabbitmq_metadata, ?RABBITMQ_KHEPRI_ROOT_PATH([rabbitmq_stream_s3, StreamId])
    ),
    ?awaitMatch({error, not_found}, rabbitmq_stream_s3_db:get(StreamId), 1000),
    ReaderMon = monitor(process, ReaderPid),
    %% Force another persist. db:put now hits the deleted node -> not_found.
    lists:foreach(
        fun(_) ->
            osiris_writer:write(Writer, Record),
            flush_writer(Writer)
        end,
        lists:seq(1, 5)
    ),
    receive
        {'DOWN', ReaderMon, process, ReaderPid, Reason} ->
            ?assertEqual(normal, Reason)
    after 5000 ->
        ct:fail("reader did not stop after a not_found persist")
    end,
    ?assertEqual(undefined, rabbitmq_stream_s3_registry:whereis_name({StreamId, node()})).

discover_attaches_to_existing_writer(Config) ->
    StreamId = ?config(stream_id, Config),

    %% Set a small fragment target and commit threshold in app env so the
    %% discovered replica reader uses them (discovery doesn't have access to
    %% per-stream remote_config).
    application:set_env(rabbitmq_stream_s3, fragment_target_size, 500),
    application:set_env(rabbitmq_stream_s3, persist_threshold, 1),

    %% Seed two segments, each with a chunk exceeding the fragment target.
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]}
    ]),
    Writer = start_writer(Config, #{}, #{fragment_target_size => 500}),
    flush_writer(Writer),
    ok = await_offset(Config, NextOffset),
    ?assert(length(list_fragment_offsets(Config)) >= 1),

    %% Kill the replica reader (simulates plugin disable).
    Pid1 = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
    ?assert(is_pid(Pid1)),
    rabbitmq_stream_s3_replica_reader_sup:stop_child(Pid1),
    ?awaitMatch(
        undefined,
        rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
        1000
    ),

    %% Discovery re-attaches.
    ok = rabbitmq_stream_s3_hooks:discover(),
    Pid2 = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
    ?assert(is_pid(Pid2)),
    ?assertNotEqual(Pid1, Pid2),

    %% Write more data via the writer (enough to exceed fragment target).
    Record = binary:copy(<<"D">>, 600),
    osiris_writer:write(Writer, Record),
    flush_writer(Writer),
    ok = await_offset(Config, NextOffset + 1),

    %% Remote + local tiers cover the full range from offset 0.
    {0, FinalOffset} = get_range(Config),
    ?assert(FinalOffset >= NextOffset + 1).

remote_retention_deletes_fragments(Config) ->
    %% 3 segments, each producing a fragment (600 bytes > 500 target).
    %% Total remote size: 1800 bytes. max_bytes = 1000 should delete 2.
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]}
    ]),
    _Writer = start_writer(
        Config,
        #{retention => [{max_bytes, 1000}]},
        #{fragment_target_size => 500}
    ),
    await_offset(Config, NextOffset),

    %% Remote retention should have deleted the first two fragments.
    %% Only the last fragment (offset 10) should remain.
    ?awaitMatch(
        [10],
        list_fragment_offsets(Config),
        2000
    ),

    %% Manifest reflects the deletion (after the retention persist completes).
    ?awaitMatch({10, NextOffset}, get_range(Config), 2000).

remote_retention_on_update(Config) ->
    %% Updating retention on a running writer triggers remote retention
    %% evaluation without requiring new writes.
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]}
    ]),
    Writer = start_writer(Config, #{}, #{fragment_target_size => 500}),
    flush_writer(Writer),
    await_offset(Config, NextOffset),

    %% All 3 fragments exist.
    ?assertEqual([0, 5, 10], list_fragment_offsets(Config)),

    %% Update retention. No new writes needed.
    osiris:update_retention(Writer, [{max_bytes, 700}]),

    %% Retention is kicked off without needing to write extra documents.
    ?awaitMatch([10], list_fragment_offsets(Config), 5000),
    ?awaitMatch({10, NextOffset}, get_range(Config), 2000).

remote_retention_survives_multiple_persist_cycles(Config) ->
    %% Exercises the scenario where multiple persist_complete cycles fire
    %% before the retention edit is persisted. With persist_threshold=1,
    %% every fragment triggers a persist. Retention must not produce
    %% duplicate edits (which would crash the replica reader).
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]}
    ]),
    _Writer = start_writer(
        Config,
        #{retention => [{max_bytes, 1000}]},
        #{fragment_target_size => 500, persist_threshold => 1}
    ),
    await_offset(Config, NextOffset),

    %% Remote retention should delete fragments until <= 1000 bytes remain.
    %% Each fragment is ~600 bytes, so only the last one should survive.
    ?awaitMatch(
        [20],
        list_fragment_offsets(Config),
        2000
    ),

    %% The replica reader must still be alive (no crash from duplicate edits).
    %% get_range advancing proves the retention persist completed successfully.
    ?awaitMatch({20, NextOffset}, get_range(Config), 2000).

uploads_rebalance_into_group(Config) ->
    %% 5 segments, each producing a fragment (600 bytes > 500 target).
    %% Rebalance threshold = 4, so after 4 fragments the oldest 4 are
    %% factored into a group. Final manifest: 1 group + 1 fragment.
    %% A consumer reading from offset 0 must traverse the group.
    StreamId = ?config(stream_id, Config),
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]}
    ]),
    _Writer = start_writer(
        Config, #{}, #{fragment_target_size => 500, rebalance_threshold => 4}
    ),
    await_offset(Config, NextOffset),

    %% Verify a group object exists in the remote tier.
    Dir = ?config(remote_dir, Config),
    MetadataDir = filename:join([Dir, <<"rabbitmq/stream">>, StreamId, <<"metadata">>]),
    {ok, Files} = file:list_dir(MetadataDir),
    GroupFiles = [F || F <- Files, lists:suffix(".group", F)],
    ?assert(length(GroupFiles) >= 1, "Expected at least one group object"),

    %% Read from offset 0 through the group and verify all records are readable.
    Manifest = rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId),
    GetGroupFun = rabbitmq_stream_s3_manifest:get_group_fun(StreamId),
    %% The iterator should be able to walk all fragments (descending into the group).
    AllRefs = rabbitmq_stream_s3_fragment_iterator:all_refs(Manifest, GetGroupFun),
    FragRefs = [R || #fragment_ref{} = R <- AllRefs],
    ?assertEqual(5, length(FragRefs)),

    %% Range covers all data.
    {0, NextOffset} = get_range(Config).

remote_retention_deletes_within_group(Config) ->
    %% 6 segments, each producing a fragment (600 bytes > 500 target).
    %% Rebalance threshold = 4: first 4 fragments factored into a group.
    %% Total remote size: 6 * 600 = 3600 bytes. max_bytes = 1500 should
    %% delete fragments within the group until total_size <= 1500.
    %% That means deleting 4 fragments (removing 2400 bytes: 3600-2400=1200 <= 1500).
    %% Wait, 3600 - 600*4 = 0... let me recalculate.
    %% Actually: delete until total <= max_bytes. 3600 > 1500, remove 600 -> 3000 > 1500,
    %% remove 600 -> 2400 > 1500, remove 600 -> 1800 > 1500, remove 600 -> 1200 <= 1500.
    %% So 4 fragments deleted. The group had 4 fragments, all deleted.
    %% The group entry should be removed. 2 fragment entries remain.
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]}
    ]),
    StreamId = ?config(stream_id, Config),
    _Writer = start_writer(
        Config,
        #{retention => [{max_bytes, 1500}]},
        #{fragment_target_size => 500, rebalance_threshold => 4}
    ),
    await_offset(Config, NextOffset),

    %% Remote retention should delete the 4 fragments in the group
    %% (entire group consumed) leaving only the last 2 fragments.
    ?awaitMatch(
        [20, 25],
        list_fragment_offsets(Config),
        2000
    ),

    %% The group object should be deleted.
    Dir = ?config(remote_dir, Config),
    MetadataDir = filename:join([Dir, <<"rabbitmq/stream">>, StreamId, <<"metadata">>]),
    ?awaitMatch(
        [],
        begin
            {ok, Files} = file:list_dir(MetadataDir),
            [F || F <- Files, lists:suffix(".group", F)]
        end,
        2000
    ),

    %% Range reflects the deletion.
    {FirstOffset, NextOffset} = get_range(Config),
    ?assertEqual(20, FirstOffset).

remote_retention_deletes_all_fragments(Config) ->
    %% 4 segments, each producing a fragment (600 bytes > 500 target).
    %% Rebalance threshold = 4: all 4 fragments factored into a group.
    %% The root then has exactly 1 group entry and 0 trailing fragments.
    %% max_bytes = 1 forces retention to delete all fragments in the group,
    %% removing the group entry and leaving the manifest empty.
    %% The system must handle this gracefully: no crash, range becomes empty,
    %% and the replica reader continues operating.
    StreamId = ?config(stream_id, Config),
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]}
    ]),
    _Writer = start_writer(
        Config,
        #{retention => [{max_bytes, 1}]},
        #{fragment_target_size => 500, rebalance_threshold => 4}
    ),
    await_offset(Config, NextOffset),

    %% Remote retention should delete all fragments and the group.
    %% The range becomes empty (no remote data).
    ?awaitMatch(empty, get_range(Config), 2000),

    %% No fragment objects remain.
    ?awaitMatch([], list_fragment_offsets(Config), 2000),

    %% The replica reader is still alive.
    ?assert(is_pid(rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}))).

old_manifest_roots_deleted(Config) ->
    StreamId = ?config(stream_id, Config),

    %% 3 segments, each producing a fragment (600 bytes > 500 target).
    %% persist_threshold=1 means each fragment triggers a persist, producing
    %% 3 manifest root objects. Only the latest should survive.
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]}
    ]),
    _Writer = start_writer(Config, #{}, #{fragment_target_size => 500}),
    await_offset(Config, NextOffset),

    %% The reaper deletes old manifest roots asynchronously.
    Dir = ?config(remote_dir, Config),
    MetadataDir = filename:join([Dir, <<"rabbitmq/stream">>, StreamId, <<"metadata">>]),
    ?awaitMatch(
        [_],
        begin
            {ok, Files} = file:list_dir(MetadataDir),
            [F || F <- Files, lists:suffix(".manifest", F)]
        end,
        2000
    ).

attach_to_stream_with_prior_retention(Config) ->
    %% Simulates enabling the plugin on a stream that has already undergone
    %% local retention. Start a writer without hooks, write enough to trigger
    %% retention, then restart with hooks enabled.
    WriterCfg0 = ?config(writer_cfg, Config),

    %% Phase 1: writer without plugin hooks, small segments, tight retention.
    WriterCfg1 = maps:merge(WriterCfg0, #{
        max_segment_size_bytes => 500,
        retention => [{max_bytes, 1000}],
        log_hooks => undefined
    }),
    {ok, Writer1} = osiris_writer:start(WriterCfg1),
    Record = binary:copy(<<"R">>, 300),
    lists:foreach(
        fun(_) ->
            osiris_writer:write(Writer1, Record),
            flush_writer(Writer1)
        end,
        lists:seq(1, 20)
    ),
    %% Wait for retention to delete old segments.
    ?awaitMatch(
        [First | _] when First > 0,
        list_segment_offsets(Config),
        1000
    ),
    ok = osiris_writer:stop(WriterCfg1),
    %% Barrier: wait for the osiris_retention process to finish all pending
    %% evaluations. Retention is a cast to a separate gen_batch_server; the
    %% writer may have sent retention evals that are still in flight. Without
    %% this barrier, restarting the writer can race with retention deleting
    %% segment files mid-init.
    ok = gen_batch_server:call(osiris_retention, barrier, 5000),

    %% Phase 2: restart with plugin hooks. The replica reader discovers the
    %% log starts at a non-zero offset and uploads from there.
    _Writer2 = start_writer(
        Config,
        #{max_segment_size_bytes => 500},
        #{fragment_target_size => 500}
    ),
    %% await_offset(20) ensures the replica reader has uploaded ALL remaining
    %% data (20 records were written in phase 1).
    await_offset(Config, 20),

    %% The manifest's first_offset must be > 0, proving the plugin started
    %% at the local log's non-zero first offset rather than offset 0.
    {FirstOffset, NextOffset} = get_range(Config),
    ?assert(FirstOffset > 0),
    ?assertEqual(20, NextOffset).

stale_retention_result_is_ignored(Config) ->
    %% A retention result whose token does not match the in-flight task (for
    %% example one the retention timeout already killed, whose message was
    %% queued before the kill) must be ignored, not routed to the apply path.
    %% Inject one with a fresh, unknown token and a payload that would crash the
    %% reader if it reached the edit-apply handler ({not_an_edit, []} would be
    %% taken as an {Edit, Refs} result), then confirm the reader is unaffected.
    StreamId = ?config(stream_id, Config),
    _ = start_writer(Config, #{}),
    ReaderPid = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
    ?assert(is_pid(ReaderPid)),
    ReaderPid ! {retention_result, make_ref(), {not_an_edit, []}},
    %% A synchronous call doubles as a barrier: it only returns once the info
    %% message above has been handled. The reader must still be the same live
    %% process (no crash, no supervised restart).
    _ = rabbitmq_stream_s3_replica_reader:evaluate_remote_retention(StreamId),
    ?assert(is_process_alive(ReaderPid)),
    ?assertEqual(ReaderPid, rabbitmq_stream_s3_registry:whereis_name({StreamId, node()})).

stale_persist_result_is_ignored(Config) ->
    %% A persist result arriving when no persist is in flight (persist_mon
    %% cleared by reset_for_recovery, the task killed, but its result message
    %% already queued) must be ignored. Before the guard, the handler matched
    %% with persist_mon = undefined and crashed on demonitor(undefined, _);
    %% reaching persist_complete on a core with no in-flight persist would also
    %% crash. The reader is idle here, so persist_mon is undefined.
    StreamId = ?config(stream_id, Config),
    _ = start_writer(Config, #{}),
    ReaderPid = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
    ?assert(is_pid(ReaderPid)),
    ReaderPid ! {persist_result, {ok, 999999}},
    %% A synchronous call doubles as a barrier: it returns only once the info
    %% message above has been handled.
    _ = rabbitmq_stream_s3_replica_reader:status(StreamId),
    ?assert(is_process_alive(ReaderPid)),
    ?assertEqual(ReaderPid, rabbitmq_stream_s3_registry:whereis_name({StreamId, node()})).

stale_group_upload_result_is_ignored(Config) ->
    %% A group upload result arriving when no rebalance is in flight (group_mon
    %% cleared by reset_for_recovery) must be ignored. Before the guard, the
    %% handler matched with group_mon = undefined and crashed on
    %% demonitor(undefined, _); group_upload_complete on a core with no pending
    %% rebalance would also crash. The reader is idle here, so group_mon is
    %% undefined.
    StreamId = ?config(stream_id, Config),
    _ = start_writer(Config, #{}),
    ReaderPid = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
    ?assert(is_pid(ReaderPid)),
    ReaderPid ! {group_upload_result, {ok, <<"stale-uid">>}},
    _ = rabbitmq_stream_s3_replica_reader:status(StreamId),
    ?assert(is_process_alive(ReaderPid)),
    ?assertEqual(ReaderPid, rabbitmq_stream_s3_registry:whereis_name({StreamId, node()})).

replication_happy_path(Config) ->
    StreamId = ?config(stream_id, Config),
    ReplicaNode = ?config(replica_node, Config),

    {Writer, _ReplicaPids} = start_cluster(
        Config, [ReplicaNode], #{max_segment_size_bytes => 500}, #{fragment_target_size => 1000}
    ),

    %% Write enough to exceed fragment target (1000 bytes).
    Record = binary:copy(<<"R">>, 300),
    [osiris:write(Writer, undefined, I, Record) || I <- lists:seq(1, 10)],

    %% Wait for commit (proves replication pipeline works).
    receive
        {osiris_written, _, _, _} -> ok
    after 1000 ->
        ct:fail(replication_pipeline_not_ready)
    end,

    %% Wait for upload on the writer node.
    ok = await_offset(StreamId, 5),

    %% Replica's manifest cache reflects the upload (broadcast arrived).
    ?awaitMatch(
        {_, NextOffset} when NextOffset > 0,
        get_range(Config, ReplicaNode),
        1000
    ),

    %% Replica has reclaimed uploaded segments (only current segment remains).
    ?awaitMatch([_], list_segment_offsets(Config, ReplicaNode), 1000).

replication_survives_reader_restart(Config) ->
    %% A writer-side replica reader that crashes is respawned by its supervisor
    %% at the SAME epoch (the epoch is the writer's, and the writer is
    %% untouched). The broadcast sequence number is the durable manifest
    %% revision, so the new incarnation resumes the sequence where the previous
    %% one left off and the replica keeps accepting its broadcasts. Were the
    %% sequence an in-memory counter, it would restart at zero, the replica would
    %% reject every post-restart sync and broadcast as stale, and its manifest
    %% would freeze - the bug this guards against.
    StreamId = ?config(stream_id, Config),
    ReplicaNode = ?config(replica_node, Config),

    {Writer, _ReplicaPids} = start_cluster(
        Config, [ReplicaNode], #{max_segment_size_bytes => 500}, #{fragment_target_size => 1000}
    ),

    Record = binary:copy(<<"R">>, 300),
    [osiris:write(Writer, undefined, I, Record) || I <- lists:seq(1, 10)],
    ok = await_offset(StreamId, 5),
    %% Replica caught up to the first batch.
    ?awaitMatch({_, N} when N > 0, get_range(Config, ReplicaNode), 1000),
    {_, N1} = get_range(Config, ReplicaNode),

    %% Restart the writer-side replica reader at the same epoch.
    ReaderPid = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
    ok = rabbitmq_stream_s3_replica_reader_sup:stop_child(ReaderPid),
    _ = start_replica_reader(Writer, Config, #{fragment_target_size => 1000}),

    %% Write more; the restarted reader uploads and broadcasts. The replica must
    %% advance past N1 rather than freezing on stale-rejected broadcasts.
    [osiris:write(Writer, undefined, I, Record) || I <- lists:seq(11, 30)],
    ok = await_offset(StreamId, N1 + 5),
    ?awaitMatch({_, N2} when N2 > N1, get_range(Config, ReplicaNode), 3000).

reconcile_recovers_replica_after_cache_restart(Config) ->
    %% The per-node manifest_replica singleton holds the replica's per-stream
    %% context and manifest cache in memory. A crash-restart drops both and
    %% nothing re-registers them, so tiering-aware local retention stalls (N5)
    %% and cross-tier reads skip the remote tier (c6). Reconciliation must
    %% re-register the context (replica side) and re-sync the cache through the
    %% writer (writer side).
    StreamId = ?config(stream_id, Config),
    ReplicaNode = ?config(replica_node, Config),
    Cache = rabbitmq_stream_s3_manifest_replica,

    {Writer, _ReplicaPids} = start_cluster(
        Config, [ReplicaNode], #{max_segment_size_bytes => 500}, #{fragment_target_size => 1000}
    ),
    Record = binary:copy(<<"R">>, 300),
    [osiris:write(Writer, undefined, I, Record) || I <- lists:seq(1, 10)],
    ok = await_offset(StreamId, 5),
    %% Replica context + cache are populated.
    ?awaitMatch(true, erpc:call(ReplicaNode, Cache, is_context_registered, [StreamId]), 2000),
    ?awaitMatch({_, N} when N > 0, get_range(Config, ReplicaNode), 2000),

    %% Restart the replica's manifest_replica: context + owned-ETS cache lost.
    OldPid = erpc:call(ReplicaNode, erlang, whereis, [Cache]),
    true = erpc:call(ReplicaNode, erlang, exit, [OldPid, kill]),
    ?awaitMatch(
        P when is_pid(P) andalso P =/= OldPid,
        erpc:call(ReplicaNode, erlang, whereis, [Cache]),
        2000
    ),
    ?assertEqual(false, erpc:call(ReplicaNode, Cache, is_context_registered, [StreamId])),

    %% Replica-side reconcile re-registers the context (N5).
    ok = erpc:call(ReplicaNode, rabbitmq_stream_s3_hooks, reconcile, []),
    ?assertEqual(true, erpc:call(ReplicaNode, Cache, is_context_registered, [StreamId])),

    %% Writer-side reconcile re-syncs the replica through the writer, re-seeding
    %% its cache (c6). Re-run reconcile each poll: the writer must first process
    %% the replica's manifest_replica DOWN that frees it for re-sync.
    ?awaitMatch(
        {_, M} when M > 0,
        begin
            ok = rabbitmq_stream_s3_hooks:reconcile(),
            get_range(Config, ReplicaNode)
        end,
        5000
    ).
