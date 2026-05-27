%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(log_reader_SUITE).
-moduledoc """
Integration tests for the consumer-side remote reader.

Uses the same infrastructure as `replica_reader_SUITE`: real upload path,
barrier techniques, no mocks. Tests that data uploaded to the remote tier
can be read back correctly by consumers.

The key barrier technique: after writing, we wait for local retention to
reclaim uploaded segments. This is the natural state where the remote tier
is the only source for old data.
""".

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("rabbitmq_stream_s3/include/rabbitmq_stream_s3.hrl").

-import(rabbitmq_stream_s3_test_helpers, [
    start_writer/2,
    start_writer/3,
    start_cluster/3,
    start_cluster/4,
    start_replica_reader/3,
    flush_writer/1,
    seed_log/2,
    write_sequential/3,
    reader_config/2,
    read_all/1,
    read_all/2,
    drain_iter/3,
    await_offset/2,
    list_segment_offsets/1,
    list_segment_offsets/2,
    list_fragment_offsets/1,
    get_range/1,
    get_range/2,
    assert_sequential/2,
    assert_sequential/3
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
            read_from_remote_first,
            read_from_remote_first_large_filter,
            read_across_fragment_boundaries,
            read_from_remote_offset,
            read_repositions_on_fragment_not_found,
            local_to_remote_transition,
            read_through_group,
            read_detects_crc_corruption
        ]},
        {with_replica, [], [
            read_from_replica_node
        ]}
    ].

init_per_suite(Config) ->
    case node() of
        nonode@nohost ->
            {ok, _} = net_kernel:start([ct_remote_reader, shortnames]);
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
        max_segment_size_bytes => 5_000
    },
    [{stream_id, StreamId}, {writer_cfg, WriterCfg} | Config].

end_per_testcase(_TestCase, Config) ->
    WriterCfg = ?config(writer_cfg, Config),
    catch osiris_writer:stop(WriterCfg),
    Config.

%% ------------------------------------------------------------------
%% Tests
%% ------------------------------------------------------------------

read_from_remote_first(Config) ->
    Writer = start_writer(Config, #{fragment_target_size => 1000}),

    N = 200,
    write_sequential(Writer, N, 5),

    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch([S] when S > 0, list_segment_offsets(Config), 1000),
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),

    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),

    Records = read_all(Reader0),
    assert_sequential(Records, N).

read_from_remote_first_large_filter(Config) ->
    %% filter_size => 255 makes each chunk 239 bytes larger than default.
    %% Exercises the index boundary calculation with variable-size chunks.
    N = 200,
    Writer = start_writer(
        Config, #{filter_size => 255}, #{fragment_target_size => 1000}
    ),

    write_sequential(Writer, N, 5),

    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch([S] when S > 0, list_segment_offsets(Config), 1000),
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),

    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),

    Records = read_all(Reader0),
    assert_sequential(Records, N).

read_across_fragment_boundaries(Config) ->
    %% 3 segments, one chunk each, 600 bytes payload, fragment target 500.
    %% Each chunk exceeds the target so each becomes its own fragment.
    %% Exactly 3 fragments at offsets [0, 10, 20].
    %% Multiple segments allow retention to reclaim uploaded data.
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 10, size => 600}}]},
        {segment, [{chunk, #{records => 10, size => 600}}]},
        {segment, [{chunk, #{records => 10, size => 600}}]}
    ]),

    Writer = start_writer(Config, #{}, #{fragment_target_size => 500}),
    flush_writer(Writer),
    await_offset(Config, NextOffset),

    ?assertEqual([0, 10, 20], list_fragment_offsets(Config)),

    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),

    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),

    Records = read_all(Reader0),
    assert_sequential(Records, NextOffset).

read_from_remote_offset(Config) ->
    Writer = start_writer(Config, #{fragment_target_size => 500}),

    N = 200,
    write_sequential(Writer, N, 5),

    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch([S] when S > 0, list_segment_offsets(Config), 1000),
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),

    %% Request a random offset in the remote tier (below first_chunk_id).
    FirstChunkId = osiris_log_shared:first_chunk_id(Shared),
    TargetOffset = rand:uniform(FirstChunkId) - 1,
    ct:pal("Attaching at target offset ~b, first local offset is ~b", [TargetOffset, FirstChunkId]),

    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(TargetOffset, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),

    Records = read_all(Reader0, TargetOffset),
    assert_sequential(Records, TargetOffset, N - 1).

read_repositions_on_fragment_not_found(Config) ->
    %% 3 fragments (600 bytes each, target 500). Remote retention with
    %% max_bytes=1000 deletes the first two, leaving only offset 10.
    %% A reader requesting offset 0 gets 404 and repositions at 10.
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]}
    ]),
    Writer = start_writer(
        Config,
        #{retention => [{max_bytes, 1000}]},
        #{fragment_target_size => 500}
    ),
    flush_writer(Writer),
    await_offset(Config, NextOffset),

    %% Wait for remote retention to delete the first two fragments.
    ?awaitMatch([10], list_fragment_offsets(Config), 2000),

    %% Wait for local retention to reclaim uploaded segments.
    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),

    %% Open a reader at offset 0. The fragment is gone (retention deleted it).
    %% The reader should reposition at the oldest available offset (10).
    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(0, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),

    Records = read_all(Reader0, 10),
    assert_sequential(Records, 10, NextOffset - 1).

local_to_remote_transition(Config) ->
    %% A reader opens locally and reads some records. Then the replica reader
    %% is started, uploads the data to S3, and local retention deletes the
    %% segment. On the next read the consumer discovers the segment is gone,
    %% checks the manifest, and transitions to the remote tier.
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]}
    ]),

    %% Start writer without plugin hooks so no upload happens yet.
    WriterCfg0 = ?config(writer_cfg, Config),
    WriterCfg = maps:merge(WriterCfg0, #{log_hooks => undefined}),
    {ok, Writer} = osiris_writer:start(WriterCfg),
    flush_writer(Writer),

    %% Open a consumer reader at offset 0. Data is local, no manifest exists.
    ReaderCfg = reader_config(Writer, Config),
    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(0, ReaderCfg),
    ?assertEqual(local, rabbitmq_stream_s3_log_reader:mode(Reader0)),

    %% Read the first chunk locally.
    {ok, _Header, Iter, Reader1} =
        rabbitmq_stream_s3_log_reader:chunk_iterator(Reader0, 1, undefined),
    LocalRecords = drain_iter(Iter, 0, []),
    ?assertEqual(5, length(LocalRecords)),
    ?assertEqual(local, rabbitmq_stream_s3_log_reader:mode(Reader1)),

    %% Now start the replica reader. This triggers upload and local retention.
    %% Three segments were seeded. The writer opens the last one as current,
    %% so retention can delete segments 0 and 1 (both fully uploaded).
    #{shared := Shared} = ReaderCfg,
    _ = start_replica_reader(Writer, Config, #{fragment_target_size => 500}),

    %% Barrier: upload complete and local retention has deleted segments 0 and 1.
    await_offset(Config, NextOffset),
    ?awaitMatch(F when F >= 10, osiris_log_shared:first_chunk_id(Shared), 2000),

    %% Read the next chunk. The reader is at offset 5 (segment 1) which is
    %% gone. It checks the manifest, transitions to remote, and continues.
    {ok, _Header2, Iter2, Reader2} =
        rabbitmq_stream_s3_log_reader:chunk_iterator(Reader1, 1, undefined),
    RemoteRecords = drain_iter(Iter2, 0, []),
    ?assertEqual(5, length(RemoteRecords)),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader2)),

    %% Verify continuity: local read offsets 0-4, remote read offsets 5-9.
    AllRecords = LocalRecords ++ RemoteRecords,
    assert_sequential(AllRecords, 10).

read_through_group(Config) ->
    %% 5 segments, each producing a fragment (600 bytes > 500 target).
    %% Rebalance threshold = 4: after 4 fragments, the oldest 4 are factored
    %% into a group. Final manifest: 1 group entry + 1 fragment entry.
    %% A consumer reading from offset 0 must descend into the group to find
    %% the first fragment, then continue through all 5 fragments.
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 10, size => 600}}]},
        {segment, [{chunk, #{records => 10, size => 600}}]},
        {segment, [{chunk, #{records => 10, size => 600}}]},
        {segment, [{chunk, #{records => 10, size => 600}}]},
        {segment, [{chunk, #{records => 10, size => 600}}]}
    ]),
    Writer = start_writer(
        Config, #{}, #{fragment_target_size => 500, rebalance_threshold => 4}
    ),
    flush_writer(Writer),
    await_offset(Config, NextOffset),

    %% Wait for local retention to reclaim uploaded segments.
    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),

    %% Read from offset 0. The iterator must descend into the group.
    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),

    Records = read_all(Reader0),
    assert_sequential(Records, NextOffset).

read_detects_crc_corruption(Config) ->
    %% When verify_crc_on_read is enabled, reading a corrupted fragment
    %% must exit with {crc_validation_failure, _}.
    Writer = start_writer(Config, #{fragment_target_size => 1000}),
    N = 50,
    write_sequential(Writer, N, 5),

    ReaderCfg0 = reader_config(Writer, Config),
    ReaderCfg = ReaderCfg0#{verify_crc_on_read => true},
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch([S] when S > 0, list_segment_offsets(Config), 1000),
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),

    %% Corrupt the first fragment file on disk.
    corrupt_first_fragment(Config),

    %% Reading should fail with CRC validation error.
    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),
    ?assertExit(
        {crc_validation_failure, _},
        read_all(Reader0)
    ).

read_from_replica_node(Config) ->
    StreamId = ?config(stream_id, Config),
    ReplicaNode = ?config(replica_node, Config),

    {Writer, [ReplicaPid]} = start_cluster(
        Config, [ReplicaNode], #{max_segment_size_bytes => 5000}, #{fragment_target_size => 1000}
    ),

    N = 200,
    write_sequential(Writer, N, 5),

    %% Wait for the full upload to reach the replica's manifest cache.
    %% Retention can only reclaim segments fully below next_offset, so we
    %% must wait for all data to be uploaded before expecting reclamation.
    ?awaitMatch(
        {_, NextOffset} when NextOffset >= N,
        get_range(Config, ReplicaNode),
        5000
    ),

    %% Wait for retention on the replica to reclaim uploaded segments.
    ?awaitMatch([S] when S > 0, list_segment_offsets(Config, ReplicaNode), 1000),

    %% Read from the replica node. Everything must run in one call since
    %% the remote reader is linked to the caller.
    Records = erpc:call(ReplicaNode, fun() ->
        Ctx = osiris_util:get_reader_context(ReplicaPid),
        RCfg = Ctx#{
            name => StreamId,
            epoch => 1,
            options => #{transport => tcp},
            readers_counter_fun => fun(_) -> ok end
        },
        {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, RCfg),
        remote = rabbitmq_stream_s3_log_reader:mode(Reader0),
        rabbitmq_stream_s3_test_helpers:read_all(Reader0)
    end),
    assert_sequential(Records, N).

%% ------------------------------------------------------------------
%% Internal helpers
%% ------------------------------------------------------------------

corrupt_first_fragment(Config) ->
    RemoteDir = ?config(remote_dir, Config),
    StreamId = ?config(stream_id, Config),
    Pattern = filename:join([
        RemoteDir,
        "rabbitmq",
        "stream",
        binary_to_list(StreamId),
        "data",
        "*.fragment"
    ]),
    [First | _] = lists:sort(filelib:wildcard(binary_to_list(iolist_to_binary(Pattern)))),
    {ok, Bin} = file:read_file(First),
    %% Flip a byte in the record data region (past 8-byte fragment header +
    %% 48-byte chunk header). This ensures the CRC will not match.
    Pos = 8 + 48 + 16,
    <<Pre:Pos/binary, Byte:8, Post/binary>> = Bin,
    Corrupted = <<Pre/binary, (Byte bxor 16#FF):8, Post/binary>>,
    ok = file:write_file(First, Corrupted).
