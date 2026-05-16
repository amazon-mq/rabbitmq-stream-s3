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

-import(rabbitmq_stream_s3_test_helpers, [
    start_writer/2,
    start_writer/3,
    start_cluster/3,
    start_cluster/4,
    flush_writer/1,
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
            resumes_after_restart,
            large_record_cuts_immediately
        ]},
        {with_replica, [], [
            replication_happy_path
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
    ok = rabbitmq_stream_s3_test_helpers:await_offset(StreamId, 199),

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

    ok = rabbitmq_stream_s3_test_helpers:await_offset(StreamId, 8),

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
    ok = rabbitmq_stream_s3_test_helpers:await_offset(StreamId, 50),
    {0, N1} = get_range(Config),
    ?assert(N1 >= 50),

    lists:foreach(
        fun(_) -> osiris_writer:write(Writer, <<"monotonic range test data!!">>) end,
        lists:seq(1, 100)
    ),
    ok = rabbitmq_stream_s3_test_helpers:await_offset(StreamId, 150),
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
    rabbitmq_stream_s3_test_helpers:await_offset(StreamId, CurrentSegment),
    Fragments = list_fragment_offsets(Config),
    ?assert(length(Fragments) > 1),

    %% Retention will eventually reclaim everything but the current segment.
    ?awaitMatch([_], list_segment_offsets(Config), 1_000).

resumes_after_restart(Config) ->
    StreamId = ?config(stream_id, Config),
    WriterCfg = ?config(writer_cfg, Config),

    Writer1 = start_writer(Config, #{fragment_target_size => 500}),

    lists:foreach(
        fun(_) -> osiris_writer:write(Writer1, <<"first generation data">>) end,
        lists:seq(1, 100)
    ),
    ok = rabbitmq_stream_s3_test_helpers:await_offset(StreamId, 50),
    {0, RangeAfterFirst} = get_range(Config),
    FragmentsAfterFirst = length(list_fragment_offsets(Config)),

    %% Stop and restart immediately, without waiting for cleanup.
    ok = osiris_writer:stop(WriterCfg),
    Writer2 = start_writer(Config, #{fragment_target_size => 500}),

    lists:foreach(
        fun(_) -> osiris_writer:write(Writer2, <<"second generation data">>) end,
        lists:seq(1, 100)
    ),
    ok = rabbitmq_stream_s3_test_helpers:await_offset(StreamId, RangeAfterFirst + 50),

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
    ok = rabbitmq_stream_s3_test_helpers:await_offset(StreamId, 1),
    ?assertEqual([0], list_fragment_offsets(Config)).

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
    ok = rabbitmq_stream_s3_test_helpers:await_offset(StreamId, 5),

    %% Replica's manifest cache reflects the upload (broadcast arrived).
    ?awaitMatch(
        {_, NextOffset} when NextOffset > 0,
        get_range(Config, ReplicaNode),
        1000
    ),

    %% Replica has reclaimed uploaded segments (only current segment remains).
    ?awaitMatch([_], list_segment_offsets(Config, ReplicaNode), 1000).
