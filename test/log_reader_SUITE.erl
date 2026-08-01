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
    assert_sequential/3,
    capture_log/2
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
            send_file_from_remote_tier,
            read_from_remote_first_large_filter,
            read_across_fragment_boundaries,
            read_from_remote_offset,
            read_repositions_on_fragment_not_found,
            local_to_remote_transition,
            read_through_group,
            read_detects_crc_corruption,
            offset_spec_at_tier_boundary,
            remote_reader_restart_self_heals,
            become_local_stops_remote_reader,
            read_retries_transient_remote_error,
            read_with_out_of_order_remote_responses,
            read_with_out_of_order_responses_and_a_failure,
            read_tolerates_slow_remote,
            read_group_fetch_error_is_surfaced
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
    %% Restore the default API backend if a test swapped in the fault backend.
    application:set_env(
        rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fs
    ),
    catch rabbitmq_stream_s3_api_fault:reset(),
    %% Restore the default prefetch sizing if a test shrank it.
    application:unset_env(rabbitmq_stream_s3, prefetch_request_size),
    application:unset_env(rabbitmq_stream_s3, prefetch_window_max),
    application:unset_env(rabbitmq_stream_s3, prefetch_max_depth),
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

send_file_from_remote_tier(Config) ->
    %% `send_file/3` is the high-throughput delivery path: the stream protocol
    %% hands it a socket and it writes chunk headers and data straight out,
    %% never assembling a chunk into one binary. Nothing else in this suite
    %% drives it - every other remote read here goes through the chunk-iterator
    %% path, which the messaging protocols use - so this is the only coverage
    %% for reading the remote tier as iodata.
    %%
    %% CRC validation is on, which is what gives the test teeth: the check runs
    %% over the bytes as they came out of the reader's blocks, so a range
    %% assembled in the wrong order, short, or with the wrong prefix taken for
    %% the check exits here rather than being written to the socket unnoticed.
    Writer = start_writer(Config, #{fragment_target_size => 1000}),
    N = 200,
    write_sequential(Writer, N, 5),

    ReaderCfg0 = reader_config(Writer, Config),
    ReaderCfg = ReaderCfg0#{verify_crc_on_read => true},
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch([S] when S > 0, list_segment_offsets(Config), 1000),
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),

    %% What the chunk-iterator path says is there, chunk by chunk: the header
    %% plus the bytes `send_file` is asked to send for it.
    {ok, Counting} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Counting)),
    Expected = expected_send_bytes(Counting, 0),

    {ok, Reader} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg),
    {Server, Client} = tcp_pair(),
    try
        Sent = send_all(Server, Reader),
        Received = recv_exactly(Client, Sent),
        ?assert(Sent > 0),
        ?assertEqual(Expected, Sent),
        ?assertEqual(Sent, byte_size(Received))
    after
        gen_tcp:close(Server),
        gen_tcp:close(Client)
    end.

%% The chunk header is sent whole and the data region is sent as the chunk
%% selector asks for it, which for user data is `data_size` bytes after the
%% filter.
expected_send_bytes(Reader0, Acc) ->
    case rabbitmq_stream_s3_log_reader:chunk_iterator(Reader0, 1, undefined) of
        {ok, #{filter_size := FilterSize, data_size := DataSize}, _Iter, Reader} ->
            expected_send_bytes(Reader, Acc + ?CHUNK_HEADER_B + FilterSize + DataSize);
        {end_of_stream, _Reader} ->
            Acc
    end.

send_all(Socket, Reader0) ->
    send_all(Socket, Reader0, 0).

send_all(Socket, Reader0, Acc) ->
    %% The callback's return is prepended to every chunk on the wire; an empty
    %% one keeps the byte count to what the reader itself sent.
    Callback = fun(_Header, _Size) -> <<>> end,
    case rabbitmq_stream_s3_log_reader:send_file(Socket, Reader0, Callback) of
        {ok, Reader} ->
            {ok, Sent} = sent_bytes(Socket),
            send_all(Socket, Reader, Sent);
        {end_of_stream, _Reader} ->
            Acc
    end.

sent_bytes(Socket) ->
    {ok, [{send_oct, Sent}]} = inet:getstat(Socket, [send_oct]),
    {ok, Sent}.

tcp_pair() ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(Listen),
    {ok, Client} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}]),
    {ok, Server} = gen_tcp:accept(Listen, 5000),
    ok = gen_tcp:close(Listen),
    {Server, Client}.

recv_exactly(Socket, Bytes) ->
    {ok, Data} = gen_tcp:recv(Socket, Bytes, 5000),
    Data.

read_retries_transient_remote_error(Config) ->
    %% A transient S3 error on a remote fragment GET must be retried, not
    %% surfaced to the consumer: the read still delivers the full stream. The
    %% fault backend injects one slow_down on the next fragment GET.
    StreamId = ?config(stream_id, Config),
    ok = application:set_env(
        rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fault
    ),
    ok = rabbitmq_stream_s3_api_fault:setup(),

    Writer = start_writer(Config, #{fragment_target_size => 1000}),
    N = 200,
    write_sequential(Writer, N, 5),
    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch([S] when S > 0, list_segment_offsets(Config), 1000),
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),

    %% The next remote fragment GET returns a transient error; the reader must
    %% retry and still deliver the full stream.
    ok = rabbitmq_stream_s3_api_fault:fail_next(get_range_async, StreamId, slow_down),

    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),
    Records = read_all(Reader0),
    assert_sequential(Records, N).

%% Shrink the prefetch sizing so each fragment takes several range requests and
%% more than one is in flight at a time. At the production request size a
%% fragment these tests write is a single request, which never interleaves.
pipeline_within_fragments() ->
    ok = application:set_env(rabbitmq_stream_s3, prefetch_request_size, 256),
    ok = application:set_env(rabbitmq_stream_s3, prefetch_window_max, 8192),
    ok = application:set_env(rabbitmq_stream_s3, prefetch_max_depth, 8).

read_with_out_of_order_remote_responses(Config) ->
    %% Several ranges of a fragment are fetched at once, so their responses
    %% interleave. The reader stages bytes for a range whose predecessors are
    %% unfinished and appends them only once it heads the queue; if that
    %% reassembly were wrong the consumer would see corrupt or missing records
    %% rather than an error. The fault backend delivers every range response
    %% from a courier after a random delay, so they land out of issue order.
    StreamId = ?config(stream_id, Config),
    ok = application:set_env(
        rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fault
    ),
    ok = rabbitmq_stream_s3_api_fault:setup(),
    ok = pipeline_within_fragments(),

    Writer = start_writer(Config, #{fragment_target_size => 1000}),
    N = 200,
    write_sequential(Writer, N, 5),
    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch([S] when S > 0, list_segment_offsets(Config), 1000),
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),

    ok = rabbitmq_stream_s3_api_fault:reorder(StreamId, 20),

    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),
    Records = read_all(Reader0),
    assert_sequential(Records, N),
    %% Guard against the test going vacuous: the reorder path must have
    %% answered several requests, or nothing was interleaved.
    ?assert(rabbitmq_stream_s3_api_fault:reorder_count() > 1).

read_with_out_of_order_responses_and_a_failure(Config) ->
    %% The same, with one range failing mid-pipeline: only that range may be
    %% re-requested, and the ranges queued behind it must keep the bytes they
    %% have already received rather than leaving a hole.
    StreamId = ?config(stream_id, Config),
    ok = application:set_env(
        rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fault
    ),
    ok = rabbitmq_stream_s3_api_fault:setup(),
    ok = pipeline_within_fragments(),

    Writer = start_writer(Config, #{fragment_target_size => 1000}),
    N = 200,
    write_sequential(Writer, N, 5),
    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch([S] when S > 0, list_segment_offsets(Config), 1000),
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),

    ok = rabbitmq_stream_s3_api_fault:reorder(StreamId, 20),
    ok = rabbitmq_stream_s3_api_fault:fail_next(get_range_async, StreamId, slow_down),

    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),
    Records = read_all(Reader0),
    assert_sequential(Records, N),
    ?assert(rabbitmq_stream_s3_api_fault:reorder_count() > 1).

read_tolerates_slow_remote(Config) ->
    %% A slow remote tier (latency on every fragment GET) must still deliver the
    %% full, correct stream.
    StreamId = ?config(stream_id, Config),
    ok = application:set_env(
        rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fault
    ),
    ok = rabbitmq_stream_s3_api_fault:setup(),

    Writer = start_writer(Config, #{fragment_target_size => 1000}),
    N = 100,
    write_sequential(Writer, N, 5),
    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch([S] when S > 0, list_segment_offsets(Config), 1000),
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),

    %% Inject latency on every remote fragment GET.
    ok = rabbitmq_stream_s3_api_fault:delay(get_range_async, StreamId, 20),

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
    %% 3 fragments (600 bytes each, target 500). A reader opens while all
    %% fragments exist, then retention deletes the first two. The reader's
    %% iterator still points to the deleted fragments. When it tries to
    %% fetch them it gets 404s and must advance to the surviving fragment.
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]},
        {segment, [{chunk, #{records => 5, size => 600}}]}
    ]),
    Writer = start_writer(Config, #{}, #{fragment_target_size => 500}),
    flush_writer(Writer),
    await_offset(Config, NextOffset),

    %% Wait for local retention to reclaim uploaded segments so the reader
    %% enters remote mode.
    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),

    %% Open a reader at offset 0 while all 3 fragments still exist.
    %% The reader's iterator references all of them.
    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(0, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),

    %% Now tighten retention. This triggers immediate evaluation and a
    %% persist cycle that flushes the deletions.
    osiris:update_retention(Writer, [{max_bytes, 700}]),
    ?awaitMatch([10], list_fragment_offsets(Config), 5000),

    %% Read from the reader. Its iterator points to the now-deleted
    %% fragments at offsets 0 and 5. It gets 404s, refreshes its iterator,
    %% and advances to the surviving fragment at offset 10.
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

read_group_fetch_error_is_surfaced(Config) ->
    %% Resolving a spec that descends into a group hitting a *transient* S3 error
    %% (here slow_down) must surface a clean error, not silently fall back to a
    %% local reader (skipping the remote tier) or crash the consumer's reader. The
    %% numeric spec goes via find_fragment; the `first` spec goes via resolve_first
    %% and the fragment iterator. Both must surface the error.
    %%
    %% A transient error is distinct from not_found: the iterator treats not_found
    %% as a group deleted by retention and skips past it (covered for find_fragment
    %% by the eunit find_fragment_group_fetch_error_is_surfaced_test), so a
    %% transient error is what exercises the resolve_first surfacing fix.
    ok = application:set_env(
        rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fault
    ),
    ok = rabbitmq_stream_s3_api_fault:setup(),

    %% 5 fragments, rebalance threshold 4: the oldest 4 are factored into a group
    %% at offset 0, so resolving offset 0 must descend into it.
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
    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),
    %% The assertions require the descent to actually fetch a group object, so
    %% barrier on the rebalance having factored the leading fragments into a
    %% group entry. first_chunk_id alone (local retention) does not imply the
    %% manifest has been grouped yet: without this the resolution can find a flat
    %% fragment, fetch no group, and return {ok, _} instead of the injected error.
    ?awaitMatch(true, manifest_has_group(Config), 5000),

    %% Every group object fetch returns a transient error (consumer reads use
    %% `get` only for group objects; index uses get_range, data uses
    %% get_range_async). fail_matching (not fail_next) so a background retention
    %% group fetch cannot consume a one-shot failure before the resolution under
    %% test reaches the backend.
    ok = rabbitmq_stream_s3_api_fault:fail_matching(get, <<"group">>, slow_down),

    %% The numeric spec resolves through find_fragment.
    ?assertMatch(
        {error, {group_fetch_failed, slow_down}},
        rabbitmq_stream_s3_log_reader:init_offset_reader(0, ReaderCfg)
    ),

    %% The `first` spec resolves through resolve_first, which descends into the
    %% leading group. A transient fetch error there must surface, not silently
    %% return a local reader at the local floor (which would skip the entire
    %% remote tier below it).
    ?assertMatch(
        {error, {group_fetch_failed, slow_down}},
        rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg)
    ).

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
    Log = capture_log(#{level => error}, fun() ->
        {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg),
        ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),
        ?assertExit(
            {crc_validation_failure, _},
            read_all(Reader0)
        )
    end),
    ?assertMatch({_, _}, binary:match(Log, <<"CRC validation failure">>)).

offset_spec_at_tier_boundary(Config) ->
    %% Tests offset spec resolution at the exact boundary between tiers.
    %%
    %% After upload and retention, the remote tier covers [0, NextOffset) and
    %% the local tier covers [FirstChunkId, NextOffset). The tiers overlap
    %% because retention keeps the current segment. The resolution boundary
    %% is FirstChunkId: offsets >= FirstChunkId resolve to local, offsets
    %% below resolve to remote.
    #{next_offset := NextOffset} = seed_log(Config, [
        {segment, [{chunk, #{records => 10, size => 600}}]},
        {segment, [{chunk, #{records => 10, size => 600}}]},
        {segment, [{chunk, #{records => 10, size => 600}}]}
    ]),

    Writer = start_writer(Config, #{}, #{fragment_target_size => 500}),
    flush_writer(Writer),
    await_offset(Config, NextOffset),

    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    %% Wait for local retention to fully settle before capturing the tier
    %% boundary. Retention removes segment files and advances the shared
    %% first_chunk_id atomic independently, and the file deletion can land
    %% before the atomic update: a barrier on the segment list alone (or on
    %% first_chunk_id > 0) can observe a single remaining segment while the
    %% atomic still holds the previous, lower floor. Capturing first_chunk_id
    %% then records a stale boundary that a later resolution contradicts (the
    %% atomic has since advanced), routing a below-floor offset to the remote
    %% tier and failing the assertions. Wait until exactly one segment remains
    %% *and* first_chunk_id has caught up to that segment's first offset.
    ?awaitMatch(
        {[Seg], Seg},
        {list_segment_offsets(Config), osiris_log_shared:first_chunk_id(Shared)},
        5000
    ),

    FirstChunkId = osiris_log_shared:first_chunk_id(Shared),
    ?assert(FirstChunkId > 0),
    ?assert(FirstChunkId =< NextOffset),

    %% Exactly at FirstChunkId: resolves to local.
    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(FirstChunkId, ReaderCfg),
    ?assertEqual(local, rabbitmq_stream_s3_log_reader:mode(Reader0)),
    rabbitmq_stream_s3_log_reader:close(Reader0),

    %% Same via {abs, _}.
    {ok, Reader1} = rabbitmq_stream_s3_log_reader:init_offset_reader(
        {abs, FirstChunkId}, ReaderCfg
    ),
    ?assertEqual(local, rabbitmq_stream_s3_log_reader:mode(Reader1)),
    rabbitmq_stream_s3_log_reader:close(Reader1),

    %% One below the boundary: resolves to remote.
    {ok, Reader2} = rabbitmq_stream_s3_log_reader:init_offset_reader(
        FirstChunkId - 1, ReaderCfg
    ),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader2)),
    rabbitmq_stream_s3_log_reader:close(Reader2).

remote_reader_restart_self_heals(Config) ->
    %% When the remote reader gen_server exits unexpectedly, the read must not
    %% crash the consumer. The log reader surfaces a clean error, restarts a
    %% fresh remote reader and the subscription self-heals.
    Writer = start_writer(Config, #{fragment_target_size => 1000}),
    N = 200,
    write_sequential(Writer, N, 5),

    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch([S] when S > 0, list_segment_offsets(Config), 1000),
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 1000),

    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),

    %% Kill the remote reader out from under the consumer.
    Pid = rabbitmq_stream_s3_log_reader:remote_pid(Reader0),
    ?assert(is_process_alive(Pid)),
    exit(Pid, kill),
    ?awaitMatch(false, is_process_alive(Pid), 1000),

    %% The next read restarts a fresh reader (a new live pid) instead of
    %% crashing on the dead one.
    {ok, _Header, Iter, Reader1} =
        rabbitmq_stream_s3_log_reader:chunk_iterator(Reader0, 1, undefined),
    NewPid = rabbitmq_stream_s3_log_reader:remote_pid(Reader1),
    ?assertNotEqual(Pid, NewPid),
    ?assert(is_process_alive(NewPid)),

    %% The full stream reads back correctly across the restart.
    FirstChunk = drain_iter(Iter, 0, []),
    Rest = read_all(Reader1),
    assert_sequential(FirstChunk ++ Rest, N).

become_local_stops_remote_reader(Config) ->
    %% A reader that transitions from the remote tier back to the local tier
    %% (become_local) must stop the remote reader gen_server rather than
    %% leaving it orphaned for the consumer's lifetime.
    %%
    %% Build a remote tier that stops short of the local tail: upload a first
    %% batch and let retention evict its early segments, then halt uploads and
    %% append a local-only tail. A reader starting below the local floor opens
    %% on the remote tier and must become_local when it reaches the tail.
    WriterCfg0 = ?config(writer_cfg, Config),
    WriterCfg = maps:merge(WriterCfg0, #{log_hooks => undefined}),
    {ok, Writer} = osiris_writer:start(WriterCfg),
    flush_writer(Writer),

    %% Batch A: uploaded and partly evicted by retention.
    write_sequential(Writer, 200, 5),
    ReaderPid = start_replica_reader(Writer, Config, #{fragment_target_size => 500}),
    await_offset(Config, 200),
    ReaderCfg = reader_config(Writer, Config),
    #{shared := Shared} = ReaderCfg,
    ?awaitMatch(F when F > 0, osiris_log_shared:first_chunk_id(Shared), 2000),

    %% Halt uploads, then append a tail that stays local-only.
    ok = rabbitmq_stream_s3_replica_reader_sup:stop_child(ReaderPid),
    write_sequential(Writer, 50, 5),
    flush_writer(Writer),

    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),
    RemotePid = rabbitmq_stream_s3_log_reader:remote_pid(Reader0),
    ?assert(is_process_alive(RemotePid)),

    %% Read forward until the reader crosses into the local tier.
    ReaderFinal = read_until_local(Reader0),
    ?assertEqual(local, rabbitmq_stream_s3_log_reader:mode(ReaderFinal)),

    %% The orphaned remote reader must have been stopped.
    ?awaitMatch(false, is_process_alive(RemotePid), 1000).

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

%% Whether the stream's cached manifest has had any fragments factored into a
%% group entry (the rebalance has run).
manifest_has_group(Config) ->
    StreamId = ?config(stream_id, Config),
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{entries = Entries} -> entries_have_group(Entries);
        _ -> false
    end.

entries_have_group(?ENTRY(_O, _F, _L, ?MANIFEST_KIND_GROUP, _S, _U, _Rest)) ->
    true;
entries_have_group(<<_:?ENTRY_B/binary, Rest/binary>>) ->
    entries_have_group(Rest);
entries_have_group(_) ->
    false.

read_until_local(Reader) ->
    case rabbitmq_stream_s3_log_reader:mode(Reader) of
        local ->
            Reader;
        remote ->
            case rabbitmq_stream_s3_log_reader:chunk_iterator(Reader, 1, undefined) of
                {ok, _Header, _Iter, Reader1} ->
                    read_until_local(Reader1);
                {end_of_stream, Reader1} ->
                    Reader1
            end
    end.

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
