%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(s3_streams_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include("include/rabbitmq_stream_s3.hrl").

all() ->
    [
        {group, cluster_size_1},
        {group, cluster_size_3}
    ].

groups() ->
    [
        {cluster_size_1, [], [
            tiered_data_generation,
            read_from_remote_tier_by_offset,
            read_from_remote_tier_by_timestamp,
            prometheus_metrics
        ]},
        {cluster_size_3, [], [
            transfer_leadership
        ]}
    ].

%% -------------------------------------------------------------------
%% Setup/teardown.
%% -------------------------------------------------------------------
init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    inets:start(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(cluster_size_1, Config) ->
    init_per_group_aux(Config, 1);
init_per_group(cluster_size_3, Config) ->
    init_per_group_aux(Config, 3).

init_per_group_aux(Config, NodesCount) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, NodesCount},
        {rmq_nodes_clustered, false},
        {rmq_nodename_suffix, ?MODULE}
    ]),
    % Configure Osiris to use S3 components and set the S3 API backend
    Config2 = rabbit_ct_helpers:merge_app_env(
        Config1,
        [
            {osiris, [
                {log_reader, rabbitmq_stream_s3_log_reader},
                {log_manifest, rabbitmq_stream_s3_log_manifest}
            ]},
            {rabbitmq_stream_s3, [
                {rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fs},
                {manifest_debounce_modifications, 1}
            ]},
            {rabbit, [
                {max_message_size, 134217728}
            ]}
        ]
    ),
    rabbit_ct_helpers:run_setup_steps(
        Config2,
        rabbit_ct_broker_helpers:setup_steps() ++
            rabbit_ct_client_helpers:setup_steps()
    ).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
        Config,
        rabbit_ct_client_helpers:teardown_steps() ++
            rabbit_ct_broker_helpers:teardown_steps()
    ).

init_per_testcase(Testcase, Config) ->
    DataDir = test_data_dir(Testcase, Config),

    % Set data directory on all nodes in the cluster
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [
        rabbit_ct_broker_helpers:rpc(
            Config,
            N,
            rabbitmq_stream_s3_api_fs,
            set_data_dir,
            [DataDir]
        )
     || N <- lists:seq(0, length(Nodes) - 1)
    ],

    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases
%% -------------------------------------------------------------------

tiered_data_generation(Config) ->
    % Generate payloads
    Payload1K = <<0:(1024 * 8)>>,
    Payload64M = <<1:(64 * 1024 * 1024 * 8)>>,

    % Open channel
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'basic.qos_ok'{} = amqp_channel:call(Ch, #'basic.qos'{prefetch_count = 1}),

    QName = <<"stream_1">>,

    % Should not be data for the stream we haven't declared yet
    ?assertMatch(
        {error, not_found},
        rabbit_ct_broker_helpers:rpc(
            Config,
            0,
            rabbitmq_stream_s3_api_fs,
            get_stream_data,
            [QName]
        )
    ),

    % Create a stream.
    ?assertEqual(ok, stream_declare(Ch, QName)),
    ?assertEqual(
        ok,
        amqp_channel:call(
            Ch,
            #'basic.publish'{routing_key = QName},
            #amqp_msg{payload = Payload1K}
        )
    ),

    % Publishing a >= 64MB message triggers upload, since there is already a
    % chunk (the 1KB one) in the stream.
    ?assertEqual(
        ok,
        amqp_channel:call(
            Ch,
            #'basic.publish'{routing_key = QName},
            #amqp_msg{payload = Payload64M}
        )
    ),

    % One fragment should exist
    ?awaitMatch(
        {ok, Manifest, [_Fragment]} when Manifest /= undefined,
        rabbit_ct_broker_helpers:rpc(
            Config,
            0,
            rabbitmq_stream_s3_api_fs,
            get_stream_data,
            [QName]
        ),
        500
    ),

    % If we delete the stream, the data in the tiered storage should eventually
    % be cleared out.
    amqp_channel:call(Ch, #'queue.delete'{queue = QName}),

    % Wait for cleanup to complete
    ?awaitMatch(
        {error, not_found},
        rabbit_ct_broker_helpers:rpc(
            Config,
            0,
            rabbitmq_stream_s3_api_fs,
            get_stream_data,
            [QName]
        ),
        10000
    ),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

prometheus_metrics(Config) ->
    Payload1K = <<0:(1024 * 8)>>,
    Payload64M = <<1:(64 * 1024 * 1024 * 8)>>,

    Ch = rabbit_ct_client_helpers:open_channel(Config),
    QName = <<"prometheus_test_stream">>,
    ?assertEqual(ok, stream_declare(Ch, QName)),

    ?assertEqual(
        ok,
        amqp_channel:call(
            Ch,
            #'basic.publish'{routing_key = QName},
            #amqp_msg{payload = Payload1K}
        )
    ),
    ?assertEqual(
        ok,
        amqp_channel:call(
            Ch,
            #'basic.publish'{routing_key = QName},
            #amqp_msg{payload = Payload64M}
        )
    ),

    % Wait for at least one fragment to be uploaded
    ?awaitMatch(
        {ok, Manifest, [_Fragment | _]} when Manifest /= undefined,
        rabbit_ct_broker_helpers:rpc(
            Config,
            0,
            rabbitmq_stream_s3_api_fs,
            get_stream_data,
            [QName]
        ),
        500
    ),

    % Scrape the Prometheus endpoint
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_prometheus),
    URI = lists:flatten(io_lib:format("http://localhost:~b/metrics", [Port])),
    {ok, {{_, 200, _}, _, Body}} = httpc:request(get, {URI, []}, [], []),

    % Verify seshat counters from rabbitmq_stream_s3_server
    ?assertMatch(
        match, re:run(Body, "^rabbitmq_stream_s3_active_tasks\\{", [{capture, none}, multiline])
    ),
    ?assertMatch(
        match,
        re:run(Body, "^rabbitmq_stream_s3_fragments_created\\{", [{capture, none}, multiline])
    ),
    ?assertMatch(
        match,
        re:run(Body, "^rabbitmq_stream_s3_task_failures\\{", [{capture, none}, multiline])
    ),
    ?assertMatch(
        match,
        re:run(Body, "^rabbitmq_stream_s3_roots_created\\{", [{capture, none}, multiline])
    ),

    % Verify seshat counters from rabbitmq_stream_s3_db
    ?assertMatch(match, re:run(Body, "^rabbitmq_stream_s3_puts\\{", [{capture, none}, multiline])),
    ?assertMatch(
        match,
        re:run(Body, "^rabbitmq_stream_s3_put_successes\\{", [{capture, none}, multiline])
    ),

    % Verify per-operation counters from rabbitmq_stream_s3_api
    ?assertMatch(match, re:run(Body, "^rabbitmq_stream_s3_put\\{", [{capture, none}, multiline])),
    ?assertMatch(
        match,
        re:run(Body, "^rabbitmq_stream_s3_get\\{", [{capture, none}, multiline])
    ),
    ?assertMatch(
        match,
        re:run(Body, "^rabbitmq_stream_s3_bytes_sent\\{", [{capture, none}, multiline])
    ),
    ?assertMatch(
        match,
        re:run(Body, "^rabbitmq_stream_s3_bytes_received\\{", [{capture, none}, multiline])
    ),

    % Verify request duration histogram with kind label
    ?assertMatch(
        match,
        re:run(Body, "^rabbitmq_stream_s3_request_duration_seconds_bucket\\{kind=\"write\"", [
            {capture, none}, multiline
        ])
    ),
    ?assertMatch(
        match,
        re:run(Body, "^rabbitmq_stream_s3_request_duration_seconds_count\\{kind=\"write\"\\}", [
            {capture, none}, multiline
        ])
    ),
    ?assertMatch(
        match,
        re:run(Body, "^rabbitmq_stream_s3_request_duration_seconds_bucket\\{kind=\"read\"", [
            {capture, none}, multiline
        ])
    ),

    amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

transfer_leadership(Config) ->
    % Generate payloads
    Payload1K = <<0:(1024 * 8)>>,
    Payload64M = <<1:(64 * 1024 * 1024 * 8)>>,
    Payload32M = <<2:(32 * 1024 * 1024 * 8)>>,

    % Open channel on node 0
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    #'basic.qos_ok'{} = amqp_channel:call(Ch, #'basic.qos'{prefetch_count = 1}),

    QName = <<"stream_transfer_leadership">>,

    ct:pal("Creating stream ~s", [QName]),
    ?assertEqual(ok, stream_declare(Ch, QName)),

    ct:pal("Publishing initial 1KB message"),
    publish_data(Ch, QName, Payload1K),

    ct:pal("Publishing 64MB message to trigger upload"),
    publish_data(Ch, QName, Payload64M),

    ct:pal("Waiting for fragment to be uploaded to tiered storage"),
    ?awaitMatch(
        {ok, _Manifest, [_Fragment]} when _Manifest /= undefined,
        rabbit_ct_broker_helpers:rpc(
            Config,
            0,
            rabbitmq_stream_s3_api_fs,
            get_stream_data,
            [QName]
        ),
        5000
    ),

    LeaderNode = find_stream_leader(Config, QName),
    ct:pal("Stream leader is on node ~p", [LeaderNode]),

    ct:pal("Stopping leader node ~p to force writer transfer", [LeaderNode]),
    ok = rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),
    ok = rabbit_ct_broker_helpers:stop_node(Config, LeaderNode),

    % Wait for new leader to be elected
    timer:sleep(2000),

    % Find a node that's still running to publish to
    AllNodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ct:pal("All nodes: ~p, Stopped node: ~p", [AllNodes, LeaderNode]),
    AvailableNode = find_available_node(Config, LeaderNode),
    ct:pal("Publishing to available node ~p after leader failure", [AvailableNode]),

    % Open a new channel to a different node
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, AvailableNode),

    % Publish more data after writer transfer
    % Publish multiple large messages to ensure we trigger fragment uploads
    ct:pal("Publishing 32MB message after writer transfer"),
    publish_data(Ch2, QName, Payload32M),

    ct:pal("Publishing first 64MB message to trigger upload"),
    publish_data(Ch2, QName, Payload64M),

    % Wait a bit for the upload to be triggered
    timer:sleep(1000),

    ct:pal("Publishing 1KB message"),
    publish_data(Ch2, QName, Payload1K),

    ct:pal("Publishing second 64MB message to trigger another upload"),
    publish_data(Ch2, QName, Payload64M),

    % Verify the data is still in tiered storage after writer transfer
    % Allow time for the new writer to process and upload
    ct:pal("Verifying tiered storage still works after writer transfer"),
    {ok, ManifestPath, Fragments} = ?awaitMatch(
        {ok, _M, _F} when _M /= undefined andalso _F /= [],
        rabbit_ct_broker_helpers:rpc(
            Config,
            AvailableNode,
            rabbitmq_stream_s3_api_fs,
            get_stream_data,
            [QName]
        ),
        15000
    ),

    ct:pal("Manifest: ~s, Fragment count: ~p", [ManifestPath, length(Fragments)]),
    ct:pal("Successfully verified tiered storage works after writer transfer"),

    % Verify we can still publish to the stream after the original leader rejoins
    % to ensure the system remains stable

    % Restart the stopped node
    ct:pal("Restarting the stopped node ~p", [LeaderNode]),
    ok = rabbit_ct_broker_helpers:start_node(Config, LeaderNode),
    DataDir = test_data_dir(?FUNCTION_NAME, Config),
    rabbit_ct_broker_helpers:rpc(
        Config,
        LeaderNode,
        rabbitmq_stream_s3_api_fs,
        set_data_dir,
        [DataDir]
    ),

    % Wait for the node to rejoin
    timer:sleep(10000),

    % Verify the stream is still accessible
    ct:pal("Verifying stream is still accessible after node restart"),
    Ch3 = rabbit_ct_client_helpers:open_channel(Config, LeaderNode),
    ?assertEqual(
        ok,
        amqp_channel:call(
            Ch3,
            #'basic.publish'{routing_key = QName},
            #amqp_msg{payload = Payload1K}
        )
    ),

    % Clean up
    amqp_channel:call(Ch2, #'queue.delete'{queue = QName}),
    rabbit_ct_client_helpers:close_channel(Ch2),
    rabbit_ct_client_helpers:close_channel(Ch3),

    ct:pal("Test completed successfully"),
    ok.

read_from_remote_tier_by_offset(Config) ->
    Payload1K = binary:copy(<<0>>, 1024),
    %% 2MB: large enough to exceed the 1MB test segment size limit (triggering
    %% a segment roll with roll_reason = segment_roll), but small enough to
    %% stay under the 64MB fragment size limit.
    PayloadLarge = binary:copy(<<1>>, 2 * 1024 * 1024),
    Payload2K = binary:copy(<<2>>, 2048),

    QName = <<"stream_read_offset">>,

    %% Delete any leftover stream from a previous run, ignoring errors.
    {ok, S0, C0} = stream_test_utils:connect(Config, 0),
    catch stream_test_utils:delete_stream(S0, C0, QName),
    gen_tcp:close(S0),

    {ok, S, C1} = stream_test_utils:connect(Config, 0),
    {ok, C2} = stream_test_utils:create_stream(S, C1, QName),

    ct:pal("Publishing messages to trigger first segment roll"),
    PubId = 1,
    {ok, C3} = stream_test_utils:declare_publisher(S, C2, QName, PubId),
    {ok, C4} = stream_test_utils:publish(S, C3, PubId, 1, [Payload1K]),
    {ok, C5} = stream_test_utils:publish(S, C4, PubId, 2, [PayloadLarge]),
    {ok, C6} = stream_test_utils:publish(S, C5, PubId, 3, [Payload2K]),

    ct:pal("Waiting for first fragment to be uploaded"),
    ?awaitMatch(
        {ok, _Manifest, [_Fragment | _]} when _Manifest /= undefined,
        rabbit_ct_broker_helpers:rpc(
            Config, 0, rabbitmq_stream_s3_api_fs, get_stream_data, [QName]
        ),
        5000
    ),

    %% Two segment rolls are needed: the first makes segment 0 eligible for
    %% deletion, the second creates the active segment that keeps segment 1
    %% as the boundary, allowing segment 0 to be deleted.
    ct:pal("Publishing extra messages to trigger second and third segment rolls"),
    {ok, C7} = stream_test_utils:publish(S, C6, PubId, 4, [PayloadLarge]),
    {ok, C8} = stream_test_utils:publish(S, C7, PubId, 5, [Payload1K]),
    {ok, C9} = stream_test_utils:delete_publisher(S, C8, PubId),

    ct:pal("Waiting for local segment deletion"),
    ?awaitMatch(
        {ok, _Manifest, [_Fragment | _]} when _Manifest /= undefined,
        rabbit_ct_broker_helpers:rpc(
            Config, 0, rabbitmq_stream_s3_api_fs, get_stream_data, [QName]
        ),
        5000
    ),
    %% Wait until the local segment 0 is deleted and first_chunk_id is updated,
    %% confirming reads will go to the remote tier.
    StreamId = get_stream_id(Config, QName),
    DataDir = rabbit_ct_broker_helpers:get_node_config(Config, 0, data_dir),
    Segment0Pattern = filename:join([
        DataDir, "..", "stream", binary_to_list(StreamId), "00000000000000000000.segment"
    ]),
    ?awaitMatch(
        [],
        rabbit_ct_broker_helpers:rpc(Config, 0, filelib, wildcard, [Segment0Pattern]),
        15000
    ),
    %% Wait until the manifest is loaded in the server, confirming the remote
    %% tier is active and timestamp/offset routing will work correctly.
    ?awaitMatch(
        {0, _},
        rabbit_ct_broker_helpers:rpc(
            Config, 0, rabbitmq_stream_s3_server, get_range, [StreamId]
        ),
        5000
    ),

    SubId = 1,

    ct:pal("Consuming from offset 0 (beginning, remote tier)"),
    {ok, C10} = stream_test_utils:subscribe(S, C9, QName, SubId, 1, #{}, 0),
    ct:pal("Subscribed, socket=~p, waiting for deliver", [S]),
    {C11, Chunk0} = receive_deliver(S, C10, SubId),
    ?assertEqual(Payload1K, extract_payload(Chunk0)),

    ct:pal("Consuming from offset 1 (middle, remote tier)"),
    {ok, C12} = stream_test_utils:unsubscribe(S, C11, SubId),
    {ok, C13} = stream_test_utils:subscribe(S, C12, QName, SubId, 1, #{}, 1),
    {C14, Chunk1} = receive_deliver(S, C13, SubId),
    ?assertEqual(byte_size(PayloadLarge), byte_size(extract_payload(Chunk1))),

    ct:pal("Consuming from offset 2 (end, remote tier)"),
    {ok, C15} = stream_test_utils:unsubscribe(S, C14, SubId),
    {ok, C16} = stream_test_utils:subscribe(S, C15, QName, SubId, 1, #{}, 2),
    {C17, Chunk2} = receive_deliver(S, C16, SubId),
    ?assertEqual(Payload2K, extract_payload(Chunk2)),

    {ok, C18} = stream_test_utils:unsubscribe(S, C17, SubId),
    {ok, C19} = stream_test_utils:delete_stream(S, C18, QName),
    stream_test_utils:close(S, C19),
    ok.

read_from_remote_tier_by_timestamp(Config) ->
    Payload1K = binary:copy(<<0>>, 1024),
    %% 2MB: large enough to exceed the 1MB test segment size limit (triggering
    %% a segment roll with roll_reason = segment_roll), but small enough to
    %% stay under the 64MB fragment size limit.
    PayloadLarge = binary:copy(<<1>>, 2 * 1024 * 1024),
    Payload2K = binary:copy(<<2>>, 2048),

    QName = <<"stream_read_timestamp">>,

    %% Delete any leftover stream from a previous run, ignoring errors.
    {ok, S0, C0} = stream_test_utils:connect(Config, 0),
    catch stream_test_utils:delete_stream(S0, C0, QName),
    gen_tcp:close(S0),

    {ok, S, C1} = stream_test_utils:connect(Config, 0),
    {ok, C2} = stream_test_utils:create_stream(S, C1, QName),

    PubId = 1,
    {ok, C3} = stream_test_utils:declare_publisher(S, C2, QName, PubId),

    ct:pal("Publishing first message and recording timestamp"),
    Timestamp1 = erlang:system_time(millisecond),
    {ok, C4} = stream_test_utils:publish(S, C3, PubId, 1, [Payload1K]),
    timer:sleep(200),

    ct:pal("Publishing large message to trigger first segment roll"),
    Timestamp2 = erlang:system_time(millisecond),
    {ok, C5} = stream_test_utils:publish(S, C4, PubId, 2, [PayloadLarge]),
    timer:sleep(200),

    ct:pal("Publishing third message"),
    timer:sleep(200),
    Timestamp3 = erlang:system_time(millisecond),
    {ok, C6} = stream_test_utils:publish(S, C5, PubId, 3, [Payload2K]),
    ct:pal("Captured T1=~p T2=~p T3=~p", [Timestamp1, Timestamp2, Timestamp3]),

    ct:pal("Waiting for first fragment to be uploaded"),
    ?awaitMatch(
        {ok, _Manifest, [_Fragment | _]} when _Manifest /= undefined,
        rabbit_ct_broker_helpers:rpc(
            Config, 0, rabbitmq_stream_s3_api_fs, get_stream_data, [QName]
        ),
        5000
    ),

    %% Two segment rolls are needed: the first makes segment 0 eligible for
    %% deletion, the second creates the active segment that keeps segment 1
    %% as the boundary, allowing segment 0 to be deleted.
    ct:pal("Publishing extra messages to trigger second and third segment rolls"),
    {ok, C7} = stream_test_utils:publish(S, C6, PubId, 4, [PayloadLarge]),
    {ok, C8} = stream_test_utils:publish(S, C7, PubId, 5, [Payload1K]),
    {ok, C9} = stream_test_utils:delete_publisher(S, C8, PubId),

    ct:pal("Waiting for local segment deletion and remote tier activation"),
    ?awaitMatch(
        {ok, _Manifest, [_Fragment | _]} when _Manifest /= undefined,
        rabbit_ct_broker_helpers:rpc(
            Config, 0, rabbitmq_stream_s3_api_fs, get_stream_data, [QName]
        ),
        5000
    ),
    StreamId = get_stream_id(Config, QName),
    DataDir = rabbit_ct_broker_helpers:get_node_config(Config, 0, data_dir),
    Segment0Pattern = filename:join([
        DataDir, "..", "stream", binary_to_list(StreamId), "00000000000000000000.segment"
    ]),
    ?awaitMatch(
        [],
        rabbit_ct_broker_helpers:rpc(Config, 0, filelib, wildcard, [Segment0Pattern]),
        15000
    ),
    %% Wait until the manifest is loaded in the server, confirming the remote
    %% tier is active and timestamp/offset routing will work correctly.
    ?awaitMatch(
        {0, _},
        rabbit_ct_broker_helpers:rpc(
            Config, 0, rabbitmq_stream_s3_server, get_range, [StreamId]
        ),
        5000
    ),
    #manifest{next_offset = NextOffset, entries = Entries} = rabbit_ct_broker_helpers:rpc(
        Config, 0, rabbitmq_stream_s3_server, get_manifest, [StreamId]
    ),
    ct:pal(
        "Manifest next_offset=~p entries=~p",
        [NextOffset, decode_entries(Entries)]
    ),

    SubId = 1,

    ct:pal("Consuming from timestamp ~p (first message, remote tier)", [Timestamp1]),
    {ok, C10} = stream_test_utils:subscribe(S, C9, QName, SubId, 1, #{}, {timestamp, Timestamp1}),
    {C11, Chunk0} = receive_deliver(S, C10, SubId),
    ?assertEqual(Payload1K, extract_payload(Chunk0)),

    ct:pal("Consuming from timestamp ~p (second message, remote tier)", [Timestamp2]),
    {ok, C12} = stream_test_utils:unsubscribe(S, C11, SubId),
    {ok, C13} = stream_test_utils:subscribe(S, C12, QName, SubId, 1, #{}, {timestamp, Timestamp2}),
    {C14, Chunk1} = receive_deliver(S, C13, SubId),
    ?assertEqual(byte_size(PayloadLarge), byte_size(extract_payload(Chunk1))),

    ct:pal("Consuming from timestamp ~p (third message, remote tier)", [Timestamp3]),
    #manifest{next_offset = NextOffset3, entries = Entries3} = rabbit_ct_broker_helpers:rpc(
        Config, 0, rabbitmq_stream_s3_server, get_manifest, [StreamId]
    ),
    ct:pal(
        "Manifest at T3 subscription: next_offset=~p entries=~p",
        [NextOffset3, decode_entries(Entries3)]
    ),
    {ok, C15} = stream_test_utils:unsubscribe(S, C14, SubId),
    {ok, C16} = stream_test_utils:subscribe(S, C15, QName, SubId, 1, #{}, {timestamp, Timestamp3}),
    {C17, Chunk2} = receive_deliver(S, C16, SubId),
    ct:pal("Chunk2 first entry size: ~p, expected: ~p", [
        byte_size(extract_payload(Chunk2)), byte_size(Payload2K)
    ]),
    ?assertEqual(Payload2K, extract_payload(Chunk2)),

    {ok, C18} = stream_test_utils:unsubscribe(S, C17, SubId),
    {ok, C19} = stream_test_utils:delete_stream(S, C18, QName),
    stream_test_utils:close(S, C19),
    ok.

%% -------------------------------------------------------------------
%% Private functions
%% -------------------------------------------------------------------

test_data_dir(Testcase, Config) ->
    BasePart = rabbit_ct_helpers:get_config(Config, priv_dir),
    TestcasePart = rabbit_ct_helpers:config_to_testcase_name(Config, Testcase),
    filename:join([BasePart, "s3_api_fs_storage", TestcasePart]).

stream_declare(Ch, StreamName) ->
    Args = [{<<"x-queue-type">>, longstr, <<"stream">>}],

    case
        amqp_channel:call(
            Ch,
            #'queue.declare'{
                queue = StreamName,
                durable = true,
                arguments = Args
            }
        )
    of
        #'queue.declare_ok'{} -> ok;
        Error -> {error, Error}
    end.

publish_data(Ch, QName, Payload) ->
    ?assertEqual(
        ok,
        amqp_channel:call(
            Ch,
            #'basic.publish'{routing_key = QName},
            #amqp_msg{payload = Payload}
        )
    ).

find_stream_leader(Config, QName) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    find_stream_leader_loop(Config, QName, Nodes, 0).

find_stream_leader_loop(Config, QName, [_Node | Rest], Idx) ->
    case
        rabbit_ct_broker_helpers:rpc(
            Config,
            Idx,
            rabbit_amqqueue,
            lookup,
            [rabbit_misc:r(<<"/">>, queue, QName)]
        )
    of
        {ok, Q} ->
            case rabbit_ct_broker_helpers:rpc(Config, Idx, amqqueue, get_leader_node, [Q]) of
                LeaderNode when is_atom(LeaderNode) ->
                    % Find which node index this corresponds to
                    AllNodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
                    find_node_index(LeaderNode, AllNodes, 0);
                _ ->
                    find_stream_leader_loop(Config, QName, Rest, Idx + 1)
            end;
        _ ->
            find_stream_leader_loop(Config, QName, Rest, Idx + 1)
    end;
find_stream_leader_loop(_Config, _QName, [], _Idx) ->
    error(stream_leader_not_found).

find_node_index(Node, [Node | _Rest], Idx) ->
    Idx;
find_node_index(Node, [_Other | Rest], Idx) ->
    find_node_index(Node, Rest, Idx + 1);
find_node_index(_Node, [], _Idx) ->
    error(node_not_found).

find_available_node(Config, StoppedNode) ->
    NodesCount = length(rabbit_ct_broker_helpers:get_node_configs(Config, nodename)),
    find_available_node_loop(NodesCount, StoppedNode, 0).

find_available_node_loop(NodesCount, StoppedNode, Idx) when Idx < NodesCount ->
    case Idx =:= StoppedNode of
        true ->
            find_available_node_loop(NodesCount, StoppedNode, Idx + 1);
        false ->
            Idx
    end;
find_available_node_loop(_NodesCount, _StoppedNode, _Idx) ->
    error(no_available_node).

%% Receive a deliver or deliver_v2 command and return the chunk binary.
%% Uses a custom loop to handle large chunks that require many recv calls.
receive_deliver(S, C0, SubId) ->
    receive_deliver_loop(S, C0, SubId).

receive_deliver_loop(S, C0, SubId) ->
    case rabbit_stream_core:next_command(C0) of
        {{deliver, SubId, Chunk}, C1} ->
            {C1, Chunk};
        {{deliver_v2, SubId, _CommittedOffset, Chunk}, C1} ->
            {C1, Chunk};
        empty ->
            case gen_tcp:recv(S, 0, 5000) of
                {ok, Data} ->
                    C1 = rabbit_stream_core:incoming_data(Data, C0),
                    receive_deliver_loop(S, C1, SubId);
                {error, Err} ->
                    ct:fail("error receiving stream data ~w", [Err])
            end;
        {_Other, C1} ->
            %% Skip non-deliver commands (e.g. metadata updates).
            receive_deliver_loop(S, C1, SubId)
    end.

%% Extract the raw payload from an osiris chunk.
extract_payload(Chunk) ->
    <<_Mag:4, _Ver:4, _Type:8, _NumEntries:16, _NumRecords:32, _Timestamp:64, _Epoch:64,
        _ChunkId:64, _Crc:32, _DataLength:32, _TrailerLength:32, BloomSize:16, _Reserved:16,
        _Bloom:BloomSize/binary, 0:1, EntrySize:31, Entry:EntrySize/binary, _/binary>> = Chunk,
    Sections = amqp10_framing:decode_bin(Entry),
    #'v1_0.data'{content = Payload} = lists:keyfind('v1_0.data', 1, Sections),
    Payload.

decode_entries(<<>>) ->
    [];
decode_entries(?ENTRY(O, FTs, LTs, _Kind, _Size, _Uid, Rest)) ->
    [{O, FTs, LTs} | decode_entries(Rest)].

get_stream_id(Config, QName) ->
    RName = rabbit_misc:r(<<"/">>, queue, QName),
    {ok, Q} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [RName]),
    TypeState = rabbit_ct_broker_helpers:rpc(Config, 0, amqqueue, get_type_state, [Q]),
    list_to_binary(maps:get(name, TypeState)).
