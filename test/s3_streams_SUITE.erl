%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(s3_streams_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include("rabbitmq_stream_s3.hrl").

all() ->
    [
        {group, cluster_size_1},
        {group, cluster_size_3}
    ].

groups() ->
    [
        {cluster_size_1, [], [
            tiered_data_generation
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
    % ?awaitMatch(
    %     {error, not_found},
    %     rabbit_ct_broker_helpers:rpc(
    %         Config,
    %         0,
    %         rabbitmq_stream_s3_api_fs,
    %         get_stream_data,
    %         [QName]
    %     ),
    %     10000
    % ),

    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

transfer_leadership(Config) ->
    % Generate payloads
    Payload1K = <<0:(1024 * 8)>>,
    Payload64M = <<1:(64 * 1024 * 1024 * 8)>>,
    Payload32M = <<2:(32 * 1024 * 1024 * 8)>>,

    % Open channel on node 0
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
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
