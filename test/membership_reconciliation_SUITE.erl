%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(membership_reconciliation_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-compile([nowarn_export_all, export_all]).

all() ->
    [add_remove].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config0) ->
    rabbit_ct_helpers:testcase_started(Config0, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config0, [
        {rmq_nodes_count, 3},
        {rmq_nodes_clustered, false},
        {rmq_nodename_suffix, Testcase},
        {testcase_name, <<?MODULE_STRING "-", (atom_to_binary(Testcase))/binary>>}
    ]),
    %% Lower the tick interval so that newly joined cluster members are added
    %% to the coordinator's membership faster.
    Config2 = rabbit_ct_helpers:merge_app_env(
        Config1,
        {rabbit, [{stream_tick_interval, 50}]}
    ),
    Config = rabbit_ct_helpers:merge_app_env(
        Config2,
        {rabbitmq_stream_s3, [
            %% This suite doesn't write stream data, but all test suites should
            %% use the FS backend.
            {rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fs},
            {membership_reconciliation_auto_remove, true}
        ]}
    ),
    rabbit_ct_helpers:run_steps(
        Config,
        rabbit_ct_broker_helpers:setup_steps() ++
            rabbit_ct_client_helpers:setup_steps()
    ).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(
        Config,
        rabbit_ct_client_helpers:teardown_steps() ++
            rabbit_ct_broker_helpers:teardown_steps()
    ),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------

add_remove(Config) ->
    [N0, N1, N2] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, N0),
    QName = <<(?config(testcase_name, Config))/binary, "-q">>,
    QRef = rabbit_misc:r(<<"/">>, queue, QName),
    ?assertMatch(
        #'queue.declare_ok'{},
        amqp_channel:call(Ch, #'queue.declare'{
            queue = QName,
            durable = true,
            auto_delete = false,
            arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]
        })
    ),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Ch),

    ?assertMatch(Members when map_size(Members) =:= 1, members(Config, N0, QRef)),
    join_cluster(N1, N0),
    ?awaitMatch([_, _], stream_coordinator_members(Config, N0), 1_000),
    %% We won't grow to an even number of nodes since it would threaten
    %% availability.
    evaluate_membership(Config, N0),
    ?assertMatch(Members when map_size(Members) =:= 1, members(Config, N0, QRef)),

    join_cluster(N2, N0),
    ?awaitMatch([_, _, _], stream_coordinator_members(Config, N0), 1_000),
    %% Once there are an odd number of candidates though, we grow to the
    %% default target size.
    evaluate_membership(Config, N0),
    ?awaitMatch(#{nodes := [_, _]}, get_type_state(Config, N0, QRef), 1_000),
    ?assertMatch(Members when map_size(Members) =:= 2, members(Config, N0, QRef)),
    %% We only grow once per evaluation so we need to evaluate twice to reach
    %% the target size. The writer also might've changed since the replica was
    %% added, so we need to find the writer node first.
    #{leader_node := Leader1} = get_type_state(Config, N0, QRef),
    evaluate_membership(Config, Leader1),
    ?awaitMatch(#{nodes := [_, _, _]}, get_type_state(Config, N0, QRef), 1_000),
    #{leader_node := Leader2, replica_nodes := Replicas1} = get_type_state(Config, N0, QRef),
    ?assertMatch(Members when map_size(Members) =:= 3, members(Config, Leader2, QRef)),

    StopNode = hd(Replicas1),
    ok = rabbit_ct_broker_helpers:rpc(Config, StopNode, rabbit, stop_and_halt, []),
    ?awaitMatch([_], rabbit_ct_broker_helpers:rpc(Config, Leader2, erlang, nodes, []), 5_000),
    ok = rabbit_ct_broker_helpers:rpc(Config, Leader2, rabbit_db_cluster, forget_member, [
        StopNode,
        false
    ]),
    ?awaitMatch([_, _], stream_coordinator_members(Config, Leader2), 1_000),
    evaluate_membership(Config, Leader2),
    ?awaitMatch(#{nodes := [_, _]}, get_type_state(Config, Leader2, QRef), 1_000),
    ?awaitMatch(Members when map_size(Members) =:= 2, members(Config, Leader2, QRef), 5_000),

    ok.

%% -------------------------------------------------------------------

members(Config, Node, QRef) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, ?MODULE, members_rpc, [QRef]).

members_rpc(QRef) ->
    {ok, Q} = rabbit_amqqueue:lookup(QRef),
    #{name := StreamId} = amqqueue:get_type_state(Q),
    {ok, Members} = rabbit_stream_coordinator:members(StreamId),
    Members.

stream_coordinator_members(Config, Node) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, ra_leaderboard, lookup_members, [
        rabbit_stream_coordinator
    ]).

get_type_state(Config, Node, QRef) ->
    {ok, Q} = rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_amqqueue, lookup, [QRef]),
    amqqueue:get_type_state(Q).

evaluate_membership(Config, Node) ->
    Changes = rabbit_ct_broker_helpers:rpc(
        Config,
        Node,
        rabbitmq_stream_s3_membership_reconciliation,
        evaluate_membership,
        []
    ),
    ct:pal("Evaluated membership on node ~tw and made the following changes: ~tw", [Node, Changes]),
    ok.

join_cluster(Server, Leader) ->
    %% NOTE: When using Khepri, `stop_app` is no longer necessary before
    %% using the `join_cluster` command.
    rabbit_control_helper:command(join_cluster, Server, [atom_to_list(Leader)], []).
