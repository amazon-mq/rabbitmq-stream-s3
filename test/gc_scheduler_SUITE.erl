%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(gc_scheduler_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-compile([nowarn_export_all, export_all]).

%% The lock resource, mirroring the ?GC_LOCK definition in
%% rabbitmq_stream_s3_gc_scheduler. The requester id is added per caller.
-define(LOCK_RESOURCE, {rabbitmq_stream_s3_gc_scheduler, sweep}).

all() ->
    [sweep_lock_is_cluster_wide].

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
        {rmq_nodes_count, 2},
        {rmq_nodes_clustered, true},
        {rmq_nodename_suffix, Testcase}
    ]),
    Config = rabbit_ct_helpers:merge_app_env(
        Config1,
        {rabbitmq_stream_s3, [
            %% All test suites use the FS backend, though this suite does not
            %% touch the remote tier.
            {rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fs}
        ]}
    ),
    rabbit_ct_helpers:run_steps(
        Config,
        rabbit_ct_broker_helpers:setup_steps()
    ).

end_per_testcase(Testcase, Config) ->
    %% Best-effort: release the lock holder in case a failed assertion aborted
    %% the testcase before it released the lock, so a leaked holder cannot
    %% outlive the case. release_lock/0 is idempotent and the node may already
    %% be gone, so ignore any error.
    catch rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, release_lock, []),
    Config1 = rabbit_ct_helpers:run_steps(
        Config,
        rabbit_ct_broker_helpers:teardown_steps()
    ),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------

%% While one node holds the cluster-wide sweep lock, another node's
%% with_sweep_lock/2 must skip its sweep; once the lock is released, the next
%% attempt runs. This is the mutual exclusion the eunit tests cannot cover: the
%% eunit VM runs without distribution, so global does not arbitrate.
sweep_lock_is_cluster_wide(Config) ->
    [N0, N1] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Nodes = [N0, N1],

    %% N0 takes and holds the lock.
    ok = rabbit_ct_broker_helpers:rpc(Config, N0, ?MODULE, hold_lock, [Nodes]),

    %% N1 cannot sweep while N0 holds the lock.
    ?assertEqual(skipped, try_sweep(Config, N1, Nodes)),

    %% After N0 releases, N1's next attempt runs the sweep function.
    ok = rabbit_ct_broker_helpers:rpc(Config, N0, ?MODULE, release_lock, []),
    ?awaitMatch({ran, swept}, try_sweep(Config, N1, Nodes), 5_000),

    ok.

%% -------------------------------------------------------------------
%% RPC targets (run on the broker nodes).
%% -------------------------------------------------------------------

%% Acquire the sweep-lock resource from a dedicated, registered holder process so
%% it outlives this RPC call and keeps the lock held until release_lock/0.
hold_lock(Nodes) ->
    Parent = self(),
    Pid = spawn(fun() ->
        true = global:set_lock({?LOCK_RESOURCE, self()}, Nodes),
        Parent ! locked,
        receive
            release -> ok
        end
    end),
    register(gc_scheduler_suite_lock_holder, Pid),
    receive
        locked -> ok
    after 5_000 ->
        error(lock_not_acquired)
    end.

%% Idempotent so end_per_testcase can call it as best-effort cleanup even when
%% the test already released the holder (or never acquired it).
release_lock() ->
    case whereis(gc_scheduler_suite_lock_holder) of
        undefined ->
            ok;
        Pid ->
            unregister(gc_scheduler_suite_lock_holder),
            Pid ! release,
            ok
    end.

%% Attempt a sweep under the cluster-wide lock, running a trivial function in
%% place of the real GC so this suite needs no remote-tier fixtures.
try_sweep_rpc(Nodes) ->
    rabbitmq_stream_s3_gc_scheduler:with_sweep_lock(Nodes, fun() -> swept end).

%% -------------------------------------------------------------------

try_sweep(Config, Node, Nodes) ->
    rabbit_ct_broker_helpers:rpc(Config, Node, ?MODULE, try_sweep_rpc, [Nodes]).
