%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_cth).
-moduledoc """
CT hook for rabbitmq_stream_s3 test suites.

Automatically sets up the FS backend, osiris data directory, and starts
the plugin application. Add to a suite with:

    -ct_hooks([rabbitmq_stream_s3_cth]).
""".

-include_lib("common_test/include/ct.hrl").

%% CT hook callbacks
-export([
    init/2,
    pre_init_per_suite/3,
    post_end_per_suite/4
]).

%% Utility for multi-node tests
-export([setup_peer/1]).

init(_Id, _Opts) ->
    {ok, #{}}.

pre_init_per_suite(_SuiteName, Config, State) ->
    _ = application:ensure_all_started(logger),
    logger:set_handler_config(default, level, error),

    PrivDir = ?config(priv_dir, Config),

    %% FS backend for remote tier storage.
    application:set_env(rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fs),
    RemoteDir = filename:join(PrivDir, "s3"),
    ok = filelib:ensure_path(RemoteDir),
    rabbitmq_stream_s3_api_fs:set_data_dir(RemoteDir),

    %% Osiris data directory.
    %% load(osiris) before set_env: osiris.app.src declares {data_dir, "/tmp/osiris"}
    %% which overwrites any set_env that precedes the load.
    OsirisDir = filename:join(PrivDir, "osiris"),
    ok = filelib:ensure_path(OsirisDir),
    application:load(osiris),
    application:set_env(osiris, data_dir, OsirisDir),
    application:set_env(osiris, replica_ip_address_family, inet),

    %% Start osiris and our supervisor directly.
    %% We cannot use ensure_all_started(rabbitmq_stream_s3) because that would
    %% pull in rabbit. The sup init handles all plugin setup (hooks, api, etc.).
    {ok, _} = application:ensure_all_started(osiris),
    {ok, SupPid} = rabbitmq_stream_s3_sup:start_link(),
    unlink(SupPid),

    {[{osiris_dir, OsirisDir}, {remote_dir, RemoteDir} | Config], State}.

post_end_per_suite(_SuiteName, _Config, Return, State) ->
    catch exit(whereis(rabbitmq_stream_s3_sup), shutdown),
    _ = application:stop(osiris),
    {Return, State}.

-doc """
Set up a peer node with the plugin infrastructure for multi-node tests.

Starts osiris and the manifest cache on the peer. Returns `{Peer, Node}`.
The caller is responsible for stopping the peer with `peer:stop(Peer)`.
""".
-spec setup_peer(ct_suite:ct_config()) -> {pid(), node()}.
setup_peer(Config) ->
    PrivDir = ?config(priv_dir, Config),
    PeerDir = filename:join(PrivDir, "replica"),
    ok = filelib:ensure_path(PeerDir),

    PaArgs = lists:flatmap(fun(P) -> ["-pa", P] end, code:get_path()),
    {ok, Peer, Node} = peer:start(#{
        name => replica,
        args => PaArgs
    }),

    %% Configure env before starting the application.
    %% load(osiris) first: see comment in pre_init_per_suite/3.
    ok = erpc:call(Node, application, load, [osiris]),
    ok = erpc:call(Node, application, set_env, [osiris, data_dir, PeerDir]),
    ok = erpc:call(Node, application, set_env, [osiris, replica_ip_address_family, inet]),
    ok = erpc:call(Node, application, set_env, [
        rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fs
    ]),
    ok = erpc:call(Node, rabbitmq_stream_s3_api_fs, set_data_dir, [?config(remote_dir, Config)]),

    %% Start osiris and our supervisor on the peer.
    {ok, _} = erpc:call(Node, application, ensure_all_started, [osiris]),
    {ok, _} = erpc:call(Node, fun() ->
        {ok, Pid} = rabbitmq_stream_s3_sup:start_link(),
        unlink(Pid),
        {ok, Pid}
    end),

    {Peer, Node}.
