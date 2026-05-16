%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    _ = seshat:new_group(rabbitmq_stream_s3),
    ok = rabbitmq_stream_s3_api:init(),
    application:set_env(osiris, log_hooks, rabbitmq_stream_s3_hooks),
    application:set_env(osiris, log_reader, rabbitmq_stream_s3_log_reader),
    catch rabbitmq_stream_s3_prometheus_collector:register(),
    catch rabbitmq_stream_s3_db:setup(),
    rabbitmq_stream_s3_registry:init(),
    SupFlags = #{strategy => one_for_one, intensity => 3, period => 5},
    UploadPool = pool_child(rabbitmq_stream_s3_upload_pool, #{
        name => rabbitmq_stream_s3_upload_pool,
        min_size => rabbitmq_stream_s3_config:upload_pool_min_size(),
        max_size => rabbitmq_stream_s3_config:upload_pool_max_size()
    }),
    GeneralPool = pool_child(rabbitmq_stream_s3_general_pool, #{
        name => rabbitmq_stream_s3_general_pool,
        min_size => rabbitmq_stream_s3_config:general_pool_min_size(),
        max_size => rabbitmq_stream_s3_config:general_pool_max_size()
    }),
    Reaper = #{
        id => rabbitmq_stream_s3_reaper,
        type => worker,
        start => {rabbitmq_stream_s3_reaper, start_link, []}
    },
    ManifestCache = #{
        id => rabbitmq_stream_s3_manifest_replica,
        type => worker,
        start => {rabbitmq_stream_s3_manifest_replica, start_link, []}
    },
    ReplicaReaderSup = #{
        id => rabbitmq_stream_s3_replica_reader_sup,
        type => supervisor,
        start => {rabbitmq_stream_s3_replica_reader_sup, start_link, []}
    },
    MembershipReconciliation = #{
        id => rabbitmq_stream_s3_membership_reconciliation,
        type => worker,
        start => {rabbitmq_stream_s3_membership_reconciliation, start_link, []}
    },
    Procs = [
        ManifestCache,
        Reaper,
        MembershipReconciliation,
        UploadPool,
        GeneralPool,
        ReplicaReaderSup
    ],
    {ok, {SupFlags, Procs}}.

pool_child(Id, Config) ->
    #{
        id => Id,
        type => worker,
        start => {rabbitmq_stream_s3_api_aws_pool, start_link, [Id, Config]}
    }.
