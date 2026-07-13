%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_sup).
-behaviour(supervisor).

-include("include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    _ = seshat:new_group(rabbitmq_stream_s3),
    ok = rabbitmq_stream_s3_api:init(),
    application:set_env(osiris, log_hooks, rabbitmq_stream_s3_hooks),
    application:set_env(osiris, log_reader, rabbitmq_stream_s3_log_reader),
    try
        rabbitmq_stream_s3_prometheus_collector:register()
    catch
        RegClass:RegReason ->
            ?LOG_WARNING(
                "Could not register the Prometheus collector: ~ts:~p. "
                "Tiered storage metrics will not be exposed on this node",
                [RegClass, RegReason],
                #{domain => ?RMQLOG_DOMAIN_STREAM_S3}
            )
    end,
    %% A failed setup disables the trigger that cleans up a deleted stream's
    %% remote objects. The plugin still tiers without it, so log loudly and carry
    %% on rather than crashing the supervisor; orphan GC remains the backstop.
    try
        ok = rabbitmq_stream_s3_db:setup()
    catch
        Class:Reason ->
            ?LOG_WARNING(
                "Stream deletion cleanup could not be set up: ~ts:~p. Remote tier "
                "objects for deleted streams will need GC or manual cleanup",
                [Class, Reason],
                #{domain => ?RMQLOG_DOMAIN_STREAM_S3}
            )
    end,
    rabbitmq_stream_s3_registry:init(),
    rabbitmq_stream_s3_manifest:init(),
    SupFlags = #{strategy => one_for_one, intensity => 3, period => 5},
    CredentialServer = #{
        id => rabbitmq_stream_s3_api_aws,
        type => worker,
        start => {rabbitmq_stream_s3_api_aws, start_link, []}
    },
    BucketMonitor = #{
        id => rabbitmq_stream_s3_bucket_monitor,
        type => worker,
        start => {rabbitmq_stream_s3_bucket_monitor, start_link, []}
    },
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
    Governor = #{
        id => rabbitmq_stream_s3_governor,
        type => worker,
        start => {rabbitmq_stream_s3_governor, start_link, [governor_opts()]}
    },
    MembershipReconciliation = #{
        id => rabbitmq_stream_s3_membership_reconciliation,
        type => worker,
        start => {rabbitmq_stream_s3_membership_reconciliation, start_link, []}
    },
    %% Re-attaches readers/contexts to local osiris processes the plugin has
    %% become detached from (writer-restart race, parked reader, manifest_replica
    %% restart). Started last so its dependencies (the registry, the replica
    %% reader factory, the manifest cache) are already up; its first tick is one
    %% interval away regardless.
    Reconciler = #{
        id => rabbitmq_stream_s3_reconciler,
        type => worker,
        start => {rabbitmq_stream_s3_reconciler, start_link, []}
    },
    %% Triggers periodic cross-stream GC when enabled; a non-blocking
    %% cluster-wide lock keeps concurrent sweeps to one node at a time.
    GcScheduler = #{
        id => rabbitmq_stream_s3_gc_scheduler,
        type => worker,
        start => {rabbitmq_stream_s3_gc_scheduler, start_link, []}
    },
    Procs = [
        CredentialServer,
        BucketMonitor,
        ManifestCache,
        Reaper,
        MembershipReconciliation,
        UploadPool,
        GeneralPool,
        Governor,
        ReplicaReaderSup,
        Reconciler,
        GcScheduler
    ],
    {ok, {SupFlags, Procs}}.

governor_opts() ->
    case rabbitmq_stream_s3_config:max_transfer_bytes_per_sec() of
        unlimited ->
            #{rate => unlimited};
        Rate ->
            Opts = #{rate => Rate},
            case rabbitmq_stream_s3_config:max_transfer_burst_bytes() of
                undefined -> Opts;
                Burst -> Opts#{burst => Burst}
            end
    end.

pool_child(Id, Config) ->
    #{
        id => Id,
        type => worker,
        start => {rabbitmq_stream_s3_api_aws_pool, start_link, [Id, Config]}
    }.
