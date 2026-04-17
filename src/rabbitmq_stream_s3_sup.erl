%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_sup).
-behaviour(supervisor).

-export([start_link/0, start_pools/0]).
-export([init/1]).

-rabbit_boot_step(
    {rabbitmq_stream_s3_http_pools, [
        {description, "S3 HTTP connection pools"},
        {mfa, {?MODULE, start_pools, []}},
        {requires, rabbitmq_stream_s3},
        {enables, rabbitmq_stream_s3_server}
    ]}
).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_pools() ->
    UploadPoolId = rabbitmq_stream_s3_upload_pool,
    ok = rabbit_sup:start_child(UploadPoolId, rabbitmq_stream_s3_api_aws_pool, [
        UploadPoolId,
        #{
            name => UploadPoolId,
            min_size => application:get_env(rabbitmq_stream_s3, upload_pool_min_size, 0),
            max_size => application:get_env(rabbitmq_stream_s3, upload_pool_max_size, 20)
        }
    ]),
    GeneralPoolId = rabbitmq_stream_s3_general_pool,
    ok = rabbit_sup:start_child(GeneralPoolId, rabbitmq_stream_s3_api_aws_pool, [
        GeneralPoolId,
        #{
            name => GeneralPoolId,
            min_size => application:get_env(rabbitmq_stream_s3, general_pool_min_size, 0),
            max_size => application:get_env(rabbitmq_stream_s3, general_pool_max_size, 50)
        }
    ]).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 3, period => 5},
    RemoteReaderSup = #{
        id => rabbitmq_stream_s3_remote_reader_sup,
        type => supervisor,
        start => {rabbitmq_stream_s3_remote_reader_sup, start_link, []}
    },
    MembershipReconciliation = #{
        id => rabbitmq_stream_s3_membership_reconciliation,
        type => worker,
        start => {rabbitmq_stream_s3_membership_reconciliation, start_link, []}
    },
    Procs = [RemoteReaderSup, MembershipReconciliation],
    {ok, {SupFlags, Procs}}.
