%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 3, period => 5},
    %% TODO: configure pool sizes.
    UploadPoolSup = #{
        id => rabbitmq_stream_s3_upload_pool,
        type => worker,
        start =>
            {rabbitmq_stream_s3_api_aws_pool, start_link, [
                rabbitmq_stream_s3_upload_pool,
                #{
                    min_size => application:get_env(rabbitmq_stream_s3, upload_pool_min_size, 4),
                    max_size => application:get_env(rabbitmq_stream_s3, upload_pool_max_size, 20)
                }
            ]}
    },
    GeneralPoolSup = #{
        id => rabbitmq_stream_s3_general_pool,
        type => worker,
        start =>
            {rabbitmq_stream_s3_api_aws_pool, start_link, [
                rabbitmq_stream_s3_general_pool,
                #{
                    min_size => application:get_env(rabbitmq_stream_s3, general_pool_min_size, 8),
                    max_size => application:get_env(rabbitmq_stream_s3, general_pool_max_size, 50)
                }
            ]}
    },
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
    Procs = [UploadPoolSup, GeneralPoolSup, RemoteReaderSup, MembershipReconciliation],
    {ok, {SupFlags, Procs}}.
