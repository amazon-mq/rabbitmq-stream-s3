%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    LogReaderSup = #{
        id => rabbitmq_stream_s3_log_reader_sup,
        type => supervisor,
        start => {rabbitmq_stream_s3_log_reader_sup, start_link, []}
    },
    MembershipReconciliation = #{
        id => rabbitmq_stream_s3_membership_reconciliation,
        type => worker,
        start => {rabbitmq_stream_s3_membership_reconciliation, start_link, []}
    },
    Procs = [LogReaderSup, MembershipReconciliation],
    {ok, {SupFlags, Procs}}.
