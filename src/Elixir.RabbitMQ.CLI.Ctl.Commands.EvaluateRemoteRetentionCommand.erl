%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.EvaluateRemoteRetentionCommand').

-behaviour('Elixir.RabbitMQ.CLI.CommandBehaviour').

-export([
    scopes/0,
    usage/0,
    usage_additional/0,
    banner/2,
    validate/2,
    merge_defaults/2,
    run/2,
    output/2,
    description/0,
    help_section/0
]).

scopes() ->
    [streams].

description() ->
    <<"Triggers retention evaluation for a stream's remote tier storage">>.

help_section() ->
    {plugin, stream}.

validate([], _Opts) ->
    {validation_failure, not_enough_args};
validate([_Name], _Opts) ->
    ok;
validate(_, _Opts) ->
    {validation_failure, too_many_args}.

merge_defaults(Args, Opts) ->
    {Args, maps:merge(#{vhost => <<"/">>}, Opts)}.

usage() ->
    <<"evaluate_remote_retention <stream> [--vhost <vhost>]">>.

usage_additional() ->
    [
        [<<"<stream>">>, <<"The name of the stream.">>],
        [<<"--vhost <vhost>">>, <<"The virtual host of the stream.">>]
    ].

run([Name], #{node := NodeName, vhost := VHost, timeout := Timeout}) ->
    rabbit_misc:rpc_call(
        NodeName,
        rabbitmq_stream_s3_replica_reader,
        evaluate_remote_retention,
        [VHost, Name],
        Timeout
    ).

banner([Name], #{vhost := VHost}) ->
    erlang:iolist_to_binary(
        io_lib:format(
            "Evaluating remote retention for stream ~ts in vhost ~ts ...",
            [Name, VHost]
        )
    ).

output(ok, _Opts) ->
    {ok, <<"Remote retention evaluated successfully.">>};
output({error, {not_found, _}}, _Opts) ->
    {error, 'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_software(),
        <<"Stream not found or replica reader not running on this node.">>};
output({error, {in_progress, Blocker}}, _Opts) ->
    {error, 'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_tempfail(),
        erlang:iolist_to_binary(
            io_lib:format(
                "A manifest change (~ts) is already in progress for this stream; "
                "remote retention evaluation cannot run concurrently. Try again "
                "once it completes.",
                [Blocker]
            )
        )};
output({error, Reason}, _Opts) ->
    {error, 'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_software(),
        erlang:iolist_to_binary(io_lib:format("~tp", [Reason]))}.
