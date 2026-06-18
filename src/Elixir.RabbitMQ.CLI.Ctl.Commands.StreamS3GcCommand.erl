%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.StreamS3GcCommand').

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
    <<"Identifies (or deletes) dangling objects in the remote tier">>.

help_section() ->
    {plugin, stream}.

validate([], _Opts) ->
    ok;
validate([_Stream], _Opts) ->
    ok;
validate(_, _Opts) ->
    {validation_failure, too_many_args}.

merge_defaults(Args, Opts) ->
    {Args, maps:merge(#{mode => <<"dry_run">>, vhost => <<"/">>}, Opts)}.

usage() ->
    <<"stream_s3_gc [<stream>] [--vhost <vhost>] [--mode <dry_run|delete>]">>.

usage_additional() ->
    [
        [
            <<"<stream>">>,
            <<"Restrict GC to a single stream. When omitted, every stream is swept.">>
        ],
        [<<"--vhost <vhost>">>, <<"The virtual host of the stream.">>],
        [
            <<"--mode <mode>">>,
            <<"dry_run (default) to report only, delete to remove dangling objects.">>
        ]
    ].

run([], #{node := NodeName, mode := ModeBin, timeout := Timeout}) ->
    Mode = parse_mode(ModeBin),
    rabbit_misc:rpc_call(
        NodeName,
        rabbitmq_stream_s3_gc,
        run,
        [#{mode => Mode}],
        Timeout
    );
run([Stream], #{node := NodeName, vhost := VHost, mode := ModeBin, timeout := Timeout}) ->
    Mode = parse_mode(ModeBin),
    rabbit_misc:rpc_call(
        NodeName,
        rabbitmq_stream_s3_gc,
        run_stream,
        [VHost, Stream, #{mode => Mode}],
        Timeout
    ).

banner([], #{mode := Mode}) ->
    erlang:iolist_to_binary(
        io_lib:format("Running tiered storage GC (mode=~ts) ...", [Mode])
    );
banner([Stream], #{mode := Mode, vhost := VHost}) ->
    erlang:iolist_to_binary(
        io_lib:format(
            "Running tiered storage GC for stream ~ts in vhost ~ts (mode=~ts) ...",
            [Stream, VHost, Mode]
        )
    ).

output({ok, Findings}, #{formatter := <<"json">>}) ->
    {ok, Findings};
output({ok, Findings}, _Opts) ->
    Lines = [format_finding(F) || F <- Findings],
    Summary = io_lib:format("~b dangling object(s) found.", [length(Findings)]),
    {ok, erlang:iolist_to_binary(lists:join($\n, Lines ++ [Summary]))};
output({error, {not_found, _}}, _Opts) ->
    {error, 'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_software(), <<"Stream not found.">>};
output({error, Reason}, _Opts) ->
    {error, 'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_software(),
        erlang:iolist_to_binary(io_lib:format("~tp", [Reason]))}.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

parse_mode(<<"dry_run">>) -> dry_run;
parse_mode(<<"delete">>) -> delete;
parse_mode(_) -> dry_run.

format_finding(#{stream_id := StreamId, key := Key, reason := Reason}) ->
    io_lib:format("~ts\t~ts\t~p", [StreamId, Key, Reason]).
