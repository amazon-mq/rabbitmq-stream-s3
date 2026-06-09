%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module('Elixir.RabbitMQ.CLI.Ctl.Commands.StreamS3StatusCommand').

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
    <<"Shows the tiered storage status for a stream">>.

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
    <<"stream_s3_status <stream> [--vhost <vhost>]">>.

usage_additional() ->
    [
        [<<"<stream>">>, <<"The name of the stream.">>],
        [<<"--vhost <vhost>">>, <<"The virtual host of the stream.">>]
    ].

run([Name], #{node := NodeName, vhost := VHost, timeout := Timeout}) ->
    rabbit_misc:rpc_call(
        NodeName,
        rabbitmq_stream_s3_replica_reader,
        status,
        [VHost, Name],
        Timeout
    ).

banner([Name], #{vhost := VHost}) ->
    erlang:iolist_to_binary(
        io_lib:format(
            "Status of tiered storage for stream ~ts in vhost ~ts ...",
            [Name, VHost]
        )
    ).

output({ok, Info}, #{formatter := <<"json">>}) ->
    {ok, flatten(Info)};
output({ok, Info}, _Opts) ->
    KVs = flatten(Info),
    Lines = [io_lib:format("~s:\t~tp", [K, V]) || {K, V} <- KVs],
    {ok, erlang:iolist_to_binary(lists:join($\n, Lines))};
output({error, {not_found, _}}, _Opts) ->
    {error, 'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_software(),
        <<"Stream not found or replica reader not running on this node.">>};
output({error, Reason}, _Opts) ->
    {error, 'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_software(),
        erlang:iolist_to_binary(io_lib:format("~tp", [Reason]))}.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

flatten(
    #{stream := StreamId, core := Core, assembly := Assembly, log_next_offset := LogNext} = Info
) ->
    Target = maps:get(fragment_target_size, Info),
    Base = [
        {stream, StreamId},
        {fragment_target_size, Target},
        {log_next_offset, LogNext}
    ],
    Base ++ flatten_core(Core) ++ flatten_assembly(Assembly).

flatten_core(undefined) ->
    [{core, <<"not initialized">>}];
flatten_core(Core) ->
    [
        {manifest_first_offset, maps:get(manifest_first_offset, Core)},
        {manifest_next_offset, maps:get(manifest_next_offset, Core)},
        {manifest_total_size, maps:get(manifest_total_size, Core)},
        {persisted_next_offset, maps:get(persisted_next_offset, Core)},
        {transfers_in_flight, maps:get(transfers_in_flight, Core)},
        {transfers_pending_order, maps:get(transfers_pending_order, Core)},
        {since_persist, maps:get(since_persist, Core)},
        {persist_in_flight, maps:get(persist_in_flight, Core)},
        {rebalance_in_flight, maps:get(rebalance_in_flight, Core)},
        {retention_in_flight, maps:get(retention_in_flight, Core)},
        {waiters, maps:get(waiters, Core)}
    ].

flatten_assembly(undefined) ->
    [{assembly, <<"not initialized">>}];
flatten_assembly(Assembly) ->
    [
        {assembly_payload_size, maps:get(payload_size, Assembly)},
        {assembly_target_size, maps:get(target_size, Assembly)},
        {assembly_num_chunks, maps:get(num_chunks, Assembly)},
        {assembly_cut, maps:get(cut, Assembly)}
    ].
