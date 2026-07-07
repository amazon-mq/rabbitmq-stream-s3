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
    formatter/0,
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

%% `status/2` runs on the target node and folds in the node-level bucket
%% accessibility itself, so a single round-trip carries both the per-stream
%% status and the `bucket` entry the sections below render.
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
    Sections = [
        bucket_section(Info),
        stream_section(Info),
        remote_tier_section(Info),
        upload_pipeline_section(Info),
        assembly_section(Info)
    ],
    %% Blank line between sections, mirroring cluster_status.
    Lines = lists:append(lists:join([<<>>], Sections)),
    {ok, erlang:iolist_to_binary(lists:join($\n, Lines))};
output({error, {not_found, _}}, _Opts) ->
    {error, 'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_software(),
        <<"Stream not found, or no replica reader is running for it.">>};
output({error, Reason}, _Opts) ->
    {error, 'Elixir.RabbitMQ.CLI.Core.ExitCodes':exit_software(),
        erlang:iolist_to_binary(io_lib:format("~tp", [Reason]))}.

formatter() ->
    'Elixir.RabbitMQ.CLI.Formatters.String'.

%% ------------------------------------------------------------------
%% Human-readable sections
%% ------------------------------------------------------------------

bucket_section(Info) ->
    Bucket = maps:get(bucket, Info, undefined),
    [
        header(<<"Remote tier bucket">>),
        <<>>,
        kv(<<"Accessible">>, bucket_accessible(Bucket))
    ].

%% Render the node-level bucket accessibility into a single line: "yes",
%% "no (<reason>)", or "unknown" when it has not been determined yet.
bucket_accessible(#{status := accessible}) ->
    <<"yes">>;
bucket_accessible(#{status := inaccessible, reason := Reason}) ->
    erlang:iolist_to_binary([<<"no (">>, bucket_reason(Reason), <<")">>]);
bucket_accessible(_) ->
    <<"unknown">>.

bucket_reason(no_such_bucket) -> <<"does not exist">>;
bucket_reason(access_denied) -> <<"access denied">>;
bucket_reason(Other) -> erlang:iolist_to_binary(io_lib:format("~tp", [Other])).

stream_section(#{stream := StreamId, node := Node, log_next_offset := LogNext}) ->
    [
        header(<<"Stream">>),
        <<>>,
        kv(<<"Stream ID">>, string(StreamId)),
        kv(<<"Node">>, atom(Node)),
        kv(<<"Log next offset">>, integer(LogNext))
    ].

remote_tier_section(#{core := undefined}) ->
    [header(<<"Remote tier">>), <<>>, not_initialized()];
remote_tier_section(#{core := Core}) ->
    [
        header(<<"Remote tier">>),
        <<>>,
        kv(<<"First offset">>, integer(maps:get(manifest_first_offset, Core))),
        kv(<<"Next offset">>, integer(maps:get(manifest_next_offset, Core))),
        kv(<<"Persisted next offset">>, integer(maps:get(persisted_next_offset, Core))),
        kv(<<"Total size">>, bytes(maps:get(manifest_total_size, Core)))
    ].

upload_pipeline_section(#{core := undefined} = Info) ->
    [
        header(<<"Upload pipeline">>),
        <<>>,
        kv(<<"Transfer deadlines armed">>, integer(maps:get(transfer_deadlines_armed, Info))),
        not_initialized()
    ];
upload_pipeline_section(#{core := Core} = Info) ->
    [
        header(<<"Upload pipeline">>),
        <<>>,
        kv(<<"Transfers in flight">>, integer(maps:get(transfers_in_flight, Core))),
        kv(
            <<"Completed transfers awaiting ordering">>,
            integer(maps:get(transfers_pending_order, Core))
        ),
        kv(<<"Transfer deadlines armed">>, integer(maps:get(transfer_deadlines_armed, Info))),
        kv(<<"Edits since last persist">>, integer(maps:get(since_persist, Core))),
        kv(<<"Edits in current persist">>, integer(maps:get(in_persist_count, Core))),
        kv(<<"Last persist">>, age(maps:get(last_persist_age_ms, Core))),
        kv(<<"Persist in flight">>, bool(maps:get(persist_in_flight, Core))),
        kv(<<"Rebalance in flight">>, bool(maps:get(rebalance_in_flight, Core))),
        kv(<<"Retention in flight">>, bool(maps:get(retention_in_flight, Core))),
        kv(<<"Consumers awaiting offset">>, integer(maps:get(waiters, Core)))
    ].

assembly_section(#{assembly := undefined} = Info) ->
    [
        header(<<"Current fragment (assembly)">>),
        <<>>,
        kv(<<"Fragment target size">>, bytes(maps:get(fragment_target_size, Info))),
        not_initialized()
    ];
assembly_section(#{assembly := Assembly} = Info) ->
    [
        header(<<"Current fragment (assembly)">>),
        <<>>,
        kv(<<"Fragment target size">>, bytes(maps:get(fragment_target_size, Info))),
        kv(<<"First offset">>, integer(maps:get(first_offset, Assembly))),
        kv(<<"Next offset">>, integer(maps:get(next_offset, Assembly))),
        kv(<<"Payload size">>, bytes(maps:get(payload_size, Assembly))),
        kv(<<"On-disk size">>, bytes(maps:get(size, Assembly))),
        kv(<<"Chunks">>, integer(maps:get(num_chunks, Assembly))),
        kv(<<"Cut">>, bool(maps:get(cut, Assembly)))
    ].

%% ------------------------------------------------------------------
%% Rendering helpers
%% ------------------------------------------------------------------

header(Title) ->
    'Elixir.RabbitMQ.CLI.Core.ANSI':bright(Title).

not_initialized() ->
    <<"(not initialized)">>.

kv(Label, Value) ->
    erlang:iolist_to_binary([Label, <<": ">>, Value]).

string(Bin) when is_binary(Bin) ->
    Bin.

atom(A) when is_atom(A) ->
    erlang:atom_to_binary(A, utf8).

bool(true) -> <<"yes">>;
bool(false) -> <<"no">>.

%% An offset that has not been set yet (e.g. an empty assembly's first offset).
integer(undefined) ->
    <<"none">>;
integer(N) when is_integer(N) ->
    group_digits(N).

%% Human-readable size with the exact byte count alongside, e.g.
%% "64.00 MiB (67,108,864 bytes)".
bytes(N) when is_integer(N) ->
    erlang:iolist_to_binary(io_lib:format("~ts (~ts bytes)", [human_bytes(N), group_digits(N)])).

human_bytes(N) when N >= (1 bsl 30) ->
    io_lib:format("~.2f GiB", [N / (1 bsl 30)]);
human_bytes(N) when N >= (1 bsl 20) ->
    io_lib:format("~.2f MiB", [N / (1 bsl 20)]);
human_bytes(N) when N >= (1 bsl 10) ->
    io_lib:format("~.2f KiB", [N / (1 bsl 10)]);
human_bytes(N) ->
    io_lib:format("~ts B", [group_digits(N)]).

%% Render a duration (milliseconds) as a coarse "Xd Yh" / "Xh Ym" / "Xm Ys" /
%% "Xs ago" string. The age is computed server-side (see the reader core's
%% format_state) so it reflects the node that owns the reader, not the CLI host.
age(Ms) when is_integer(Ms), Ms < 1000 ->
    <<"just now">>;
age(Ms) when is_integer(Ms) ->
    Secs = Ms div 1000,
    erlang:iolist_to_binary([coarse_duration(Secs), <<" ago">>]).

coarse_duration(Secs) when Secs < 60 ->
    io_lib:format("~bs", [Secs]);
coarse_duration(Secs) when Secs < 3600 ->
    io_lib:format("~bm ~bs", [Secs div 60, Secs rem 60]);
coarse_duration(Secs) when Secs < 86400 ->
    io_lib:format("~bh ~bm", [Secs div 3600, (Secs rem 3600) div 60]);
coarse_duration(Secs) ->
    io_lib:format("~bd ~bh", [Secs div 86400, (Secs rem 86400) div 3600]).

%% Group an integer's digits into thousands with commas, e.g. 1421556 ->
%% "1,421,556".
group_digits(N) when is_integer(N), N < 0 ->
    [$- | group_digits(-N)];
group_digits(N) when is_integer(N) ->
    Reversed = lists:reverse(integer_to_list(N)),
    lists:reverse(insert_commas(Reversed)).

insert_commas([A, B, C, D | Rest]) ->
    [A, B, C, $, | insert_commas([D | Rest])];
insert_commas(Rest) ->
    Rest.

%% ------------------------------------------------------------------
%% JSON projection (flat map, machine-readable)
%% ------------------------------------------------------------------

flatten(
    #{
        stream := StreamId,
        node := Node,
        core := Core,
        assembly := Assembly,
        log_next_offset := LogNext
    } =
        Info
) ->
    Target = maps:get(fragment_target_size, Info),
    Base = [
        {stream, StreamId},
        {node, Node},
        {fragment_target_size, Target},
        {transfer_deadlines_armed, maps:get(transfer_deadlines_armed, Info)},
        {log_next_offset, LogNext}
    ],
    Base ++ flatten_bucket(maps:get(bucket, Info, undefined)) ++
        flatten_core(Core) ++ flatten_assembly(Assembly).

flatten_bucket(#{status := Status} = Bucket) ->
    [
        {bucket_accessible, Status =:= accessible},
        {bucket_status, Status},
        {bucket_reason, maps:get(reason, Bucket, undefined)}
    ];
flatten_bucket(_) ->
    [{bucket_status, unknown}].

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
        {in_persist_count, maps:get(in_persist_count, Core)},
        {last_persist_age_ms, maps:get(last_persist_age_ms, Core)},
        {persist_in_flight, maps:get(persist_in_flight, Core)},
        {rebalance_in_flight, maps:get(rebalance_in_flight, Core)},
        {retention_in_flight, maps:get(retention_in_flight, Core)},
        {waiters, maps:get(waiters, Core)}
    ].

flatten_assembly(undefined) ->
    [{assembly, <<"not initialized">>}];
flatten_assembly(Assembly) ->
    [
        {assembly_first_offset, maps:get(first_offset, Assembly)},
        {assembly_next_offset, maps:get(next_offset, Assembly)},
        {assembly_payload_size, maps:get(payload_size, Assembly)},
        {assembly_size, maps:get(size, Assembly)},
        {assembly_target_size, maps:get(target_size, Assembly)},
        {assembly_num_chunks, maps:get(num_chunks, Assembly)},
        {assembly_cut, maps:get(cut, Assembly)}
    ].
