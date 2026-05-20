%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(broker_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

all() ->
    [{group, cluster_size_3}].

groups() ->
    [
        {cluster_size_3, [shuffle], [
            publish_uploads_to_remote_tier,
            stream_deletion_cleans_remote_tier,
            consumer_reads_across_tiers,
            leadership_transfer_continues_upload,
            plugin_disable_reenable,
            prometheus_metrics
        ]}
    ].

%% -------------------------------------------------------------------
%% Setup / teardown
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    {ok, _} = application:ensure_all_started(inets),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(cluster_size_3, Config) ->
    DataDir = filename:join([?config(priv_dir, Config), "remote_tier"]),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 3},
        {rmq_nodename_suffix, ?MODULE}
    ]),
    Config2 = rabbit_ct_helpers:merge_app_env(Config1, [
        {rabbitmq_stream_s3, [
            {rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fs},
            {fragment_target_size, 1000},
            {persist_threshold, 1},
            {api_fs_data_dir, DataDir}
        ]}
    ]),
    rabbit_ct_helpers:run_setup_steps(
        Config2,
        rabbit_ct_broker_helpers:setup_steps() ++
            rabbit_ct_client_helpers:setup_steps() ++
            [fun enable_plugin/1]
    ).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
        Config,
        rabbit_ct_client_helpers:teardown_steps() ++
            rabbit_ct_broker_helpers:teardown_steps()
    ).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Tests
%% -------------------------------------------------------------------

publish_uploads_to_remote_tier(Config) ->
    QName = <<"publish_uploads">>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    stream_declare(Ch, QName),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),

    Payload = binary:copy(<<0>>, 600),
    [publish(Ch, QName, Payload) || _ <- lists:seq(1, 3)],
    true = amqp_channel:wait_for_confirms(Ch, 30),

    #{stream_id := StreamId, writer_node := WriterNode} = get_stream_info(Config, QName),
    ok = await_offset(Config, WriterNode, StreamId, 3),

    Fragments = list_fragment_keys(Config, WriterNode, StreamId),
    ?assert(length(Fragments) >= 1),

    amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok.

stream_deletion_cleans_remote_tier(Config) ->
    QName = <<"deletion_test">>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    stream_declare(Ch, QName),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),

    Payload = binary:copy(<<0>>, 600),
    [publish(Ch, QName, Payload) || _ <- lists:seq(1, 3)],
    true = amqp_channel:wait_for_confirms(Ch, 30),

    #{stream_id := StreamId, writer_node := WriterNode} = get_stream_info(Config, QName),
    ok = await_offset(Config, WriterNode, StreamId, 3),
    ?assert(list_fragment_keys(Config, WriterNode, StreamId) =/= []),

    amqp_channel:call(Ch, #'queue.delete'{queue = QName}),

    ?awaitMatch([], list_fragment_keys(Config, WriterNode, StreamId), 5000),
    ok.

consumer_reads_across_tiers(Config) ->
    QName = <<"reads_across_tiers">>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    stream_declare(Ch, QName, [{<<"x-stream-max-segment-size-bytes">>, long, 5000}]),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),

    Payload = binary:copy(<<0>>, 600),
    N = 20,
    [publish(Ch, QName, Payload) || _ <- lists:seq(1, N)],
    true = amqp_channel:wait_for_confirms(Ch, 30),

    #{stream_id := StreamId, writer_node := WriterNode} = get_stream_info(Config, QName),
    ok = await_offset(Config, WriterNode, StreamId, N),

    %% Wait for retention to reclaim the earliest segment.
    ?awaitMatch([S | _] when S > 0, list_segments(Config, WriterNode, StreamId), 5000),

    %% Subscribe from offset 0 and read all messages.
    {ok, S, C0} = stream_test_utils:connect(Config, 0),
    SubId = 1,
    {ok, C1} = stream_test_utils:subscribe(S, C0, QName, SubId, 10000, #{}, 0),
    Messages = receive_messages(S, C1, SubId, N),
    gen_tcp:close(S),

    ?assertEqual(N, length(Messages)),
    [?assertEqual(Payload, M) || M <- Messages],

    amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok.

leadership_transfer_continues_upload(Config) ->
    QName = <<"transfer_test">>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    stream_declare(Ch, QName),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),

    Payload = binary:copy(<<0>>, 600),
    [publish(Ch, QName, Payload) || _ <- lists:seq(1, 3)],
    true = amqp_channel:wait_for_confirms(Ch, 30),

    #{stream_id := StreamId, writer_node := WriterNode, epoch := Epoch1} =
        get_stream_info(Config, QName),
    ok = await_offset(Config, WriterNode, StreamId, 3),

    %% Restart the stream — writer gets a new epoch.
    {ok, Q} = rabbit_ct_broker_helpers:rpc(
        Config,
        0,
        rabbit_amqqueue,
        lookup,
        [rabbit_misc:r(<<"/">>, queue, QName)]
    ),
    {ok, _} = rabbit_ct_broker_helpers:rpc(
        Config,
        0,
        rabbit_stream_coordinator,
        restart_stream,
        [Q]
    ),

    %% restart_stream returns when the Ra command is applied, but the
    %% database record update (phase_update_mnesia) is async. Poll until
    %% the epoch advances.
    ?awaitMatch(
        #{epoch := Epoch2} when Epoch2 > Epoch1,
        get_stream_info(Config, QName),
        5000
    ),
    #{writer_node := WriterNode2, epoch := Epoch2} = get_stream_info(Config, QName),
    ?assert(Epoch2 > Epoch1),

    %% Publish more data under the new writer.
    [publish(Ch, QName, Payload) || _ <- lists:seq(1, 3)],
    true = amqp_channel:wait_for_confirms(Ch, 30),

    ok = await_offset(Config, WriterNode2, StreamId, 6),

    %% Fragments cover the full range (no gap).
    Fragments = list_fragment_keys(Config, WriterNode2, StreamId),
    ?assert(length(Fragments) >= 2),

    amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok.

plugin_disable_reenable(Config) ->
    QName = <<"disable_reenable_test">>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    stream_declare(Ch, QName),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),

    Payload = binary:copy(<<0>>, 600),
    N1 = 3,
    [publish(Ch, QName, Payload) || _ <- lists:seq(1, N1)],
    true = amqp_channel:wait_for_confirms(Ch, 30),

    #{stream_id := StreamId, writer_node := WriterNode} = get_stream_info(Config, QName),
    ok = await_offset(Config, WriterNode, StreamId, N1),

    %% Disable the plugin on all nodes.
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [
        rabbit_ct_broker_helpers:disable_plugin(Config, I, rabbitmq_stream_s3)
     || I <- lists:seq(0, length(Nodes) - 1)
    ],

    %% Replica reader is gone.
    ?awaitMatch(
        undefined,
        rabbit_ct_broker_helpers:rpc(
            Config,
            WriterNode,
            rabbitmq_stream_s3_registry,
            whereis_name,
            [{StreamId, WriterNode}]
        ),
        5000
    ),

    %% Stream still works without the plugin.
    N2 = 3,
    [publish(Ch, QName, Payload) || _ <- lists:seq(1, N2)],
    true = amqp_channel:wait_for_confirms(Ch, 30),

    %% Re-enable the plugin on all nodes.
    [
        rabbit_ct_broker_helpers:enable_plugin(Config, I, rabbitmq_stream_s3)
     || I <- lists:seq(0, length(Nodes) - 1)
    ],

    %% Discovery attaches a new replica reader.
    ?awaitMatch(
        Pid when is_pid(Pid),
        rabbit_ct_broker_helpers:rpc(
            Config,
            WriterNode,
            rabbitmq_stream_s3_registry,
            whereis_name,
            [{StreamId, WriterNode}]
        ),
        5000
    ),

    %% All data (including what was published while disabled) gets uploaded.
    Total = N1 + N2,
    ok = await_offset(Config, WriterNode, StreamId, Total),

    %% Remote + local tiers cover the full stream range.
    Fragments = list_fragment_keys(Config, WriterNode, StreamId),
    ?assert(length(Fragments) >= 1),

    amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok.

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

prometheus_metrics(Config) ->
    QName = <<"prometheus_test_stream">>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    stream_declare(Ch, QName),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),

    Payload = binary:copy(<<0>>, 600),
    [publish(Ch, QName, Payload) || _ <- lists:seq(1, 3)],
    true = amqp_channel:wait_for_confirms(Ch, 30),

    #{stream_id := StreamId, writer_node := WriterNode} = get_stream_info(Config, QName),
    ok = await_offset(Config, WriterNode, StreamId, 3),

    %% Scrape the default /metrics endpoint. Per-stream gauges fold into
    %% labelless aggregates here. Per-stream counters are dropped from
    %% this view; their aggregate value comes from the node-level shadow
    %% counter (which has a `module=` label but no queue/vhost label).
    Default = scrape(Config, WriterNode, "/metrics"),

    %% Aggregate gauges from the replica reader appear with no labels
    %% (the labelless fold of per-stream values).
    AggregateGauges = [
        <<"rabbitmq_stream_s3_manifest_next_offset">>,
        <<"rabbitmq_stream_s3_remote_bytes">>,
        <<"rabbitmq_stream_s3_remote_messages">>
    ],
    [
        ?assertMatch(
            match,
            re:run(Default, <<"^", M/binary, " ">>, [{capture, none}, multiline]),
            #{metric => M}
        )
     || M <- AggregateGauges
    ],

    %% Per-stream pipeline gauges are also folded into labelless aggregates.
    PipelineGauges = [
        <<"rabbitmq_stream_s3_bytes_in_assembly">>,
        <<"rabbitmq_stream_s3_bytes_in_transfer">>,
        <<"rabbitmq_stream_s3_bytes_in_persist">>
    ],
    [
        ?assertMatch(
            match,
            re:run(Default, <<"^", M/binary, " ">>, [{capture, none}, multiline]),
            #{metric => M}
        )
     || M <- PipelineGauges
    ],

    %% Per-stream counters appear via their node-level shadow with the
    %% `module=` label (no queue label). These must remain monotonic
    %% across stream lifecycle changes, which is why we don't sum
    %% per-stream counter values into a labelless aggregate.
    ShadowCounters = [
        <<"rabbitmq_stream_s3_transfers_completed">>,
        <<"rabbitmq_stream_s3_bytes_transferred">>,
        <<"rabbitmq_stream_s3_persists_completed">>,
        <<"rabbitmq_stream_s3_roots_created">>,
        <<"rabbitmq_stream_s3_manifests_resolved_empty">>,
        <<"rabbitmq_stream_s3_bytes_drained_total">>,
        <<"rabbitmq_stream_s3_bytes_persisted_total">>
    ],
    [
        ?assertMatch(
            match,
            re:run(
                Default,
                <<"^", M/binary, "\\{module=\"rabbitmq_stream_s3_replica_reader\"\\} ">>,
                [{capture, none}, multiline]
            ),
            #{metric => M}
        )
     || M <- ShadowCounters
    ],

    %% Per-stream counter values must NOT appear on the default endpoint
    %% (queue= label). The shadow counter has only a module label.
    [
        ?assertMatch(
            nomatch,
            re:run(
                Default,
                <<"^", M/binary, "\\{[^}]*queue=">>,
                [{capture, none}, multiline]
            ),
            #{metric => M}
        )
     || M <- ShadowCounters
    ],

    %% Per-node-only counters from other modules pass through unchanged.
    NodeOnlyMetrics = [
        <<"rabbitmq_stream_s3_governor_submissions_received">>,
        <<"rabbitmq_stream_s3_objects_deleted">>,
        <<"rabbitmq_stream_s3_put">>,
        <<"rabbitmq_stream_s3_request_duration_seconds_bucket">>
    ],
    [
        ?assertMatch(
            match,
            re:run(Default, <<"^", M/binary, "[ {]">>, [{capture, none}, multiline]),
            #{metric => M}
        )
     || M <- NodeOnlyMetrics
    ],

    %% Scrape the per-object endpoint. This emits per-stream metrics with
    %% queue= and vhost= labels alongside any node-level entries.
    PerObject = scrape(Config, WriterNode, "/metrics/per-object"),
    [
        ?assertMatch(
            match,
            re:run(
                PerObject,
                <<"^", M/binary, "\\{[^}]*queue=\"prometheus_test_stream\"">>,
                [{capture, none}, multiline]
            ),
            #{metric => M}
        )
     || M <- [
            <<"rabbitmq_stream_s3_transfers_completed">>,
            <<"rabbitmq_stream_s3_bytes_drained_total">>,
            <<"rabbitmq_stream_s3_bytes_persisted_total">>,
            <<"rabbitmq_stream_s3_bytes_in_assembly">>,
            <<"rabbitmq_stream_s3_bytes_in_transfer">>,
            <<"rabbitmq_stream_s3_bytes_in_persist">>
        ]
    ],

    amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

scrape(Config, Node, Path) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, Node, tcp_port_prometheus),
    URI = lists:flatten(io_lib:format("http://localhost:~b~ts", [Port, Path])),
    {ok, {{_, 200, _}, _, Body}} = httpc:request(get, {URI, []}, [], []),
    iolist_to_binary(Body).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

enable_plugin(Config) ->
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [
        rabbit_ct_broker_helpers:enable_plugin(Config, N, rabbitmq_stream_s3)
     || N <- lists:seq(0, length(Nodes) - 1)
    ],
    Config.

stream_declare(Ch, QName) ->
    stream_declare(Ch, QName, []).

stream_declare(Ch, QName, ExtraArgs) ->
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{
        queue = QName,
        durable = true,
        arguments = [{<<"x-queue-type">>, longstr, <<"stream">>} | ExtraArgs]
    }).

publish(Ch, QName, Payload) ->
    ok = amqp_channel:call(
        Ch,
        #'basic.publish'{routing_key = QName},
        #amqp_msg{payload = Payload}
    ).

get_stream_info(Config, QName) ->
    RName = rabbit_misc:r(<<"/">>, queue, QName),
    {ok, Q} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [RName]),
    TypeState = amqqueue:get_type_state(Q),
    #{
        stream_id => list_to_binary(maps:get(name, TypeState)),
        writer_node => maps:get(leader_node, TypeState),
        epoch => maps:get(epoch, TypeState)
    }.

await_offset(Config, WriterNode, StreamId, Offset) ->
    ct:pal("await_offset: node=~p stream=~ts offset=~b", [WriterNode, StreamId, Offset]),
    Result = rabbit_ct_broker_helpers:rpc(
        Config, WriterNode, ?MODULE, do_await_offset, [StreamId, WriterNode, Offset]
    ),
    case Result of
        ok ->
            ct:pal("await_offset: ok"),
            ok;
        {timeout, State} ->
            ct:pal("await_offset TIMEOUT: replica_reader state=~p", [State]),
            error({await_offset_timeout, #{
                stream => StreamId, node => WriterNode,
                offset => Offset, state => State
            }})
    end.

%% Exported for RPC from await_offset/4.
do_await_offset(StreamId, WriterNode, Offset) ->
    try
        gen_server:call(
            {via, rabbitmq_stream_s3_registry, {StreamId, WriterNode}},
            {await_offset, Offset},
            10000
        )
    catch
        exit:{timeout, _} ->
            State = try
                sys:get_state(
                    {via, rabbitmq_stream_s3_registry, {StreamId, WriterNode}},
                    5000
                )
            catch _:_ -> unavailable
            end,
            {timeout, State}
    end.

list_fragment_keys(Config, Node, StreamId) ->
    Prefix = rabbitmq_stream_s3:stream_prefix(StreamId),
    {ok, Keys, _} = rabbit_ct_broker_helpers:rpc(
        Config, Node, rabbitmq_stream_s3_api, list, [Prefix]
    ),
    [K || K <- Keys, binary:match(K, <<".fragment">>) =/= nomatch].

list_segments(Config, Node, StreamId) ->
    rabbit_ct_broker_helpers:rpc(
        Config,
        Node,
        rabbitmq_stream_s3_test_helpers,
        list_segment_offsets_local,
        [StreamId]
    ).

receive_messages(S, C0, SubId, N) ->
    receive_messages(S, C0, SubId, N, []).

%% Local recv loop, intentionally not `stream_test_utils:receive_stream_commands/3`.
%% That helper loops up to 10 times calling gen_tcp:recv/3, accumulating bytes
%% into a rabbit_stream_core state, and returns the atom `empty` when no full
%% frame has assembled within the loop. The bug: on `empty`, the helper
%% discards the updated core state, so a caller that retries with the original
%% state re-reads from an empty buffer and loses the bytes already received.
%% For chunks that span several recv() calls (large payloads, slow consumers)
%% the frame can never be assembled. Until that is fixed upstream we drive the
%% recv loop ourselves.
receive_messages(_S, _C, _SubId, 0, Acc) ->
    lists:reverse(Acc);
receive_messages(S, C0, SubId, Remaining, Acc) ->
    case rabbit_stream_core:next_command(C0) of
        {{deliver, SubId, Chunk}, C1} ->
            Payloads = extract_payloads(Chunk),
            receive_messages(S, C1, SubId, Remaining - length(Payloads), Acc ++ Payloads);
        {{deliver_v2, SubId, _CommittedOffset, Chunk}, C1} ->
            Payloads = extract_payloads(Chunk),
            receive_messages(S, C1, SubId, Remaining - length(Payloads), Acc ++ Payloads);
        empty ->
            case gen_tcp:recv(S, 0, 5000) of
                {ok, Data} ->
                    C1 = rabbit_stream_core:incoming_data(Data, C0),
                    receive_messages(S, C1, SubId, Remaining, Acc);
                {error, Err} ->
                    ct:fail("recv error: ~p, got ~b of ~b messages", [
                        Err, length(Acc), length(Acc) + Remaining
                    ])
            end;
        {_Other, C1} ->
            receive_messages(S, C1, SubId, Remaining, Acc)
    end.

extract_payloads(Chunk) ->
    <<_:4, _:4, _:8, _:16, NumRecords:32, _:64, _:64, _:64, _:32, _DataLength:32, _TrailerLength:32,
        BloomSize:16, _:16, _Bloom:BloomSize/binary, Data/binary>> = Chunk,
    extract_records(Data, NumRecords, []).

extract_records(_Data, 0, Acc) ->
    lists:reverse(Acc);
extract_records(<<0:1, Size:31, Entry:Size/binary, Rest/binary>>, N, Acc) ->
    Sections = amqp10_framing:decode_bin(Entry),
    #'v1_0.data'{content = Payload} = lists:keyfind('v1_0.data', 1, Sections),
    extract_records(Rest, N - 1, [Payload | Acc]).
