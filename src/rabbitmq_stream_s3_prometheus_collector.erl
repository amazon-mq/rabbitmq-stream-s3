%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_prometheus_collector).
-moduledoc """
Prometheus collector for the `rabbitmq_stream_s3` plugin.

The plugin maintains two kinds of seshat counter sets:

- Per-node counters (one ref per module, no per-stream label): the
  module's identity is carried by the `module` label. These are emitted
  on every endpoint.
- Per-stream counters (one ref per replica reader, labels include
  `vhost` and `queue`): emitted as a fold-and-sum aggregate on the
  default `/metrics` endpoint and emitted with full labels on
  `/metrics/per-object`.

The two sets are distinguished in seshat output by the presence of the
`queue` label.
""".

-behaviour(prometheus_collector).

-export([
    register/0,
    deregister/0,
    deregister_cleanup/1,
    collect_mf/2
]).

-import(prometheus_model_helpers, [create_mf/4]).

-define(METRIC_NAME_PREFIX, <<"rabbitmq_stream_s3_">>).
-define(REGISTRY_PER_OBJECT, 'per-object').

%% Per-stream counter sets are detected by the presence of either of these
%% label keys. The collector folds them on the default endpoint and emits
%% them verbatim on the per-object endpoint.
-define(PER_STREAM_LABEL_KEYS, [<<"queue">>, queue]).

register() ->
    %% Default registry: emits aggregates only.
    ok = prometheus_registry:register_collector(?MODULE),
    %% Per-object registry (created by rabbitmq_prometheus's dispatcher):
    %% emits per-stream metrics with full labels in addition to the
    %% aggregates. Wrapped in `catch` because the registry may not exist
    %% in environments without the rabbitmq_prometheus plugin (e.g. some
    %% test setups). prometheus:register_collectors is idempotent.
    catch prometheus_registry:register_collector(?REGISTRY_PER_OBJECT, ?MODULE),
    ok.

deregister() ->
    prometheus_registry:deregister_collector(?MODULE),
    catch prometheus_registry:deregister_collector(?REGISTRY_PER_OBJECT, ?MODULE),
    ok.

deregister_cleanup(_) ->
    ok.

collect_mf(Registry, Callback) ->
    PerObject = (Registry =:= ?REGISTRY_PER_OBJECT),
    Seshat = seshat:format(rabbitmq_stream_s3, #{labels => as_binary}),
    emit_seshat(Seshat, PerObject, Callback),
    %% Histograms are not per-stream so they pass through unchanged.
    maps:foreach(
        fun(Name, #{type := Type, help := Help, values := Values}) ->
            Callback(
                create_mf(
                    <<(?METRIC_NAME_PREFIX)/binary, (atom_to_binary(Name))/binary>>,
                    Help,
                    Type,
                    Values
                )
            )
        end,
        rabbitmq_stream_s3_api:request_duration_prometheus_format()
    ),
    maps:foreach(
        fun(Name, #{type := Type, help := Help, values := Values}) ->
            Callback(
                create_mf(
                    <<(?METRIC_NAME_PREFIX)/binary, (atom_to_binary(Name))/binary>>,
                    Help,
                    Type,
                    Values
                )
            )
        end,
        rabbitmq_stream_s3_remote_reader:prefetch_window_prometheus_format()
    ).

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

%% seshat:format/2 returns a map keyed by metric name, where each value is
%% `#{type, help, values}`. `values` is a map of `Labels => Value` (Labels
%% is a binary like `<<"label1=\"x\",label2=\"y\"">>` when
%% `labels => as_binary` is requested).
%%
%% For each metric:
%% - On the per-object endpoint, emit per-stream values with their labels
%%   alongside any node-level entries. Both kinds pass through unchanged.
%% - On the default endpoint, behaviour depends on the metric type.
%%   Gauges fold per-stream values into a single labelless aggregate
%%   (entries without a queue label pass through unchanged). Counters
%%   drop per-stream entries entirely; the cluster-wide cumulative value
%%   appears via the node-level shadow counter (which has no queue label
%%   and is thus emitted unchanged). This preserves Prometheus
%%   monotonicity when streams are deleted: summing per-stream counters
%%   would drop on deletion and look like a counter reset.
emit_seshat(Seshat, PerObject, Callback) ->
    maps:foreach(
        fun(Name, #{type := Type, help := Help, values := Values}) ->
            Out =
                case PerObject of
                    true -> Values;
                    false -> aggregate_per_stream(Values, Type)
                end,
            Callback(
                create_mf(
                    <<(?METRIC_NAME_PREFIX)/binary, Name/binary>>,
                    Help,
                    Type,
                    Out
                )
            )
        end,
        Seshat
    ).

%% Gauge: sum per-stream entries into a labelless aggregate. Entries
%% without a queue label pass through unchanged.
%%
%% Counter: drop per-stream entries entirely. The labelless aggregate
%% comes from the node-level shadow counter (which has no queue label
%% and so passes through). Summing per-stream counters would violate
%% Prometheus monotonicity when streams are deleted.
aggregate_per_stream(Values, gauge) when is_map(Values) ->
    {PerStreamSum, PerStreamCount, KeptValues} =
        maps:fold(
            fun(Labels, Value, {Sum, Count, Kept}) ->
                case is_per_stream(Labels) of
                    true ->
                        {Sum + Value, Count + 1, Kept};
                    false ->
                        {Sum, Count, Kept#{Labels => Value}}
                end
            end,
            {0, 0, #{}},
            Values
        ),
    case PerStreamCount of
        0 ->
            KeptValues;
        _ ->
            KeptValues#{<<>> => PerStreamSum}
    end;
aggregate_per_stream(Values, counter) when is_map(Values) ->
    maps:filter(
        fun(Labels, _) -> not is_per_stream(Labels) end,
        Values
    ).

%% Labels arrive as a binary like `<<"module=\"...\",queue=\"...\",vhost=\"...\"">>`
%% when `labels => as_binary` is requested. Detect a per-stream label set
%% by the presence of `queue=`.
is_per_stream(Labels) when is_binary(Labels) ->
    binary:match(Labels, <<"queue=\"">>) =/= nomatch;
is_per_stream(_) ->
    false.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

aggregate_per_stream_gauge_drops_per_stream_labels_test() ->
    Values = #{
        <<"module=\"rabbitmq_stream_s3_replica_reader\",queue=\"q1\",vhost=\"/\"">> => 5,
        <<"module=\"rabbitmq_stream_s3_replica_reader\",queue=\"q2\",vhost=\"/\"">> => 7
    },
    ?assertEqual(#{<<>> => 12}, aggregate_per_stream(Values, gauge)).

aggregate_per_stream_gauge_keeps_aggregates_test() ->
    Values = #{<<"module=\"rabbitmq_stream_s3_governor\"">> => 42},
    ?assertEqual(
        #{<<"module=\"rabbitmq_stream_s3_governor\"">> => 42},
        aggregate_per_stream(Values, gauge)
    ).

aggregate_per_stream_gauge_mixed_test() ->
    Values = #{
        <<"module=\"rabbitmq_stream_s3_governor\"">> => 100,
        <<"module=\"rabbitmq_stream_s3_replica_reader\",queue=\"q1\",vhost=\"/\"">> => 5,
        <<"module=\"rabbitmq_stream_s3_replica_reader\",queue=\"q2\",vhost=\"/\"">> => 7
    },
    Out = aggregate_per_stream(Values, gauge),
    %% Aggregate carries empty label, kept values keep their original labels.
    ?assertEqual(2, map_size(Out)),
    ?assertEqual(100, maps:get(<<"module=\"rabbitmq_stream_s3_governor\"">>, Out)),
    ?assertEqual(12, maps:get(<<>>, Out)).

aggregate_per_stream_counter_drops_per_stream_test() ->
    %% Counters: per-stream entries are dropped entirely. The labelless
    %% aggregate comes from the node-level shadow (no queue label).
    Values = #{
        <<"module=\"rabbitmq_stream_s3_replica_reader\",queue=\"q1\",vhost=\"/\"">> => 5,
        <<"module=\"rabbitmq_stream_s3_replica_reader\",queue=\"q2\",vhost=\"/\"">> => 7
    },
    ?assertEqual(#{}, aggregate_per_stream(Values, counter)).

aggregate_per_stream_counter_keeps_node_level_test() ->
    %% Node-level shadow has no queue label and passes through unchanged.
    Values = #{<<"module=\"rabbitmq_stream_s3_replica_reader\"">> => 12},
    ?assertEqual(
        #{<<"module=\"rabbitmq_stream_s3_replica_reader\"">> => 12},
        aggregate_per_stream(Values, counter)
    ).

aggregate_per_stream_counter_mixed_test() ->
    Values = #{
        <<"module=\"rabbitmq_stream_s3_governor\"">> => 100,
        <<"module=\"rabbitmq_stream_s3_replica_reader\"">> => 12,
        <<"module=\"rabbitmq_stream_s3_replica_reader\",queue=\"q1\",vhost=\"/\"">> => 5,
        <<"module=\"rabbitmq_stream_s3_replica_reader\",queue=\"q2\",vhost=\"/\"">> => 7
    },
    Out = aggregate_per_stream(Values, counter),
    %% Per-stream entries dropped, two node-level entries pass through.
    ?assertEqual(2, map_size(Out)),
    ?assertEqual(100, maps:get(<<"module=\"rabbitmq_stream_s3_governor\"">>, Out)),
    ?assertEqual(12, maps:get(<<"module=\"rabbitmq_stream_s3_replica_reader\"">>, Out)).

is_per_stream_test() ->
    ?assert(is_per_stream(<<"queue=\"foo\"">>)),
    ?assert(is_per_stream(<<"module=\"x\",queue=\"foo\",vhost=\"/\"">>)),
    ?assertNot(is_per_stream(<<"module=\"x\"">>)),
    ?assertNot(is_per_stream(<<>>)).

-endif.
