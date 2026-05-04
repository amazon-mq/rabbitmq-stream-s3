%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_server_ets).
-moduledoc """
ETS-backed implementation of the `rabbitmq_stream_s3_server` behaviour.
Used in tests to control manifest and range data without starting the real server.
""".

-behaviour(rabbitmq_stream_s3_server).

-include("include/rabbitmq_stream_s3.hrl").

-export([
    setup/0,
    teardown/0,
    set_manifest/2,
    set_range/3,
    get_manifest/1,
    get_range/1
]).

-define(MANIFEST_TAB, rabbitmq_stream_s3_server_ets_manifests).
-define(RANGE_TAB, rabbitmq_stream_s3_server_ets_ranges).

-spec setup() -> ok.
setup() ->
    _ = ets:new(?MANIFEST_TAB, [named_table, public]),
    _ = ets:new(?RANGE_TAB, [named_table, public]),
    ok.

-spec teardown() -> ok.
teardown() ->
    ets:delete(?MANIFEST_TAB),
    ets:delete(?RANGE_TAB),
    ok.

-spec set_manifest(rabbitmq_stream_s3:stream_id(), #manifest{}) -> ok.
set_manifest(StreamId, Manifest) ->
    ets:insert(?MANIFEST_TAB, {StreamId, Manifest}),
    ok.

-spec set_range(rabbitmq_stream_s3:stream_id(), osiris:offset(), osiris:offset()) -> ok.
set_range(StreamId, First, Last) ->
    ets:insert(?RANGE_TAB, {StreamId, First, Last}),
    ok.

-spec get_manifest(rabbitmq_stream_s3:stream_id()) -> #manifest{} | undefined.
get_manifest(StreamId) ->
    case ets:lookup(?MANIFEST_TAB, StreamId) of
        [{StreamId, Manifest}] -> Manifest;
        [] -> undefined
    end.

-spec get_range(rabbitmq_stream_s3:stream_id()) -> rabbitmq_stream_s3:range().
get_range(StreamId) ->
    case ets:lookup(?RANGE_TAB, StreamId) of
        [{StreamId, First, Last}] -> {First, Last};
        [] -> empty
    end.
