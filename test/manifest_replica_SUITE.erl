%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(manifest_replica_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbitmq_stream_s3/include/rabbitmq_stream_s3.hrl").

-import(rabbitmq_stream_s3_test_helpers, [build_manifest/1]).

all() ->
    [
        unknown_stream,
        put_and_get,
        apply_append_edit,
        apply_retention_edit,
        range_updates_on_edit,
        sequenced_edits_applied_in_order,
        gap_triggers_resync
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_TC, Config) ->
    {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
    unlink(Pid),
    [{cache_pid, Pid} | Config].

end_per_testcase(_TC, Config) ->
    Pid = ?config(cache_pid, Config),
    gen_server:stop(Pid),
    Config.

%% ------------------------------------------------------------------
%% Tests
%% ------------------------------------------------------------------

unknown_stream(_Config) ->
    ?assertEqual(undefined, rabbitmq_stream_s3_manifest_replica:get_manifest(<<"unknown">>)),
    ?assertEqual(empty, rabbitmq_stream_s3_manifest_replica:get_range(<<"unknown">>)).

put_and_get(_Config) ->
    StreamId = <<"stream-1">>,
    {Manifest, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000}},
        {fragment, #{offset => 50, size => 2000}}
    ]),
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest),
    ?assertEqual(Manifest, rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId)),
    ?assertEqual({0, 51}, rabbitmq_stream_s3_manifest_replica:get_range(StreamId)).

apply_append_edit(_Config) ->
    StreamId = <<"stream-2">>,
    {Manifest0, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000}}
    ]),
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest0),

    %% Append a new fragment via edit.
    NewEntry = ?ENTRY(50, 500, 600, ?MANIFEST_KIND_FRAGMENT, 2000, 42),
    Edit = #edit{
        first_offset = 0,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = 51,
        size = 2000,
        entries = NewEntry,
        pos = byte_size(Manifest0#manifest.entries),
        len = 0
    },
    ok = rabbitmq_stream_s3_manifest_replica:apply_edit(StreamId, Edit),

    Manifest1 = rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId),
    ?assertEqual(51, Manifest1#manifest.next_offset),
    ?assertEqual(3000, Manifest1#manifest.total_size),
    ?assertEqual({0, 51}, rabbitmq_stream_s3_manifest_replica:get_range(StreamId)).

apply_retention_edit(_Config) ->
    StreamId = <<"stream-3">>,
    {Manifest0, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000, uid => 1}},
        {fragment, #{offset => 50, size => 2000, uid => 2}},
        {fragment, #{offset => 100, size => 3000, uid => 3}}
    ]),
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest0),

    %% Retention removes the first entry.
    Edit = #edit{
        first_offset = 50,
        first_timestamp = 500,
        first_last_timestamp = 510,
        next_offset = undefined,
        size = -1000,
        entries = <<>>,
        pos = 0,
        len = ?ENTRY_B
    },
    ok = rabbitmq_stream_s3_manifest_replica:apply_edit(StreamId, Edit),

    Manifest1 = rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId),
    ?assertEqual(50, Manifest1#manifest.first_offset),
    ?assertEqual(5000, Manifest1#manifest.total_size),
    ?assertEqual({50, 101}, rabbitmq_stream_s3_manifest_replica:get_range(StreamId)).

range_updates_on_edit(_Config) ->
    StreamId = <<"stream-4">>,
    {Manifest0, _} = build_manifest([
        {fragment, #{offset => 100, size => 5000}}
    ]),
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest0),
    ?assertEqual({100, 101}, rabbitmq_stream_s3_manifest_replica:get_range(StreamId)),

    %% Append another fragment.
    NewEntry = ?ENTRY(200, 2000, 2100, ?MANIFEST_KIND_FRAGMENT, 6000, 99),
    Edit = #edit{
        first_offset = 100,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = 201,
        size = 6000,
        entries = NewEntry,
        pos = byte_size(Manifest0#manifest.entries),
        len = 0
    },
    ok = rabbitmq_stream_s3_manifest_replica:apply_edit(StreamId, Edit),
    ?assertEqual({100, 201}, rabbitmq_stream_s3_manifest_replica:get_range(StreamId)).

sequenced_edits_applied_in_order(_Config) ->
    StreamId = <<"stream-seq">>,
    {Manifest0, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000}}
    ]),
    %% Sync establishes the baseline: seq=0, epoch=1.
    ok = rabbitmq_stream_s3_manifest_replica:sync(StreamId, 0, 1, Manifest0),
    ?assertEqual(Manifest0, rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId)),

    %% Edit at seq=1 is applied.
    Edit1 = #edit{
        first_offset = 0,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = 51,
        size = 2000,
        entries = ?ENTRY(50, 500, 600, ?MANIFEST_KIND_FRAGMENT, 2000, 42),
        pos = byte_size(Manifest0#manifest.entries),
        len = 0
    },
    ok = rabbitmq_stream_s3_manifest_replica:apply_edits(StreamId, [Edit1], 1, 1),
    ?assertEqual(
        51, (rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId))#manifest.next_offset
    ),

    %% Edit at seq=2 is applied.
    Edit2 = #edit{
        first_offset = 0,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = 101,
        size = 3000,
        entries = ?ENTRY(100, 1000, 1100, ?MANIFEST_KIND_FRAGMENT, 3000, 43),
        pos = byte_size(Manifest0#manifest.entries) + ?ENTRY_B,
        len = 0
    },
    ok = rabbitmq_stream_s3_manifest_replica:apply_edits(StreamId, [Edit2], 2, 1),
    ?assertEqual(
        101, (rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId))#manifest.next_offset
    ).

gap_triggers_resync(_Config) ->
    StreamId = <<"stream-gap">>,
    {Manifest0, _} = build_manifest([
        {fragment, #{offset => 0, size => 1000}}
    ]),

    %% Register a fake writer in the registry so the resync request has a target.
    rabbitmq_stream_s3_registry:init(),
    Self = self(),
    WriterPid = spawn_link(fun() -> fake_writer(Self) end),
    yes = rabbitmq_stream_s3_registry:register_name({StreamId, node()}, WriterPid),

    %% Sync establishes baseline: seq=0, epoch=1.
    ok = rabbitmq_stream_s3_manifest_replica:sync(StreamId, 0, 1, Manifest0),

    %% Send edit with seq=2 (gap: expected 1). Should trigger resync.
    Edit = #edit{
        first_offset = 0,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = 51,
        size = 2000,
        entries = ?ENTRY(50, 500, 600, ?MANIFEST_KIND_FRAGMENT, 2000, 42),
        pos = byte_size(Manifest0#manifest.entries),
        len = 0
    },
    {error, gap} = rabbitmq_stream_s3_manifest_replica:apply_edits(StreamId, [Edit], 2, 1),

    %% The edit was NOT applied (gap detected).
    ?assertEqual(Manifest0, rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId)),

    %% The fake writer received a resync request.
    receive
        {resync_received, Node} -> ?assertEqual(node(), Node)
    after 1000 ->
        ct:fail("resync request not received by writer")
    end,

    rabbitmq_stream_s3_registry:unregister_name({StreamId, node()}),
    unlink(WriterPid),
    exit(WriterPid, kill).

fake_writer(TestPid) ->
    receive
        {'$gen_cast', {resync, Node}} ->
            TestPid ! {resync_received, Node}
    end.
