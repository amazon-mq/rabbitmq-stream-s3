%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(db_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbit_common/include/resource.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

suite() ->
    [{ct_hooks, [rabbitmq_stream_s3_cth]}].

all() ->
    [
        create_and_get,
        update_with_correct_revision,
        conflict_on_stale_revision,
        epoch_fencing,
        list_and_list_consistent,
        keep_while_deletes_on_queue_removal,
        put_anchor_present_and_absent,
        anchor_removed_on_queue_removal
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

create_and_get(_Config) ->
    StreamId = <<"db_suite_create">>,
    Uid = rabbitmq_stream_s3:uid(),
    {ok, undefined, Rev} = rabbitmq_stream_s3_db:put(StreamId, StreamId, 1, 0, Uid),
    ?assert(is_integer(Rev)),
    ?assertMatch(
        {ok, #{uid := Uid, epoch := 1, revision := Rev}},
        rabbitmq_stream_s3_db:get(StreamId)
    ).

update_with_correct_revision(_Config) ->
    StreamId = <<"db_suite_update">>,
    Uid1 = rabbitmq_stream_s3:uid(),
    {ok, undefined, Rev1} = rabbitmq_stream_s3_db:put(StreamId, StreamId, 1, 0, Uid1),
    Uid2 = rabbitmq_stream_s3:uid(),
    {ok, {Uid1, 1}, Rev2} = rabbitmq_stream_s3_db:put(StreamId, StreamId, 1, Rev1, Uid2),
    ?assert(Rev2 > Rev1),
    ?assertMatch(
        {ok, #{uid := Uid2, revision := Rev2}},
        rabbitmq_stream_s3_db:get(StreamId)
    ).

conflict_on_stale_revision(_Config) ->
    StreamId = <<"db_suite_conflict">>,
    Uid1 = rabbitmq_stream_s3:uid(),
    {ok, undefined, Rev1} = rabbitmq_stream_s3_db:put(StreamId, StreamId, 1, 0, Uid1),
    Uid2 = rabbitmq_stream_s3:uid(),
    {ok, _, _Rev2} = rabbitmq_stream_s3_db:put(StreamId, StreamId, 1, Rev1, Uid2),
    %% Using the stale Rev1 should conflict.
    Uid3 = rabbitmq_stream_s3:uid(),
    ?assertMatch(
        {error, {conflict, #{revision := _, uid := Uid2}}},
        rabbitmq_stream_s3_db:put(StreamId, StreamId, 1, Rev1, Uid3)
    ).

epoch_fencing(_Config) ->
    StreamId = <<"db_suite_epoch">>,
    Uid1 = rabbitmq_stream_s3:uid(),
    {ok, undefined, Rev1} = rabbitmq_stream_s3_db:put(StreamId, StreamId, 2, 0, Uid1),
    %% A lower epoch should be rejected (the condition is >=, so same epoch is fine).
    Uid2 = rabbitmq_stream_s3:uid(),
    ?assertMatch(
        {error, {conflict, _}},
        rabbitmq_stream_s3_db:put(StreamId, StreamId, 1, Rev1, Uid2)
    ),
    %% Same epoch succeeds.
    Uid3 = rabbitmq_stream_s3:uid(),
    ?assertMatch(
        {ok, _, _},
        rabbitmq_stream_s3_db:put(StreamId, StreamId, 2, Rev1, Uid3)
    ).

%% list/0 (local) and list_consistent/0 (quorum) return the same committed entry
%% for every stream. list_consistent/0 lets the cross-stream GC sweep read all
%% streams in a single quorum round trip instead of one per stream.
list_and_list_consistent(_Config) ->
    Ids = [<<"db_suite_list_a">>, <<"db_suite_list_b">>, <<"db_suite_list_c">>],
    Expected = maps:from_list([
        begin
            Uid = rabbitmq_stream_s3:uid(),
            {ok, undefined, Rev} = rabbitmq_stream_s3_db:put(Id, Id, 3, 0, Uid),
            {Id, #{uid => Uid, epoch => 3, revision => Rev}}
        end
     || Id <- Ids
    ]),
    {ok, Consistent} = rabbitmq_stream_s3_db:list_consistent(),
    {ok, Local} = rabbitmq_stream_s3_db:list(),
    %% The store is shared across the suite, so assert on our streams rather than
    %% the whole map: each appears with its committed entry in both views.
    lists:foreach(
        fun(Id) ->
            ?assertEqual(maps:get(Id, Expected), maps:get(Id, Consistent)),
            ?assertEqual(maps:get(Id, Expected), maps:get(Id, Local))
        end,
        Ids
    ).

keep_while_deletes_on_queue_removal(_Config) ->
    StreamId = <<"db_suite_keep_while">>,
    QueueRef = #resource{virtual_host = <<"/">>, kind = queue, name = <<"test-queue-kw">>},
    %% Create the queue node that keep_while points to.
    ok = khepri:put(rabbitmq_metadata, rabbitmq_stream_s3_db:queue_path(QueueRef), queue_exists),
    %% Create the stream entry with keep_while referencing the queue.
    Uid = rabbitmq_stream_s3:uid(),
    {ok, undefined, _Rev} = rabbitmq_stream_s3_db:put(StreamId, QueueRef, 1, 0, Uid),
    ?assertMatch({ok, #{uid := Uid}}, rabbitmq_stream_s3_db:get(StreamId)),
    %% Delete the queue node. Khepri should automatically remove our entry.
    ok = khepri:delete(rabbitmq_metadata, rabbitmq_stream_s3_db:queue_path(QueueRef)),
    ?awaitMatch({error, not_found}, rabbitmq_stream_s3_db:get(StreamId), 1000).

put_anchor_present_and_absent(_Config) ->
    StreamId = <<"db_suite_anchor">>,
    %% No anchor yet.
    ?assertEqual({ok, false}, rabbitmq_stream_s3_db:anchor_exists_consistent(StreamId)),
    ok = rabbitmq_stream_s3_db:put_anchor(StreamId, StreamId),
    ?assertEqual({ok, true}, rabbitmq_stream_s3_db:anchor_exists_consistent(StreamId)),
    %% An unrelated stream still has no anchor.
    ?assertEqual(
        {ok, false}, rabbitmq_stream_s3_db:anchor_exists_consistent(<<"db_suite_anchor_other">>)
    ).

%% A never-committed stream (anchor written, no manifest pointer) is still cleaned
%% up when its queue is removed: the keep_while on the container removes the whole
%% subtree, anchor included. This is the window the anchor closes.
anchor_removed_on_queue_removal(_Config) ->
    StreamId = <<"db_suite_anchor_kw">>,
    QueueRef = #resource{
        virtual_host = <<"/">>, kind = queue, name = <<"test-queue-anchor-kw">>
    },
    ok = khepri:put(rabbitmq_metadata, rabbitmq_stream_s3_db:queue_path(QueueRef), queue_exists),
    ok = rabbitmq_stream_s3_db:put_anchor(StreamId, QueueRef),
    ?assertEqual({ok, true}, rabbitmq_stream_s3_db:anchor_exists_consistent(StreamId)),
    %% Never committed: the anchor exists but there is no manifest pointer.
    ?assertEqual({error, not_found}, rabbitmq_stream_s3_db:get(StreamId)),
    %% Removing the queue removes the container subtree and the anchor with it.
    ok = khepri:delete(rabbitmq_metadata, rabbitmq_stream_s3_db:queue_path(QueueRef)),
    ?awaitMatch(
        {ok, false}, rabbitmq_stream_s3_db:anchor_exists_consistent(StreamId), 1000
    ).
