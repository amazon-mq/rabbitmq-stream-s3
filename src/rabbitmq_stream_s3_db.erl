%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_db).
-moduledoc """
Khepri-based database operations for tracking state with strong consistency.

This plugin uses Khepri to store a minimal amount of information per-stream: we
store a mapping between the `stream_id()` and the currently active manifest
incarnation (`rabbitmq_stream_s3:uid()`) and the epoch of the writer who
created that incarnation.

Two stream writers may exist, but the deposed writer would need to be
partitioned and totally unaware of the new writer. This is possible but
expected to be rare and short-lived, so an optimistic lock is appropriate.
With Khepri this is done with the "advanced" API and an `#if_payload_version{}`
condition. We also check the writer's epoch. This can help fence off the
deposed writer so that, once the new writer has updated a manifest, the old
writer cannot make progress anymore.
""".

-include("include/rabbitmq_stream_s3.hrl").

-include_lib("kernel/include/logger.hrl").
-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit/include/rabbit_khepri.hrl").
-include_lib("rabbit_common/include/resource.hrl").

-define(PATH(StreamId), ?RABBITMQ_KHEPRI_ROOT_PATH([rabbitmq_stream_s3, StreamId])).
-define(STREAM_QUEUE_DELETION_TRIGGER_ID, rabbitmq_stream_s3_db_sq_deletion).

-doc """
Version number of a manifest object.

Zero indicates that the manifest has not been created yet.
""".
-type revision() :: khepri:payload_version() | 0.

-type entry() :: #{
    uid := rabbitmq_stream_s3:uid(),
    epoch := osiris:epoch(),
    revision := revision()
}.

-export_type([revision/0, entry/0]).

-export([setup/0]).

-export([get/1, list/0, count/0, put/5]).

-spec setup() -> ok.
setup() ->
    %% Register a stored procedure which is triggered on deletion of each
    %% stream created with `put/5`. Streams created with `put/5` are
    %% automatically deleted by keep-while conditions when a stream queue
    %% is removed from `rabbit_db_queue` and that triggers a cleanup task which
    %% deletes all objects belonging to the stream from the remote tier.
    StoredProcPath = ?RABBITMQ_KHEPRI_ROOT_PATH([
        stored_procedures,
        ?MODULE,
        ?STREAM_QUEUE_DELETION_TRIGGER_ID
    ]),
    ok = khepri:put(
        rabbit_khepri:get_store_id(),
        StoredProcPath,
        khepri_payload:sproc(fun handle_queue_deletion/1),
        #{async => true}
    ),
    EvtFilter = khepri_evf:tree(?PATH(#if_has_data{}), #{on_actions => [delete]}),
    ok = khepri:register_trigger(
        rabbit_khepri:get_store_id(),
        ?STREAM_QUEUE_DELETION_TRIGGER_ID,
        EvtFilter,
        StoredProcPath,
        #{async => true}
    ),
    ok.

%% TODO: contribute this type to Khepri?
-type sproc_props() :: #{
    on_action := [create | update | delete],
    path := khepri_path:native_path()
}.

-spec handle_queue_deletion(sproc_props()) -> ok.
handle_queue_deletion(#{path := ?PATH(StreamId)}) ->
    %% NOTE: A Khepri trigger executes its stored procedures on the current
    %% Khepri leader node. This may not be the same node as the stream's writer
    %% process. And in larger clusters (5, 7, 9 nodes, etc..) this might not
    %% be a node of a replica either.
    ok = rabbitmq_stream_s3_server:delete_stream(StreamId).

-doc "Gets the latest-known manifest root UID and revision.".
-spec get(stream_id()) -> {ok, entry()} | {error, not_found | any()}.
get(StreamId) ->
    Path = ?PATH(StreamId),
    case rabbit_khepri:adv_get(Path) of
        {ok, #{Path := #{data := {Uid, Epoch}, payload_version := Revision}}} ->
            {ok, #{uid => Uid, epoch => Epoch, revision => Revision}};
        {error, ?khepri_error(node_not_found, _Props)} ->
            {error, not_found};
        {error, _} = Err ->
            Err
    end.

-doc "Lists all streams known to the metadata store.".
-spec list() -> {ok, #{stream_id() => entry()}} | {error, any()}.
list() ->
    case rabbit_khepri:adv_get_many(?PATH(#if_has_data{})) of
        {ok, NodeProps} ->
            Entries =
                #{
                    StreamId => #{uid => Uid, epoch => Epoch, revision => Revision}
                 || ?PATH(StreamId) := #{data := {Uid, Epoch}, payload_version := Revision} <-
                        NodeProps
                },
            {ok, Entries};
        {error, _} = Err ->
            Err
    end.

-doc "Returns the count of streams known to the metadata store.".
-spec count() -> {ok, non_neg_integer()} | {error, any()}.
count() ->
    rabbit_khepri:count(?PATH(#if_has_data{})).

-doc """
Sets the UID for the given stream ID if the current revision matches the given
expected revision and the new epoch is at least as high as the old epoch.

The metadata store ensures strong consistency of the active manifest version.

This function returns the new `revision()` which can be used for future `put/5`
requests.

The epoch is checked to be greater than or equal to the prior epoch. This is
not a robust check on its own but it can prevent deposed writers from making
modifications which would inconvenience the successor writer.
""".
-spec put(
    stream_id(),
    Q :: rabbit_amqqueue:name(),
    osiris:epoch(),
    Expected :: revision(),
    rabbitmq_stream_s3:uid()
) ->
    {ok, Old :: {rabbitmq_stream_s3:uid(), osiris:epoch()} | undefined, New :: revision()}
    | {error, {conflict, entry()}}
    | {error, not_found}
    | {error, any()}.
put(
    StreamId,
    #resource{virtual_host = VHost, kind = queue, name = QName},
    Epoch,
    ExpectedRevision,
    Uid
) when is_binary(StreamId) andalso is_integer(ExpectedRevision) andalso is_binary(Uid) ->
    Path = ?PATH(StreamId),
    Conditions =
        case ExpectedRevision of
            0 ->
                [#if_node_exists{exists = false}];
            _ ->
                %% NOTE: `#if_payload_version{}` is not robust for an
                %% optimistic lock unless the `Path` is also unique for an
                %% incarnation of the resource. After a deletion the version
                %% is reset, so checking payload version is not deletion-safe.
                %% Luckily `stream_id()` is unique per incarnation of a stream,
                %% so we can safely use `#if_payload_version{}`.
                [
                    #if_payload_version{version = ExpectedRevision},
                    #if_data_matches{
                        pattern = {'_', '$1'},
                        conditions = [{'>=', Epoch, '$1'}]
                    }
                ]
        end,
    VersionedPath = khepri_path:combine_with_conditions(Path, Conditions),
    Options = #{
        %% Automatically clean up this entry if the stream queue is deleted.
        %% This triggers the stored procedure which attempts to delete the
        %% remote tier data.
        keep_while => #{?RABBITMQ_KHEPRI_QUEUE_PATH(VHost, QName) => #if_node_exists{}}
    },
    case rabbit_khepri:adv_put(VersionedPath, {Uid, Epoch}, Options) of
        {ok, #{Path := #{payload_version := NewRevision, data := {OldUid, OldEpoch}}}} ->
            {ok, {OldUid, OldEpoch}, NewRevision};
        {ok, #{Path := #{payload_version := NewRevision}}} ->
            {ok, undefined, NewRevision};
        {error,
            ?khepri_error(mismatching_node, #{
                node_props := #{
                    payload_version := ActualRevision,
                    data := {ActualUid, ActualEpoch}
                }
            })} ->
            %% This branch covers a failed expectation if the node actually
            %% exists, so `data` must be defined here.
            Entry = #{revision => ActualRevision, uid => ActualUid, epoch => ActualEpoch},
            {error, {conflict, Entry}};
        {error, ?khepri_error(node_not_found, _Props)} ->
            %% The metadata store entry might've been deleted since the last
            %% update.
            {error, not_found};
        {error, _} = Err ->
            Err
    end.
