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
%% Tombstone path for a deleted stream. Deliberately under a separate root from
%% ?PATH/1 and with no keep-while condition, so it survives the keep-while
%% removal of the stream entry (that removal is what fires the deletion trigger).
%% A tombstone records that a stream's remote-tier prefix holds no live data and
%% may be swept in full, the positive "stream deleted" signal that lets GC
%% reclaim objects left by an upload that completed after the deletion sweep
%% (issue #196). StreamIds embed a nanosecond timestamp, so a recreated stream
%% gets a distinct id and cannot alias an existing tombstone.
-define(TOMBSTONE_PATH(StreamId),
    ?RABBITMQ_KHEPRI_ROOT_PATH([rabbitmq_stream_s3_deleted, StreamId])
).
-define(STREAM_QUEUE_DELETION_TRIGGER_ID, rabbitmq_stream_s3_db_sq_deletion).
%% Bounds a consistent read so it cannot block indefinitely waiting for a quorum
%% that will not form (for example on a minority partition); timing out there
%% surfaces as an error, which a fail-closed caller treats as "skip".
-define(CONSISTENT_READ_TIMEOUT_MS, 30_000).

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

%% Payload of a deletion tombstone. Carries whatever is useful for diagnosis or
%% a future expiry policy; `deleted_at` is wall-clock milliseconds at deletion.
-type tombstone() :: #{deleted_at := integer()}.

-export_type([revision/0, entry/0, tombstone/0]).

-export([setup/0]).

-export([get/1, get_consistent/1, list/0, count/0, put/5, queue_path/1]).
-export([write_tombstone/1, list_tombstones/0, delete_tombstone/1]).

-define(C_SPROC_TRIGGERS, 1).
-define(C_GETS, 2).
-define(C_PUTS, 3).
-define(C_PUT_SUCCESSES, 4).
-define(C_PUT_CONFLICTS, 5).
-define(C_PUT_NOT_FOUNDS, 6).
-define(C_PUT_ERRORS, 7).
-define(COUNTERS, [
    {sproc_triggers, ?C_SPROC_TRIGGERS, counter, "Number of stored procedures triggered"},
    {gets, ?C_GETS, counter, "Total number of attempted get requests"},
    {puts, ?C_PUTS, counter, "Total number of attempted put requests"},
    {put_successes, ?C_PUT_SUCCESSES, counter, "Number of put requests which succeeded"},
    {put_conflicts, ?C_PUT_CONFLICTS, counter,
        "Number of put requests which failed because the conditions did not match"},
    {put_not_founds, ?C_PUT_NOT_FOUNDS, counter,
        "Number of put requests which failed because the tree node was not found"},
    {put_errors, ?C_PUT_ERRORS, counter,
        "Number of put requests which failed for a non-conflict and non-not-found reason"}
]).
-define(COUNTER_KEY, {?MODULE, counter}).

-spec setup() -> ok.
setup() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?COUNTER_KEY, Cnt),
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
    counters:add(counter(), ?C_SPROC_TRIGGERS, 1),
    %% NOTE: A Khepri trigger executes its stored procedures on the current
    %% Khepri leader node. This may not be the same node as the stream's writer
    %% process. And in larger clusters (5, 7, 9 nodes, etc..) this might not
    %% be a node of a replica either.
    %%
    %% The sproc runs synchronously inside the single khepri_event_handler
    %% gen_server (Khepri dispatches triggered sprocs there via a mod_call
    %% effect). A synchronous khepri:put from here would block that one handler
    %% on its own Ra command and stall all trigger processing, so the tombstone
    %% is written from the reaper's independent task process instead; see
    %% rabbitmq_stream_s3_reaper:delete_stream/1.
    ok = rabbitmq_stream_s3_reaper:delete_stream(StreamId).

-doc "Gets the latest-known manifest root UID and revision with a low-latency local read.".
-spec get(stream_id()) -> {ok, entry()} | {error, not_found | any()}.
get(StreamId) ->
    do_get(StreamId, #{}).

-doc """
Gets the latest-known manifest root UID and revision with a strongly consistent,
quorum-requiring read.

Unlike get/1, a low-latency local read that can return stale state, this fails
when the node cannot reach a quorum (for example on a minority partition).
Callers that must fail closed when this node is not the committed authority rely
on that. Bounded by a timeout so it cannot block indefinitely on a quorum that
will not form.
""".
-spec get_consistent(stream_id()) -> {ok, entry()} | {error, not_found | any()}.
get_consistent(StreamId) ->
    do_get(StreamId, #{favor => consistency, timeout => ?CONSISTENT_READ_TIMEOUT_MS}).

do_get(StreamId, Options) ->
    counters:add(counter(), ?C_GETS, 1),
    Path = ?PATH(StreamId),
    case rabbit_khepri:adv_get(Path, Options) of
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
Write a tombstone marking a stream as deleted.

Recorded by the queue-deletion trigger before the remote-tier sweep. The payload
is a map of whatever is useful for diagnosis and a possible future expiry; for
now the wall-clock deletion time. The tombstone is removed once the prefix is
confirmed empty (by the reaper after its sweep, or by GC after reclaiming a
straggler). See ?TOMBSTONE_PATH and issue #196.
""".
-spec write_tombstone(stream_id()) -> ok | {error, any()}.
write_tombstone(StreamId) ->
    Tombstone = #{deleted_at => erlang:system_time(millisecond)},
    khepri:put(rabbit_khepri:get_store_id(), ?TOMBSTONE_PATH(StreamId), Tombstone).

-doc "Lists the stream IDs that currently have a deletion tombstone.".
-spec list_tombstones() -> {ok, #{stream_id() => tombstone()}} | {error, any()}.
list_tombstones() ->
    case rabbit_khepri:adv_get_many(?TOMBSTONE_PATH(#if_has_data{})) of
        {ok, NodeProps} ->
            Tombstones =
                #{
                    StreamId => Data
                 || ?TOMBSTONE_PATH(StreamId) := #{data := Data} <- NodeProps
                },
            {ok, Tombstones};
        {error, _} = Err ->
            Err
    end.

-doc """
Remove a stream's deletion tombstone.

Called once the stream's remote-tier prefix is confirmed empty, so tombstones do
not accumulate. Idempotent: deleting an absent tombstone is a no-op.
""".
-spec delete_tombstone(stream_id()) -> ok | {error, any()}.
delete_tombstone(StreamId) ->
    khepri:delete(rabbit_khepri:get_store_id(), ?TOMBSTONE_PATH(StreamId)).

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
    Q :: rabbit_amqqueue:name() | term(),
    osiris:epoch(),
    Expected :: revision(),
    rabbitmq_stream_s3:uid()
) ->
    {ok, Old :: {rabbitmq_stream_s3:uid(), osiris:epoch()} | undefined, New :: revision()}
    | {error, {conflict, entry()}}
    | {error, not_found}
    | {error, any()}.
put(StreamId, Reference, Epoch, ExpectedRevision, Uid) when
    is_binary(StreamId) andalso is_integer(ExpectedRevision) andalso is_integer(Uid)
->
    do_put(StreamId, Epoch, ExpectedRevision, Uid, keep_while_options(Reference)).

-spec do_put(stream_id(), osiris:epoch(), revision(), rabbitmq_stream_s3:uid(), map()) ->
    {ok, {rabbitmq_stream_s3:uid(), osiris:epoch()} | undefined, revision()}
    | {error, {conflict, entry()}}
    | {error, not_found}
    | {error, any()}.
do_put(StreamId, Epoch, ExpectedRevision, Uid, Options0) ->
    Cnt = counter(),
    counters:add(Cnt, ?C_PUTS, 1),
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
    case rabbit_khepri:adv_put(VersionedPath, {Uid, Epoch}, Options0) of
        {ok, #{Path := #{payload_version := NewRevision, data := {OldUid, OldEpoch}}}} ->
            counters:add(Cnt, ?C_PUT_SUCCESSES, 1),
            {ok, {OldUid, OldEpoch}, NewRevision};
        {ok, #{Path := #{payload_version := NewRevision}}} ->
            counters:add(Cnt, ?C_PUT_SUCCESSES, 1),
            {ok, undefined, NewRevision};
        {error,
            ?khepri_error(mismatching_node, #{
                node_props := #{
                    payload_version := ActualRevision,
                    data := {ActualUid, ActualEpoch}
                }
            })} ->
            counters:add(Cnt, ?C_PUT_CONFLICTS, 1),
            %% This branch covers a failed expectation if the node actually
            %% exists, so `data` must be defined here.
            Entry = #{revision => ActualRevision, uid => ActualUid, epoch => ActualEpoch},
            {error, {conflict, Entry}};
        {error, ?khepri_error(node_not_found, _Props)} ->
            %% The metadata store entry might've been deleted since the last
            %% update.
            counters:add(Cnt, ?C_PUT_NOT_FOUNDS, 1),
            {error, not_found};
        {error, _} = Err ->
            counters:add(Cnt, ?C_PUT_ERRORS, 1),
            Err
    end.

counter() ->
    persistent_term:get({?MODULE, counter}).

-spec keep_while_options(term()) -> map().
keep_while_options(#resource{virtual_host = VHost, kind = queue, name = QName}) ->
    #{keep_while => #{?RABBITMQ_KHEPRI_QUEUE_PATH(VHost, QName) => #if_node_exists{}}};
keep_while_options(_) ->
    #{}.

-doc "Returns the Khepri path for a queue resource. Useful for test setup.".
-spec queue_path(rabbit_amqqueue:name()) -> khepri_path:native_path().
queue_path(#resource{virtual_host = VHost, kind = queue, name = QName}) ->
    ?RABBITMQ_KHEPRI_QUEUE_PATH(VHost, QName).
