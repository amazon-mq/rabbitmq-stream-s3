%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_db).
-moduledoc """
Khepri-based database operations for tracking state with strong consistency.

This plugin uses Khepri to store a minimal amount of information per-stream. Each
stream owns a small subtree under `[rabbitmq_stream_s3, StreamId]`:

  * the per-stream node itself is a `container`, kept alive by a keep_while
    condition on the stream queue. Its deletion (when the queue is removed) fires
    the cleanup trigger and, because it is the subtree root, removes the children
    below it.
  * the `manifest` child maps to the currently active manifest incarnation
    (`rabbitmq_stream_s3:uid()`) and the epoch of the writer who created that
    incarnation. This is the optimistically-locked pointer.
  * the `anchor` child (written by `put_anchor/2` before the first remote-tier
    fragment) records, by its mere presence, that the stream's S3 prefix belongs
    to a live stream. Its absence is the by-construction signal that a prefix is
    junk. See `rabbitmq_stream_s3_gc`.

Keeping the manifest pointer in a child rather than on the container node lets the
anchor be written first (it only needs the container to exist) without disturbing
the manifest pointer's optimistic-lock conditions.

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

%% The per-stream container node. Kept alive by a keep_while on the stream queue;
%% its deletion fires the cleanup trigger and removes the subtree below it.
-define(PATH(StreamId), ?RABBITMQ_KHEPRI_ROOT_PATH([rabbitmq_stream_s3, StreamId])).
%% The optimistically-locked manifest pointer ({uid, epoch}).
-define(MANIFEST_PATH(StreamId),
    ?RABBITMQ_KHEPRI_ROOT_PATH([rabbitmq_stream_s3, StreamId, manifest])
).
%% Presence marks the stream's S3 prefix as live; absence marks it as junk.
-define(ANCHOR_PATH(StreamId), ?RABBITMQ_KHEPRI_ROOT_PATH([rabbitmq_stream_s3, StreamId, anchor])).
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

-export_type([revision/0, entry/0]).

-export([setup/0]).

-export([get/1, get_consistent/1, list/0, list_consistent/0, count/0, put/5, queue_path/1]).
-export([put_anchor/2, anchor_exists_consistent/1]).

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
    %% per-stream container node. The container is kept alive by a keep_while
    %% condition on the stream queue, so when the queue is removed from
    %% `rabbit_db_queue` the container (and its subtree) is automatically
    %% deleted, which triggers a cleanup task that deletes all objects belonging
    %% to the stream from the remote tier. The container is created by
    %% `put_anchor/2` before the first fragment, or by `put/5` at the first
    %% manifest commit, whichever comes first.
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
    Path = ?MANIFEST_PATH(StreamId),
    case rabbit_khepri:adv_get(Path, Options) of
        {ok, #{Path := #{data := {Uid, Epoch}, payload_version := Revision}}} ->
            {ok, #{uid => Uid, epoch => Epoch, revision => Revision}};
        {error, ?khepri_error(node_not_found, _Props)} ->
            {error, not_found};
        {error, _} = Err ->
            Err
    end.

-doc "Lists all streams known to the metadata store with a low-latency local read.".
-spec list() -> {ok, #{stream_id() => entry()}} | {error, any()}.
list() ->
    do_list(#{}).

-doc """
Lists all streams known to the metadata store with a consistent read.

A consistent read ensures that the query either reflects the latest possible
information across the cluster, or fails.
""".
-spec list_consistent() -> {ok, #{stream_id() => entry()}} | {error, any()}.
list_consistent() ->
    do_list(#{favor => consistency, timeout => ?CONSISTENT_READ_TIMEOUT_MS}).

do_list(Options) ->
    case rabbit_khepri:adv_get_many(?MANIFEST_PATH(?KHEPRI_WILDCARD_STAR), Options) of
        {ok, NodeProps} ->
            Entries =
                #{
                    StreamId => #{uid => Uid, epoch => Epoch, revision => Revision}
                 || ?MANIFEST_PATH(StreamId) := #{data := {Uid, Epoch}, payload_version := Revision} <-
                        NodeProps
                },
            {ok, Entries};
        {error, _} = Err ->
            Err
    end.

-doc "Returns the count of streams known to the metadata store.".
-spec count() -> {ok, non_neg_integer()} | {error, any()}.
count() ->
    rabbit_khepri:count(?MANIFEST_PATH(?KHEPRI_WILDCARD_STAR)).

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
    do_put(StreamId, Reference, Epoch, ExpectedRevision, Uid).

-spec do_put(stream_id(), term(), osiris:epoch(), revision(), rabbitmq_stream_s3:uid()) ->
    {ok, {rabbitmq_stream_s3:uid(), osiris:epoch()} | undefined, revision()}
    | {error, {conflict, entry()}}
    | {error, not_found}
    | {error, any()}.
do_put(StreamId, Reference, Epoch, ExpectedRevision, Uid) ->
    Cnt = counter(),
    counters:add(Cnt, ?C_PUTS, 1),
    Path = ?MANIFEST_PATH(StreamId),
    %% Ensure the per-stream container exists with the keep_while that drives
    %% cleanup before creating the manifest child under it. The manifest child
    %% itself needs no keep_while: deleting the container removes the subtree.
    case ensure_container(StreamId, ExpectedRevision, Reference) of
        ok ->
            do_put_manifest(Cnt, Path, Epoch, ExpectedRevision, Uid);
        {error, _} = Err ->
            counters:add(Cnt, ?C_PUT_ERRORS, 1),
            Err
    end.

%% Create the per-stream container with its keep_while when the manifest is first
%% created (ExpectedRevision == 0); later commits find it already present. This is
%% idempotent with put_anchor/2, which creates the same container before the first
%% fragment. When the reference is not a queue resource (e.g. some tests) there is
%% no keep_while, which is fine: those streams are not cleaned up by queue removal.
-spec ensure_container(stream_id(), revision(), term()) -> ok | {error, any()}.
ensure_container(StreamId, 0, Reference) ->
    rabbit_khepri:put(?PATH(StreamId), StreamId, keep_while_options(Reference));
ensure_container(_StreamId, _ExpectedRevision, _Reference) ->
    ok.

do_put_manifest(Cnt, Path, Epoch, ExpectedRevision, Uid) ->
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
    %% The container (not this manifest child) carries the keep_while, so no
    %% keep_while option is needed here.
    case rabbit_khepri:adv_put(VersionedPath, {Uid, Epoch}, #{}) of
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

-doc """
Writes the per-stream anchor before the first remote-tier fragment is uploaded.

Creates the per-stream container (kept alive by a keep_while on the stream queue)
and the `anchor` child under it. The anchor's presence is the by-construction
signal that the stream's S3 prefix belongs to a live stream; its absence (the
keep_while removes it atomically with the queue) marks the prefix as junk. The
caller must ensure this commits before the first fragment PUT, so that no object
can exist under a prefix whose anchor is absent.
""".
-spec put_anchor(stream_id(), term()) -> ok | {error, any()}.
put_anchor(StreamId, Reference) when is_binary(StreamId) ->
    KeepWhile = keep_while_options(Reference),
    case rabbit_khepri:put(?PATH(StreamId), StreamId, KeepWhile) of
        ok ->
            rabbit_khepri:put(?ANCHOR_PATH(StreamId), StreamId, KeepWhile);
        {error, _} = Err ->
            Err
    end.

-doc """
Returns whether the stream's anchor exists, using a strongly consistent,
quorum-requiring read.

A consistent read is required: a stale local read can report the anchor absent
for a stream whose anchor has just committed while its first fragment is already
in S3, which would let a sweep reap live data. Fails when this node cannot reach a
quorum, which a fail-closed caller treats as "do not reap".
""".
-spec anchor_exists_consistent(stream_id()) -> {ok, boolean()} | {error, any()}.
anchor_exists_consistent(StreamId) when is_binary(StreamId) ->
    Path = ?ANCHOR_PATH(StreamId),
    Options = #{favor => consistency, timeout => ?CONSISTENT_READ_TIMEOUT_MS},
    case rabbit_khepri:adv_get(Path, Options) of
        {ok, #{Path := _NodeProps}} ->
            {ok, true};
        {error, ?khepri_error(node_not_found, _Props)} ->
            {ok, false};
        {error, _} = Err ->
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
