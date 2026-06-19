%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_hooks).
-moduledoc """
Implementation of `osiris_log_hooks` for the tiered storage plugin.

This module is set as the `log_hooks` application env for osiris on
plugin start. It receives callbacks at writer/acceptor init and when
retention is updated.
""".

-behaviour(osiris_log_hooks).

-include("include/rabbitmq_stream_s3.hrl").
-include("include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-export([
    on_init/3,
    on_retention_updated/2,
    on_retention_evaluated/2,
    local_retention_fun/1,
    discover/0
]).

-doc """
Called early in `osiris_log:init/2` before the config is consumed.

For writers: spawns the remote replica reader.
For acceptors: registers with the writer's remote replica reader for
manifest broadcast.
Both: appends the local retention function.
""".
-spec on_init(writer | acceptor, pid(), osiris_log:config()) -> osiris_log:config().
on_init(writer, Pid, #{name := Name, dir := Dir, shared := Shared, counter := Counter} = Config) ->
    StreamId = rabbitmq_stream_s3:ensure_stream_id(Name),
    RemoteConfig = maps:get(remote_config, Config, #{}),
    Reference = maps:get(reference, Config, undefined),
    Epoch = maps:get(epoch, Config, 0),
    %% Pass user retention specs (max_bytes, max_age) for remote tier evaluation.
    %% Filter out the {'fun', ...} specs we add — those are for local retention only.
    UserRetention = [S || S <- maps:get(retention, Config, []), element(1, S) =/= 'fun'],
    StartResult = rabbitmq_stream_s3_replica_reader_sup:start_child(
        RemoteConfig#{
            stream => StreamId,
            writer_pid => Pid,
            dir => iolist_to_binary(Dir),
            shared => Shared,
            counter => Counter,
            reference => Reference,
            epoch => Epoch,
            retention => UserRetention
        }
    ),
    case StartResult of
        {ok, _} ->
            ok;
        %% A replica reader may already exist for this stream on this node:
        %% discover/0 ran first at plugin (re)enable, or a prior incarnation
        %% has not finished terminating. The reader is registered by
        %% {StreamId, node()}, not by writer pid, so start_child returns
        %% {already_started, _} on that race. Tolerate it instead of letting a
        %% badmatch crash osiris_log:init/2 and take down the writer's log init
        %% on the discovery/restart race. Mirrors the discovery path in
        %% attach_writer/1.
        {error, {already_started, _}} ->
            ok;
        %% A genuine failure to start the tiering reader must not crash the
        %% writer's log init: the stream still functions on local disk. Surface
        %% it rather than swallowing it. (A genuine start failure still leaves
        %% the stream un-tiered; surfacing that more loudly is a separate,
        %% larger change.)
        {error, Reason} ->
            ?LOG_WARNING(
                "Failed to start remote replica reader for stream ~ts: ~p",
                [StreamId, Reason],
                #{domain => ?RMQLOG_DOMAIN_STREAM_S3}
            )
    end,
    append_retention(StreamId, Config);
on_init(acceptor, _Pid, #{name := Name, leader_pid := LeaderPid, counter := Counter} = Config) ->
    StreamId = rabbitmq_stream_s3:ensure_stream_id(Name),
    WriterNode = node(LeaderPid),
    gen_server:cast(
        {via, rabbitmq_stream_s3_registry, {StreamId, WriterNode}},
        {register_acceptor, node()}
    ),
    Shared = maps:get(shared, Config),
    Dir = maps:get(dir, Config),
    rabbitmq_stream_s3_manifest_replica:register_replica_context(StreamId, Dir, Shared, Counter),
    append_retention(StreamId, Config);
on_init(acceptor, _Pid, #{name := Name, counter := Counter} = Config) ->
    StreamId = rabbitmq_stream_s3:ensure_stream_id(Name),
    Shared = maps:get(shared, Config),
    Dir = maps:get(dir, Config),
    rabbitmq_stream_s3_manifest_replica:register_replica_context(StreamId, Dir, Shared, Counter),
    append_retention(StreamId, Config).

-doc """
Called when retention is updated on a running stream.

Re-appends the local retention function and notifies the remote replica
reader of the new user spec (if one exists locally).
""".
-spec on_retention_updated([osiris:retention_spec()], map()) -> [osiris:retention_spec()].
on_retention_updated(Retention, #{name := Name}) ->
    StreamId = rabbitmq_stream_s3:ensure_stream_id(Name),
    case rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}) of
        undefined -> ok;
        Pid -> gen_server:cast(Pid, {retention_updated, Retention})
    end,
    [{'fun', local_retention_fun(StreamId)} | Retention].

-doc """
Called after retention evaluation sets the first_offset counter.

Overrides `?C_FIRST_OFFSET` with the manifest's first offset when the
remote tier holds older data than the local tier. Without this, the
management UI reports only the local segment window as the message count.
""".
-spec on_retention_evaluated(counters:counters_ref(), map()) -> ok.
on_retention_evaluated(Cnt, #{name := Name}) ->
    StreamId = rabbitmq_stream_s3:ensure_stream_id(Name),
    case rabbitmq_stream_s3_manifest_replica:get_range(StreamId) of
        {RemoteFirst, _} ->
            LocalFirst = counters:get(Cnt, ?C_OSIRIS_LOG_FIRST_OFFSET),
            counters:put(Cnt, ?C_OSIRIS_LOG_FIRST_OFFSET, min(LocalFirst, RemoteFirst));
        _ ->
            ok
    end.

-doc """
Discover existing osiris writers and replicas on this node and attach
the plugin to them. Called on plugin start to handle streams that were
already running before the plugin was enabled (or re-enabled).
""".
-spec discover() -> ok.
discover() ->
    Children =
        try
            supervisor:which_children(osiris_server_sup)
        catch
            exit:{noproc, _} -> []
        end,
    lists:foreach(fun discover_child/1, Children).

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

append_retention(StreamId, Config) ->
    Fun = {'fun', local_retention_fun(StreamId)},
    maps:update_with(retention, fun(R) -> [Fun | R] end, [Fun], Config).

discover_child({_Id, Pid, worker, [osiris_writer]}) when is_pid(Pid) ->
    try
        attach_writer(Pid)
    catch
        Class:Reason ->
            ?LOG_WARNING(
                "Failed to attach tiering to discovered writer ~p: ~ts:~p. "
                "This stream will not tier until its writer restarts",
                [Pid, Class, Reason],
                #{domain => ?RMQLOG_DOMAIN_STREAM_S3}
            )
    end;
discover_child({_Id, Pid, worker, [osiris_replica]}) when is_pid(Pid) ->
    try
        attach_replica(Pid)
    catch
        Class:Reason ->
            ?LOG_WARNING(
                "Failed to attach tiering to discovered replica ~p: ~ts:~p",
                [Pid, Class, Reason],
                #{domain => ?RMQLOG_DOMAIN_STREAM_S3}
            )
    end;
discover_child(_) ->
    ok.

attach_writer(Pid) ->
    #{name := Name, dir := Dir, shared := Shared, reference := Reference} =
        osiris_util:get_reader_context(Pid),
    StreamId = rabbitmq_stream_s3:ensure_stream_id(Name),
    %% Seshat registers writer counters under {osiris_writer, Reference}.
    Counter = osiris_counters:fetch({osiris_writer, Reference}),
    #{epoch := Epoch} = osiris_counters:overview({osiris_writer, Reference}),
    %% Start the replica reader with no remote retention: its authoritative
    %% value is set just below by the update_retention/2 call, whose
    %% on_retention_updated/2 hook casts the preserved policy to the reader.
    Config = #{
        stream => StreamId,
        writer_pid => Pid,
        dir => iolist_to_binary(Dir),
        shared => Shared,
        counter => Counter,
        reference => Reference,
        epoch => Epoch,
        retention => []
    },
    case rabbitmq_stream_s3_replica_reader_sup:start_child(Config) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            ?LOG_WARNING(
                "Failed to attach tiering reader to discovered writer for "
                "stream ~ts: ~p. This stream will not tier until its writer "
                "restarts",
                [StreamId, Reason],
                #{domain => ?RMQLOG_DOMAIN_STREAM_S3}
            )
    end,
    %% Inject the local-tier retention fun into the running writer while
    %% preserving its configured max-bytes/max-age policy. Updating via a
    %% transform reads the writer's *current* spec, so discovery neither
    %% re-reads the policy from the queue record (which can fail transiently and
    %% would then wipe the live policy) nor clobbers it. osiris runs
    %% on_retention_updated/2 on the result, which re-adds exactly one fun and
    %% casts the preserved policy to the replica reader started above.
    osiris:update_retention(Pid, retention_transform()).

attach_replica(Pid) ->
    #{name := Name, dir := Dir, shared := Shared, reference := Reference} =
        osiris_util:get_reader_context(Pid),
    StreamId = rabbitmq_stream_s3:ensure_stream_id(Name),
    %% Seshat registers replica counters under {osiris_replica, Reference}.
    Counter = osiris_counters:fetch({osiris_replica, Reference}),
    rabbitmq_stream_s3_manifest_replica:register_replica_context(StreamId, Dir, Shared, Counter),
    %% Register with the writer's replica reader for manifest broadcast.
    %% If the writer's replica reader isn't up yet, the cast is dropped;
    %% the replica reader will proactively sync us on its startup.
    case rabbit_amqqueue:lookup(Reference) of
        {ok, Q} ->
            WriterNode = maps:get(leader_node, amqqueue:get_type_state(Q)),
            gen_server:cast(
                {via, rabbitmq_stream_s3_registry, {StreamId, WriterNode}},
                {register_acceptor, node()}
            );
        _ ->
            ok
    end,
    %% Inject the local-tier retention fun into the running replica while
    %% preserving its configured policy, via the same current-spec transform
    %% as attach_writer/1.
    osiris:update_retention(Pid, retention_transform()).

%% Transform passed to osiris:update_retention/2 on discovery re-attach. It
%% drops any local-tier retention fun from the stream's current spec and keeps
%% the user's max-bytes/max-age policy. osiris then runs on_retention_updated/2
%% on the result, which re-adds exactly one fun (so funs do not accumulate
%% across repeated discovery) and casts the preserved policy to the replica
%% reader. Reading the policy from the stream's own current spec is why
%% discovery no longer depends on a queue-record lookup: a transient lookup
%% failure used to be collapsed to [], which silently wiped the configured
%% policy and stopped local-tier offload (filling local disk).
-spec retention_transform() -> fun(([osiris:retention_spec()]) -> [osiris:retention_spec()]).
retention_transform() ->
    fun(CurrentSpec) ->
        [Spec || Spec <- CurrentSpec, element(1, Spec) =/= 'fun']
    end.

local_retention_fun(StreamId) ->
    fun(IdxFiles) ->
        case rabbitmq_stream_s3_manifest_replica:get_range(StreamId) of
            {_FirstOffset, NextOffset} ->
                eval_local_retention(IdxFiles, NextOffset);
            empty ->
                {[], IdxFiles}
        end
    end.

-spec eval_local_retention(IdxFiles :: [filename()], osiris:offset()) ->
    {ToDelete :: [filename()], ToKeep :: [filename(), ...]}.
eval_local_retention(IdxFiles, NextTieredOffset) ->
    %% Always keep the current active segment no matter what the last tiered
    %% offset is.
    eval_local_retention(lists:reverse(IdxFiles), NextTieredOffset, [], []).

eval_local_retention([], _NextTieredOffset, ToDelete, ToKeep) ->
    %% Always keep the current active segment no matter what the last tiered
    %% offset is.
    {lists:reverse(ToDelete), ToKeep};
eval_local_retention([IdxFile | Rest], NextTieredOffset, ToDelete, ToKeep) ->
    Offset = rabbitmq_stream_s3:index_file_offset(IdxFile),
    %% NOTE: if `Offset =:= NextTieredOffset`, then the segment file before
    %% this was fully uploaded since `NextTieredOffset` is the last offset to
    %% be successfully uploaded, plus one.
    case Offset > NextTieredOffset of
        true ->
            eval_local_retention(Rest, NextTieredOffset, ToDelete, [IdxFile | ToKeep]);
        false ->
            {lists:reverse(Rest), [IdxFile | ToKeep]}
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

eval_local_retention_test() ->
    Idxs = fun(Offsets) ->
        [rabbitmq_stream_s3:offset_filename(O, <<"index">>) || O <- Offsets]
    end,
    IdxFiles = Idxs([0, 100, 200, 300, 400]),
    ?assertEqual(
        {Idxs([0, 100]), Idxs([200, 300, 400])},
        eval_local_retention(IdxFiles, 251)
    ),
    %% Always keep the current segment:
    ?assertEqual(
        {Idxs([0, 100, 200, 300]), Idxs([400])},
        eval_local_retention(IdxFiles, 451)
    ),
    ?assertEqual(
        {Idxs([0, 100, 200]), Idxs([300, 400])},
        eval_local_retention(IdxFiles, 301)
    ),
    ?assertEqual(
        {[], IdxFiles},
        eval_local_retention(IdxFiles, 0)
    ),
    ok.

-endif.
