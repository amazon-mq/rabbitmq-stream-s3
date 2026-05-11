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

-export([
    on_init/3,
    on_retention_updated/2,
    local_retention_fun/1
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
    StreamId = iolist_to_binary(Name),
    RemoteConfig = maps:get(remote_config, Config, #{}),
    Reference = maps:get(reference, Config, undefined),
    Epoch = maps:get(epoch, Config, 0),
    %% Pass user retention specs (max_bytes, max_age) for remote tier evaluation.
    %% Filter out the {'fun', ...} specs we add — those are for local retention only.
    UserRetention = [S || S <- maps:get(retention, Config, []), element(1, S) =/= 'fun'],
    {ok, _} = rabbitmq_stream_s3_replica_reader_sup:start_child(
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
    append_retention(StreamId, Config);
on_init(acceptor, _Pid, #{name := Name, leader_pid := LeaderPid, counter := Counter} = Config) ->
    StreamId = iolist_to_binary(Name),
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
    StreamId = iolist_to_binary(Name),
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
    StreamId = iolist_to_binary(Name),
    case rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}) of
        undefined -> ok;
        Pid -> gen_server:cast(Pid, {retention_updated, Retention})
    end,
    [{'fun', local_retention_fun(StreamId)} | Retention].

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

append_retention(StreamId, Config) ->
    Fun = {'fun', local_retention_fun(StreamId)},
    maps:update_with(retention, fun(R) -> [Fun | R] end, [Fun], Config).

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
