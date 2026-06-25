%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_registry).
-moduledoc """
Process registry for remote replica readers.

Implements the `{via, Module, Name}` callbacks for use with
gen_server:start_link, gen_server:call, gen_server:cast, etc.

Names are `{StreamId, Node}`. Local lookups use ETS directly.
Cross-node lookups use `erpc:call/4` to read the remote node's table.
""".

-include("include/rabbitmq_stream_s3.hrl").

-export([
    register_name/2,
    unregister_name/1,
    whereis_name/1,
    send/2
]).

-export([overview/0]).

-define(TABLE, rabbitmq_stream_s3_registry).

-doc "Create the registry ETS table.".
-spec init() -> ok.
-export([init/0]).
init() ->
    _ = ets:new(?TABLE, [named_table, public, set]),
    ok.

-doc "Register a pid under the given name.".
-spec register_name({stream_id(), node()}, pid()) -> yes | no.
register_name({StreamId, Node}, Pid) when Node =:= node() ->
    case ets:insert_new(?TABLE, {StreamId, Pid}) of
        true -> yes;
        false -> register_over_existing(StreamId, Pid)
    end.

%% insert_new failed: an entry already exists for the name. If it is a stale
%% dead pid - a reader that died without unregistering (e.g. a brutal kill) -
%% reclaim it; otherwise a live reader holds the name, so refuse. Without this a
%% stale entry permanently blocks re-attaching tiering to the stream.
-spec register_over_existing(stream_id(), pid()) -> yes | no.
register_over_existing(StreamId, Pid) ->
    case ets:lookup(?TABLE, StreamId) of
        [{_, Existing}] when Existing =/= Pid ->
            reclaim_if_dead(StreamId, Existing, Pid);
        _ ->
            %% Raced away, or already ours; retry once.
            retry_insert(StreamId, Pid)
    end.

-spec reclaim_if_dead(stream_id(), pid(), pid()) -> yes | no.
reclaim_if_dead(StreamId, Existing, Pid) ->
    case is_process_alive(Existing) of
        true ->
            no;
        false ->
            %% Swap the pid in place in a single atomic op. Unlike
            %% delete_object + insert_new there is no window in which the key is
            %% absent, so a concurrent whereis_name never observes the name as
            %% momentarily unregistered.
            true = ets:update_element(?TABLE, StreamId, {2, Pid}),
            yes
    end.

-spec retry_insert(stream_id(), pid()) -> yes | no.
retry_insert(StreamId, Pid) ->
    case ets:insert_new(?TABLE, {StreamId, Pid}) of
        true -> yes;
        false -> no
    end.

-doc "Unregister a name.".
-spec unregister_name({stream_id(), node()}) -> ok.
unregister_name({StreamId, Node}) when Node =:= node() ->
    _ = ets:delete(?TABLE, StreamId),
    ok.

-doc "Look up the pid for a name. Returns `undefined` if not registered.".
-spec whereis_name({stream_id(), node()}) -> pid() | undefined.
whereis_name({StreamId, Node}) when Node =:= node() ->
    try ets:lookup(?TABLE, StreamId) of
        [{_, Pid}] ->
            %% Report a stale dead pid as absent so callers (the reconciler, a
            %% sender) treat the stream as having no reader rather than wedging
            %% on a phantom. register_name/2 reclaims the stale entry.
            case is_process_alive(Pid) of
                true -> Pid;
                false -> undefined
            end;
        [] ->
            undefined
    catch
        error:badarg -> undefined
    end;
whereis_name({StreamId, Node}) ->
    try erpc:call(Node, ets, lookup, [?TABLE, StreamId]) of
        [{_, Pid}] -> Pid;
        [] -> undefined
    catch
        _:_ -> undefined
    end.

-doc "Send a message to the process registered under the given name.".
-spec send({stream_id(), node()}, term()) -> pid().
send(Name, Msg) ->
    case whereis_name(Name) of
        undefined ->
            erlang:error(badarg, [Name, Msg]);
        Pid ->
            erlang:send(Pid, Msg),
            Pid
    end.

-doc "Return the registry as a mapping of stream ID to PID.".
-spec overview() -> #{stream_id() => pid()}.
overview() ->
    #{StreamId => Pid || {StreamId, Pid} <- ets:tab2list(?TABLE)}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% A reader that dies without unregistering must not wedge the stream: a stale
%% dead pid is reported as absent and is replaced when a fresh reader registers.
stale_dead_pid_is_reclaimed_test() ->
    catch ets:delete(?TABLE),
    init(),
    Name = {<<"s">>, node()},
    Dead = spawn(fun() -> ok end),
    DeadRef = monitor(process, Dead),
    receive
        {'DOWN', DeadRef, _, _, _} -> ok
    after 1000 -> error(dead_proc_timeout)
    end,
    ?assertEqual(yes, register_name(Name, Dead)),
    %% The dead pid is reported as absent.
    ?assertEqual(undefined, whereis_name(Name)),
    %% A fresh live reader registers, reclaiming the stale entry.
    Live = spawn(fun() -> timer:sleep(infinity) end),
    ?assertEqual(yes, register_name(Name, Live)),
    ?assertEqual(Live, whereis_name(Name)),
    %% A second live registrant is refused while the name is held.
    Other = spawn(fun() -> timer:sleep(infinity) end),
    ?assertEqual(no, register_name(Name, Other)),
    exit(Live, kill),
    exit(Other, kill),
    catch ets:delete(?TABLE),
    ok.

-endif.
