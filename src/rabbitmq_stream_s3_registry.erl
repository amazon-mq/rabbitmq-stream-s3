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
        [{_, Pid}] -> Pid;
        [] -> undefined
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
