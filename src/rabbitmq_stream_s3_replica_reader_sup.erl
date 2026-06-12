%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_replica_reader_sup).
-moduledoc """
Factory supervisor for per-stream replica readers.

This is the top layer of a two-level structure. Its dynamic children are
per-stream supervisors (`rabbitmq_stream_s3_stream_sup`), each owning exactly
one replica reader. See that module for why the extra layer exists.

The per-stream supervisors are `temporary`: the factory never restarts them.
A stream that exhausts its own restart budget exits with reason `shutdown`
and parks; a stream whose writer goes away auto-shuts-down normally. In both
cases the factory neither restarts the child nor spends its own restart
budget, so one stream's failure cannot cascade to other streams.
""".

-behaviour(supervisor).

-export([start_link/0, start_child/1, stop_child/1]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-doc """
Start a per-stream supervisor and its replica reader.

Returns `{ok, ReaderPid}` where `ReaderPid` is the replica reader worker
itself (not the per-stream supervisor), preserving the contract for callers
that address the reader by pid. Returns `{error, {already_started, Pid}}`
when a reader for this stream is already registered, matching the previous
single-layer behaviour (the reader registers a
`{via, registry, {StreamId, node()}}` name, so a duplicate start would
otherwise fail deep inside the per-stream supervisor's init).
""".
-spec start_child(rabbitmq_stream_s3_replica_reader:config()) ->
    {ok, pid()} | {error, term()}.
start_child(#{stream := StreamId} = Args) ->
    Name = {StreamId, node()},
    case rabbitmq_stream_s3_registry:whereis_name(Name) of
        Pid when is_pid(Pid) ->
            {error, {already_started, Pid}};
        undefined ->
            case supervisor:start_child(?MODULE, [Args]) of
                {ok, SupPid} ->
                    {ok, reader_pid(SupPid)};
                {error, _} = Err ->
                    %% A concurrent start may have registered the reader
                    %% between our check and the start attempt. Surface the
                    %% same already_started result the caller expects rather
                    %% than the nested per-stream-supervisor start error.
                    case rabbitmq_stream_s3_registry:whereis_name(Name) of
                        P when is_pid(P) -> {error, {already_started, P}};
                        undefined -> Err
                    end
            end
    end.

-doc """
Stop the per-stream supervisor that owns the given replica reader pid.

Callers pass the reader (worker) pid, typically obtained from the registry.
This terminates the enclosing per-stream supervisor, which shuts the reader
down gracefully: the reader traps exits and its `terminate/2` unregisters
from the registry. The call is synchronous, so the registry entry is cleared
by the time it returns.
""".
-spec stop_child(pid()) -> ok.
stop_child(ReaderPid) when is_pid(ReaderPid) ->
    case stream_sup_of(ReaderPid) of
        {ok, SupPid} ->
            _ = supervisor:terminate_child(?MODULE, SupPid),
            ok;
        error ->
            ok
    end.

init([]) ->
    ChildSpec = #{
        id => rabbitmq_stream_s3_stream_sup,
        start => {rabbitmq_stream_s3_stream_sup, start_link, []},
        %% Per-stream supervisors are never restarted by the factory. They
        %% either park (own budget exhausted) or auto-shut-down (writer
        %% gone). Restarting here would resurrect a reader with no live
        %% writer, and would couple one stream's failures to the factory's
        %% shared budget, which is exactly what this layer avoids.
        restart => temporary,
        shutdown => infinity,
        type => supervisor,
        modules => [rabbitmq_stream_s3_stream_sup]
    },
    %% The factory's own intensity/period is largely inert: temporary
    %% children are never restarted, so per-stream terminations (parking or
    %% auto-shutdown) do not consume it. It only guards against a per-stream
    %% supervisor that repeatedly fails to start.
    {ok, {{simple_one_for_one, 3, 10}, [ChildSpec]}}.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

%% Return the replica reader worker pid inside a per-stream supervisor. The
%% worker is started synchronously during the per-stream supervisor's init,
%% so it is present by the time a successful start_child returns.
reader_pid(SupPid) ->
    case lists:keyfind(rabbitmq_stream_s3_replica_reader, 1, supervisor:which_children(SupPid)) of
        {_, Pid, _, _} when is_pid(Pid) -> Pid;
        _ -> undefined
    end.

%% Find the per-stream supervisor whose replica reader is ReaderPid. Linear
%% in the number of streams on the node, but stop_child is a rare,
%% maintenance/test-time operation (plugin disable tears down the whole tree
%% instead of stopping children one by one).
stream_sup_of(ReaderPid) ->
    SupPids = [P || {_, P, supervisor, _} <- supervisor:which_children(?MODULE), is_pid(P)],
    find_sup(SupPids, ReaderPid).

find_sup([], _ReaderPid) ->
    error;
find_sup([SupPid | Rest], ReaderPid) ->
    case lists:keyfind(rabbitmq_stream_s3_replica_reader, 1, supervisor:which_children(SupPid)) of
        {_, ReaderPid, _, _} -> {ok, SupPid};
        _ -> find_sup(Rest, ReaderPid)
    end.
