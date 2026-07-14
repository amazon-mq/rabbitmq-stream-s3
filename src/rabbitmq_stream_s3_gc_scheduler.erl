%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_gc_scheduler).
-moduledoc """
Periodically triggers a cross-stream garbage collection sweep.

`rabbitmq_stream_s3_gc:run/1` is otherwise only invoked on demand via the
`rabbitmq-streams stream_s3_gc` CLI command (and the per-stream `run_stream/2`
path the replica reader uses after a manifest reset). Without a periodic trigger,
a straggler object left by the deletion race (an upload that completes after the
one-shot deletion sweep) and its tombstone persist until an operator runs GC by
hand.

This server runs a sweep on a timer to make straggler reclamation and tombstone
cleanup self-healing. A full sweep is a bucket-wide LIST plus one
strongly-consistent metadata read per stream, so it is off by default and runs on
a conservative interval when enabled.

The sweep runs in `delete` mode by default. It can instead be configured to run
in `dry_run` mode, which only identifies and logs dangling objects without
reclaiming them.

Reclamation is eventual, not immediate. Objects orphaned by a deleted stream are
identified from Khepri via a strongly-consistent anchor read, so any sweeping node
reclaims them. An object orphaned below a live stream's first offset, however, is
recognised only against that node's local manifest replica cache, so a single
round reclaims those only for the streams the sweeping node caches; an orphan
under a stream cached only on other nodes waits for a round one of those nodes
wins. Because the lock is not fair, that can take several rounds, but every node
runs its own timer, so each stream's caching nodes get their turn.

Every node runs its own timer, but a sweep must run on only one node at a time: a
concurrent sweep would duplicate the LIST and quorum-read cost with no benefit. A
non-blocking cluster-wide lock (`global:trans/4` with zero retries) elects the
sweeper for each round; a node that cannot take the lock skips that round and
retries on its next tick. There is no fixed leader, so the sweep survives the loss
of any node.
""".

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("include/logging.hrl").

-record(?MODULE, {timer :: reference() | undefined}).

-define(SERVER, ?MODULE).
-define(SWEEP, sweep).
%% Cluster-wide lock id. The requester id (the second element) must be `self()`,
%% not a shared constant: `global` grants a lock re-entrantly to an identical
%% requester id, so a fixed id would let every node hold it at once and defeat
%% the mutual exclusion. See with_sweep_lock/2.
-define(GC_LOCK, {{?MODULE, sweep}, self()}).

-export([start_link/0]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% Exported for the multi-node gc_scheduler_SUITE, which exercises the
%% cluster-wide lock (a distributed property the non-distributed eunit VM cannot
%% reproduce).
-export([with_sweep_lock/2]).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
    case rabbitmq_stream_s3_config:gc_enabled() of
        true ->
            ?LOG_INFO(
                "Automatic tiered storage GC is enabled; scheduling a ~p sweep every ~b ms.",
                [rabbitmq_stream_s3_config:gc_mode(), rabbitmq_stream_s3_config:gc_interval()]
            ),
            {ok, #?MODULE{timer = schedule()}};
        false ->
            ?LOG_INFO("Automatic tiered storage GC is disabled."),
            ignore
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(?SWEEP, #?MODULE{} = State) ->
    %% A sweep error must never kill the timer loop: catch everything and
    %% reschedule regardless.
    try
        run_sweep()
    catch
        Class:Reason:Stack ->
            ?LOG_WARNING(
                "Automatic tiered storage GC sweep failed: ~ts:~p~n~p",
                [Class, Reason, Stack]
            )
    end,
    {noreply, State#?MODULE{timer = schedule()}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

schedule() ->
    erlang:send_after(rabbitmq_stream_s3_config:gc_interval(), self(), ?SWEEP).

run_sweep() ->
    %% `[node() | nodes()]` is the set `global` can arbitrate over (its own
    %% default node list, and what rabbit_amqqueue's non-blocking rebalance lock
    %% uses). It is more correct here than rabbit_nodes:list_running/0: `global`
    %% locks at the Erlang distribution layer, and a rabbit membership view can
    %% transiently diverge from distribution connectivity, which would let two
    %% nodes lock over different node sets and sweep at once.
    _ = with_sweep_lock([node() | nodes()], fun sweep/0),
    ok.

%% Run `Fun` under a non-blocking cluster-wide lock so only one node sweeps per
%% round. `global:trans/4` with zero retries runs `Fun` when the lock is free and
%% returns `aborted` (without running it) when another node holds it. Returns
%% `skipped` when the lock was held elsewhere, or `{ran, Result}` otherwise.
%%
%% `Fun`'s result is wrapped in `{ran, _}` inside the transaction so a run can
%% never be mistaken for `global:trans/4`'s own `aborted` return, even if `Fun`
%% itself returns the atom `aborted`.
-spec with_sweep_lock([node()], fun(() -> Result)) -> skipped | {ran, Result}.
with_sweep_lock(Nodes, Fun) ->
    case global:trans(?GC_LOCK, fun() -> {ran, Fun()} end, Nodes, 0) of
        aborted ->
            ?LOG_DEBUG(
                "Skipping automatic tiered storage GC sweep: another node holds "
                "the sweep lock this round."
            ),
            skipped;
        {ran, _} = Result ->
            Result
    end.

sweep() ->
    Mode = rabbitmq_stream_s3_config:gc_mode(),
    ?LOG_INFO("Starting automatic tiered storage GC sweep (mode: ~p).", [Mode]),
    case rabbitmq_stream_s3_gc:run(#{mode => Mode}) of
        {ok, Findings} ->
            ?LOG_INFO(
                "Automatic tiered storage GC sweep complete: ~b dangling object(s) ~ts.",
                [length(Findings), verb(Mode)]
            );
        {error, Reason} ->
            ?LOG_WARNING(
                "Automatic tiered storage GC sweep could not complete: ~p.",
                [Reason]
            )
    end.

verb(delete) -> "deleted";
verb(dry_run) -> "found".

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% When GC is disabled (the default), the server does not start.
init_disabled_returns_ignore_test() ->
    application:unset_env(rabbitmq_stream_s3, gc_enabled),
    ?assertEqual(ignore, init([])).

%% When GC is enabled, the server starts and arms its timer.
init_enabled_schedules_sweep_test() ->
    application:set_env(rabbitmq_stream_s3, gc_enabled, true),
    application:set_env(rabbitmq_stream_s3, gc_interval, 60_000),
    try
        {ok, #?MODULE{timer = Timer}} = init([]),
        ?assert(is_reference(Timer)),
        _ = erlang:cancel_timer(Timer)
    after
        application:unset_env(rabbitmq_stream_s3, gc_enabled),
        application:unset_env(rabbitmq_stream_s3, gc_interval)
    end.

%% The cluster-wide lock's mutual exclusion is a distributed property: the eunit
%% VM runs with `-kernel start_distribution false` (see erlang.mk), so
%% `global_name_server` is absent and `global:trans/4` does not arbitrate. That
%% path is exercised in the multi-node gc_scheduler_SUITE instead.

-endif.
