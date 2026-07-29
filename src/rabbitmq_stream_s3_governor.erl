%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_governor).
-moduledoc """
Per-node transfer pacing process.

Accepts transfer submissions from replica readers, paces them via a
token bucket, spawns tasks, and reports completions back to the caller.
The transfer function is opaque to the governor.

When the configured rate is `unlimited`, submissions execute immediately
with no pacing.

A caller can cancel a batch of submissions by their `Ref`s via `cancel/1`:
every admitted task for a `Ref` is killed, a still-queued item is dropped
before ever being spawned, and the pending queue is scanned once for the whole
batch. A single `Ref` can have both more than one running task and a queued
item at the same time, because a resubmit reuses its `Ref`; `cancel/1` reaches
all of them.

Cancellation does not depend on the requester asking for it. The requester is
monitored for as long as the governor holds any of its work, queued or running,
and that work is discarded when the requester dies, whatever killed it. A
requester has many ways to die and only one of them can call `cancel/1` on the
way out (a brutal kill can call nothing), so relying on an explicit cancel per
teardown path leaves uploads running for a stream nobody is reading. This is the
same policy already applied at intake: never transfer for a requester that is
gone.

The monitor is taken at intake rather than when a task is spawned. A queued item
can wait a long time behind the token bucket, and monitoring only running tasks
left a requester whose items were all still queued unmonitored: its death was
noticed only if one of its items happened to reach the head of the queue on a
refill tick, since `drain_pending/1` inspects the head alone and stops at the
first item it cannot admit. Behind a live head that is short of tokens, an
arbitrary number of dead requesters' items could sit in the queue indefinitely,
counted in `governor_pending_submissions` and invisible to
`governor_dropped_dead_replyto`.

Spawned tasks are monitored, not linked, so a governor restart does not
kill tasks already running - they keep delivering their results directly to
`ReplyTo` regardless of the governor's own lifecycle.

A planned shutdown (the application or node stopping, or an explicit
`gen_server:stop/1`) is different from a restart: `terminate/2` kills every
still-running task, since nothing will be left running to service them.
A crash takes neither path - it is not a planned shutdown, and it is not a
restart either (the governor itself, not its tasks, is what's restarting) -
so tasks are deliberately left alone, consistent with the monitor-not-link
choice above.

## Derived gauges

Both gauges are pure functions of the state they describe, published by
`derive_gauges/1` on every handler return, not independently mutated counters:

- `governor_tasks_in_flight`     = `map_size(tasks)`
- `governor_pending_submissions` = `queue:len(pending)`

The counters live in `persistent_term` and so outlive the process, while `tasks`
and `pending` do not. An inc/dec gauge therefore ratchets permanently on the one
path that deliberately runs no cleanup: a crash leaves every running task's
`'DOWN'` in a dead mailbox, and the fresh incarnation starts from `tasks = #{}`
with no decrement left to issue. A derived gauge self-heals instead, because
`init/1` publishes from the empty state and each later mutation republishes the
truth.
""".

-behaviour(gen_server).

-export([start_link/1, submit/4, cancel/1]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    format_status/1,
    terminate/2
]).

-export([init_counters/0]).

-include("include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-define(REFILL_INTERVAL_MS, 100).

%% Per-node counters.
-define(C_SUBMISSIONS_RECEIVED, 1).
-define(C_TASKS_IN_FLIGHT, 2).
-define(C_PENDING_SUBMISSIONS, 3).
-define(C_OVERSIZED_ADMISSIONS, 4).
-define(C_DROPPED_DEAD_REPLYTO, 5).
-define(COUNTERS, [
    {governor_submissions_received, ?C_SUBMISSIONS_RECEIVED, counter,
        "Total transfer submissions received by the governor"},
    {governor_tasks_in_flight, ?C_TASKS_IN_FLIGHT, gauge,
        "Number of transfer tasks currently executing"},
    {governor_pending_submissions, ?C_PENDING_SUBMISSIONS, gauge,
        "Number of submissions queued waiting for token-bucket capacity"},
    {governor_oversized_admissions, ?C_OVERSIZED_ADMISSIONS, counter,
        "Transfers admitted whose size exceeded the token-bucket burst (admitted "
        "on credit, driving the bucket into debt). A persistently climbing value "
        "means the configured burst is smaller than typical fragments"},
    {governor_dropped_dead_replyto, ?C_DROPPED_DEAD_REPLYTO, counter,
        "Submissions dropped because their requester was already dead: either "
        "at intake, or while queued behind the token bucket. Protects against "
        "any requester death (crash, restart), not just graceful deletion"}
]).
-define(COUNTER_KEY, {?MODULE, counter}).

-record(state, {
    bucket :: rabbitmq_stream_s3_token_bucket:t() | unlimited,
    %% Pending submissions waiting for tokens.
    pending :: queue:queue(pending_item()),
    timer_ref :: reference() | undefined,
    %% MonRef => {Pid, Ref, ReplyTo} for every task currently executing, one
    %% entry per live attempt. Keyed by the monitor reference rather than the
    %% caller-minted Ref because a resubmit reuses the same Ref: keying by Ref
    %% let a second live attempt overwrite the first, orphaning a running task
    %% that neither cancel/1 nor kill_tasks/1 could then reach.
    tasks = #{} :: #{reference() => {pid(), reference(), pid()}},
    %% Ref => [MonRef], the secondary index cancel/1 uses to reach every live
    %% attempt belonging to a caller-minted Ref.
    tasks_by_ref = #{} :: #{reference() => [reference()]},
    %% ReplyTo => {MonRef, ItemCount} for every requester with at least one
    %% outstanding item, queued or running. One monitor per requester however
    %% many items it has, so the count is what decides when to demonitor.
    %% Counting queued items and not just running tasks is what lets a
    %% requester's death be noticed while its work is still behind the token
    %% bucket; see requester_down/3.
    requesters = #{} :: #{pid() => {reference(), pos_integer()}}
}).

-type pending_item() :: {
    Fun :: fun(() -> term()),
    Size :: non_neg_integer(),
    ReplyTo :: pid(),
    Ref :: reference()
}.

%% ------------------------------------------------------------------
%% API
%% ------------------------------------------------------------------

-spec start_link(map()) -> gen_server:start_ret().
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

-doc """
Submit a transfer for execution.

`Fun` is a zero-arity function that performs the transfer and returns
`{ok, Result}` or `{error, Reason}`. `Size` is the byte cost for
pacing. On completion, `ReplyTo` receives `{transfer_result, Ref, Result}`
where Result is the return value of `Fun`.
""".
-spec submit(fun(() -> term()), non_neg_integer(), pid(), reference()) -> ok.
submit(Fun, Size, ReplyTo, Ref) ->
    gen_server:cast(?MODULE, {submit, Fun, Size, ReplyTo, Ref}).

-doc """
Cancel a batch of previously submitted transfers.
""".
-spec cancel([reference()]) -> ok.
cancel([]) ->
    ok;
cancel(Refs) ->
    gen_server:cast(?MODULE, {cancel, Refs}).

%% ------------------------------------------------------------------
%% gen_server callbacks
%% ------------------------------------------------------------------

init(Opts) ->
    %% Without this, a supervisor-issued shutdown signal (application/node
    %% stop) would just kill this process outright, and terminate/2 below -
    %% which cancels running tasks on that specific path - would never run.
    %% This is the flag's only purpose here. It also makes exit signals from
    %% any other link land in handle_info/2, which has an explicit clause to
    %% keep them from being silently discarded.
    process_flag(trap_exit, true),
    Bucket =
        case maps:get(rate, Opts, unlimited) of
            unlimited ->
                unlimited;
            Rate when is_integer(Rate), Rate > 0 ->
                Burst = maps:get(burst, Opts, Rate div 5),
                rabbitmq_stream_s3_token_bucket:new(Rate, Burst)
        end,
    TimerRef =
        case Bucket of
            unlimited -> undefined;
            _ -> schedule_refill()
        end,
    %% Counter is created by init_counters/0 from the API init step. It lives in
    %% persistent_term, so it outlives this process: publish the gauges from the
    %% empty state so a restart does not inherit the previous incarnation's
    %% values (whose tasks and queue are gone along with its state).
    {ok, derive_gauges(#state{bucket = Bucket, pending = queue:new(), timer_ref = TimerRef})}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast({submit, Fun, Size, ReplyTo, Ref}, State) ->
    inc(?C_SUBMISSIONS_RECEIVED, 1),
    case is_process_alive(ReplyTo) of
        true ->
            {noreply, derive_gauges(dispatch({Fun, Size, ReplyTo, Ref}, State))};
        false ->
            %% The requester died between calling submit/4 and this cast
            %% landing (e.g. a stream deletion or a crash). Drop it now
            %% rather than pacing, spawning, and uploading for nobody: its
            %% result would just be dropped into a dead mailbox anyway.
            inc(?C_DROPPED_DEAD_REPLYTO, 1),
            {noreply, State}
    end;
handle_cast({cancel, Refs}, #state{tasks = Tasks, tasks_by_ref = ByRef} = State) ->
    %% Kill every admitted attempt for these Refs; tasks/tasks_by_ref removal
    %% happens in the 'DOWN' handler below, the single cleanup path shared by
    %% normal completion, a crash and a kill alike. A Ref can have more than one
    %% live attempt (a same-Ref resubmit while the earlier one is still
    %% running), so all of them are killed.
    %% A Ref can also be admitted and still queued at the same time, so every
    %% Ref is passed to cancel_pending/2 as well: a still-queued copy must not
    %% be left behind just because an admitted attempt for the same Ref existed.
    lists:foreach(
        fun(Ref) ->
            lists:foreach(
                fun(MonRef) ->
                    {Pid, _Ref, _ReplyTo} = maps:get(MonRef, Tasks),
                    exit(Pid, kill)
                end,
                maps:get(Ref, ByRef, [])
            )
        end,
        Refs
    ),
    {noreply, derive_gauges(cancel_pending(Refs, State))};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(refill, #state{bucket = Bucket0} = State0) ->
    Bucket = rabbitmq_stream_s3_token_bucket:refill(Bucket0),
    State1 = State0#state{bucket = Bucket},
    State2 = drain_pending(State1),
    TimerRef =
        case queue:is_empty(State2#state.pending) of
            true -> undefined;
            false -> schedule_refill()
        end,
    {noreply, derive_gauges(State2#state{timer_ref = TimerRef})};
handle_info(
    {'DOWN', MonRef, process, Pid, _Reason},
    #state{tasks = Tasks, tasks_by_ref = ByRef} = State
) ->
    case maps:take(MonRef, Tasks) of
        {{_, Ref, ReplyTo}, Tasks1} ->
            %% Drop only this attempt's monitor from the Ref's list; a same-Ref
            %% resubmit may still have another attempt running under it. Remove
            %% the Ref key entirely once its last attempt is gone, so the index
            %% does not accumulate empty lists.
            ByRef1 =
                case maps:get(Ref, ByRef, []) -- [MonRef] of
                    [] -> maps:remove(Ref, ByRef);
                    Rest -> ByRef#{Ref => Rest}
                end,
            State1 = State#state{tasks = Tasks1, tasks_by_ref = ByRef1},
            {noreply, derive_gauges(unwatch_requester(ReplyTo, State1))};
        error ->
            %% Not a task: a requester died. Kill everything it submitted -
            %% running or still queued - since its results now have nowhere to
            %% go. This is the only cancellation path a brutal kill, a crash,
            %% or a supervisor shutdown of the requester can reach.
            {noreply, derive_gauges(requester_down(MonRef, Pid, State))}
    end;
handle_info({'EXIT', Pid, Reason}, State) when Reason =/= normal ->
    %% `gen_server` turns only the *parent's* exit signal into a `terminate/2`
    %% call; a signal from any other link is delivered here instead. `trap_exit`
    %% is set for the parent-shutdown path alone (see `init/1`), so its effect on
    %% these other signals is incidental, and letting them fall through to the
    %% catchall below is strictly weaker than the untrapped default, where an
    %% abnormal signal from a link takes the process down. Restore that default
    %% rather than discard the signal: nothing links to the governor today, so
    %% arriving here means an unexpected link exists, and dying loudly surfaces
    %% it. `terminate/2` leaves running tasks alone on this reason, which is the
    %% correct choice for a crash-like exit (see the moduledoc).
    ?LOG_WARNING(
        "Governor received an exit signal from linked process ~p: ~p. Stopping, "
        "since only the supervisor is expected to be linked",
        [Pid, Reason],
        #{domain => ?RMQLOG_DOMAIN_STREAM_S3}
    ),
    {stop, Reason, State};
handle_info(_Info, State) ->
    %% Includes `{'EXIT', _, normal}`, which an untrapped process ignores too.
    {noreply, State}.

%% Handle the death of a requester with outstanding items, queued or running.
%% The monitor ref is matched against the recorded one so a stale 'DOWN' (a
%% demonitor/flush that raced) cannot kill a fresh incarnation's tasks: a
%% restarted reader is a new pid, but the pid alone is not proof of which
%% monitor the 'DOWN' belongs to.
requester_down(MonRef, ReplyTo, #state{requesters = Requesters} = State) ->
    case Requesters of
        #{ReplyTo := {MonRef, _Count}} ->
            %% Drop the queued items by requester, not via the Refs its running
            %% tasks carry: a requester can have queued items and no admitted
            %% task at all, and those must go too. Then drop the whole
            %% `requesters` entry - the monitor is already gone, and the running
            %% tasks being killed below clean up through their own 'DOWN'.
            State1 = drop_pending_for_requester(ReplyTo, State),
            State2 = State1#state{
                requesters = maps:remove(ReplyTo, State1#state.requesters)
            },
            kill_requester_tasks(ReplyTo, State2);
        _ ->
            State
    end.

%% Kill this requester's running tasks. The entries themselves are removed by
%% each task's own 'DOWN', the single cleanup path shared with completion,
%% a crash and cancel/1.
kill_requester_tasks(ReplyTo, #state{tasks = Tasks} = State) ->
    maps:foreach(
        fun
            (_MonRef, {Pid, _Ref, Owner}) when Owner =:= ReplyTo -> exit(Pid, kill);
            (_MonRef, _Task) -> ok
        end,
        Tasks
    ),
    State.

format_status(#{state := State} = Status) ->
    Status#{state := format_state(State)}.

%% Cancel every running task on a planned shutdown (application/node stop, or
%% an explicit gen_server:stop/1) so nothing keeps running once nothing is
%% left to service it. A crash takes a different path here - Reason is none
%% of normal/shutdown/{shutdown, _} - and deliberately leaves tasks alone:
%% they are spawn_monitor'd, not linked, precisely so a governor restart
%% does not kill uploads already in flight (see moduledoc).
terminate(Reason, #state{tasks = Tasks}) when
    Reason =:= normal; Reason =:= shutdown
->
    kill_tasks(Tasks);
terminate({shutdown, _}, #state{tasks = Tasks}) ->
    kill_tasks(Tasks);
terminate(_Reason, _State) ->
    ok.

kill_tasks(Tasks) ->
    maps:foreach(fun(_MonRef, {Pid, _Ref, _ReplyTo}) -> exit(Pid, kill) end, Tasks),
    ok.

format_state(#state{bucket = Bucket, pending = Pending}) ->
    #{
        bucket =>
            case Bucket of
                unlimited -> unlimited;
                _ -> rabbitmq_stream_s3_token_bucket:info(Bucket)
            end,
        pending => queue:len(Pending)
    }.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

%% Take the requester's watch here, once, covering the item for as long as the
%% governor holds it - queued, running, or moving between the two. Watching only
%% from spawn_task/2 left a requester whose items were all still queued
%% unmonitored, so its death went unnoticed until one of its items reached the
%% head of the queue on a refill tick (see drain_pending/1).
dispatch({_Fun, _Size, ReplyTo, _Ref} = Item, State0) ->
    State = watch_requester(ReplyTo, State0),
    dispatch1(Item, State).

dispatch1(Item, #state{bucket = unlimited} = State) ->
    spawn_task(Item, State);
dispatch1(Item, #state{pending = Pending} = State) ->
    case queue:is_empty(Pending) of
        false ->
            %% Items are already waiting for tokens. Queue behind them rather
            %% than checking the bucket directly: letting a new submission jump
            %% ahead lets a steady stream of smaller transfers permanently
            %% starve a larger one stuck at the head of the queue. Only
            %% drain_pending admits queued items, head first.
            enqueue(Item, State);
        true ->
            try_admit(Item, State)
    end.

try_admit({_Fun, Size, _ReplyTo, _Ref} = Item, #state{bucket = Bucket0} = State) ->
    case rabbitmq_stream_s3_token_bucket:request(Size, Bucket0) of
        {ok, Bucket} ->
            count_oversized(Size, Bucket0),
            spawn_task(Item, State#state{bucket = Bucket});
        {insufficient, _, _} ->
            enqueue(Item, State)
    end.

%% Remove every Ref in `Refs` from `pending` in a single pass; a no-op for
%% any Ref whose task was already admitted and cleaned up, or that never had
%% a live submission.
cancel_pending([], State) ->
    State;
cancel_pending(Refs, State) ->
    RefSet = sets:from_list(Refs, [{version, 2}]),
    {_Dropped, State1} = drop_pending(
        fun({_Fun, _Size, _ReplyTo, ItemRef}) -> sets:is_element(ItemRef, RefSet) end,
        State
    ),
    State1.

%% Remove every queued item belonging to `ReplyTo`. Used when a requester dies:
%% its items must go whether or not it also had a task admitted, so they are
%% matched by requester rather than by the Refs its running tasks happen to
%% carry. Each one counts as dropped-for-a-dead-requester, the same as if
%% drain_pending/1 had found it at the head.
drop_pending_for_requester(ReplyTo, State) ->
    {Dropped, State1} = drop_pending(
        fun({_Fun, _Size, ItemReplyTo, _Ref}) -> ItemReplyTo =:= ReplyTo end,
        State
    ),
    inc(?C_DROPPED_DEAD_REPLYTO, Dropped),
    State1.

%% Drop every queued item `Drop` selects, releasing each one's requester watch.
%% One pass over the queue however many items match. Returns how many were
%% dropped, so callers can account for them.
drop_pending(Drop, #state{pending = Pending0} = State) ->
    {Dropped, Kept} = lists:partition(Drop, queue:to_list(Pending0)),
    State1 = lists:foldl(
        fun({_Fun, _Size, ReplyTo, _Ref}, Acc) -> unwatch_requester(ReplyTo, Acc) end,
        State#state{pending = queue:from_list(Kept)},
        Dropped
    ),
    {length(Dropped), State1}.

%% Append a submission to the pending queue and ensure the refill timer is
%% running so it will eventually drain. The requester's watch is already held by
%% dispatch/2 and spans the item's whole life, so there is nothing to take here.
enqueue(Item, State) ->
    Pending = queue:in(Item, State#state.pending),
    State1 = State#state{pending = Pending},
    case State1#state.timer_ref of
        undefined -> State1#state{timer_ref = schedule_refill()};
        _ -> State1
    end.

drain_pending(#state{pending = Pending0, bucket = Bucket0} = State) ->
    case queue:peek(Pending0) of
        empty ->
            State;
        {value, {_Fun, _Size, ReplyTo, _Ref} = Item} ->
            case is_process_alive(ReplyTo) of
                false ->
                    %% The requester died while this item waited behind the
                    %% token bucket, and its 'DOWN' has not been processed yet
                    %% (once it is, requester_down/3 drops every one of its
                    %% items at once). Drop it for free (no tokens spent) and
                    %% keep draining: the head is being discarded outright,
                    %% not skipped-and-retried, so this doesn't let a later
                    %% item unfairly jump the FIFO order (see enqueue/2).
                    inc(?C_DROPPED_DEAD_REPLYTO, 1),
                    Pending = queue:drop(Pending0),
                    State1 = unwatch_requester(ReplyTo, State#state{pending = Pending}),
                    drain_pending(State1);
                true ->
                    drain_pending_admit(Item, Pending0, Bucket0, State)
            end
    end.

drain_pending_admit({_Fun, Size, _ReplyTo, _Ref} = Item, Pending0, Bucket0, State) ->
    case rabbitmq_stream_s3_token_bucket:request(Size, Bucket0) of
        {ok, Bucket} ->
            count_oversized(Size, Bucket0),
            Pending = queue:drop(Pending0),
            %% The item moves from `pending` to `tasks` and keeps the single
            %% watch it took at dispatch; nothing to adjust here.
            drain_pending(
                spawn_task(Item, State#state{
                    pending = Pending,
                    bucket = Bucket
                })
            );
        {insufficient, _, _} ->
            State
    end.

spawn_task({Fun, _Size, ReplyTo, Ref}, #state{tasks = Tasks, tasks_by_ref = ByRef} = State) ->
    {Pid, MonRef} = spawn_monitor(fun() ->
        logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
        Result =
            try
                Fun()
            catch
                Class:Reason -> {error, {Class, Reason}}
            end,
        ReplyTo ! {transfer_result, Ref, Result}
    end),
    %% A resubmit reuses the same Ref while the earlier attempt may still be
    %% running, so append to this Ref's monitor list rather than replacing it.
    MonRefs = maps:get(Ref, ByRef, []),
    %% The requester's watch was taken by dispatch/2 and is released by this
    %% task's own 'DOWN', so it is not touched here.
    State#state{
        tasks = Tasks#{MonRef => {Pid, Ref, ReplyTo}},
        tasks_by_ref = ByRef#{Ref => [MonRef | MonRefs]}
    }.

%% Start (or refcount) a monitor on a requester that now has one more
%% outstanding item, queued or running. One monitor covers all of a requester's
%% items: a reader submits a fragment at a time and each extra monitor would
%% mean an extra 'DOWN' to correlate.
watch_requester(ReplyTo, #state{requesters = Requesters} = State) ->
    case Requesters of
        #{ReplyTo := {ReqMon, Count}} ->
            State#state{requesters = Requesters#{ReplyTo => {ReqMon, Count + 1}}};
        _ ->
            ReqMon = monitor(process, ReplyTo),
            State#state{requesters = Requesters#{ReplyTo => {ReqMon, 1}}}
    end.

%% Release one item's share of a requester's monitor, demonitoring once its last
%% outstanding item is gone - whether that item completed, was cancelled while
%% queued, or was dropped as dead. The requester's own 'DOWN' does not come
%% through here: requester_down/3 drops the whole entry in one step.
unwatch_requester(ReplyTo, #state{requesters = Requesters} = State) ->
    case Requesters of
        #{ReplyTo := {ReqMon, 1}} ->
            demonitor(ReqMon, [flush]),
            State#state{requesters = maps:remove(ReplyTo, Requesters)};
        #{ReplyTo := {ReqMon, Count}} ->
            State#state{requesters = Requesters#{ReplyTo => {ReqMon, Count - 1}}};
        _ ->
            State
    end.

schedule_refill() ->
    erlang:send_after(?REFILL_INTERVAL_MS, self(), refill).

%% Count an admitted transfer whose size exceeds the bucket's burst. Such a
%% transfer is admitted on credit (the bucket goes into debt) rather than
%% deadlocking. Bucket0 is the pre-request bucket; burst is invariant across
%% request/2 so reading it here is correct.
count_oversized(Size, Bucket) ->
    #{burst := Burst} = rabbitmq_stream_s3_token_bucket:info(Bucket),
    case Size > Burst of
        true -> inc(?C_OVERSIZED_ADMISSIONS, 1);
        false -> ok
    end.

%% ------------------------------------------------------------------
%% Counters
%% ------------------------------------------------------------------

-spec init_counters() -> ok.
init_counters() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?COUNTER_KEY, Cnt),
    ok.

counter() ->
    persistent_term:get(?COUNTER_KEY, undefined).

inc(Idx, N) ->
    case counter() of
        undefined -> ok;
        Cnt -> counters:add(Cnt, Idx, N)
    end.

set(Idx, V) ->
    case counter() of
        undefined -> ok;
        Cnt -> counters:put(Cnt, Idx, V)
    end.

%% Publish both gauges from the state they describe, rather than incrementing
%% and decrementing them independently. Called on every callback return that can
%% change `tasks` or `pending`, and from `init/1`, so a gauge is never more than
%% one in-progress callback away from the truth. See the moduledoc.
derive_gauges(#state{tasks = Tasks, pending = Pending} = State) ->
    set(?C_TASKS_IN_FLIGHT, map_size(Tasks)),
    set(?C_PENDING_SUBMISSIONS, queue:len(Pending)),
    State.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Barrier budget for await_state/2, generous because it is only ever paid in
%% full when a test is about to fail anyway.
-define(AWAIT_MS, 2000).
-define(AWAIT_INTERVAL_MS, 5).

unlimited_passes_through_test() ->
    {ok, Pid} = start_link(#{rate => unlimited}),
    Ref = make_ref(),
    Self = self(),
    submit(fun() -> {ok, done} end, 1000, Self, Ref),
    receive
        {transfer_result, Ref, {ok, done}} -> ok
    after 1000 ->
        error(timeout)
    end,
    gen_server:stop(Pid).

completion_routes_back_test() ->
    {ok, Pid} = start_link(#{rate => unlimited}),
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Self = self(),
    submit(fun() -> {ok, first} end, 100, Self, Ref1),
    submit(fun() -> {error, boom} end, 100, Self, Ref2),
    Results = collect_results([Ref1, Ref2], 1000),
    ?assertEqual({ok, first}, maps:get(Ref1, Results)),
    ?assertEqual({error, boom}, maps:get(Ref2, Results)),
    gen_server:stop(Pid).

%% cancel/1 on an admitted, running task kills it: no result is ever
%% delivered, and the task is dropped from the governor's bookkeeping.
cancel_kills_running_task_test() ->
    {ok, Pid} = start_link(#{rate => unlimited}),
    Self = self(),
    Ref = make_ref(),
    submit(
        fun() ->
            receive
                never -> ok
            end
        end,
        100,
        Self,
        Ref
    ),
    %% submit/1 and cancel/1 are both casts from this process to the same
    %% registered name, so send order is preserved: by the time cancel is
    %% handled, the task is already admitted and in `tasks`.
    cancel([Ref]),
    receive
        {transfer_result, Ref, _} -> error(unexpected_result)
    after 300 ->
        %% Also gives the kill and the resulting 'DOWN' time to land.
        ok
    end,
    ?assertEqual(#{}, (sys:get_state(Pid))#state.tasks),
    gen_server:stop(Pid).

%% cancel/1 on a still-queued item removes it before it is ever admitted.
cancel_removes_pending_item_test() ->
    {ok, Pid} = start_link(#{rate => 1000, burst => 1000}),
    Self = self(),
    RefA = make_ref(),
    RefQueued = make_ref(),
    submit(fun() -> {ok, a} end, 1000, Self, RefA),
    receive
        {transfer_result, RefA, _} -> ok
    after 1000 -> error(a_timeout)
    end,
    %% Bucket is now exhausted; this one queues (see try_admit/enqueue).
    submit(fun() -> error(should_not_run) end, 500, Self, RefQueued),
    cancel([RefQueued]),
    receive
        {transfer_result, RefQueued, _} -> error(unexpected_result)
    after 500 -> ok
    end,
    ?assertEqual(0, queue:len((sys:get_state(Pid))#state.pending)),
    gen_server:stop(Pid).

%% cancel/1 accepts a batch: a single call can kill an admitted, running task
%% and remove a still-queued item together, in one pass over the pending
%% queue.
cancel_batch_test() ->
    {ok, Pid} = start_link(#{rate => 1000, burst => 1000}),
    Self = self(),
    RefRunning = make_ref(),
    RefQueued = make_ref(),
    %% Bucket starts full: admitted immediately, then blocks forever,
    %% leaving the bucket exhausted.
    submit(
        fun() ->
            receive
                never -> ok
            end
        end,
        1000,
        Self,
        RefRunning
    ),
    %% Bucket is now exhausted; this one queues (see try_admit/enqueue).
    submit(fun() -> error(should_not_run) end, 500, Self, RefQueued),
    cancel([RefRunning, RefQueued]),
    receive
        {transfer_result, RefRunning, _} -> error(unexpected_result)
    after 300 ->
        %% Also gives the kill and the resulting 'DOWN' time to land.
        ok
    end,
    receive
        {transfer_result, RefQueued, _} -> error(unexpected_result)
    after 0 -> ok
    end,
    ?assertEqual(#{}, (sys:get_state(Pid))#state.tasks),
    ?assertEqual(0, queue:len((sys:get_state(Pid))#state.pending)),
    gen_server:stop(Pid).

%% A resubmit (a transient-retry resubmission reuses the same Ref) can install
%% a second, still-running task under a Ref while the earlier attempt is also
%% still running. Both attempts must be tracked independently, and a late
%% 'DOWN' from the earlier one must not disturb the live sibling.
resubmit_tracks_both_attempts_test() ->
    {ok, Pid} = start_link(#{rate => unlimited}),
    Self = self(),
    Ref = make_ref(),
    Fun = fun() ->
        receive
            never -> ok
        end
    end,
    submit(Fun, 100, Self, Ref),
    #state{tasks = Tasks0, tasks_by_ref = ByRef0} = sys:get_state(Pid),
    [OldMonRef] = maps:get(Ref, ByRef0),
    {OldTaskPid, Ref, Self} = maps:get(OldMonRef, Tasks0),
    %% Resubmit with the same Ref, as a transient-retry resubmission does. The
    %% earlier attempt is still running, so both are now tracked.
    submit(Fun, 100, Self, Ref),
    #state{tasks = Tasks1, tasks_by_ref = ByRef1} = sys:get_state(Pid),
    ?assertEqual(2, map_size(Tasks1)),
    [NewMonRef] = maps:get(Ref, ByRef1) -- [OldMonRef],
    {NewTaskPid, Ref, Self} = maps:get(NewMonRef, Tasks1),
    ?assertNotEqual(OldTaskPid, NewTaskPid),
    %% The earlier attempt's own 'DOWN' must drop only its entry, leaving the
    %% live sibling reachable.
    Pid ! {'DOWN', OldMonRef, process, OldTaskPid, normal},
    %% Sync: sys:get_status round-trips through the gen_server, guaranteeing
    %% the DOWN above has already been processed.
    _ = sys:get_status(Pid),
    #state{tasks = Tasks2, tasks_by_ref = ByRef2} = sys:get_state(Pid),
    ?assertEqual([NewMonRef], maps:get(Ref, ByRef2)),
    ?assertEqual({NewTaskPid, Ref, Self}, maps:get(NewMonRef, Tasks2)),
    %% cancel/1 must still be able to reach (and kill) the live task.
    cancel([Ref]),
    receive
        {transfer_result, Ref, _} -> error(unexpected_result)
    after 300 -> ok
    end,
    ?assertEqual(#{}, (sys:get_state(Pid))#state.tasks),
    ?assertEqual(#{}, (sys:get_state(Pid))#state.tasks_by_ref),
    gen_server:stop(Pid).

%% cancel/1 must kill EVERY live attempt under a Ref, not just the most recent
%% one. Before keying `tasks` by monitor reference, a same-Ref resubmit
%% overwrote the earlier attempt's entry, so cancel/1 killed only the newer
%% task and the earlier one kept streaming its fragment to S3 for a deleted
%% stream, holding an upload-pool connection and writing an orphan object.
cancel_kills_every_attempt_under_a_ref_test() ->
    {ok, Pid} = start_link(#{rate => unlimited}),
    Self = self(),
    Ref = make_ref(),
    Fun = fun() ->
        receive
            never -> ok
        end
    end,
    submit(Fun, 100, Self, Ref),
    submit(Fun, 100, Self, Ref),
    #state{tasks = Tasks, tasks_by_ref = ByRef} = sys:get_state(Pid),
    ?assertEqual(2, map_size(Tasks)),
    MonRefs = maps:get(Ref, ByRef),
    ?assertEqual(2, length(MonRefs)),
    Pids = [P || MonRef <- MonRefs, {P, _, _} <- [maps:get(MonRef, Tasks)]],
    Mons = [monitor(process, P) || P <- Pids],
    cancel([Ref]),
    %% Both attempts must die, not just the most recently installed one.
    lists:foreach(
        fun(Mon) ->
            receive
                {'DOWN', Mon, process, _, killed} -> ok
            after 1000 -> error(attempt_not_killed)
            end
        end,
        Mons
    ),
    ?assertEqual(#{}, (sys:get_state(Pid))#state.tasks),
    ?assertEqual(#{}, (sys:get_state(Pid))#state.tasks_by_ref),
    gen_server:stop(Pid).

%% cancel/1 on a Ref that is both admitted (a running task) and still queued
%% (a same-Ref resubmit while the earlier attempt is still running) must reach
%% both: the running task is killed and the queued copy is dropped. Before the
%% fix, finding a running task for the Ref filtered it out of the batch handed
%% to cancel_pending/2, so the queued copy survived and would later be admitted
%% and uploaded for an already-deleted stream.
cancel_reaches_admitted_and_queued_same_ref_test() ->
    {ok, Pid} = start_link(#{rate => 1000, burst => 1000}),
    Self = self(),
    Ref = make_ref(),
    %% Attempt 1: admitted immediately (bucket full), then blocks forever,
    %% leaving the bucket exhausted.
    submit(
        fun() ->
            receive
                never -> ok
            end
        end,
        1000,
        Self,
        Ref
    ),
    %% Attempt 2 under the SAME Ref: bucket is exhausted, so it queues. The Ref
    %% is now both in `tasks` (attempt 1) and in `pending` (attempt 2).
    submit(fun() -> error(should_not_run) end, 500, Self, Ref),
    #state{tasks_by_ref = ByRef, pending = Pending} = sys:get_state(Pid),
    ?assert(is_map_key(Ref, ByRef)),
    ?assertEqual(1, queue:len(Pending)),
    cancel([Ref]),
    receive
        {transfer_result, Ref, _} -> error(unexpected_result)
    after 300 ->
        %% Also gives the kill and the resulting 'DOWN' time to land.
        ok
    end,
    ?assertEqual(#{}, (sys:get_state(Pid))#state.tasks),
    ?assertEqual(0, queue:len((sys:get_state(Pid))#state.pending)),
    gen_server:stop(Pid).

%% A planned stop (gen_server:stop/1, reason `normal`) kills every
%% still-running task rather than leaving it to finish on its own.
terminate_kills_tasks_on_normal_stop_test() ->
    {ok, Pid} = start_link(#{rate => unlimited}),
    Self = self(),
    Ref = make_ref(),
    submit(
        fun() ->
            receive
                never -> ok
            end
        end,
        100,
        Self,
        Ref
    ),
    #state{tasks = Tasks, tasks_by_ref = ByRef} = sys:get_state(Pid),
    [MonRef] = maps:get(Ref, ByRef),
    {TaskPid, Ref, Self} = maps:get(MonRef, Tasks),
    Mon = monitor(process, TaskPid),
    gen_server:stop(Pid),
    receive
        {'DOWN', Mon, process, TaskPid, killed} -> ok
    after 1000 -> error(task_not_killed)
    end.

%% terminate/2 leaves tasks alone for any reason other than a planned
%% shutdown - specifically so a governor crash-restart does not kill uploads
%% already in flight (see moduledoc).
terminate_leaves_tasks_on_crash_reason_test() ->
    TaskPid = spawn(fun() ->
        receive
            never -> ok
        end
    end),
    MonRef = monitor(process, TaskPid),
    Ref = make_ref(),
    State = #state{
        bucket = unlimited,
        pending = queue:new(),
        tasks = #{MonRef => {TaskPid, Ref, self()}},
        tasks_by_ref = #{Ref => [MonRef]}
    },
    ok = terminate({error, some_crash}, State),
    timer:sleep(50),
    ?assert(is_process_alive(TaskPid)),
    exit(TaskPid, kill).

%% `trap_exit` is set for the parent-shutdown path, but `gen_server` routes
%% only the parent's signal to `terminate/2`: every other link's signal is
%% delivered to `handle_info/2`. Without an explicit clause the catchall
%% swallowed those, leaving the governor running where an untrapped process
%% would have died - a link's crash became invisible.
abnormal_exit_from_non_parent_link_stops_test() ->
    {ok, Pid} = start_link(#{rate => unlimited}),
    %% start_link/1 links the governor to this test process, which is therefore
    %% its parent. Drop that link before provoking the stop, or the governor's
    %% exit reason propagates here and fails the test from the outside.
    unlink(Pid),
    Mon = monitor(process, Pid),
    %% A process other than the parent links itself and then crashes. `link/1`
    %% runs before the receive, so the link is established once `go` arrives.
    Linker = spawn(fun() ->
        link(Pid),
        receive
            go -> exit(boom)
        end
    end),
    Linker ! go,
    receive
        {'DOWN', Mon, process, Pid, boom} -> ok
    after ?AWAIT_MS -> error(governor_survived_linked_crash)
    end.

%% A `normal` exit from a link is ignored, matching the untrapped default. The
%% governor must not stop because some linked helper finished cleanly.
normal_exit_from_non_parent_link_is_ignored_test() ->
    {ok, Pid} = start_link(#{rate => unlimited}),
    Linker = spawn(fun() ->
        link(Pid),
        receive
            go -> ok
        end
    end),
    LinkerMon = monitor(process, Linker),
    Linker ! go,
    %% Wait for the linker to actually exit: only then is its signal guaranteed
    %% to be in the governor's queue, so the round-trip below is ordered after
    %% it. Asserting liveness without this barrier would pass either way.
    receive
        {'DOWN', LinkerMon, process, Linker, normal} -> ok
    after ?AWAIT_MS -> error(linker_did_not_exit)
    end,
    _ = sys:get_state(Pid),
    ?assert(is_process_alive(Pid)),
    gen_server:stop(Pid).

%% A requester that dies after its task was admitted must have that task
%% killed. Only one reader teardown path calls cancel/1, so without the
%% requester monitor a writer-DOWN stop, a crash, a supervisor shutdown or a
%% brutal kill all left the upload streaming a fragment to S3 for a stream
%% nobody is reading, holding an upload-pool connection.
requester_death_kills_running_task_test() ->
    {ok, Pid} = start_link(#{rate => unlimited}),
    Parent = self(),
    %% Stands in for a replica reader: submits, then dies without cancelling.
    Requester = spawn(fun() ->
        Ref = make_ref(),
        submit(
            fun() ->
                receive
                    never -> ok
                end
            end,
            100,
            self(),
            Ref
        ),
        %% Sync inside the requester: the state read below must see the task.
        _ = sys:get_status(?MODULE),
        Parent ! submitted,
        receive
            die -> ok
        end
    end),
    receive
        submitted -> ok
    after 1000 -> error(setup_failed)
    end,
    #state{tasks = Tasks, requesters = Requesters} = sys:get_state(Pid),
    [{_MonRef, {TaskPid, _Ref, Requester}}] = maps:to_list(Tasks),
    ?assert(is_map_key(Requester, Requesters)),
    TaskMon = monitor(process, TaskPid),
    Requester ! die,
    %% The task is killed as a direct consequence of the requester's death.
    receive
        {'DOWN', TaskMon, process, TaskPid, killed} -> ok
    after 1000 -> error(task_not_killed)
    end,
    %% This 'DOWN' is delivered to the test; the governor's own copy is a
    %% separate signal with no ordering relation to it, so await the governor's
    %% state rather than assuming its 'DOWN' has already been processed.
    await_state(Pid, fun(#state{tasks = T, tasks_by_ref = B, requesters = R}) ->
        T =:= #{} andalso B =:= #{} andalso R =:= #{}
    end),
    gen_server:stop(Pid).

%% A dying requester's still-queued submissions are dropped as well as its
%% running ones: admitting a queued item after its requester is gone would
%% spend tokens uploading a fragment whose result has nowhere to go.
requester_death_drops_queued_submissions_test() ->
    {ok, Pid} = start_link(#{rate => 1000, burst => 1000}),
    Parent = self(),
    Requester = spawn(fun() ->
        %% Attempt 1 is admitted immediately and blocks, exhausting the bucket.
        submit(
            fun() ->
                receive
                    never -> ok
                end
            end,
            1000,
            self(),
            make_ref()
        ),
        %% A second Ref queues behind the exhausted bucket.
        submit(fun() -> error(should_not_run) end, 500, self(), make_ref()),
        _ = sys:get_status(?MODULE),
        Parent ! submitted,
        receive
            die -> ok
        end
    end),
    receive
        submitted -> ok
    after 1000 -> error(setup_failed)
    end,
    ?assertEqual(1, map_size((sys:get_state(Pid))#state.tasks)),
    ?assertEqual(1, queue:len((sys:get_state(Pid))#state.pending)),
    Requester ! die,
    %% Two 'DOWN's have to land: the requester's, which kills the task and
    %% drops the queued item, then the killed task's own, which clears the
    %% bookkeeping. Await the end state rather than guessing how long that takes.
    await_state(Pid, fun(#state{tasks = T, pending = P, requesters = R}) ->
        T =:= #{} andalso queue:is_empty(P) andalso R =:= #{}
    end),
    gen_server:stop(Pid).

%% One requester's death must not disturb another's tasks. The requester
%% monitor is refcounted per requester, so the surviving reader keeps both its
%% task and its monitor.
requester_death_spares_other_requesters_test() ->
    {ok, Pid} = start_link(#{rate => unlimited}),
    Parent = self(),
    Blocker = fun() ->
        submit(
            fun() ->
                receive
                    never -> ok
                end
            end,
            100,
            self(),
            make_ref()
        ),
        _ = sys:get_status(?MODULE),
        Parent ! submitted,
        receive
            die -> ok
        end
    end,
    Doomed = spawn(Blocker),
    receive
        submitted -> ok
    after 1000 -> error(setup_failed)
    end,
    Survivor = spawn(Blocker),
    receive
        submitted -> ok
    after 1000 -> error(setup_failed)
    end,
    #state{tasks = Tasks} = sys:get_state(Pid),
    ?assertEqual(2, map_size(Tasks)),
    [SurvivorTask] = [P || {P, _R, Owner} <- maps:values(Tasks), Owner =:= Survivor],
    Doomed ! die,
    %% Await the doomed requester being fully reaped, then assert the survivor
    %% was left untouched. Awaiting the reap first is what makes the survivor
    %% assertions meaningful: they run after the governor has finished acting
    %% on a death, not before it has started.
    await_state(Pid, fun(#state{tasks = T, requesters = R}) ->
        map_size(T) =:= 1 andalso maps:keys(R) =:= [Survivor]
    end),
    ?assert(is_process_alive(SurvivorTask)),
    #state{tasks = Tasks1} = sys:get_state(Pid),
    ?assertEqual([Survivor], [Owner || {_P, _R, Owner} <- maps:values(Tasks1)]),
    Survivor ! die,
    gen_server:stop(Pid).

%% A requester submitting several transfers is monitored once, and the monitor
%% is released only when its last task is gone: an unbalanced refcount would
%% either leak monitors or stop watching a requester that still has uploads
%% running.
requester_monitor_is_refcounted_test() ->
    {ok, Pid} = start_link(#{rate => unlimited}),
    Self = self(),
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Fun = fun() ->
        receive
            never -> ok
        end
    end,
    submit(Fun, 100, Self, Ref1),
    submit(Fun, 100, Self, Ref2),
    #state{requesters = Requesters} = sys:get_state(Pid),
    ?assertMatch(#{Self := {_Mon, 2}}, Requesters),
    %% One task ends: the requester is still watched, with one task left.
    cancel([Ref1]),
    await_state(Pid, fun(#state{requesters = R}) ->
        case R of
            #{Self := {_Mon, Count}} -> Count =:= 1;
            _ -> false
        end
    end),
    %% The last one ends: the monitor is released.
    cancel([Ref2]),
    await_state(Pid, fun(#state{requesters = R}) -> R =:= #{} end),
    gen_server:stop(Pid).

%% A submission whose ReplyTo is already dead is dropped at intake, before
%% ever touching the bucket or spawning anything.
dispatch_drops_dead_replyto_test() ->
    {ok, Pid} = start_link(#{rate => unlimited}),
    DeadPid = spawn(fun() -> ok end),
    Mon = monitor(process, DeadPid),
    receive
        {'DOWN', Mon, process, DeadPid, _} -> ok
    after 1000 -> error(setup_failed)
    end,
    submit(fun() -> error(should_not_run) end, 100, DeadPid, make_ref()),
    %% Sync: a second, live submission proves the first cast was processed.
    Self = self(),
    SyncRef = make_ref(),
    submit(fun() -> {ok, sync} end, 1, Self, SyncRef),
    receive
        {transfer_result, SyncRef, _} -> ok
    after 1000 -> error(sync_timeout)
    end,
    ?assertEqual(#{}, (sys:get_state(Pid))#state.tasks),
    gen_server:stop(Pid).

%% A queued item whose ReplyTo dies before its turn to drain is dropped for
%% free, without ever being admitted.
drain_drops_dead_replyto_test() ->
    {ok, Pid} = start_link(#{rate => 1000, burst => 1000}),
    Self = self(),
    DeadPid = spawn(fun() -> ok end),
    Mon = monitor(process, DeadPid),
    receive
        {'DOWN', Mon, process, DeadPid, _} -> ok
    after 1000 -> error(setup_failed)
    end,
    RefA = make_ref(),
    submit(fun() -> {ok, a} end, 1000, Self, RefA),
    receive
        {transfer_result, RefA, _} -> ok
    after 1000 -> error(a_timeout)
    end,
    %% Bucket is now exhausted; this queues behind it with a dead ReplyTo.
    %% The periodic refill timer (armed by enqueue/2) may drop it on its own
    %% before the explicit refill below, so this doesn't assert on the
    %% queue's length in between - only that the item never runs and the
    %% queue eventually settles at 0.
    submit(fun() -> error(should_not_run) end, 500, DeadPid, make_ref()),
    %% Force a drain synchronously rather than relying solely on the timer.
    Pid ! refill,
    RefSync = make_ref(),
    submit(fun() -> {ok, sync} end, 1, Self, RefSync),
    receive
        {transfer_result, RefSync, _} -> ok
    after 1000 -> error(sync_timeout)
    end,
    ?assertEqual(0, queue:len((sys:get_state(Pid))#state.pending)),
    gen_server:stop(Pid).

%% A dead requester's queued items must be reclaimed even when they are buried
%% behind a live item that cannot be admitted. drain_pending/1 only ever looks at
%% the head and stops at the first item short of tokens, so before the requester
%% was monitored from intake these items sat in `pending` for as long as the head
%% was stuck: counted in the gauge, uncounted in dropped_dead_replyto, and
%% holding their funs (and whatever those close over) live.
requester_death_drops_items_buried_behind_a_live_head_test() ->
    with_counter(fun(Cnt) ->
        %% Rate 1000 B/s, burst 1000. The first item exhausts the bucket.
        {ok, Pid} = start_link(#{rate => 1000, burst => 1000}),
        Self = self(),
        RefDrain = make_ref(),
        submit(fun() -> {ok, drained} end, 1000, Self, RefDrain),
        receive
            {transfer_result, RefDrain, _} -> ok
        after 1000 -> error(drain_timeout)
        end,
        %% A live requester's item at the head: exactly burst, so it is not
        %% "oversized" (which would be admitted on credit) but there are no
        %% tokens, so it stays queued and blocks the drain.
        submit(fun() -> {ok, head} end, 1000, Self, make_ref()),
        %% Three items from a requester that dies, buried behind that head.
        Parent = self(),
        Requester = spawn(fun() ->
            [submit(fun() -> error(should_not_run) end, 1, self(), make_ref()) || _ <- [1, 2, 3]],
            _ = sys:get_status(?MODULE),
            Parent ! submitted,
            receive
                die -> ok
            end
        end),
        receive
            submitted -> ok
        after 1000 -> error(setup_failed)
        end,
        ?assertEqual(4, queue:len((sys:get_state(Pid))#state.pending)),
        Before = counters:get(Cnt, ?C_DROPPED_DEAD_REPLYTO),
        Requester ! die,
        %% All three go on the requester's 'DOWN', without waiting for the head
        %% to be admitted. Only the live head is left.
        await_state(Pid, fun(#state{pending = P, requesters = R}) ->
            queue:len(P) =:= 1 andalso not maps:is_key(Requester, R)
        end),
        ?assertEqual(Before + 3, counters:get(Cnt, ?C_DROPPED_DEAD_REPLYTO)),
        %% The gauge follows the queue, and the surviving head is the live one.
        ?assertEqual(1, counters:get(Cnt, ?C_PENDING_SUBMISSIONS)),
        [{_Fun, _Size, HeadReplyTo, _Ref}] = queue:to_list((sys:get_state(Pid))#state.pending),
        ?assertEqual(Self, HeadReplyTo),
        gen_server:stop(Pid)
    end).

%% A requester with only queued work - nothing admitted - must still be
%% monitored. Watching from spawn_task/2 alone left this requester invisible.
queued_only_requester_is_monitored_test() ->
    {ok, Pid} = start_link(#{rate => 1000, burst => 1000}),
    Self = self(),
    RefDrain = make_ref(),
    submit(fun() -> {ok, drained} end, 1000, Self, RefDrain),
    receive
        {transfer_result, RefDrain, _} -> ok
    after 1000 -> error(drain_timeout)
    end,
    Parent = self(),
    Requester = spawn(fun() ->
        submit(fun() -> error(should_not_run) end, 500, self(), make_ref()),
        _ = sys:get_status(?MODULE),
        Parent ! submitted,
        receive
            die -> ok
        end
    end),
    receive
        submitted -> ok
    after 1000 -> error(setup_failed)
    end,
    %% The drain task's result arrives before its 'DOWN' is processed, so wait
    %% for `tasks` to settle empty rather than asserting it outright. The
    %% requester holds only a queued item, no admitted task, yet must be watched.
    await_state(Pid, fun(#state{tasks = T, requesters = R}) ->
        map_size(T) =:= 0 andalso maps:is_key(Requester, R)
    end),
    Requester ! die,
    await_state(Pid, fun(#state{pending = P, requesters = R}) ->
        queue:is_empty(P) andalso R =:= #{}
    end),
    gen_server:stop(Pid).

pacing_delays_when_exhausted_test() ->
    %% Rate: 1000 bytes/sec, burst: 1000 bytes.
    %% Submit 1500 bytes. First 1000 should go immediately,
    %% remaining 500 should wait for refill.
    {ok, Pid} = start_link(#{rate => 1000, burst => 1000}),
    Ref1 = make_ref(),
    Ref2 = make_ref(),
    Self = self(),
    submit(fun() -> {ok, a} end, 1000, Self, Ref1),
    submit(fun() -> {ok, b} end, 500, Self, Ref2),
    %% First should complete quickly.
    receive
        {transfer_result, Ref1, {ok, a}} -> ok
    after 200 ->
        error(first_timeout)
    end,
    %% Second should be delayed (needs refill).
    receive
        {transfer_result, Ref2, {ok, b}} -> ok
    after 2000 ->
        error(second_timeout)
    end,
    gen_server:stop(Pid).

%% A transfer larger than the burst must not deadlock the governor. It is
%% admitted on credit (the bucket goes into debt), and a smaller transfer
%% queued behind it must still complete once the debt is repaid. Before the
%% fix the oversized item at the head of the queue blocked everything forever.
oversized_transfer_does_not_deadlock_test() ->
    %% rate 1000 B/s, burst 1000 B; transfer of 5000 B exceeds burst.
    {ok, Pid} = start_link(#{rate => 1000, burst => 1000}),
    Self = self(),
    RefBig = make_ref(),
    RefSmall = make_ref(),
    submit(fun() -> {ok, big} end, 5000, Self, RefBig),
    submit(fun() -> {ok, small} end, 100, Self, RefSmall),
    %% Debt of ~4000 tokens repays at 1000 B/s, so the small item clears in
    %% ~4-5s. Generous timeout; the point is that it completes at all.
    Results = collect_results([RefBig, RefSmall], 15000),
    ?assertEqual({ok, big}, maps:get(RefBig, Results)),
    ?assertEqual({ok, small}, maps:get(RefSmall, Results)),
    gen_server:stop(Pid).

%% A submission must not jump ahead of items already waiting in the queue.
%% Before the fix each new submission checked the bucket directly, so a steady
%% stream of small transfers could permanently starve a larger transfer stuck
%% at the head of the queue: the small ones drained the very tokens the queued
%% item was waiting to accumulate. With FIFO admission, once anything is queued
%% later submissions queue behind it and only drain_pending admits, head first.
queued_item_is_not_starved_by_later_submissions_test() ->
    {ok, Pid} = start_link(#{rate => 1000, burst => 1000}),
    Self = self(),
    RefA = make_ref(),
    RefHead = make_ref(),
    RefSmall = make_ref(),
    %% Drain the bucket, then queue a large item behind the drain.
    submit(fun() -> {ok, a} end, 1000, Self, RefA),
    submit(fun() -> {ok, head} end, 800, Self, RefHead),
    receive
        {transfer_result, RefA, _} -> ok
    after 1000 -> error(a_timeout)
    end,
    %% Let the bucket partially refill: enough for the small item, not yet for
    %% the queued head. A small item submitted now must not jump ahead.
    timer:sleep(300),
    submit(fun() -> {ok, small} end, 100, Self, RefSmall),
    %% The head (submitted first) must complete before the later small item.
    ?assertEqual([RefHead, RefSmall], collect_order([RefHead, RefSmall], 5000)),
    gen_server:stop(Pid).

%% The tasks gauge must equal `map_size(tasks)` at all times, including after a
%% crash and restart. The counters live in persistent_term and outlive the
%% process; `tasks` does not. An inc/dec gauge ratcheted permanently here: a
%% crash leaves each running task's 'DOWN' in a dead mailbox, so the decrement
%% is never issued, and the new incarnation starts from `tasks = #{}` with
%% nothing left to decrement.
tasks_gauge_is_derived_from_state_test() ->
    with_counter(fun(Cnt) ->
        ?assertEqual(0, counters:get(Cnt, ?C_TASKS_IN_FLIGHT)),
        {ok, Pid} = start_link(#{rate => unlimited}),
        Self = self(),
        Block = fun() ->
            receive
                never -> ok
            end
        end,
        submit(Block, 100, Self, make_ref()),
        submit(Block, 100, Self, make_ref()),
        await_state(Pid, fun(#state{tasks = T}) -> map_size(T) =:= 2 end),
        ?assertEqual(2, counters:get(Cnt, ?C_TASKS_IN_FLIGHT)),
        #state{tasks = Tasks} = sys:get_state(Pid),
        TaskPids = [P || {P, _Ref, _Owner} <- maps:values(Tasks)],
        %% Kill the governor outright, as a supervisor shutdown timeout does.
        %% No terminate/2 runs, and the tasks outlive it (they are monitored,
        %% not linked), so both 'DOWN's are lost with the mailbox.
        unlink(Pid),
        Mon = monitor(process, Pid),
        exit(Pid, kill),
        receive
            {'DOWN', Mon, process, Pid, _} -> ok
        after 1000 -> error(governor_survived_kill)
        end,
        %% The restart the supervisor would do. init/1 publishes from the empty
        %% state, so the stale 2 is corrected rather than inherited forever.
        {ok, Pid2} = start_link(#{rate => unlimited}),
        ?assertEqual(0, counters:get(Cnt, ?C_TASKS_IN_FLIGHT)),
        lists:foreach(fun(P) -> exit(P, kill) end, TaskPids),
        gen_server:stop(Pid2)
    end).

%% The pending gauge must equal `queue:len(pending)`, including on the paths
%% that shrink the queue without admitting anything.
pending_gauge_is_derived_from_state_test() ->
    with_counter(fun(Cnt) ->
        {ok, Pid} = start_link(#{rate => 1000, burst => 1000}),
        Self = self(),
        RefA = make_ref(),
        RefQueued = make_ref(),
        %% Drain the bucket so the next submission has to queue.
        submit(fun() -> {ok, a} end, 1000, Self, RefA),
        receive
            {transfer_result, RefA, _} -> ok
        after 1000 -> error(a_timeout)
        end,
        submit(fun() -> {ok, queued} end, 900, Self, RefQueued),
        await_state(Pid, fun(#state{pending = P}) -> queue:len(P) =:= 1 end),
        ?assertEqual(1, counters:get(Cnt, ?C_PENDING_SUBMISSIONS)),
        cancel([RefQueued]),
        await_state(Pid, fun(#state{pending = P}) -> queue:is_empty(P) end),
        ?assertEqual(0, counters:get(Cnt, ?C_PENDING_SUBMISSIONS)),
        gen_server:stop(Pid)
    end).

%% Run Fun with a private counter installed, restoring whatever was there
%% before. The key is global to the node, so a test must not leave the module's
%% real counter erased or replaced for whatever runs next.
with_counter(Fun) ->
    Previous = persistent_term:get(?COUNTER_KEY, undefined),
    Cnt = counters:new(length(?COUNTERS), []),
    persistent_term:put(?COUNTER_KEY, Cnt),
    try
        Fun(Cnt)
    after
        case Previous of
            undefined -> persistent_term:erase(?COUNTER_KEY);
            _ -> persistent_term:put(?COUNTER_KEY, Previous)
        end
    end.

%% Block until the governor's own state satisfies Fun, or fail the test.
%% A killed task's 'DOWN' is delivered to the governor asynchronously, so the
%% bookkeeping it drives (tasks, tasks_by_ref, requesters, pending) settles
%% some time after the kill. Poll the state rather than sleeping a guessed
%% duration: a sleep is either flaky under load or slower than it needs to be.
%% sys:get_state is a sound barrier here because the governor is a plain
%% gen_server (see docs/conventions.md on gen_batch_server, where it is not).
await_state(Pid, Fun) ->
    await_state(Pid, Fun, ?AWAIT_MS).

await_state(Pid, _Fun, Remaining) when Remaining =< 0 ->
    error({timeout_awaiting_state, sys:get_state(Pid)});
await_state(Pid, Fun, Remaining) ->
    case Fun(sys:get_state(Pid)) of
        true ->
            ok;
        false ->
            timer:sleep(?AWAIT_INTERVAL_MS),
            await_state(Pid, Fun, Remaining - ?AWAIT_INTERVAL_MS)
    end.

collect_results(Refs, Timeout) ->
    collect_results(Refs, Timeout, #{}).
collect_results([], _Timeout, Acc) ->
    Acc;
collect_results([Ref | Rest], Timeout, Acc) ->
    receive
        {transfer_result, Ref, Result} ->
            collect_results(Rest, Timeout, Acc#{Ref => Result})
    after Timeout ->
        error({timeout_waiting_for, Ref})
    end.

%% Record the order in which the given refs' results arrive, ignoring others.
collect_order(Refs, Timeout) ->
    collect_order(Refs, Timeout, []).
collect_order([], _Timeout, Acc) ->
    lists:reverse(Acc);
collect_order(Refs, Timeout, Acc) ->
    receive
        {transfer_result, Ref, _} ->
            case lists:member(Ref, Refs) of
                true -> collect_order(Refs -- [Ref], Timeout, [Ref | Acc]);
                false -> collect_order(Refs, Timeout, Acc)
            end
    after Timeout ->
        error({timeout_waiting_for, Refs})
    end.

-endif.
