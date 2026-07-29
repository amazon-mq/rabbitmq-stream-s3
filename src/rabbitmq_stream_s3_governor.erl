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

A caller can cancel a batch of submissions by their `Ref`s via `cancel/1`: an
admitted task is killed, a still-queued item is dropped before ever being
spawned, and the pending queue is scanned once for the whole batch.
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
    %% Ref => {Pid, MonRef} for every task currently executing, so cancel/1
    %% can reach an admitted task by its caller-minted Ref.
    tasks = #{} :: #{reference() => {pid(), reference()}},
    %% MonRef => Ref, the reverse index the 'DOWN' handler uses to find which
    %% submission a completed, crashed or killed monitor belonged to.
    tasks_rev = #{} :: #{reference() => reference()}
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
    %% Counter is created by init_counters/0 from the API init step.
    {ok, #state{bucket = Bucket, pending = queue:new(), timer_ref = TimerRef}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast({submit, Fun, Size, ReplyTo, Ref}, State) ->
    inc(?C_SUBMISSIONS_RECEIVED, 1),
    case is_process_alive(ReplyTo) of
        true ->
            {noreply, dispatch({Fun, Size, ReplyTo, Ref}, State)};
        false ->
            %% The requester died between calling submit/4 and this cast
            %% landing (e.g. a stream deletion or a crash). Drop it now
            %% rather than pacing, spawning, and uploading for nobody: its
            %% result would just be dropped into a dead mailbox anyway.
            inc(?C_DROPPED_DEAD_REPLYTO, 1),
            {noreply, State}
    end;
handle_cast({cancel, Refs0}, #state{tasks = Tasks} = State) ->
    %% tasks/tasks_rev removal and the TASKS_IN_FLIGHT decrement happen in
    %% the 'DOWN' handler below, the single cleanup path shared by normal
    %% completion, a crash and a kill alike.
    Refs = lists:filter(
        fun(Ref) ->
            case Tasks of
                #{Ref := {Pid, _MonRef}} ->
                    exit(Pid, kill),
                    false;
                #{} ->
                    true
            end
        end,
        Refs0
    ),
    {noreply, cancel_pending(Refs, State)};
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
    {noreply, State2#state{timer_ref = TimerRef}};
handle_info(
    {'DOWN', MonRef, process, _Pid, _Reason},
    #state{tasks = Tasks, tasks_rev = TasksRev} = State
) ->
    case maps:take(MonRef, TasksRev) of
        {Ref, TasksRev1} ->
            dec(?C_TASKS_IN_FLIGHT, 1),
            %% A resubmit (same-Ref retry) may have already overwritten this
            %% Ref's entry with a newer, still-running task by the time this
            %% DOWN (from the earlier attempt) is processed. Only remove the
            %% entry if it still belongs to this MonRef, so a stale DOWN can't
            %% erase a live task and leave it unreachable by cancel/1.
            Tasks1 =
                case Tasks of
                    #{Ref := {_, MonRef}} -> maps:remove(Ref, Tasks);
                    #{} -> Tasks
                end,
            {noreply, State#state{tasks = Tasks1, tasks_rev = TasksRev1}};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

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
    maps:foreach(fun(_Ref, {Pid, _MonRef}) -> exit(Pid, kill) end, Tasks),
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

dispatch(Item, #state{bucket = unlimited} = State) ->
    spawn_task(Item, State);
dispatch(Item, #state{pending = Pending} = State) ->
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
cancel_pending(Refs, #state{pending = Pending0} = State) ->
    RefSet = sets:from_list(Refs, [{version, 2}]),
    Pending = queue:filter(
        fun({_Fun, _Size, _ReplyTo, ItemRef}) -> not sets:is_element(ItemRef, RefSet) end,
        Pending0
    ),
    Len0 = queue:len(Pending0),
    Len = queue:len(Pending),
    case Len of
        Len0 ->
            State;
        _ ->
            set(?C_PENDING_SUBMISSIONS, Len),
            State#state{pending = Pending}
    end.

%% Append a submission to the pending queue and ensure the refill timer is
%% running so it will eventually drain.
enqueue(Item, State) ->
    Pending = queue:in(Item, State#state.pending),
    set(?C_PENDING_SUBMISSIONS, queue:len(Pending)),
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
                    %% token bucket. Drop it for free (no tokens spent) and
                    %% keep draining: the head is being discarded outright,
                    %% not skipped-and-retried, so this doesn't let a later
                    %% item unfairly jump the FIFO order (see enqueue/2).
                    inc(?C_DROPPED_DEAD_REPLYTO, 1),
                    Pending = queue:drop(Pending0),
                    set(?C_PENDING_SUBMISSIONS, queue:len(Pending)),
                    drain_pending(State#state{pending = Pending});
                true ->
                    drain_pending_admit(Item, Pending0, Bucket0, State)
            end
    end.

drain_pending_admit({_Fun, Size, _ReplyTo, _Ref} = Item, Pending0, Bucket0, State) ->
    case rabbitmq_stream_s3_token_bucket:request(Size, Bucket0) of
        {ok, Bucket} ->
            count_oversized(Size, Bucket0),
            Pending = queue:drop(Pending0),
            set(?C_PENDING_SUBMISSIONS, queue:len(Pending)),
            drain_pending(
                spawn_task(Item, State#state{
                    pending = Pending,
                    bucket = Bucket
                })
            );
        {insufficient, _, _} ->
            State
    end.

spawn_task({Fun, _Size, ReplyTo, Ref}, #state{tasks = Tasks, tasks_rev = TasksRev} = State) ->
    inc(?C_TASKS_IN_FLIGHT, 1),
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
    State#state{
        tasks = Tasks#{Ref => {Pid, MonRef}},
        tasks_rev = TasksRev#{MonRef => Ref}
    }.

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

dec(Idx, N) ->
    case counter() of
        undefined -> ok;
        Cnt -> counters:sub(Cnt, Idx, N)
    end.

set(Idx, V) ->
    case counter() of
        undefined -> ok;
        Cnt -> counters:put(Cnt, Idx, V)
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

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

%% Regression test for a Ref-reuse race: a resubmit (a transient-retry
%% resubmission reuses the same Ref) can install a new, live task under a Ref
%% before the earlier attempt's own 'DOWN' (sent by the runtime when that
%% attempt's process exits) is processed by the governor. Before the identity
%% check in the 'DOWN' handler, that stale DOWN would erase the live task's
%% entry, leaving it unreachable - and therefore unkillable - by cancel/1.
resubmit_survives_stale_down_test() ->
    {ok, Pid} = start_link(#{rate => unlimited}),
    Self = self(),
    Ref = make_ref(),
    Fun = fun() ->
        receive
            never -> ok
        end
    end,
    submit(Fun, 100, Self, Ref),
    #state{tasks = Tasks0} = sys:get_state(Pid),
    {OldTaskPid, OldMonRef} = maps:get(Ref, Tasks0),
    %% Resubmit with the same Ref, as a transient-retry resubmission does:
    %% installs a new, still-running task under the same key.
    submit(Fun, 100, Self, Ref),
    #state{tasks = Tasks1} = sys:get_state(Pid),
    {NewTaskPid, NewMonRef} = maps:get(Ref, Tasks1),
    ?assertNotEqual({OldTaskPid, OldMonRef}, {NewTaskPid, NewMonRef}),
    %% Simulate the earlier attempt's own 'DOWN' arriving late, after the
    %% resubmit already overwrote the entry.
    Pid ! {'DOWN', OldMonRef, process, OldTaskPid, normal},
    %% Sync: sys:get_status round-trips through the gen_server, guaranteeing
    %% the DOWN above has already been processed.
    _ = sys:get_status(Pid),
    ?assertEqual({NewTaskPid, NewMonRef}, maps:get(Ref, (sys:get_state(Pid))#state.tasks)),
    %% cancel/1 must still be able to reach (and kill) the live task.
    cancel([Ref]),
    receive
        {transfer_result, Ref, _} -> error(unexpected_result)
    after 300 -> ok
    end,
    ?assertEqual(#{}, (sys:get_state(Pid))#state.tasks),
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
    #state{tasks = Tasks} = sys:get_state(Pid),
    {TaskPid, _MonRef} = maps:get(Ref, Tasks),
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
        tasks = #{Ref => {TaskPid, MonRef}},
        tasks_rev = #{MonRef => Ref}
    },
    ok = terminate({error, some_crash}, State),
    timer:sleep(50),
    ?assert(is_process_alive(TaskPid)),
    exit(TaskPid, kill).

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
