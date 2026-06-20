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
""".

-behaviour(gen_server).

-export([start_link/1, submit/4]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    format_status/1
]).

-export([init_counters/0]).

-include("include/logging.hrl").

-define(REFILL_INTERVAL_MS, 100).

%% Per-node counters.
-define(C_SUBMISSIONS_RECEIVED, 1).
-define(C_TASKS_IN_FLIGHT, 2).
-define(C_PENDING_SUBMISSIONS, 3).
-define(C_OVERSIZED_ADMISSIONS, 4).
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
        "means the configured burst is smaller than typical fragments"}
]).
-define(COUNTER_KEY, {?MODULE, counter}).

-record(state, {
    bucket :: rabbitmq_stream_s3_token_bucket:t() | unlimited,
    %% Pending submissions waiting for tokens.
    pending :: queue:queue(pending_item()),
    timer_ref :: reference() | undefined
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

%% ------------------------------------------------------------------
%% gen_server callbacks
%% ------------------------------------------------------------------

init(Opts) ->
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
    {noreply, dispatch({Fun, Size, ReplyTo, Ref}, State)};
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
handle_info(_Info, State) ->
    {noreply, State}.

format_status(#{state := State} = Status) ->
    Status#{state := format_state(State)}.

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
    spawn_task(Item),
    State;
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
            spawn_task(Item),
            State#state{bucket = Bucket};
        {insufficient, _, _} ->
            enqueue(Item, State)
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
        {value, {_Fun, Size, _ReplyTo, _Ref} = Item} ->
            case rabbitmq_stream_s3_token_bucket:request(Size, Bucket0) of
                {ok, Bucket} ->
                    count_oversized(Size, Bucket0),
                    spawn_task(Item),
                    Pending = queue:drop(Pending0),
                    set(?C_PENDING_SUBMISSIONS, queue:len(Pending)),
                    drain_pending(State#state{
                        pending = Pending,
                        bucket = Bucket
                    });
                {insufficient, _, _} ->
                    State
            end
    end.

spawn_task({Fun, _Size, ReplyTo, Ref}) ->
    inc(?C_TASKS_IN_FLIGHT, 1),
    spawn(fun() ->
        logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
        Result =
            try
                Fun()
            catch
                Class:Reason -> {error, {Class, Reason}}
            end,
        dec(?C_TASKS_IN_FLIGHT, 1),
        ReplyTo ! {transfer_result, Ref, Result}
    end).

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
