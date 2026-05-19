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
-define(COUNTERS, [
    {governor_submissions_received, ?C_SUBMISSIONS_RECEIVED, counter,
        "Total transfer submissions received by the governor"},
    {governor_tasks_in_flight, ?C_TASKS_IN_FLIGHT, gauge,
        "Number of transfer tasks currently executing"},
    {governor_pending_submissions, ?C_PENDING_SUBMISSIONS, gauge,
        "Number of submissions queued waiting for token-bucket capacity"}
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
dispatch({_Fun, Size, _ReplyTo, _Ref} = Item, #state{bucket = Bucket0} = State) ->
    case rabbitmq_stream_s3_token_bucket:request(Size, Bucket0) of
        {ok, Bucket} ->
            spawn_task(Item),
            State#state{bucket = Bucket};
        {insufficient, _, _} ->
            %% Queue for later. Start timer if not running.
            Pending = queue:in(Item, State#state.pending),
            set(?C_PENDING_SUBMISSIONS, queue:len(Pending)),
            State1 = State#state{pending = Pending},
            case State1#state.timer_ref of
                undefined -> State1#state{timer_ref = schedule_refill()};
                _ -> State1
            end
    end.

drain_pending(#state{pending = Pending0, bucket = Bucket0} = State) ->
    case queue:peek(Pending0) of
        empty ->
            State;
        {value, {_Fun, Size, _ReplyTo, _Ref} = Item} ->
            case rabbitmq_stream_s3_token_bucket:request(Size, Bucket0) of
                {ok, Bucket} ->
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

-endif.
