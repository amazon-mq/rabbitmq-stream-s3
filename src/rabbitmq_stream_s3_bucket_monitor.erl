%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_bucket_monitor).
-moduledoc """
Periodically probes whether the configured remote tier bucket is accessible.

A wrong or unreachable bucket does not stop a stream working: it keeps running
on local disk and uploads fail at runtime, retrying indefinitely. That makes a
misconfiguration (a nonexistent bucket, or one the credentials cannot use)
nearly silent. This singleton issues a `HeadBucket` probe at startup and on a
slow interval, and surfaces the result three ways:

- an `ERROR` log when the bucket becomes inaccessible and an `INFO` log when it
  recovers (logged on transition only, never on every tick),
- a node-level `bucket_accessible` gauge (1 or 0) for alerting, and
- the `stream_s3_status` CLI command.

It deliberately does NOT raise a `rabbit_alarm`: the only alarm primitive
(`resource_limit`) flow-blocks every publisher on the node regardless of source,
which would turn degraded tiering into a publishing outage. The signal here is
loud but side-effect free.

The startup probe is deferred until credentials are available: while the probe
returns a non-definitive error (credentials not yet loaded, a transient network
error), the monitor retries on a short interval and leaves the gauge at its
optimistic initial value rather than reporting a false inaccessibility.
""".

-behaviour(gen_server).

-include("include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/0, status/0, init_counters/0]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(CHECK, check).
%% Short retry while the first probe cannot reach a definitive answer (for
%% example credentials are still loading at boot). Bounded by the configured
%% interval so a small interval is never exceeded.
-define(INITIAL_RETRY_MS, 5_000).
%% Generous timeout for a single HeadBucket probe. A HEAD is cheap; this only
%% needs to tolerate a slow TLS handshake or a queued pooled connection.
-define(PROBE_TIMEOUT_MS, 30_000).

-define(C_BUCKET_ACCESSIBLE, 1).
-define(COUNTERS, [
    {bucket_accessible, ?C_BUCKET_ACCESSIBLE, gauge,
        "Whether the configured remote tier bucket is accessible (1) or not (0)"}
]).
-define(COUNTER_KEY, {?MODULE, counter}).

-record(?MODULE, {
    timer :: reference() | undefined,
    %% The in-flight probe, if any: its pid and monitor reference. Probes run in
    %% a separate process so a slow HeadBucket never blocks status/0 or the timer
    %% loop.
    probe :: {pid(), reference()} | undefined,
    status = unknown :: accessible | {inaccessible, term()} | unknown,
    last_checked_ms :: integer() | undefined
}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-doc """
Returns the last known accessibility of the configured bucket.

`last_checked_age_ms` is `undefined` until the first definitive result.
""".
-spec status() ->
    #{
        status := accessible | inaccessible | unknown,
        reason := undefined | term(),
        last_checked_age_ms := non_neg_integer() | undefined
    }.
status() ->
    gen_server:call(?SERVER, status).

%% ------------------------------------------------------------------
%% gen_server
%% ------------------------------------------------------------------

init([]) ->
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
    case rabbitmq_stream_s3_config:bucket_check_enabled() of
        true ->
            {ok, #?MODULE{timer = schedule(?INITIAL_RETRY_MS)}};
        false ->
            ?LOG_INFO("Tiered storage bucket accessibility checks are disabled."),
            ignore
    end.

handle_call(status, _From, #?MODULE{status = Status, last_checked_ms = LastMs} = State) ->
    {reply, format_status(Status, LastMs), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(?CHECK, #?MODULE{probe = undefined} = State) ->
    {noreply, State#?MODULE{timer = undefined, probe = start_probe()}};
handle_info(?CHECK, #?MODULE{} = State) ->
    %% A probe is still in flight from a previous tick; let it finish. It
    %% reschedules the next tick when it reports back.
    {noreply, State};
handle_info({bucket_check_result, Pid, Result}, #?MODULE{probe = {Pid, MRef}} = State0) ->
    %% Flush the monitor so the probe's imminent DOWN is dropped rather than
    %% mistaken for a probe that died without reporting.
    _ = erlang:demonitor(MRef, [flush]),
    State1 = apply_result(Result, State0),
    {noreply, State1#?MODULE{probe = undefined, timer = schedule_next(State1)}};
handle_info({'DOWN', MRef, process, Pid, Reason}, #?MODULE{probe = {Pid, MRef}} = State) ->
    %% The probe process exited without reporting a result. Treat it like a
    %% non-definitive (transient) outcome: leave the gauge and status untouched
    %% and reschedule.
    ?LOG_DEBUG("Bucket accessibility probe ended without a result: ~p", [Reason]),
    {noreply, State#?MODULE{probe = undefined, timer = schedule_next(State)}};
handle_info(_Info, State) ->
    %% A stale result or DOWN from a probe we no longer track.
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

%% Run the probe in a separate, monitored process so a slow HeadBucket cannot
%% block the monitor (and therefore status/0). The result is reported back by
%% message; if the process dies first, the monitor's DOWN handler reschedules.
start_probe() ->
    Self = self(),
    spawn_monitor(fun() ->
        Result = rabbitmq_stream_s3_api:check_bucket(#{timeout => ?PROBE_TIMEOUT_MS}),
        Self ! {bucket_check_result, self(), Result}
    end).

apply_result(ok, #?MODULE{status = Prev} = State) ->
    case Prev of
        {inaccessible, _} ->
            ?LOG_INFO(
                "Configured remote tier bucket ~ts is accessible again",
                [bucket_name()]
            );
        _ ->
            ok
    end,
    set_gauge(1),
    State#?MODULE{status = accessible, last_checked_ms = now_ms()};
apply_result({error, no_such_bucket}, State) ->
    mark_inaccessible(no_such_bucket, "it does not exist", State);
apply_result({error, access_denied}, State) ->
    mark_inaccessible(access_denied, "access was denied", State);
apply_result({error, Reason}, State) ->
    %% Non-definitive: credentials not yet available, a transient network error,
    %% or a throttling response. Do not flip the gauge or status; a blip is not
    %% an inaccessible bucket. Reschedule (a short retry while still unknown).
    ?LOG_DEBUG("Bucket accessibility probe was inconclusive: ~p", [Reason]),
    State.

%% Log only on a transition into inaccessibility (or a change of reason) so a
%% persistently misconfigured bucket does not log an ERROR on every tick.
mark_inaccessible(Reason, Description, #?MODULE{status = Prev} = State) ->
    case Prev of
        {inaccessible, Reason} ->
            ok;
        _ ->
            ?LOG_ERROR(
                "Configured remote tier bucket ~ts is not accessible: ~ts. Streams "
                "continue on local disk but data will not be tiered until this is "
                "resolved",
                [bucket_name(), Description]
            )
    end,
    set_gauge(0),
    State#?MODULE{status = {inaccessible, Reason}, last_checked_ms = now_ms()}.

%% While no definitive answer has been reached (typically credentials still
%% loading at boot), retry quickly; otherwise use the configured interval.
schedule_next(#?MODULE{status = unknown}) ->
    schedule(min(?INITIAL_RETRY_MS, rabbitmq_stream_s3_config:bucket_check_interval()));
schedule_next(#?MODULE{}) ->
    schedule(rabbitmq_stream_s3_config:bucket_check_interval()).

schedule(DelayMs) ->
    erlang:send_after(DelayMs, self(), ?CHECK).

format_status(unknown, _LastMs) ->
    #{status => unknown, reason => undefined, last_checked_age_ms => undefined};
format_status(accessible, LastMs) ->
    #{status => accessible, reason => undefined, last_checked_age_ms => age(LastMs)};
format_status({inaccessible, Reason}, LastMs) ->
    #{status => inaccessible, reason => Reason, last_checked_age_ms => age(LastMs)}.

age(undefined) -> undefined;
age(LastMs) -> max(0, now_ms() - LastMs).

now_ms() ->
    erlang:system_time(millisecond).

bucket_name() ->
    case application:get_env(rabbitmq_stream_s3, bucket) of
        {ok, Bucket} -> Bucket;
        undefined -> <<"(not configured)">>
    end.

%% ------------------------------------------------------------------
%% Counters
%% ------------------------------------------------------------------

-spec init_counters() -> ok.
init_counters() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    %% Start optimistic: assume the bucket is accessible until a probe proves
    %% otherwise, so boot (before the first probe completes) does not report a
    %% false inaccessibility.
    counters:put(Cnt, ?C_BUCKET_ACCESSIBLE, 1),
    persistent_term:put(?COUNTER_KEY, Cnt),
    ok.

set_gauge(Value) ->
    case persistent_term:get(?COUNTER_KEY, undefined) of
        undefined -> ok;
        Cnt -> counters:put(Cnt, ?C_BUCKET_ACCESSIBLE, Value)
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

setup_counters() ->
    {ok, _} = application:ensure_all_started(seshat),
    _ = seshat:new_group(rabbitmq_stream_s3),
    ok = init_counters().

gauge() ->
    counters:get(persistent_term:get(?COUNTER_KEY), ?C_BUCKET_ACCESSIBLE).

state_machine_test_() ->
    {setup, fun setup_counters/0, fun(_) ->
        [
            {"initial gauge is optimistic (1)", ?_assertEqual(1, gauge())},
            {"definitive inaccessible flips the gauge to 0 and records the reason", fun() ->
                S0 = #?MODULE{},
                S1 = apply_result({error, no_such_bucket}, S0),
                ?assertMatch(#?MODULE{status = {inaccessible, no_such_bucket}}, S1),
                ?assertEqual(0, gauge()),
                ?assertNotEqual(undefined, S1#?MODULE.last_checked_ms)
            end},
            {"recovery flips the gauge back to 1", fun() ->
                S0 = #?MODULE{status = {inaccessible, access_denied}},
                set_gauge(0),
                S1 = apply_result(ok, S0),
                ?assertMatch(#?MODULE{status = accessible}, S1),
                ?assertEqual(1, gauge())
            end},
            {"a non-definitive error neither flips the gauge nor changes status", fun() ->
                set_gauge(1),
                S0 = #?MODULE{status = accessible, last_checked_ms = 123},
                S1 = apply_result({error, timeout}, S0),
                ?assertEqual(S0, S1),
                ?assertEqual(1, gauge())
            end},
            {"a non-definitive error at boot leaves status unknown (deferral)", fun() ->
                S0 = #?MODULE{status = unknown},
                S1 = apply_result({error, no_credentials}, S0),
                ?assertEqual(unknown, S1#?MODULE.status),
                ?assertEqual(undefined, S1#?MODULE.last_checked_ms)
            end}
        ]
    end}.

%% schedule_next/1 must retry quickly while still unknown and back off to the
%% configured interval once a definitive result is known.
schedule_next_interval_test() ->
    %% While unknown: bounded by the short initial retry.
    application:set_env(rabbitmq_stream_s3, bucket_check_interval, 300_000),
    try
        TRef1 = schedule_next(#?MODULE{status = unknown}),
        ?assert(erlang:read_timer(TRef1) =< ?INITIAL_RETRY_MS),
        erlang:cancel_timer(TRef1),
        %% Once known: the full configured interval.
        TRef2 = schedule_next(#?MODULE{status = accessible}),
        ?assert(erlang:read_timer(TRef2) > ?INITIAL_RETRY_MS),
        erlang:cancel_timer(TRef2)
    after
        application:unset_env(rabbitmq_stream_s3, bucket_check_interval)
    end,
    %% Drain any CHECK message a fired timer may have left in the mailbox.
    receive
        ?CHECK -> ok
    after 0 -> ok
    end.

format_status_test() ->
    ?assertMatch(
        #{status := unknown, reason := undefined, last_checked_age_ms := undefined},
        format_status(unknown, undefined)
    ),
    ?assertMatch(
        #{status := accessible, reason := undefined, last_checked_age_ms := Age} when
            is_integer(Age),
        format_status(accessible, now_ms())
    ),
    ?assertMatch(
        #{status := inaccessible, reason := access_denied},
        format_status({inaccessible, access_denied}, now_ms())
    ).

-endif.
