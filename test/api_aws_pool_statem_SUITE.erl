%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(api_aws_pool_statem_SUITE).
-moduledoc """
Property-based (proper_statem) model of rabbitmq_stream_s3_api_aws_pool.

The pool's `open/0` calls `gun:open/3` directly and `usable/1` calls
`gun:info/1` (which internally makes a `sys:get_state/1` call to the gun
`gen_statem`), so exercising it against a real state-machine bug needs either
a real S3 endpoint or, per
https://github.com/amazon-mq/rabbitmq-stream-s3/issues/178, injectable
`open_fun`/`usable_fun`/`close_fun`. This suite starts the pool with fakes
that spawn/track/kill plain Erlang processes instead of `gun` connections, and
drives the real gen_server through randomized concurrent checkout/checkin/kill
sequences (checkout callers are spawned processes, since `checkout/2` is a
single blocking `gen_server:call` -- there is no non-blocking variant to
model).

The postconditions are the properties this refactor exists to regression-test
(see #177, #178):

1. `checkouts` and `checkouts_rev` are always exact mirror images of each
   other, and every connection in `available` or `checkouts` is a key of
   `monitors` (checked after every command).
2. A caller dying while holding a checked-out connection (the #177
   regression) causes that connection to be dropped -- never returned to
   `available` -- and a replacement grown.
3. A connection process dying (simulating a `gun` crash) is removed from all
   tracking and a replacement is grown.
4. A caller queued in `pending` (pool at `max_size`, all checked out) is
   served once a connection frees up, and a pending caller that dies is
   removed from the queue without leaking its entry.

Idle-timeout behavior (#5 in the issue's checklist) is not modeled: it would
require also making `?IDLE_TIMEOUT_MS` injectable (out of scope for #178,
which only asks for open/usable/close) and, at a real 10s window, would make
each `?FORALL` iteration far more expensive for little additional coverage
over the invariants above.
""".

-compile([export_all, nowarn_export_all]).

-behaviour(proper_statem).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("proper/include/proper.hrl").

-define(POOL, statem_aws_pool).
-define(MIN_SIZE, 1).
-define(MAX_SIZE, 2).
%% Comfortably larger than the real `checkout/2` timeout below, so a poll for
%% the caller's outcome always has time to observe either a served checkout
%% or the caller's own timeout-triggered cancellation.
-define(CHECKOUT_TIMEOUT_MS, 300).
-define(RELEASE_AWAIT_MS, 1500).
-define(AWAIT_MS, 1500).

all() ->
    [
        no_leak_or_inconsistency,
        one_process_may_hold_several_checkouts
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(seshat),
    ok = application:set_env(
        rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_aws
    ),
    Config.

end_per_suite(Config) ->
    Config.

%% ------------------------------------------------------------------
%% Test case
%% ------------------------------------------------------------------

%% A remote reader pipelines its range GETs, so one process holds several
%% checkouts at once. The statem model spawns a fresh caller per checkout and
%% never covers that: checkouts are keyed by connection with a monitor taken
%% per checkout, so the same caller monitored several times must not collapse
%% the `checkouts`/`checkouts_rev` mirror.
one_process_may_hold_several_checkouts(_Config) ->
    _ = seshat:new_group(rabbitmq_stream_s3),
    Config = maps:merge(pool_config(), #{min_size => 0, max_size => 3}),
    {ok, Pid} = rabbitmq_stream_s3_api_aws_pool:start_link(?POOL, Config),
    unlink(Pid),
    try
        Conns = [Conn || {ok, Conn} <- [checkout() || _ <- lists:seq(1, 3)]],
        ?assertEqual(3, length(Conns)),
        ?assertEqual(3, length(lists:usort(Conns))),
        ?assert(invariant_ok()),
        #{checkouts := Checkouts, available := Available} = test_inspect(),
        ?assertEqual(3, map_size(Checkouts)),
        ?assertEqual(0, length(Available)),
        %% The pool is now saturated, and the same process asking for a fourth
        %% waits and then reports it busy rather than being served twice over.
        ?assertEqual({error, pool_busy}, checkout()),
        [ok = rabbitmq_stream_s3_api_aws_pool:checkin(?POOL, Conn) || Conn <- Conns],
        ?assert(invariant_ok()),
        #{checkouts := Checkouts1, available := Available1} = test_inspect(),
        ?assertEqual(0, map_size(Checkouts1)),
        ?assertEqual(3, length(Available1))
    after
        gen_server:stop(?POOL)
    end.

checkout() ->
    rabbitmq_stream_s3_api_aws_pool:checkout(?POOL, ?CHECKOUT_TIMEOUT_MS).

no_leak_or_inconsistency(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_no_leak_or_inconsistency/0, [], 150).

prop_no_leak_or_inconsistency() ->
    ?FORALL(
        Cmds,
        commands(?MODULE),
        begin
            _ = seshat:new_group(rabbitmq_stream_s3),
            {ok, Pid} = rabbitmq_stream_s3_api_aws_pool:start_link(?POOL, pool_config()),
            unlink(Pid),
            {History, State, Result} = run_commands(?MODULE, Cmds),
            cleanup_callers(State),
            gen_server:stop(?POOL),
            ?WHENFAIL(
                io:format(
                    "~n~nFailing command sequence~nHistory: ~p~nState: ~p~nResult: ~p~n",
                    [History, State, Result]
                ),
                Result =:= ok
            )
        end
    ).

pool_config() ->
    #{
        name => ?POOL,
        min_size => ?MIN_SIZE,
        max_size => ?MAX_SIZE,
        open_fun => fun fake_open/0,
        usable_fun => fun fake_usable/1,
        close_fun => fun fake_close/1
    }.

%% Reap any caller whose checkout was never released or killed during this
%% ?FORALL iteration; it only blocks on `receive ... end`, so nothing but a
%% `stop`/`checkin` message or an explicit kill ever ends it.
cleanup_callers(State) ->
    maps:foreach(
        fun
            (_CallerId, idle) -> ok;
            (_CallerId, {busy, Pid}) -> catch exit(Pid, kill)
        end,
        State
    ).

%% ------------------------------------------------------------------
%% proper_statem callbacks
%% ------------------------------------------------------------------

%% One caller slot more than `?MAX_SIZE` so concurrent checkouts reliably
%% queue in `pending`.
caller_ids() -> [c1, c2, c3].

initial_state() ->
    #{Id => idle || Id <- caller_ids()}.

command(S) ->
    frequency([
        {6,
            ?LET(
                CallerId,
                elements(caller_ids()),
                command_for_caller(CallerId, maps:get(CallerId, S))
            )},
        {1, {call, ?MODULE, do_kill_conn, []}}
    ]).

command_for_caller(CallerId, idle) ->
    {call, ?MODULE, do_checkout, [CallerId]};
command_for_caller(CallerId, {busy, Pid}) ->
    oneof([
        {call, ?MODULE, do_release, [CallerId, Pid]},
        {call, ?MODULE, do_kill, [CallerId, Pid]}
    ]).

precondition(_S, _Call) ->
    true.

next_state(S, V, {call, _, do_checkout, [CallerId]}) ->
    S#{CallerId => {busy, V}};
next_state(S, _V, {call, _, do_release, [CallerId, _Pid]}) ->
    S#{CallerId => idle};
next_state(S, _V, {call, _, do_kill, [CallerId, _Pid]}) ->
    S#{CallerId => idle};
next_state(S, _V, {call, _, do_kill_conn, []}) ->
    S.

postcondition(_S, {call, _, do_checkout, [_CallerId]}, Pid) ->
    is_pid(Pid) andalso invariant_ok();
postcondition(_S, {call, _, do_release, [_CallerId, _Pid]}, Result) ->
    invariant_ok() andalso
        case Result of
            {released, Conn} ->
                %% Property 1: a checked-in connection is tracked and either
                %% back on the shelf or has already been handed to a pending
                %% caller -- never simply dropped. (A concurrent `do_kill_conn`
                %% may have already reaped it entirely in the meantime; that is
                %% also fine, just not "checked out and back in `available`
                %% while still claiming to be untracked".)
                ok =:=
                    await(fun() ->
                        #{available := A, checkouts := C, monitors := M} = test_inspect(),
                        not maps:is_key(Conn, M) orelse
                            (lists:member(Conn, A) orelse maps:is_key(Conn, C))
                    end);
            {not_checked_out, _} ->
                true
        end;
postcondition(_S, {call, _, do_kill, [_CallerId, Pid]}, Conn) ->
    invariant_ok() andalso
        case Conn of
            none ->
                %% Caller died without ever holding a connection: it must
                %% not linger in the pending queue.
                ok =:= await(fun() -> not caller_in_pending(Pid) end);
            _ when is_pid(Conn) ->
                %% Property 2 (#177 regression): a connection whose caller
                %% died while it was checked out must never be handed back
                %% out, and the pool must grow a replacement.
                ok =:=
                    await(fun() ->
                        #{available := A, checkouts := C, monitors := M} = test_inspect(),
                        not lists:member(Conn, A) andalso
                            not maps:is_key(Conn, C) andalso
                            not maps:is_key(Conn, M) andalso
                            map_size(M) >= ?MIN_SIZE
                    end)
        end;
postcondition(_S, {call, _, do_kill_conn, []}, no_conn) ->
    true;
postcondition(_S, {call, _, do_kill_conn, []}, Conn) when is_pid(Conn) ->
    %% Property 3: a connection process dying is removed from all tracking
    %% and a replacement is grown.
    invariant_ok() andalso
        ok =:=
            await(fun() ->
                #{available := A, checkouts := C, monitors := M} = test_inspect(),
                not lists:member(Conn, A) andalso
                    not maps:is_key(Conn, C) andalso
                    not maps:is_key(Conn, M) andalso
                    map_size(M) >= ?MIN_SIZE
            end).

%% ------------------------------------------------------------------
%% Command implementations (real calls against the running gen_server)
%% ------------------------------------------------------------------

%% Spawns a caller process that performs the real (blocking) checkout call
%% and reports its outcome back to us, then waits to be released or killed.
%% Modeling checkout this way (rather than calling it inline) is what lets a
%% single ?FORALL iteration exercise concurrent/pending checkouts and kill a
%% caller mid-checkout.
do_checkout(CallerId) ->
    TestPid = self(),
    spawn(fun() ->
        %% checkout/2 returns {ok, Conn} on success and {error, pool_busy} on a
        %% saturated-pool timeout; the catch only guards against an unexpected
        %% exit (e.g. a dead pool), which this model never induces.
        Result =
            try
                rabbitmq_stream_s3_api_aws_pool:checkout(?POOL, ?CHECKOUT_TIMEOUT_MS)
            catch
                C:E -> {error, {C, E}}
            end,
        TestPid ! {checkout_result, CallerId, self(), Result},
        receive
            stop -> ok
        end
    end).

%% Waits (bounded, comfortably past the caller's own checkout timeout) for
%% the caller's outcome, checks the connection back in if it got one, and
%% tells the caller to stop.
do_release(_CallerId, Pid) ->
    receive
        {checkout_result, _, Pid, {ok, Conn}} ->
            ok = rabbitmq_stream_s3_api_aws_pool:checkin(?POOL, Conn),
            Pid ! stop,
            {released, Conn};
        {checkout_result, _, Pid, {error, _}} ->
            Pid ! stop,
            {not_checked_out, error}
    after ?RELEASE_AWAIT_MS ->
        Pid ! stop,
        {not_checked_out, timeout}
    end.

%% Kills the caller outright, whether it is still waiting on the checkout
%% call (possibly queued in `pending`) or already holding a connection. A
%% short bounded peek at our mailbox first: fake connections have no real I/O,
%% so a checkout that can be served at all is served near-instantly; this
%% window is to let that race settle (not to wait out a genuinely pending
%% checkout, which will still time out here and correctly fall to the
%% pending-queue-death branch). If the caller already reported a successful
%% checkout, its connection is the one the #177 regression must never hand
%% back out.
do_kill(_CallerId, Pid) ->
    Conn =
        receive
            {checkout_result, _, Pid, {ok, C}} -> C;
            {checkout_result, _, Pid, {error, _}} -> none
        after 100 -> none
        end,
    Ref = monitor(process, Pid),
    exit(Pid, kill),
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after 2000 -> ok
    end,
    Conn.

%% Kills a live fake connection outright, simulating a `gun` crash.
do_kill_conn() ->
    #{monitors := Monitors} = test_inspect(),
    case maps:keys(Monitors) of
        [] ->
            no_conn;
        Conns ->
            Conn = lists:nth(rand:uniform(length(Conns)), Conns),
            exit(Conn, kill),
            Conn
    end.

%% ------------------------------------------------------------------
%% Fakes (config injection points; see issue #178)
%% ------------------------------------------------------------------

%% A fake connection is a plain process that only understands `stop`. Real
%% `gun` only reports a connection usable once it sends `gun_up` (see
%% `handle_gun_up/2`); `open_fun` runs synchronously inside the pool's own
%% `grow/2` (called from `init/1`, `handle_call/3`, etc.), so `self()` here is
%% the pool itself -- sending `gun_up` to it immediately, queued right behind
%% the message currently being handled, mimics that as closely as a fake
%% connection can without speaking gun's wire protocol.
fake_open() ->
    Pid = spawn(fun() -> fake_conn_loop() end),
    self() ! {gun_up, Pid, http},
    {ok, Pid}.

fake_conn_loop() ->
    receive
        stop -> ok
    end.

%% A fake connection is usable exactly as long as it is alive: no real
%% `gun`/`sys:get_state` protocol to speak to (that call is what motivated
%% this injection point -- see moduledoc).
fake_usable(Conn) ->
    is_process_alive(Conn).

fake_close(Conn) ->
    catch exit(Conn, kill),
    ok.

%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------

test_inspect() ->
    rabbitmq_stream_s3_api_aws_pool:test_inspect(?POOL).

caller_in_pending(Pid) ->
    #{pending := Pending} = test_inspect(),
    lists:any(fun({P, _Tag}) -> P =:= Pid end, Pending).

await(Fun) ->
    await(Fun, ?AWAIT_MS).

await(_Fun, Remaining) when Remaining =< 0 ->
    {error, timeout};
await(Fun, Remaining) ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(20),
            await(Fun, Remaining - 20)
    end.

%% ------------------------------------------------------------------
%% Global structural invariants (checked after every command)
%% ------------------------------------------------------------------

invariant_ok() ->
    #{
        available := Available,
        monitors := Monitors,
        checkouts := Checkouts,
        checkouts_rev := CheckoutsRev
    } = test_inspect(),
    %% `checkouts` and `checkouts_rev` are always exact mirror images.
    map_size(Checkouts) =:= map_size(CheckoutsRev) andalso
        lists:all(
            fun({Conn, MRef}) -> maps:get(MRef, CheckoutsRev, undefined) =:= Conn end,
            maps:to_list(Checkouts)
        ) andalso
        %% Every connection in `available` or `checkouts` is tracked by
        %% `monitors`.
        lists:all(fun(Conn) -> maps:is_key(Conn, Monitors) end, Available) andalso
        lists:all(fun(Conn) -> maps:is_key(Conn, Monitors) end, maps:keys(Checkouts)).
