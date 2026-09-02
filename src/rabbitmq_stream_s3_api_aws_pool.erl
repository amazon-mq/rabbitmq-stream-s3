%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_api_aws_pool).
-moduledoc """
A pool of connections to S3.

This plugin makes requests to S3 very often. A pool helps reduce wasted TLS
handshakes.

This module can be used to spawn independent pools. Writes take longer than
reads because they upload tens of megabytes, so separating the writer pool from
the reader pool avoids reader starvation.
""".

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("include/logging.hrl").

-behaviour(gen_server).

%% Reap proactively before S3 closes an idle connection from under us.
-define(IDLE_TIMEOUT_MS, 10_000).

-define(C_CHECKOUTS, 1).
-define(C_CHECKINS, 2).
-define(C_CHECKOUT_QUEUED, 3).
-define(C_CHECKOUT_CANCELLED, 4).
-define(COUNTERS, [
    {checkouts, ?C_CHECKOUTS, counter, "Total successful pool checkouts"},
    {checkins, ?C_CHECKINS, counter, "Total pool checkins"},
    {checkout_queued, ?C_CHECKOUT_QUEUED, counter,
        "Number of checkout requests that had to wait for a connection"},
    {checkout_cancelled, ?C_CHECKOUT_CANCELLED, counter,
        "Number of checkout requests that were cancelled (caller timeout or exit)"}
]).

-record(pending, {
    from :: gen_server:from(),
    checkout :: checkout(),
    mref :: reference()
}).

-record(?MODULE, {
    min_size :: non_neg_integer(),
    max_size :: pos_integer(),
    %% A stack of connections which are available to be checked out of the
    %% pool. This is a stack rather than a queue so that the most recently used
    %% connection (which is most 'hot' and least likely to be closed because of
    %% idleness) is prioritized for a checkout.
    available = [] :: [conn()],
    %% Monitors on all connections whether they are available, checked out,
    %% or down.
    monitors = #{} :: #{conn() => MRef :: reference()},
    %% A mapping of checked out connections. The value is the monitor ref of
    %% the process which checked out the connection. (The monitor ref is used
    %% to cancel the monitor when the connection is checked back in.)
    checkouts = #{} :: #{conn() => MRef :: reference()},
    %% Same as above but reversed.
    checkouts_rev = #{} :: #{MRef :: reference() => conn()},
    %% A queue of requests to check out a connection.
    pending = queue:new() :: queue:queue(#pending{}),
    %% Idle timers for available connections.
    idle_timers = #{} :: #{conn() => reference()},
    %% Native monotonic timestamp at which each connection was opened; used
    %% to report age when a connection goes down unexpectedly.
    created = #{} :: #{conn() => integer()},
    counter :: counters:counters_ref(),
    %% Injection points so this pool can be driven in tests without a real S3
    %% endpoint or `gun` connection. Default to the real behavior; see
    %% `config()`.
    open_fun :: open_fun(),
    usable_fun :: usable_fun(),
    close_fun :: close_fun()
}).

%% Gun connection pid.
-type conn() :: pid().

-type pool() :: atom().

%% An attempt to check out a conn. (Internal.)
-type checkout() :: reference().

-type open_fun() :: fun(() -> {ok, conn()} | {error, any()}).
-type usable_fun() :: fun((conn()) -> boolean()).
-type close_fun() :: fun((conn()) -> any()).

-type config() :: #{
    name := pool(),
    min_size := non_neg_integer(),
    max_size := pos_integer(),
    %% Test-only injection points; default to the real `open/0`, `usable/1`
    %% and `gun:close/1` when absent. See issue #178.
    open_fun => open_fun(),
    usable_fun => usable_fun(),
    close_fun => close_fun()
}.

%% API
-export([
    checkout/2,
    checkin/2,
    with/3
]).

%% gen_server
-export([
    start_link/2,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    format_status/1,
    terminate/2,
    code_change/3
]).

-ifdef(TEST).
%% Full-fidelity state dump for api_aws_pool_statem_SUITE. `format_state/1`
%% (used for logging/`sys:get_status`) only reports sizes/counts, which is not
%% enough to assert the `checkouts`/`checkouts_rev` mirror-image invariant.
-export([test_inspect/1]).
-endif.

%%---------------------------------------------------------------------------

-spec checkout(pool(), timeout()) -> {ok, conn()} | {error, pool_busy}.
checkout(Pool, Timeout) ->
    %% NOTE: the server monitors the caller, but we also send a cancellation
    %% message. The cancellation message ensures the checkout attempt is
    %% dropped when the checkout times out (the caller is no longer waiting for
    %% a connection). The monitor catches cases where the cancellation message
    %% can't be sent (disconnects, brutal kills, etc.).
    Checkout = erlang:make_ref(),
    try
        {ok, gen_server:call(Pool, {checkout, Checkout}, Timeout)}
    catch
        %% A saturated pool times the checkout out with a gen_server:call exit.
        %% Convert it to a named term here, where the knowledge that the
        %% checkout is a timed gen_server:call lives, so the raw OTP exit never
        %% escapes the pool boundary and callers classify pool_busy by name.
        exit:{timeout, _} ->
            gen_server:cast(Pool, {cancel_checkout, Checkout}),
            {error, pool_busy};
        %% Any other exit (e.g. the pool is down) is not a saturation signal.
        %% Send the cancellation and re-raise, preserving the caller's view of
        %% a genuinely broken pool.
        C:E:Stack ->
            gen_server:cast(Pool, {cancel_checkout, Checkout}),
            erlang:raise(C, E, Stack)
    end.

-spec checkin(pool(), conn()) -> ok.
checkin(Pool, Conn) ->
    gen_server:cast(Pool, {checkin, Conn}).

-spec with(pool(), timeout(), fun((conn()) -> term())) -> term() | {error, pool_busy}.
with(Pool, Timeout, Fun) ->
    case checkout(Pool, Timeout) of
        {ok, Conn} ->
            try
                Fun(Conn)
            after
                ok = checkin(Pool, Conn)
            end;
        {error, pool_busy} = Err ->
            Err
    end.

%%---------------------------------------------------------------------------

-spec start_link(pool(), config()) -> gen_server:start_ret().
start_link(Name, Config) ->
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

init(#{min_size := MinSize, max_size := MaxSize, name := Name} = Config) ->
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
    case rabbitmq_stream_s3_api:backend() of
        rabbitmq_stream_s3_api_aws ->
            Cnt = seshat:new(rabbitmq_stream_s3, Name, ?COUNTERS, #{
                module => ?MODULE,
                pool => Name
            }),
            self() ! grow,
            {ok, #?MODULE{
                min_size = MinSize,
                max_size = MaxSize,
                counter = Cnt,
                open_fun = maps:get(open_fun, Config, fun open/0),
                usable_fun = maps:get(usable_fun, Config, fun usable/1),
                close_fun = maps:get(close_fun, Config, fun gun:close/1)
            }};
        _ ->
            ignore
    end.

handle_call(
    {checkout, Checkout},
    {Pid, _} = From,
    #?MODULE{pending = Pending0, counter = Cnt} = State0
) ->
    case take_available(Pid, State0) of
        {ok, Conn, State} ->
            counters:add(Cnt, ?C_CHECKOUTS, 1),
            {reply, Conn, State};
        {empty, State1} ->
            counters:add(Cnt, ?C_CHECKOUT_QUEUED, 1),
            MRef = erlang:monitor(process, Pid),
            P = #pending{from = From, checkout = Checkout, mref = MRef},
            State2 = State1#?MODULE{pending = queue:in(P, Pending0)},
            {noreply, grow(State2)}
    end;
handle_call(Request, From, State) ->
    ?LOG_INFO(?MODULE_STRING " received unexpected call from ~p: ~W", [From, Request, 10]),
    {noreply, State}.

%% Record a connection as checked out and account the request. `add_checkout`
%% and `del_checkout` are the only two places `checkouts`/`checkouts_rev` change,
%% so api_aws's `active_requests` gauge - one in-flight request per checkout,
%% shared across every pool and therefore incremented rather than derived - is
%% welded to the map and cannot drift from it, whichever way the checkout ends.
add_checkout(Conn, MRef, #?MODULE{checkouts = Checkouts, checkouts_rev = CheckoutsRev} = State) ->
    ok = rabbitmq_stream_s3_api_aws:note_request_started(),
    State#?MODULE{
        checkouts = Checkouts#{Conn => MRef},
        checkouts_rev = CheckoutsRev#{MRef => Conn}
    }.

del_checkout(Conn, MRef, #?MODULE{checkouts = Checkouts, checkouts_rev = CheckoutsRev} = State) ->
    ok = rabbitmq_stream_s3_api_aws:note_request_finished(),
    State#?MODULE{
        checkouts = maps:remove(Conn, Checkouts),
        checkouts_rev = maps:remove(MRef, CheckoutsRev)
    }.

%% Return a checked-in connection to `available` if it is still monitored
%% (alive as far as we know). Otherwise drop it; the `DOWN` handler owns the
%% cleanup of a connection that died while checked out.
maybe_make_available(Conn, #?MODULE{monitors = Monitors} = State) when
    is_map_key(Conn, Monitors)
->
    make_available(Conn, State);
maybe_make_available(_Conn, State) ->
    State.

handle_checkin(
    Conn,
    #?MODULE{checkouts = Checkouts0, counter = Cnt} = State0
) when is_map_key(Conn, Checkouts0) ->
    MRef = maps:get(Conn, Checkouts0),
    counters:add(Cnt, ?C_CHECKINS, 1),
    erlang:demonitor(MRef, [flush]),
    State1 = del_checkout(Conn, MRef, State0),
    {noreply, maybe_make_available(Conn, State1)};
handle_checkin(_Conn, State) ->
    {noreply, State}.

handle_cancel_checkout(Checkout, #?MODULE{pending = Pending0, counter = Cnt} = State0) ->
    Pending = queue:delete_with(
        fun
            (#pending{checkout = C, mref = MRef}) when C =:= Checkout ->
                counters:add(Cnt, ?C_CHECKOUT_CANCELLED, 1),
                erlang:demonitor(MRef, [flush]),
                true;
            (_Pending) ->
                false
        end,
        Pending0
    ),
    {noreply, State0#?MODULE{pending = Pending}}.

handle_cast({checkin, Conn}, State) ->
    handle_checkin(Conn, State);
handle_cast({cancel_checkout, Checkout}, State) ->
    handle_cancel_checkout(Checkout, State);
handle_cast(Message, State) ->
    ?LOG_DEBUG(?MODULE_STRING " received unexpected cast: ~W", [Message, 10]),
    {noreply, State}.

get_conn_age(Conn, #?MODULE{created = Created}) when is_map_key(Conn, Created) ->
    CreatedAt = maps:get(Conn, Created),
    rabbitmq_stream_s3_util:elapsed_ms(CreatedAt);
get_conn_age(_Conn, _State) ->
    undefined.

handle_gun_up(
    Conn, #?MODULE{monitors = Monitors, available = Available, checkouts = Checkouts} = State
) when is_map_key(Conn, Monitors) ->
    ConnAge = get_conn_age(Conn, State),
    ?LOG_DEBUG(
        "Pool ~p gun_up: connection ~p ready in ~twms"
        " (available=~b total=~b checked_out=~b)",
        [
            self(),
            Conn,
            ConnAge,
            length(Available),
            map_size(Monitors),
            map_size(Checkouts)
        ]
    ),
    {noreply, make_available(Conn, State)};
handle_gun_up(Conn, #?MODULE{close_fun = CloseFun} = State) ->
    %% Stale gun_up from a connection opened by a previous pool instance.
    CloseFun(Conn),
    {noreply, State}.

%% With retry=>0, gun stops the connection process immediately after sending
%% this message (with reason 'normal' for a clean close, or
%% '{shutdown, Reason}' otherwise). Remove the connection from `available`
%% now so it cannot be checked out in the window before the monitor `DOWN`
%% arrives; a request fired onto a down connection is silently lost until it
%% times out. `monitors` is left intact so the `DOWN` handler still runs the
%% final cleanup and grows a replacement.
handle_gun_down(Conn, normal, _KilledStreams, State) ->
    %% Avoid a warning log even if `KilledStreams` is not empty. The current
    %% version of gun in use today (2.4.0) does not clean up the HTTP/1 request
    %% stream when the request's response has the header `connection: close`
    %% and does not have a body, so the `gun_down` message reports a reason of
    %% `normal` and a `KilledStreams` list of length one.
    {noreply, remove_available(Conn, State)};
handle_gun_down(Conn, closed, _KilledStreams = [], State) ->
    %% Avoid a warning log if the remote closed an idle connection.
    {noreply, remove_available(Conn, State)};
handle_gun_down(Conn, Reason, KilledStreams, #?MODULE{checkouts = Checkouts} = State) ->
    ConnAge = get_conn_age(Conn, State),
    ?LOG_WARNING(
        "S3 connection ~tw down: reason=~0p killed_streams=~b checked_out=~tw age=~twms",
        [Conn, Reason, length(KilledStreams), is_map_key(Conn, Checkouts), ConnAge]
    ),
    {noreply, remove_available(Conn, State)}.

%% A connection is down. Replace it unconditionally: the connection was in
%% use (or at least monitored), so demand exists regardless of whether
%% anyone is in the Pending queue.
handle_down(MRef, Pid, #?MODULE{monitors = Monitors} = State0) when
    is_map_key(Pid, Monitors)
->
    Conn = Pid,
    ?assert(MRef =:= maps:get(Conn, Monitors)),
    State1 = State0#?MODULE{
        monitors = maps:remove(Conn, Monitors),
        created = maps:remove(Conn, State0#?MODULE.created)
    },
    State2 = cancel(Conn, State1),
    {noreply, grow(1, State2)};
%% A caller process is down while holding a checked-out connection. The
%% connection may be in any state on the wire (e.g. mid-body in a chunked
%% PUT if the caller died between `gun:headers/4` and `gun:data/4 fin`).
%% Reusing it would land a `headers` cast on a connection whose `out` field
%% is not `head`, crashing gun's gen_statem with `function_clause`. Drop the
%% connection and grow a replacement.
%% See: https://github.com/amazon-mq/rabbitmq-stream-s3/issues/177
%%
%% This is also the only place that reliably learns a request was abandoned
%% (e.g. the caller was killed by rabbitmq_stream_s3_governor:cancel/1) rather
%% than finishing normally. `del_checkout` accounts it like any other checkout
%% end: the caller never ran a completion path, so nothing else would.
handle_down(
    MRef,
    _Pid,
    #?MODULE{checkouts_rev = CheckoutsRev} = State0
) when is_map_key(MRef, CheckoutsRev) ->
    Conn = maps:get(MRef, CheckoutsRev),
    State1 = del_checkout(Conn, MRef, State0),
    {noreply, grow(close_connection(Conn, State1))};
%% A pending caller process is down. Remove it from the pending queue.
handle_down(_MRef, Pid, #?MODULE{pending = Pending0} = State0) ->
    Pending1 = queue:delete_with(
        fun(#pending{from = {P, _}}) -> P =:= Pid end, Pending0
    ),
    {noreply, State0#?MODULE{pending = Pending1}}.

%% Closing this connection would drop below min_size. Reset the idle timer
%% to keep the warm floor intact.
handle_idle_timeout(
    Conn,
    #?MODULE{idle_timers = Timers, monitors = Monitors, min_size = MinSize} = State0
) when is_map_key(Conn, Timers) andalso map_size(Monitors) =< MinSize ->
    TRef = erlang:send_after(?IDLE_TIMEOUT_MS, self(), {idle_timeout, Conn}),
    {noreply, State0#?MODULE{idle_timers = Timers#{Conn := TRef}}};
%% Connection has been idle too long and the pool is above min_size. Close it.
handle_idle_timeout(Conn, #?MODULE{idle_timers = Timers} = State0) when
    is_map_key(Conn, Timers)
->
    State1 = State0#?MODULE{idle_timers = maps:remove(Conn, Timers)},
    State2 = cancel(Conn, State1),
    {noreply, close_connection(Conn, State2)};
%% Timer was already cancelled (stale message). Ignore.
handle_idle_timeout(_Conn, State) ->
    {noreply, State}.

handle_info(grow, State) ->
    {noreply, grow(State)};
handle_info({gun_up, Conn, _Protocol}, State) ->
    handle_gun_up(Conn, State);
handle_info({gun_down, Conn, _Protocol, Reason, KilledStreams}, State) ->
    handle_gun_down(Conn, Reason, KilledStreams, State);
handle_info({'DOWN', MRef, process, Pid, _Reason}, State) ->
    handle_down(MRef, Pid, State);
handle_info({idle_timeout, Conn}, State) ->
    handle_idle_timeout(Conn, State);
handle_info(Message, State) ->
    ?LOG_DEBUG(
        ?MODULE_STRING " received unexpected message: ~W",
        [Message, 10]
    ),
    {noreply, State}.

format_status(#{state := State0} = Status0) ->
    Status0#{state := format_state(State0)}.

terminate(_Reason, #?MODULE{monitors = Monitors, close_fun = CloseFun}) ->
    _ = [CloseFun(Conn) || Conn := _ <- Monitors],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%---------------------------------------------------------------------------

grow(
    #?MODULE{
        min_size = MinSize,
        max_size = MaxSize,
        available = Available,
        checkouts = Checkouts,
        pending = Pending,
        monitors = Monitors0
    } = State0
) ->
    N = grow_count(#{
        min_size => MinSize,
        max_size => MaxSize,
        open => map_size(Monitors0),
        available => length(Available),
        checkouts => map_size(Checkouts),
        pending => queue:len(Pending)
    }),
    grow(N, State0).

-doc """
How many connections to open, given what the pool currently holds.

Separated from the state it is read out of because it is the pool's whole growth
policy and nothing else in this module is: it is pure arithmetic over six
integers, and it is the answer to "why did the pool not already have a
connection for this caller?".

`InFlight` is the connections already being opened - `open` counts those as well
as the established ones - so demand a handshake is already on its way to serving
does not open a second connection for the same caller. That is what makes growth
*reactive*: a burst of N concurrent checkouts does not open N connections, it
opens as many as are not already covered, and the rest of the callers wait in
`pending` behind the handshakes in progress.
""".
-spec grow_count(#{
    min_size := non_neg_integer(),
    max_size := pos_integer(),
    open := non_neg_integer(),
    available := non_neg_integer(),
    checkouts := non_neg_integer(),
    pending := non_neg_integer()
}) -> non_neg_integer().
grow_count(#{
    min_size := MinSize,
    max_size := MaxSize,
    open := Count,
    available := Available,
    checkouts := Checkouts,
    pending := Pending
}) ->
    InFlight = Count - Available - Checkouts,
    Demand = Pending - InFlight,
    Target = max(MinSize - Count, max(Demand, 0)),
    max(0, min(Target, MaxSize - Count)).

grow(0, State) ->
    State;
grow(N, #?MODULE{monitors = Monitors0, created = Created0, open_fun = OpenFun} = State0) ->
    case OpenFun() of
        {ok, Conn} ->
            State = State0#?MODULE{
                monitors = Monitors0#{Conn => erlang:monitor(process, Conn)},
                created = Created0#{Conn => rabbitmq_stream_s3_util:now()}
            },
            grow(N - 1, State);
        {error, Reason} ->
            ?LOG_WARNING("Failed to open S3 connection: ~0p", [Reason]),
            erlang:send_after(1_000, self(), grow),
            State0
    end.

-doc """
Opens a connection to S3 in the configured region.
""".
-spec open() -> {ok, pid()} | {error, any()}.
open() ->
    %% NOTE: unfortunately, `inet:hostname()` is a string not a binary.
    case rabbitmq_stream_s3_api_aws:hostname() of
        {ok, HostBin} ->
            Host = binary_to_list(HostBin),
            Opts = #{
                transport => tls,
                %% AWS S3 only supports HTTP/1.1.
                protocols => [http],
                tls_opts => [
                    {verify, verify_peer},
                    {cacerts, public_key:cacerts_get()},
                    {customize_hostname_check, [
                        {match_fun, public_key:pkix_verify_hostname_match_fun(https)}
                    ]},
                    %% Connections are mostly data pipes for large refc binaries.
                    %% Full sweeps are nearly free since the process's heap doesn't
                    %% contain much, and the immediate cleanup of dead refc binary
                    %% references lets the binary allocator free promptly rather than
                    %% holding until the nth minor GC triggers a full sweep.
                    {receiver_spawn_opts, [{fullsweep_after, 0}]},
                    {sender_spawn_opts, [{fullsweep_after, 0}]}
                ],
                %% retry => 0: gun does not reconnect a closed connection. The
                %% pool manages connection lifecycle itself - a connection closed
                %% from idleness or error is discarded and a fresh one is grown,
                %% rather than gun silently reconnecting underneath the pool (a
                %% reconnecting gun process sits in not_connected/domain_lookup
                %% for tens of ms while the pool still believes it is usable).
                %% With TLSv1.3 reopening costs one round trip; this pool
                %% optimizes for resource use over consistently low latency (a
                %% fully idle broker should have a min-sized pool).
                %%
                %% Consequence: with retry => 0, a request issued on a connection
                %% gun has already moved out of the `connected` state is
                %% postponed and then lost when the process stops, surfacing only
                %% as a request timeout (#279). The checkout path must therefore
                %% verify a connection is still usable before handing it out, and
                %% the gun_down handler must evict it promptly. See `usable/1` and
                %% the gun_down clause. The alternative - a small retry plus a
                %% short "no response started" timeout so gun self-heals and a
                %% stranded request fails fast - is a larger lifecycle change that
                %% has not been made.
                retry => 0
            },
            gun:open(Host, 443, Opts);
        {error, _} = Err ->
            %% Region is not yet known (e.g. IMDS lookup not yet successful).
            %% Surface a clean error rather than crashing the pool.
            Err
    end.

take_available(
    Pid,
    #?MODULE{
        available = Available0,
        usable_fun = UsableFun
    } = State0
) ->
    case Available0 of
        [Conn | Available] ->
            case UsableFun(Conn) of
                true ->
                    MRef = erlang:monitor(process, Pid),
                    State = add_checkout(Conn, MRef, State0#?MODULE{available = Available}),
                    {ok, Conn, cancel_idle_timer(Conn, State)};
                false ->
                    %% Connection is down or disconnected; drop it and try the
                    %% next. `monitors` is left intact so the `DOWN` handler
                    %% still runs the final cleanup and grows a replacement.
                    State = cancel_idle_timer(Conn, State0#?MODULE{available = Available}),
                    take_available(Pid, State)
            end;
        [] ->
            {empty, State0}
    end.

checkout(
    #?MODULE{
        pending = Pending0,
        available = Available0,
        counter = Cnt,
        usable_fun = UsableFun
    } = State0
) ->
    case {queue:out(Pending0), Available0} of
        {{{value, #pending{from = From, mref = MRef}}, Pending}, [Conn | Available]} ->
            case UsableFun(Conn) of
                true ->
                    counters:add(Cnt, ?C_CHECKOUTS, 1),
                    gen_server:reply(From, Conn),
                    State1 = cancel_idle_timer(
                        Conn,
                        add_checkout(Conn, MRef, State0#?MODULE{
                            pending = Pending,
                            available = Available
                        })
                    ),
                    checkout(State1);
                false ->
                    %% Down connection at the head; drop it (leaving `monitors`
                    %% for the `DOWN` handler) and retry without consuming the
                    %% pending entry, so the waiting caller still gets served.
                    State1 = cancel_idle_timer(Conn, State0#?MODULE{available = Available}),
                    checkout(State1)
            end;
        _ ->
            State0
    end.

cancel(Conn, #?MODULE{checkouts = Checkouts0} = State0) when
    is_map_key(Conn, Checkouts0)
->
    CallerMRef = maps:get(Conn, Checkouts0),
    erlang:demonitor(CallerMRef, [flush]),
    del_checkout(Conn, CallerMRef, State0);
cancel(Conn, State) ->
    %% Not checked out; if it is available, removing it from `available` (and
    %% cancelling its idle timer) is exactly `remove_available/2`.
    remove_available(Conn, State).

%% A connection is usable for a new request only if its gun process is alive
%% and in the `connected` state. After S3 closes an idle connection gun clears
%% its socket and moves to `not_connected` (then stops, with retry=>0); a
%% request fired at it in that window is silently lost until the request times
%% out. Checked on checkout so such a connection is never handed out, covering
%% the window where the checkout is processed before gun's `gun_down`.
usable(Conn) ->
    is_process_alive(Conn) andalso
        try gun:info(Conn) of
            #{state_name := connected} -> true;
            _ -> false
        catch
            _:_ -> false
        end.

%% Remove a connection from `available` and cancel its idle timer, leaving
%% `monitors` and `checkouts` untouched. Used when gun reports the connection
%% down: it must not be checked out, but the monitor `DOWN` handler still owns
%% the final cleanup (removing it from `monitors` and growing a replacement).
%% A no-op if the connection is not currently available (e.g. checked out).
remove_available(Conn, #?MODULE{available = Available0} = State) ->
    case lists:member(Conn, Available0) of
        true ->
            State1 = State#?MODULE{available = lists:delete(Conn, Available0)},
            cancel_idle_timer(Conn, State1);
        false ->
            State
    end.

make_available(Conn, #?MODULE{available = Available, idle_timers = Timers} = State) ->
    TRef = erlang:send_after(?IDLE_TIMEOUT_MS, self(), {idle_timeout, Conn}),
    checkout(State#?MODULE{
        available = [Conn | Available],
        idle_timers = Timers#{Conn => TRef}
    }).

%% Close a connection and stop monitoring it. Used when the connection has
%% been idle too long, or when its caller died holding it (and we cannot
%% trust its on-wire state). A no-op if the connection is not in `monitors`.
close_connection(
    Conn, #?MODULE{monitors = Monitors, created = Created, close_fun = CloseFun} = State
) when
    is_map_key(Conn, Monitors)
->
    ConnMRef = maps:get(Conn, Monitors),
    erlang:demonitor(ConnMRef, [flush]),
    CloseFun(Conn),
    State#?MODULE{
        monitors = maps:remove(Conn, Monitors),
        created = maps:remove(Conn, Created)
    };
close_connection(_Conn, State) ->
    State.

cancel_idle_timer(Conn, #?MODULE{idle_timers = Timers0} = State) when
    is_map_key(Conn, Timers0)
->
    TRef = maps:get(Conn, Timers0),
    ok = erlang:cancel_timer(TRef, [{async, true}, {info, false}]),
    State#?MODULE{idle_timers = maps:remove(Conn, Timers0)};
cancel_idle_timer(_Conn, State) ->
    State.

format_state(#?MODULE{
    min_size = MinSize,
    max_size = MaxSize,
    available = Available,
    monitors = Monitors,
    checkouts = Checkouts,
    pending = Pending
}) ->
    #{
        min_size => MinSize,
        max_size => MaxSize,
        available => length(Available),
        monitors => map_size(Monitors),
        checkouts => map_size(Checkouts),
        pending => queue:len(Pending)
    }.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

test_inspect(Pool) ->
    #?MODULE{
        available = Available,
        monitors = Monitors,
        checkouts = Checkouts,
        checkouts_rev = CheckoutsRev,
        pending = Pending,
        idle_timers = IdleTimers
    } = sys:get_state(Pool),
    #{
        available => Available,
        monitors => Monitors,
        checkouts => Checkouts,
        checkouts_rev => CheckoutsRev,
        pending => [
            From
         || #pending{from = From} <- queue:to_list(Pending)
        ],
        idle_timers => IdleTimers
    }.

%% A saturated pool must surface as {error, pool_busy}, not a raw
%% gen_server:call exit escaping the pool boundary (issue #332).
checkout_timeout_returns_pool_busy_test() ->
    {ok, _} = application:ensure_all_started(seshat),
    ok = application:set_env(
        rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_aws
    ),
    _ = seshat:new_group(rabbitmq_stream_s3),
    Config = #{
        name => busy_pool,
        min_size => 1,
        max_size => 1,
        open_fun => fun test_open/0,
        usable_fun => fun erlang:is_process_alive/1,
        close_fun => fun test_close/1
    },
    {ok, Pid} = start_link(busy_pool, Config),
    try
        %% Take the pool's only connection and hold it, so the next checkout
        %% cannot be served and must time out.
        {ok, Conn} = checkout(busy_pool, 1000),
        ?assertEqual({error, pool_busy}, checkout(busy_pool, 100)),
        %% The connection held above is still valid and checks back in cleanly.
        ok = checkin(busy_pool, Conn)
    after
        gen_server:stop(Pid)
    end.

%% api_aws's active_requests gauge is owned here. Prove it is balanced across
%% every way a checkout ends: a clean check-in, the caller dying while holding
%% the connection (the abandon path a governor-cancel kill takes), and the
%% connection itself dying under a live caller.
active_requests_balanced_across_checkout_ends_test() ->
    with_active_requests_counter(fun(ActiveRequests) ->
        Config = #{
            name => balance_pool,
            min_size => 2,
            max_size => 2,
            open_fun => fun test_open/0,
            usable_fun => fun erlang:is_process_alive/1,
            close_fun => fun test_close/1
        },
        {ok, Pid} = start_link(balance_pool, Config),
        try
            ?assertEqual(0, ActiveRequests()),

            %% Clean check-in.
            {ok, Conn1} = checkout(balance_pool, 1000),
            ?assertEqual(1, ActiveRequests()),
            ok = checkin(balance_pool, Conn1),
            await(fun() -> ActiveRequests() =:= 0 end),

            %% Caller dies while holding the connection: the pool's caller-DOWN
            %% handler must account the end, since the caller ran no completion
            %% path. This is the leak the old inc-in-caller design had.
            Parent = self(),
            Holder = spawn(fun() ->
                {ok, _} = checkout(balance_pool, 1000),
                Parent ! checked_out,
                receive
                    die -> ok
                end
            end),
            receive
                checked_out -> ok
            after 1000 -> error(holder_setup_failed)
            end,
            ?assertEqual(1, ActiveRequests()),
            Holder ! die,
            await(fun() -> ActiveRequests() =:= 0 end),

            %% Connection dies under a live caller: the connection-DOWN handler
            %% must account the end too.
            {ok, Conn3} = checkout(balance_pool, 1000),
            ?assertEqual(1, ActiveRequests()),
            exit(Conn3, kill),
            await(fun() -> ActiveRequests() =:= 0 end)
        after
            gen_server:stop(Pid)
        end
    end).

%% Give Fun a reader for api_aws's active_requests gauge, which this module
%% moves but api_aws owns. Delegating keeps the counter's key, size and indices
%% in the one module that defines them.
with_active_requests_counter(Fun) ->
    rabbitmq_stream_s3_api_aws:with_counter(fun(Read) ->
        Fun(fun() -> Read(active_requests) end)
    end).

await(Fun) ->
    await(Fun, 2000).

await(_Fun, Remaining) when Remaining =< 0 ->
    error(timeout_awaiting_condition);
await(Fun, Remaining) ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(5),
            await(Fun, Remaining - 5)
    end.

%% A fake connection is a plain process that only understands `stop`; the pool
%% sends itself `gun_up` synchronously, mirroring the real `open/0` path.
test_open() ->
    Pid = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    self() ! {gun_up, Pid, http},
    {ok, Pid}.

test_close(Conn) ->
    catch exit(Conn, kill),
    ok.
-endif.
