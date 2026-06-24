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
    counter :: counters:counters_ref()
}).

%% Gun connection pid.
-type conn() :: pid().

-type pool() :: atom().

%% An attempt to check out a conn. (Internal.)
-type checkout() :: reference().

-type config() :: #{
    name := pool(),
    min_size := non_neg_integer(),
    max_size := pos_integer()
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

%%---------------------------------------------------------------------------

-spec checkout(pool(), timeout()) -> conn().
checkout(Pool, Timeout) ->
    %% NOTE: the server monitors the caller, but we also send a cancellation
    %% message. The cancellation message ensures the checkout attempt is
    %% dropped if the caller is catching the error re-raised in the catch block
    %% below. The monitor catches cases where the cancellation message can't
    %% be sent (disconnects, brutal kills, etc.).
    Checkout = erlang:make_ref(),
    try
        gen_server:call(Pool, {checkout, Checkout}, Timeout)
    catch
        C:E:Stack ->
            gen_server:cast(Pool, {cancel_checkout, Checkout}),
            erlang:raise(C, E, Stack)
    end.

-spec checkin(pool(), conn()) -> ok.
checkin(Pool, Conn) ->
    gen_server:cast(Pool, {checkin, Conn}).

-spec with(pool(), timeout(), fun((conn()) -> term())) -> term().
with(Pool, Timeout, Fun) ->
    Conn = checkout(Pool, Timeout),
    try
        Fun(Conn)
    after
        ok = checkin(Pool, Conn)
    end.

%%---------------------------------------------------------------------------

-spec start_link(pool(), config()) -> gen_server:start_ret().
start_link(Name, Config) ->
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

init(#{min_size := MinSize, max_size := MaxSize, name := Name}) ->
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
    case rabbitmq_stream_s3_api:backend() of
        rabbitmq_stream_s3_api_aws ->
            Cnt = seshat:new(rabbitmq_stream_s3, Name, ?COUNTERS, #{
                module => ?MODULE,
                pool => Name
            }),
            self() ! grow,
            {ok, #?MODULE{min_size = MinSize, max_size = MaxSize, counter = Cnt}};
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

handle_cast(
    {checkin, Conn},
    #?MODULE{
        checkouts = Checkouts0,
        checkouts_rev = CheckoutsRev0,
        monitors = Monitors,
        counter = Cnt
    } = State0
) ->
    case Checkouts0 of
        #{Conn := MRef} ->
            counters:add(Cnt, ?C_CHECKINS, 1),
            erlang:demonitor(MRef, [flush]),
            State1 = State0#?MODULE{
                checkouts = maps:remove(Conn, Checkouts0),
                checkouts_rev = maps:remove(MRef, CheckoutsRev0)
            },
            case Monitors of
                #{Conn := _} ->
                    %% Connection is still alive as far as we know, and should
                    %% be reused.
                    {noreply, make_available(Conn, State1)};
                _ ->
                    {noreply, State1}
            end;
        _ ->
            {noreply, State0}
    end;
handle_cast({cancel_checkout, Checkout}, #?MODULE{pending = Pending0, counter = Cnt} = State0) ->
    Pending = queue:delete_with(
        fun(#pending{checkout = C, mref = MRef}) ->
            case C =:= Checkout of
                true ->
                    counters:add(Cnt, ?C_CHECKOUT_CANCELLED, 1),
                    erlang:demonitor(MRef, [flush]),
                    true;
                false ->
                    false
            end
        end,
        Pending0
    ),
    {noreply, State0#?MODULE{pending = Pending}};
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
handle_gun_up(Conn, State) ->
    %% Stale gun_up from a connection opened by a previous pool instance.
    gun:close(Conn),
    {noreply, State}.

%% With retry=>0, gun stops the connection process immediately after sending
%% this message (with reason 'normal' for a clean close, or
%% '{shutdown, Reason}' otherwise). Remove the connection from `available`
%% now so it cannot be checked out in the window before the monitor `DOWN`
%% arrives; a request fired onto a down connection is silently lost until it
%% times out. `monitors` is left intact so the `DOWN` handler still runs the
%% final cleanup and grows a replacement.
%%
%% Log abnormal closes with the connection's age and checkout status.
%% Clean idle closes are not logged: reason=normal is gun's own clean
%% shutdown; reason=closed is the remote side (S3) closing an idle
%% keep-alive connection.
handle_gun_down(Conn, Reason, _KilledStreams = [], State) when
    Reason =:= normal orelse Reason =:= closed
->
    {noreply, remove_available(Conn, State)};
handle_gun_down(Conn, Reason, KilledStreams, #?MODULE{checkouts = Checkouts} = State) ->
    ConnAge = get_conn_age(Conn, State),
    ?LOG_WARNING(
        "S3 connection ~tw down: reason=~0p killed_streams=~b checked_out=~tw age=~twms",
        [Conn, Reason, length(KilledStreams), is_map_key(Conn, Checkouts), ConnAge]
    ),
    {noreply, remove_available(Conn, State)}.

handle_info(grow, State0) ->
    {noreply, grow(State0)};
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

handle_down(
    MRef,
    Pid,
    #?MODULE{
        checkouts = Checkouts0,
        checkouts_rev = CheckoutsRev0,
        monitors = Monitors0,
        pending = Pending0
    } = State0
) ->
    case Monitors0 of
        #{Pid := MRef} ->
            %% A connection is down. Replace it unconditionally: the connection
            %% was in use (or at least monitored), so demand exists regardless
            %% of whether anyone is in the Pending queue.
            Conn = Pid,
            State1 = State0#?MODULE{
                monitors = maps:remove(Conn, Monitors0),
                created = maps:remove(Conn, State0#?MODULE.created)
            },
            State2 = cancel(Pid, State1),
            {noreply, grow(1, State2)};
        _ ->
            case CheckoutsRev0 of
                #{MRef := Conn} ->
                    %% A caller process is down while holding a checked-out
                    %% connection. The connection may be in any state on the
                    %% wire (e.g. mid-body in a chunked PUT if the caller
                    %% died between `gun:headers/4` and `gun:data/4 fin`).
                    %% Reusing it would land a `headers` cast on a connection
                    %% whose `out` field is not `head`, crashing gun's
                    %% gen_statem with `function_clause`. Drop the connection
                    %% and grow a replacement.
                    %% See: https://github.com/amazon-mq/rabbitmq-stream-s3/issues/177
                    State1 = State0#?MODULE{
                        checkouts = maps:remove(Conn, Checkouts0),
                        checkouts_rev = maps:remove(MRef, CheckoutsRev0)
                    },
                    {noreply, grow(close_connection(Conn, State1))};
                _ ->
                    %% A pending caller process is down. Remove it from the
                    %% pending queue.
                    Pending = queue:delete_with(
                        fun(#pending{from = {P, _}}) -> P =:= Pid end, Pending0
                    ),
                    State = State0#?MODULE{pending = Pending},
                    {noreply, State}
            end
    end.

handle_idle_timeout(
    Conn, #?MODULE{idle_timers = Timers, monitors = Monitors, min_size = MinSize} = State0
) ->
    case Timers of
        #{Conn := _} when map_size(Monitors) =< MinSize ->
            %% Closing this connection would drop below min_size. Reset the
            %% idle timer to keep the warm floor intact.
            TRef = erlang:send_after(?IDLE_TIMEOUT_MS, self(), {idle_timeout, Conn}),
            {noreply, State0#?MODULE{idle_timers = Timers#{Conn := TRef}}};
        #{Conn := _} ->
            %% Connection has been idle too long and the pool is above
            %% min_size. Close it.
            State1 = State0#?MODULE{idle_timers = maps:remove(Conn, Timers)},
            State2 = cancel(Conn, State1),
            {noreply, close_connection(Conn, State2)};
        _ ->
            %% Timer was already cancelled (stale message). Ignore.
            {noreply, State0}
    end.

format_status(#{state := State0} = Status0) ->
    Status0#{state := format_state(State0)}.

terminate(_Reason, #?MODULE{monitors = Monitors}) ->
    _ = [gun:close(Conn) || Conn := _ <- Monitors],
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
    Count = map_size(Monitors0),
    InFlight = Count - length(Available) - map_size(Checkouts),
    Demand = queue:len(Pending) - InFlight,
    Target = max(MinSize - Count, max(Demand, 0)),
    N = min(Target, MaxSize - Count),
    grow(N, State0).

grow(0, State) ->
    State;
grow(N, #?MODULE{monitors = Monitors0, created = Created0} = State0) ->
    case open() of
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
        checkouts = Checkouts0,
        checkouts_rev = CheckoutsRev0
    } = State0
) ->
    case Available0 of
        [Conn | Available] ->
            case usable(Conn) of
                true ->
                    MRef = erlang:monitor(process, Pid),
                    State = State0#?MODULE{
                        available = Available,
                        checkouts = Checkouts0#{Conn => MRef},
                        checkouts_rev = CheckoutsRev0#{MRef => Conn}
                    },
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
        checkouts = Checkouts0,
        checkouts_rev = CheckoutsRev0,
        counter = Cnt
    } = State0
) ->
    case {queue:out(Pending0), Available0} of
        {{{value, #pending{from = From, mref = MRef}}, Pending}, [Conn | Available]} ->
            case usable(Conn) of
                true ->
                    counters:add(Cnt, ?C_CHECKOUTS, 1),
                    gen_server:reply(From, Conn),
                    State1 = cancel_idle_timer(Conn, State0#?MODULE{
                        pending = Pending,
                        available = Available,
                        checkouts = Checkouts0#{Conn => MRef},
                        checkouts_rev = CheckoutsRev0#{MRef => Conn}
                    }),
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

cancel(
    Conn,
    #?MODULE{
        available = Available0,
        checkouts = Checkouts0,
        checkouts_rev = CheckoutsRev0
    } = State0
) ->
    case Checkouts0 of
        #{Conn := CallerMRef} ->
            erlang:demonitor(CallerMRef, [flush]),
            State0#?MODULE{
                checkouts = maps:remove(Conn, Checkouts0),
                checkouts_rev = maps:remove(CallerMRef, CheckoutsRev0)
            };
        _ ->
            case lists:member(Conn, Available0) of
                true ->
                    State1 = State0#?MODULE{available = lists:delete(Conn, Available0)},
                    cancel_idle_timer(Conn, State1);
                false ->
                    State0
            end
    end.

%% A connection is usable for a new request only if its gun process is alive
%% and in the `connected` state. After S3 closes an idle connection gun clears
%% its socket and moves to `not_connected` (then stops, with retry=>0); a
%% request fired at it in that window is silently lost until the request times
%% out. Checked on checkout so such a connection is never handed out, covering
%% the window where the checkout is processed before gun's `gun_down`.
usable(Conn) ->
    is_process_alive(Conn) andalso
        case catch gun:info(Conn) of
            #{state_name := connected} -> true;
            _ -> false
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
close_connection(Conn, #?MODULE{monitors = Monitors, created = Created} = State) ->
    case Monitors of
        #{Conn := ConnMRef} ->
            erlang:demonitor(ConnMRef, [flush]),
            gun:close(Conn),
            State#?MODULE{
                monitors = maps:remove(Conn, Monitors),
                created = maps:remove(Conn, Created)
            };
        _ ->
            State
    end.

cancel_idle_timer(Conn, #?MODULE{idle_timers = Timers0} = State) ->
    case Timers0 of
        #{Conn := TRef} ->
            ok = erlang:cancel_timer(TRef, [{async, true}, {info, false}]),
            State#?MODULE{idle_timers = maps:remove(Conn, Timers0)};
        _ ->
            State
    end.

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
