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

-behaviour(gen_server).

%% Reap proactively before S3 closes an idle connection from under us.
-define(IDLE_TIMEOUT_MS, 10_000).

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
    idle_timers = #{} :: #{conn() => reference()}
}).

%% Gun connection pid.
-type conn() :: pid().

-type pool() :: atom().

%% An attempt to check out a conn. (Internal.)
-type checkout() :: reference().

-type config() :: #{
    min_size := non_neg_integer(),
    max_size := pos_integer()
}.

%% API
-export([
    checkout/2,
    try_checkout/1,
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

-doc """
Non-blocking checkout. Returns `{ok, Conn}` if a connection is immediately
available, or `busy` if the pool is exhausted. Unlike `checkout/2`, this
never queues a pending request, so no `cancel_checkout` is needed on timeout.
""".
-spec try_checkout(pool()) -> {ok, conn()} | busy.
try_checkout(Pool) ->
    gen_server:call(Pool, {try_checkout, erlang:make_ref()}, 5000).

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

init(#{min_size := MinSize, max_size := MaxSize}) ->
    case rabbitmq_stream_s3_api:backend() of
        rabbitmq_stream_s3_api_aws ->
            self() ! grow,
            {ok, #?MODULE{min_size = MinSize, max_size = MaxSize}};
        _ ->
            ignore
    end.

handle_call(
    {checkout, Checkout},
    {Pid, _} = From,
    #?MODULE{
        pending = Pending0,
        available = Available0,
        checkouts = Checkouts0,
        checkouts_rev = CheckoutsRev0
    } = State0
) ->
    MRef = erlang:monitor(process, Pid),
    case Available0 of
        [Conn | Available] ->
            State = State0#?MODULE{
                available = Available,
                checkouts = Checkouts0#{Conn => MRef},
                checkouts_rev = CheckoutsRev0#{MRef => Conn}
            },
            {reply, Conn, cancel_idle_timer(Conn, State)};
        [] ->
            P = #pending{from = From, checkout = Checkout, mref = MRef},
            State1 = State0#?MODULE{pending = queue:in(P, Pending0)},
            {noreply, grow(State1)}
    end;
handle_call(
    {try_checkout, _Checkout},
    {Pid, _} = _From,
    #?MODULE{
        available = Available0,
        checkouts = Checkouts0,
        checkouts_rev = CheckoutsRev0
    } = State0
) ->
    case Available0 of
        [Conn | Available] ->
            MRef = erlang:monitor(process, Pid),
            State = State0#?MODULE{
                available = Available,
                checkouts = Checkouts0#{Conn => MRef},
                checkouts_rev = CheckoutsRev0#{MRef => Conn}
            },
            {reply, {ok, Conn}, cancel_idle_timer(Conn, State)};
        [] ->
            {reply, busy, State0}
    end;
handle_call(Request, From, State) ->
    ?LOG_INFO(?MODULE_STRING " received unexpected call from ~p: ~W", [From, Request, 10]),
    {noreply, State}.

handle_cast(
    {checkin, Conn},
    #?MODULE{
        checkouts = Checkouts0,
        checkouts_rev = CheckoutsRev0,
        monitors = Monitors
    } = State0
) ->
    case Checkouts0 of
        #{Conn := MRef} ->
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
handle_cast({cancel_checkout, Checkout}, #?MODULE{pending = Pending0} = State0) ->
    Pending = queue:delete_with(
        fun(#pending{checkout = C, mref = MRef}) ->
            case C =:= Checkout of
                true ->
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

handle_info(grow, State0) ->
    {noreply, grow(State0)};
handle_info(
    {gun_up, Conn, _Protocol},
    #?MODULE{monitors = Monitors} = State0
) ->
    case is_map_key(Conn, Monitors) of
        true ->
            {noreply, make_available(Conn, State0)};
        false ->
            %% Stale gun_up from a connection opened by a previous pool instance.
            gun:close(Conn),
            {noreply, State0}
    end;
handle_info({gun_down, _Conn, _Protocol, _Reason, _KilledStreams}, #?MODULE{} = State) ->
    %% With retry=>0, gun stops the connection process immediately after sending
    %% this message (with reason 'normal' for a clean close, or
    %% '{shutdown, Reason}' otherwise). The 'DOWN' handler does all cleanup.
    {noreply, State};
handle_info(
    {'DOWN', MRef, process, Pid, _Reason},
    #?MODULE{
        checkouts = Checkouts0,
        checkouts_rev = CheckoutsRev0,
        monitors = Monitors0,
        pending = Pending0
    } = State0
) ->
    case Monitors0 of
        #{Pid := MRef} ->
            %% A connection is down. Forget it and maybe grow a new one.
            Conn = Pid,
            State1 = State0#?MODULE{monitors = maps:remove(Conn, Monitors0)},
            State2 = cancel(Pid, State1),
            {noreply, grow(State2)};
        _ ->
            case CheckoutsRev0 of
                #{MRef := Conn} ->
                    %% A caller process is down which has a conn checked out.
                    %% Return the conn.
                    State1 = State0#?MODULE{
                        checkouts = maps:remove(Conn, Checkouts0),
                        checkouts_rev = maps:remove(MRef, CheckoutsRev0)
                    },
                    {noreply, make_available(Conn, State1)};
                _ ->
                    %% A pending caller process is down. Remove it from the
                    %% pending queue.
                    Pending = queue:delete_with(
                        fun(#pending{from = {P, _}}) -> P =:= Pid end, Pending0
                    ),
                    State = State0#?MODULE{pending = Pending},
                    {noreply, State}
            end
    end;
handle_info({idle_timeout, Conn}, #?MODULE{idle_timers = Timers, monitors = Monitors} = State0) ->
    case Timers of
        #{Conn := _} ->
            %% Timer is still active (wasn't cancelled by a checkout), so the
            %% connection has been idle too long. Close it.
            State1 = State0#?MODULE{idle_timers = maps:remove(Conn, Timers)},
            State2 = cancel(Conn, State1),
            case Monitors of
                #{Conn := ConnMRef} ->
                    erlang:demonitor(ConnMRef, [flush]),
                    gun:close(Conn),
                    {noreply, State2#?MODULE{monitors = maps:remove(Conn, Monitors)}};
                _ ->
                    {noreply, State2}
            end;
        _ ->
            %% Timer was already cancelled (stale message). Ignore.
            {noreply, State0}
    end;
handle_info(Message, State) ->
    ?LOG_DEBUG(
        ?MODULE_STRING " received unexpected message: ~W",
        [Message, 10]
    ),
    {noreply, State}.

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
grow(N, #?MODULE{monitors = Monitors0} = State0) ->
    case open() of
        {ok, Conn} ->
            State = State0#?MODULE{monitors = Monitors0#{Conn => erlang:monitor(process, Conn)}},
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
    Host = binary_to_list(rabbitmq_stream_s3_api_aws:hostname()),
    Opts = #{
        transport => tls,
        %% AWS S3 only supports HTTP/1.1.
        protocols => [http],
        tls_opts => [
            %% Connections are mostly data pipes for large refc binaries.
            %% Full sweeps are nearly free since the process's heap doesn't
            %% contain much, and the immediate cleanup of dead refc binary
            %% references lets the binary allocator free promptly rather than
            %% holding until the nth minor GC triggers a full sweep.
            {receiver_spawn_opts, [{fullsweep_after, 0}]},
            {sender_spawn_opts, [{fullsweep_after, 0}]}
        ],
        %% Let connections which were closed from idleness (or errors) close
        %% and be reopened lazily. With TLSv1.3 it only costs one round trip.
        %% This pool optimizes for best resource use rather than consistently
        %% low latency. (A fully idle broker should have a min-sized pool.)
        retry => 0
    },
    gun:open(Host, 443, Opts).

checkout(
    #?MODULE{
        pending = Pending0,
        available = Available0,
        checkouts = Checkouts0,
        checkouts_rev = CheckoutsRev0
    } = State0
) ->
    case {queue:out(Pending0), Available0} of
        {{{value, #pending{from = From, mref = MRef}}, Pending}, [Conn | Available]} ->
            gen_server:reply(From, Conn),
            State1 = cancel_idle_timer(Conn, State0#?MODULE{
                pending = Pending,
                available = Available,
                checkouts = Checkouts0#{Conn => MRef},
                checkouts_rev = CheckoutsRev0#{MRef => Conn}
            }),
            checkout(State1);
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

make_available(Conn, #?MODULE{available = Available, idle_timers = Timers} = State) ->
    TRef = erlang:send_after(?IDLE_TIMEOUT_MS, self(), {idle_timeout, Conn}),
    checkout(State#?MODULE{
        available = [Conn | Available],
        idle_timers = Timers#{Conn => TRef}
    }).

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
