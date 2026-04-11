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
    %% Connections that received gun_down but not yet gun_up.
    down = #{} :: #{conn() => true}
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
            {reply, Conn, State};
        [] ->
            P = #pending{from = From, checkout = Checkout, mref = MRef},
            State1 = State0#?MODULE{pending = queue:in(P, Pending0)},
            {noreply, grow(State1)}
    end;
handle_call(Request, From, State) ->
    ?LOG_INFO(?MODULE_STRING " received unexpected call from ~p: ~W", [From, Request, 10]),
    {noreply, State}.

handle_cast(
    {checkin, Conn},
    #?MODULE{
        available = Available0,
        checkouts = Checkouts0,
        checkouts_rev = CheckoutsRev0,
        down = Down
    } = State0
) ->
    case Checkouts0 of
        #{Conn := MRef} ->
            erlang:demonitor(MRef, [flush]),
            State1 = State0#?MODULE{
                checkouts = maps:remove(Conn, Checkouts0),
                checkouts_rev = maps:remove(MRef, CheckoutsRev0)
            },
            case Down of
                #{Conn := _} ->
                    %% Connection is reconnecting. Don't return to available.
                    {noreply, State1};
                _ ->
                    State2 = State1#?MODULE{available = [Conn | Available0]},
                    {noreply, checkout(State2)}
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
    #?MODULE{
        available = Available0,
        monitors = Monitors,
        down = Down0
    } = State0
) ->
    %% `gun_up` messages cannot arrive from connections not opened by this pool.
    ?assert(is_map_key(Conn, Monitors)),
    State1 = State0#?MODULE{
        available = [Conn | Available0],
        down = maps:remove(Conn, Down0)
    },
    {noreply, checkout(State1)};
handle_info({gun_down, Conn, _Protocol, _Reason, _KilledStreams}, #?MODULE{down = Down0} = State0) ->
    %% The connection is down. Gun automatically attempts re-connection so
    %% remove it from available or checkouts until it comes back up.
    State1 = State0#?MODULE{down = Down0#{Conn => true}},
    {noreply, cancel(Conn, State1)};
handle_info(
    {'DOWN', MRef, process, Pid, _Reason},
    #?MODULE{
        available = Available0,
        checkouts = Checkouts0,
        checkouts_rev = CheckoutsRev0,
        monitors = Monitors0,
        pending = Pending0,
        down = Down0
    } = State0
) ->
    case Monitors0 of
        #{Pid := MRef} ->
            %% A connection is down. Forget it and grow a new one.
            Conn = Pid,
            State1 = State0#?MODULE{
                monitors = maps:remove(Conn, Monitors0),
                down = maps:remove(Conn, Down0)
            },
            State2 = cancel(Pid, State1),
            {noreply, grow(State2)};
        _ ->
            case CheckoutsRev0 of
                #{MRef := Conn} ->
                    %% A caller process is down which has a conn checked out.
                    %% Return the conn.
                    State1 = State0#?MODULE{
                        available = [Conn | Available0],
                        checkouts = maps:remove(Conn, Checkouts0),
                        checkouts_rev = maps:remove(MRef, CheckoutsRev0)
                    },
                    {noreply, checkout(State1)};
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
            {hibernate_after, 5000},
            %% Connections are mostly data pipes for large refc binaries.
            %% Full sweeps are nearly free since the process's heap doesn't
            %% contain much, and the immediate cleanup of dead refc binary
            %% references lets the binary allocator free promptly rather than
            %% holding until the nth minor GC triggers a full sweep.
            {receiver_spawn_opts, [{fullsweep_after, 0}]},
            {sender_spawn_opts, [{fullsweep_after, 0}]}
        ]
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
            State1 = State0#?MODULE{
                pending = Pending,
                available = Available,
                checkouts = Checkouts0#{Conn => MRef},
                checkouts_rev = CheckoutsRev0#{MRef => Conn}
            },
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
                    State0#?MODULE{available = lists:delete(Conn, Available0)};
                false ->
                    %% The conn could be spawned but never sent a
                    %% gun_up, so it might not be checked out or
                    %% available.
                    State0
            end
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
