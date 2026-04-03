%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_membership_reconciliation).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-record(?MODULE, {timer :: reference()}).

-define(SERVER, ?MODULE).
-define(EVAL, evaluate_membership).

-export([
    is_enabled/0,
    schedule/0,
    writer_started/0,
    evaluate_membership/0
]).

-export([
    start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%%----------------------------------------------------------------------------

is_enabled() ->
    application:get_env(
        rabbitmq_stream_s3,
        membership_reconciliation_enabled,
        false
    ).

schedule() ->
    gen_server:cast(?SERVER, {schedule, manual}).

writer_started() ->
    gen_server:cast(?SERVER, {schedule, writer_started}).

-doc """
Evaluates all streams with writers on this node immediately, even if automatic
evaluation is not enabled.
""".
evaluate_membership() ->
    Qs = [
        Q
     || Q <- rabbit_amqqueue:list_by_type(rabbit_stream_queue),
        amqqueue:get_leader_node(Q) =:= node()
    ],
    AllNodes = rabbit_nodes:list_members(),
    Running = rabbit_nodes:filter_running(AllNodes),
    evaluate_membership(Qs, AllNodes, Running, target_group_size(), auto_remove(), #{}).

%%----------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    case is_enabled() of
        true ->
            ?LOG_INFO(?MODULE_STRING " is enabled. Scheduling membership evaluation."),
            rabbitmq_stream_s3_membership_reconciliation_events:register(self()),
            {ok, #?MODULE{timer = erlang:send_after(trigger_interval(), self(), ?EVAL)}};
        false ->
            ?LOG_INFO(?MODULE_STRING " is not enabled. Exiting."),
            ignore
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(
    {schedule, Reason},
    #?MODULE{timer = Timer} = State0
) ->
    ?LOG_DEBUG("Stream membership reconciliation scheduled because of ~0p", [Reason]),
    _ = erlang:cancel_timer(Timer),
    {noreply, State0#?MODULE{timer = erlang:send_after(trigger_interval(), self(), ?EVAL)}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(?EVAL, #?MODULE{} = State0) ->
    Changes = evaluate_membership(),
    Timeout =
        case map_size(Changes) of
            0 ->
                interval();
            _ ->
                ?LOG_INFO(?MODULE_STRING ": evaluated memberships and made changes ~0p", [Changes]),
                trigger_interval()
        end,
    {noreply, State0#?MODULE{timer = erlang:send_after(Timeout, self(), ?EVAL)}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #?MODULE{}) ->
    rabbitmq_stream_s3_membership_reconciliation_events:unregister(self()),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

interval() ->
    application:get_env(
        rabbitmq_stream_s3,
        membership_reconciliation_interval,
        60_000 * 60
    ).
trigger_interval() ->
    application:get_env(
        rabbitmq_stream_s3,
        membership_reconciliation_trigger_interval,
        10_000
    ).
target_group_size() ->
    application:get_env(
        rabbitmq_stream_s3,
        membership_reconciliation_target_group_size,
        undefined
    ).
auto_remove() ->
    application:get_env(
        rabbitmq_stream_s3,
        membership_reconciliation_auto_remove,
        false
    ).

evaluate_membership(_Qs, [], _Running, _TargetSize, _AutoRemove, Changes) ->
    %% If there was an error attempting to list members, skip this round of evaluation.
    Changes;
evaluate_membership([], _AllNodes, _Running, _TargetSize, _AutoRemove, Changes) ->
    Changes;
evaluate_membership([Q | Qs], AllNodes, Running, TargetSize, AutoRemove, Changes0) ->
    maybe
        #{nodes := MemberNodes, leader_node := LeaderNode} = amqqueue:get_type_state(Q),
        LeaderNode ?= node(),
        false ?= rabbit_maintenance:is_being_drained_local_read(node()),
        DanglingNodes = MemberNodes -- AllNodes,
        Changes =
            case DanglingNodes of
                [_ | _] when AutoRemove ->
                    remove_members(Q, DanglingNodes, Changes0);
                _ ->
                    maybe_add_member(Q, Running, MemberNodes, TargetSize, Changes0)
            end,
        evaluate_membership(Qs, AllNodes, Running, TargetSize, AutoRemove, Changes)
    else
        {timeout, _Leader} ->
            increment_key(timeout, Changes0);
        _ ->
            Changes0
    end.

increment_key(Key, Map) ->
    maps:update_with(Key, fun(V) -> V + 1 end, 1, Map).

remove_members(_Q, [], Changes) ->
    Changes;
remove_members(Q, [Node | Rest], Changes0) ->
    QName = amqqueue:get_name(Q),
    ?LOG_DEBUG(?MODULE_STRING ": removing member of ~ts on node ~tw", [
        rabbit_misc:rs(QName), Node
    ]),
    Changes =
        case delete_replica(Q, Node) of
            ok ->
                increment_key(delete_ok, Changes0);
            {error, Reason} ->
                ?LOG_INFO(
                    ?MODULE_STRING ": failed to remove member of ~ts on node ~tw, error: ~0p",
                    [rabbit_misc:rs(QName), Node, Reason]
                ),
                increment_key(delete_error, Changes0)
        end,
    remove_members(Q, Rest, Changes).

delete_replica(Q, Node) ->
    #{name := StreamId} = amqqueue:get_type_state(Q),
    case rabbit_stream_coordinator:delete_replica(StreamId, Node) of
        {ok, Result, _} ->
            Result;
        {error, _} = Err ->
            Err
    end.

maybe_add_member(Q, Running, MemberNodes, TargetSize, Changes0) ->
    QName = amqqueue:get_name(Q),
    New = rabbit_maintenance:filter_out_drained_nodes_local_read(Running -- MemberNodes),
    case should_add_node(MemberNodes, New, TargetSize) of
        true ->
            Node = select_node(New),
            ?LOG_DEBUG(?MODULE_STRING ": adding member of ~ts on node ~tw", [
                rabbit_misc:rs(QName), Node
            ]),
            case rabbit_stream_coordinator:add_replica(Q, Node) of
                ok ->
                    increment_key(add_ok, Changes0);
                {error, Reason} ->
                    QName = amqqueue:get_name(Q),
                    ?LOG_INFO(
                        ?MODULE_STRING ": failed to add member of ~ts on node ~tw, error: ~0p",
                        [rabbit_misc:rs(QName), Node, Reason]
                    ),
                    increment_key(add_error, Changes0)
            end;
        false ->
            Changes0
    end.

should_add_node(MemberNodes, New, TargetSize) ->
    CurrentSize = length(MemberNodes),
    NumberOfNewNodes = length(New),
    maybe
        true ?= NumberOfNewNodes > 0,
        true ?= CurrentSize < TargetSize,
        true ?= rabbit_misc:is_even(CurrentSize) orelse NumberOfNewNodes > 1,
        true ?= rabbit_nodes:is_running(lists:delete(node(), MemberNodes))
    end.

select_node([Node]) ->
    Node;
select_node(Nodes) ->
    %% This could be improved to sort the nodes by viability.
    lists:nth(rand:uniform(length(Nodes)), Nodes).
