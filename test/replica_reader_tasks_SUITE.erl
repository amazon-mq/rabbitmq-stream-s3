%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(replica_reader_tasks_SUITE).
-moduledoc """
Property and falsification tests for the pure async-task-lifecycle model
`rabbitmq_stream_s3_replica_reader_tasks`.

The point of this suite is not to confirm the model is green. It is to show the
harness is strong enough to *falsify* the two bug classes the model is meant to
make impossible:

1. The recovery-seam staleness class: an async result from a task that is no
   longer the live one must never be delivered to the core. The soundness oracle
   (`is_legitimate/3`) re-derives, from the pre-state and the event alone,
   whether a delivery is valid - independently of how `step/2` decides. A `step`
   that delivers a stale result disagrees with the oracle and is caught.

2. The gauge-drift class: the pipeline gauges (transfers-in-flight, bytes in
   transfer, bytes in persist) are *derived* from task state rather than
   maintained as independently-mutated counters. The conservation oracle
   (`gauge_delta_ok/3`) re-derives, from the pre-state and the event, the only
   gauge movements the transition is allowed to make. A byte mis-accounting (for
   example a failed persist that drops its bytes instead of returning them to the
   pending pool) disagrees with the oracle and is caught.

To prove each oracle has discriminating power we run a deliberately wrong variant
through it and require a counterexample:

- `step_buggy_genonly/2` (generation-only retention correlation) and
  `step_buggy_slotonly/2` (monitor-field-only persist correlation) are faithful
  reproductions of the historical schemes this model replaces; the soundness
  property finds a counterexample for each. The generation-only variant is the
  exact timeout-race that was previously found by hand; the slot-only variant is
  the original cross-recovery mis-attribution.
- `indep_buggy/2` maintains the transfers-in-flight gauge as a separate counter
  (the historical design) with one missing decrement on the deadline path; the
  gauge property finds the drift the derived gauge structurally cannot have.
""".

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(M, rabbitmq_stream_s3_replica_reader_tasks).

all() ->
    [
        correct_step_is_sound,
        correct_step_preserves_structural_invariants,
        property_catches_timeout_race,
        property_catches_cross_recovery_mis_attribution,
        timeout_race_is_deterministically_caught,
        cross_recovery_is_deterministically_caught,
        gauges_are_nonnegative,
        recover_zeroes_all_gauges,
        gauges_are_conserved,
        persist_byte_oracle_rejects_lost_bytes,
        derived_gauge_matches_disciplined_counter,
        property_catches_gauge_drift,
        gauge_drift_is_deterministically_caught
    ].

%% =========================================================================
%% Soundness property cases
%% =========================================================================

%% The real step never delivers a stale result, for any interleaving.
correct_step_is_sound(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun() -> prop_sound(fun ?M:step/2) end, [], 5000
    ).

%% The real step keeps the structural invariants (generation monotonic and only
%% bumped by recover; recover abandons every in-flight task and zeroes the
%% gauges).
correct_step_preserves_structural_invariants(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun() -> prop_structural(fun ?M:step/2) end, [], 5000
    ).

%% Falsification: the generation-only correlation (B's pre-fix retention logic)
%% must be caught - this is the timeout-race rediscovered automatically.
property_catches_timeout_race(_Config) ->
    CE = proper:counterexample(
        prop_sound(fun step_buggy_genonly/2), [{numtests, 5000}, quiet]
    ),
    ct:pal("generation-only counterexample: ~p", [CE]),
    ?assertNotEqual(true, CE).

%% Falsification: the slot-only correlation (the original pre-generation logic)
%% must be caught - this is the cross-recovery mis-attribution.
property_catches_cross_recovery_mis_attribution(_Config) ->
    CE = proper:counterexample(
        prop_sound(fun step_buggy_slotonly/2), [{numtests, 5000}, quiet]
    ),
    ct:pal("slot-only counterexample: ~p", [CE]),
    ?assertNotEqual(true, CE).

%% =========================================================================
%% Gauge property cases
%% =========================================================================

%% The derived gauges are never negative, for any interleaving.
gauges_are_nonnegative(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun() -> prop_gauges_nonnegative() end, [], 5000
    ).

%% recover resets every gauge (and the pending-bytes pool) to zero.
recover_zeroes_all_gauges(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun() -> prop_recover_zeroes_gauges() end, [], 5000
    ).

%% Over reachable operational sequences, each transition moves the gauges only
%% by the amount the conservation oracle allows.
gauges_are_conserved(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun() -> prop_gauge_conservation() end, [], 5000
    ).

%% The derived gauge equals a correctly-disciplined independent counter.
derived_gauge_matches_disciplined_counter(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun() -> prop_gauge_matches_independent(fun indep_correct/2) end, [], 5000
    ).

%% Falsification: an independently-maintained counter with one missing decrement
%% (the historical gauge-drift bug) must be caught.
property_catches_gauge_drift(_Config) ->
    CE = proper:counterexample(
        prop_gauge_matches_independent(fun indep_buggy/2), [{numtests, 5000}, quiet]
    ),
    ct:pal("gauge-drift counterexample: ~p", [CE]),
    ?assertNotEqual(true, CE).

%% =========================================================================
%% Deterministic pins
%% =========================================================================

%% A retention result that arrives after its own timeout (slot freed, generation
%% unchanged): the correct step drops it; the generation-only step delivers it on
%% an idle slot, which the oracle flags.
timeout_race_is_deterministically_caught(_Config) ->
    Seq = [spawn_retention, {retention_timeout, 0}, {retention_result, 0, unchanged}],
    ?assert(replay_sound(fun ?M:step/2, Seq)),
    ?assertNot(replay_sound(fun step_buggy_genonly/2, Seq)).

%% A persist result from before a recovery, arriving while a new persist occupies
%% the slot at the next generation: the correct step drops it; the slot-only step
%% delivers it to the new task, which the oracle flags.
cross_recovery_is_deterministically_caught(_Config) ->
    Seq = [
        spawn_persist,
        recover,
        spawn_persist,
        {persist_result, 0, {ok, 99}}
    ],
    ?assert(replay_sound(fun ?M:step/2, Seq)),
    ?assertNot(replay_sound(fun step_buggy_slotonly/2, Seq)).

%% A failed persist must return its snapshotted bytes to the pending pool so the
%% next persist covers them; bytes_in_persist is unchanged. The conservation
%% oracle rejects a transition that instead drops the bytes - which is exactly
%% the post-state a successful persist produces, so we feed the success post-state
%% under a failure event and require the oracle to flag it.
persist_byte_oracle_rejects_lost_bytes(_Config) ->
    Pre = run(fun ?M:step/2, [
        {spawn_transfer, r1, 7, t1}, {transfer_result, r1, {ok, <<"u">>}}, spawn_persist
    ]),
    %% Sanity: the snapshot is non-zero, so dropping vs returning it is
    %% observable.
    {in_flight, G, Snap} = ?M:persist_slot(Pre),
    ?assert(Snap > 0),
    {PostOk, _} = ?M:step({persist_result, G, {ok, 1}}, Pre),
    {PostErr, _} = ?M:step({persist_result, G, {error, boom}}, Pre),
    %% The real transitions satisfy the oracle.
    ?assert(gauge_delta_ok(Pre, {persist_result, G, {ok, 1}}, PostOk)),
    ?assert(gauge_delta_ok(Pre, {persist_result, G, {error, boom}}, PostErr)),
    %% A failure that produced the success post-state (bytes dropped, not
    %% returned to pending) is rejected.
    ?assertNot(gauge_delta_ok(Pre, {persist_result, G, {error, boom}}, PostOk)).

%% A transfer that ends via its liveness deadline must drop out of the
%% transfers-in-flight gauge. The derived gauge does; an independent counter that
%% forgets to decrement on the deadline path leaks.
gauge_drift_is_deterministically_caught(_Config) ->
    Ops = [{spawn_transfer, r1, 5, t1}, {transfer_deadline, r1, t1}],
    ?assert(gauge_lockstep_ok(fun indep_correct/2, Ops)),
    ?assertNot(gauge_lockstep_ok(fun indep_buggy/2, Ops)).

%% =========================================================================
%% Properties
%% =========================================================================

prop_sound(StepFun) ->
    ?FORALL(Events, events(), replay_sound(StepFun, Events)).

prop_structural(StepFun) ->
    ?FORALL(Events, events(), replay_structural(StepFun, Events)).

prop_gauges_nonnegative() ->
    ?FORALL(Events, events(), all_states_nonnegative(Events)).

prop_recover_zeroes_gauges() ->
    ?FORALL(Events, events(), begin
        S = run(fun ?M:step/2, Events),
        {S2, _} = ?M:step(recover, S),
        gauges(S2) =:= {0, 0, 0} andalso ?M:persist_pending_bytes(S2) =:= 0
    end).

prop_gauge_conservation() ->
    ?FORALL(Ops, ops(), replay_gauge_conservation(Ops)).

prop_gauge_matches_independent(IndepFun) ->
    ?FORALL(Ops, ops(), gauge_lockstep_ok(IndepFun, Ops)).

%% =========================================================================
%% Replay folds
%% =========================================================================

%% Fold StepFun over the events; every decision at every step must be legitimate
%% with respect to the state *before* that step.
replay_sound(StepFun, Events) ->
    {_Final, Ok} = lists:foldl(
        fun
            (Ev, {S, true}) ->
                {S2, Decisions} = StepFun(Ev, S),
                {S2, lists:all(fun(D) -> is_legitimate(S, Ev, D) end, Decisions)};
            (_Ev, {S, false}) ->
                {S, false}
        end,
        {?M:init(), true},
        Events
    ),
    Ok.

replay_structural(StepFun, Events) ->
    {_Final, Ok} = lists:foldl(
        fun
            (Ev, {S, true}) ->
                {S2, _Decisions} = StepFun(Ev, S),
                {S2, structural_ok(S, Ev, S2)};
            (_Ev, {S, false}) ->
                {S, false}
        end,
        {?M:init(), true},
        Events
    ),
    Ok.

replay_gauge_conservation(Ops) ->
    {_Final, Ok} = lists:foldl(
        fun
            (Ev, {S, true}) ->
                {S2, _} = ?M:step(Ev, S),
                {S2, gauge_delta_ok(S, Ev, S2)};
            (_Ev, {S, false}) ->
                {S, false}
        end,
        {?M:init(), true},
        Ops
    ),
    Ok.

all_states_nonnegative(Events) ->
    {_Final, Ok} = lists:foldl(
        fun
            (Ev, {S, true}) ->
                {S2, _} = ?M:step(Ev, S),
                {S2, gauges_nonnegative(S2)};
            (_Ev, {S, false}) ->
                {S, false}
        end,
        {?M:init(), gauges_nonnegative(?M:init())},
        Events
    ),
    Ok.

%% Fold StepFun over the events, threading the model and the independent counter
%% in lockstep; the model's derived transfers-in-flight gauge must equal the
%% independent count after every step.
gauge_lockstep_ok(IndepFun, Ops) ->
    {_S, _I, Ok} = lists:foldl(
        fun
            (Ev, {S, I, true}) ->
                {S2, _} = ?M:step(Ev, S),
                I2 = IndepFun(Ev, I),
                {S2, I2, ?M:transfers_in_flight(S2) =:= element(1, I2)};
            (_Ev, Acc = {_, _, false}) ->
                Acc
        end,
        {?M:init(), {0, #{}}, true},
        Ops
    ),
    Ok.

%% Run StepFun over a sequence and return the final state, discarding decisions.
run(StepFun, Events) ->
    lists:foldl(fun(Ev, S) -> element(1, StepFun(Ev, S)) end, ?M:init(), Events).

%% =========================================================================
%% Faithful reproductions of the historical correlation schemes this model
%% replaces, expressed through the model's public API. The property must find a
%% counterexample for each; that is the evidence the harness is strong enough.
%% =========================================================================

%% Generation-only retention correlation (B's pre-fix logic, before the
%% retention_mon guard was restored): a result whose generation matches the
%% current one is delivered without checking the slot is still occupied. A result
%% that raced its own timeout - slot already idle, generation unchanged - is
%% wrongly delivered. The state transition is delegated to the real step; only
%% the (buggy) decision is overridden.
step_buggy_genonly({retention_result, G, Result} = Ev, S) ->
    case ?M:generation(S) =:= G of
        true ->
            {S2, _} = ?M:step(Ev, S),
            {S2, [classify_retention(Result)]};
        false ->
            ?M:step(Ev, S)
    end;
step_buggy_genonly(Ev, S) ->
    ?M:step(Ev, S).

%% Monitor-field-only correlation (the original pre-generation logic): a result
%% is delivered to whatever task occupies the slot, ignoring which incarnation it
%% belongs to. After a recover plus a re-spawn, a result from the old task is
%% wrongly delivered to the new one.
step_buggy_slotonly({persist_result, _G, Result} = Ev, S) ->
    case ?M:persist_slot(S) of
        {in_flight, _SlotG, _} ->
            {S2, _} = ?M:step(Ev, S),
            {S2, [classify_persist(Result)]};
        idle ->
            ?M:step(Ev, S)
    end;
step_buggy_slotonly(Ev, S) ->
    ?M:step(Ev, S).

%% Local mirrors of the model's private classifiers, for the buggy variants.
classify_retention(unchanged) -> {deliver, retention_failed, unchanged};
classify_retention({failed, Reason}) -> {deliver, retention_failed, Reason};
classify_retention({_Edit, _Refs}) -> {deliver, retention_complete, edit}.

classify_persist({ok, Revision}) -> {deliver, persist_complete, Revision};
classify_persist({error, {conflict, _}}) -> {deliver, persist_failed, conflict};
classify_persist({error, Reason}) -> {deliver, persist_failed, Reason}.

%% =========================================================================
%% Independent transfers-in-flight counters (the historical design): a count and
%% the set of refs it believes are outstanding. The correct discipline
%% decrements on every path that removes a transfer; the buggy one forgets the
%% deadline path, so its count drifts above the derived gauge.
%% =========================================================================

indep_correct({spawn_transfer, Ref, _Size, _Token}, {C, Refs}) ->
    case is_map_key(Ref, Refs) of
        true -> {C, Refs};
        false -> {C + 1, Refs#{Ref => true}}
    end;
indep_correct({transfer_result, Ref, _}, State) ->
    indep_remove(Ref, State);
indep_correct({transfer_deadline, Ref, _}, State) ->
    indep_remove(Ref, State);
indep_correct(recover, _State) ->
    {0, #{}};
indep_correct(_Ev, State) ->
    State.

indep_buggy({transfer_deadline, Ref, _}, {C, Refs}) ->
    %% Removes the ref from the tracked set but forgets to decrement the count -
    %% the gauge leaks by one per deadline.
    case is_map_key(Ref, Refs) of
        true -> {C, maps:remove(Ref, Refs)};
        false -> {C, Refs}
    end;
indep_buggy(Ev, State) ->
    indep_correct(Ev, State).

indep_remove(Ref, {C, Refs}) ->
    case is_map_key(Ref, Refs) of
        true -> {C - 1, maps:remove(Ref, Refs)};
        false -> {C, Refs}
    end.

%% =========================================================================
%% Soundness oracle: independent of step/2, decides whether a decision is valid
%% given only the pre-state and the event.
%% =========================================================================

is_legitimate(_S, _Event, {drop, _Reason}) ->
    true;
is_legitimate(S, {persist_result, G, _}, {deliver, C, _}) when
    C =:= persist_complete; C =:= persist_failed
->
    matches_slot(?M:persist_slot(S), G);
is_legitimate(S, {group_result, G, _}, {deliver, C, _}) when
    C =:= group_complete; C =:= group_failed
->
    matches_slot(?M:group_slot(S), G);
is_legitimate(S, {retention_result, G, _}, {deliver, C, _}) when
    C =:= retention_complete; C =:= retention_failed
->
    matches_slot(?M:retention_slot(S), G);
is_legitimate(S, {retention_timeout, G}, {deliver, retention_failed, timeout}) ->
    matches_slot(?M:retention_slot(S), G);
is_legitimate(S, {transfer_result, Ref, _}, {deliver, C, _}) when
    C =:= transfer_complete; C =:= transfer_failed
->
    is_map_key(Ref, ?M:transfers(S));
is_legitimate(S, {transfer_deadline, Ref, Token}, {deliver, transfer_failed, _}) ->
    case maps:find(Ref, ?M:transfers(S)) of
        {ok, {_Size, Token}} -> true;
        _ -> false
    end;
is_legitimate(S, {retry_transfer, Ref}, {resubmit, Ref}) ->
    is_map_key(Ref, ?M:transfers(S));
%% Any other deliver (wrong completion for the event, or a deliver with no
%% matching live task) or any unexpected resubmit is illegitimate.
is_legitimate(_S, _Event, {deliver, _, _}) ->
    false;
is_legitimate(_S, _Event, {resubmit, _}) ->
    false.

matches_slot({in_flight, G, _Data}, G) -> true;
matches_slot(_Slot, _G) -> false.

%% =========================================================================
%% Structural invariants over a single transition.
%% =========================================================================

structural_ok(Pre, Event, Post) ->
    generation_monotonic(Pre, Event, Post) andalso recover_abandons_all(Event, Post).

generation_monotonic(Pre, Event, Post) ->
    GPre = ?M:generation(Pre),
    GPost = ?M:generation(Post),
    case Event of
        recover -> GPost =:= GPre + 1;
        _ -> GPost =:= GPre
    end.

recover_abandons_all(recover, Post) ->
    ?M:persist_slot(Post) =:= idle andalso
        ?M:group_slot(Post) =:= idle andalso
        ?M:retention_slot(Post) =:= idle andalso
        map_size(?M:transfers(Post)) =:= 0 andalso
        gauges(Post) =:= {0, 0, 0} andalso
        ?M:persist_pending_bytes(Post) =:= 0;
recover_abandons_all(_Event, _Post) ->
    true.

%% =========================================================================
%% Gauge conservation oracle: independent of step/2's internal byte arithmetic,
%% it re-derives the only gauge movement each transition is allowed to make from
%% the pre-state observed through the accessors.
%% =========================================================================

gauge_delta_ok(Pre, Event, Post) ->
    {TifP, BitP, BipP} = gauges(Pre),
    {TifQ, BitQ, BipQ} = gauges(Post),
    Unchanged = {TifQ, BitQ, BipQ} =:= {TifP, BitP, BipP},
    case Event of
        recover ->
            {TifQ, BitQ, BipQ} =:= {0, 0, 0};
        spawn_persist ->
            %% The pending pool is snapshotted into the persist slot; the total
            %% bytes awaiting durability are unchanged, transfers untouched.
            Unchanged;
        {spawn_transfer, Ref, Size, _Token} ->
            Old = transfer_size(Pre, Ref),
            New =
                case is_map_key(Ref, ?M:transfers(Pre)) of
                    true -> 0;
                    false -> 1
                end,
            TifQ =:= TifP + New andalso
                BitQ =:= BitP - Old + Size andalso
                BipQ =:= BipP;
        {persist_result, G, Result} ->
            case ?M:persist_slot(Pre) of
                {in_flight, G, Snap} ->
                    TifQ =:= TifP andalso BitQ =:= BitP andalso
                        case Result of
                            {ok, _} -> BipQ =:= BipP - Snap;
                            _ -> BipQ =:= BipP
                        end;
                _ ->
                    Unchanged
            end;
        {transfer_result, Ref, Result} ->
            case maps:find(Ref, ?M:transfers(Pre)) of
                {ok, {Size, _Token}} ->
                    TifQ =:= TifP - 1 andalso BitQ =:= BitP - Size andalso
                        case Result of
                            {ok, _} -> BipQ =:= BipP + Size;
                            _ -> BipQ =:= BipP
                        end;
                error ->
                    Unchanged
            end;
        {transfer_deadline, Ref, Token} ->
            case maps:find(Ref, ?M:transfers(Pre)) of
                {ok, {Size, Token}} ->
                    TifQ =:= TifP - 1 andalso BitQ =:= BitP - Size andalso BipQ =:= BipP;
                _ ->
                    Unchanged
            end;
        _ ->
            Unchanged
    end.

gauges(S) ->
    {?M:transfers_in_flight(S), ?M:bytes_in_transfer(S), ?M:bytes_in_persist(S)}.

gauges_nonnegative(S) ->
    {Tif, Bit, Bip} = gauges(S),
    Tif >= 0 andalso Bit >= 0 andalso Bip >= 0 andalso ?M:persist_pending_bytes(S) >= 0.

transfer_size(S, Ref) ->
    case maps:find(Ref, ?M:transfers(S)) of
        {ok, {Size, _Token}} -> Size;
        error -> 0
    end.

%% =========================================================================
%% Generators
%% =========================================================================

%% Arbitrary interleavings for the soundness and structural properties.
%% Generations, refs and tokens are drawn from small pools so that
%% result/timeout/deadline events frequently reference a task that is no longer
%% (or was never) the live one - the stale interleavings are where the staleness
%% bug class lives.
events() ->
    list(event()).

event() ->
    oneof([
        spawn_persist,
        {spawn_group, group_kind()},
        spawn_retention,
        {spawn_transfer, ref_id(), range(0, 1000), token()},
        {persist_result, gen(), persist_result_value()},
        {group_result, gen(), group_result_value()},
        {retention_result, gen(), retention_result_value()},
        {retention_timeout, gen()},
        {transfer_result, ref_id(), transfer_result_value()},
        {transfer_deadline, ref_id(), token()},
        {retry_transfer, ref_id()},
        recover
    ]).

gen() -> range(0, 3).

ref_id() -> oneof([r1, r2, r3, r4]).

token() -> oneof([t1, t2, t3]).

group_kind() -> oneof([group, kilo_group, mega_group]).

persist_result_value() ->
    oneof([{ok, range(1, 1000)}, {error, {conflict, entry}}, {error, slow_down}]).

group_result_value() ->
    oneof([{ok, <<"uid">>}, {error, slow_down}]).

%% The {Edit, Refs} shape is modelled as a 2-tuple whose first element is not one
%% of the reserved atoms.
retention_result_value() ->
    oneof([unchanged, {failed, boom}, {an_edit, []}]).

transfer_result_value() ->
    oneof([{ok, <<"uid">>}, {error, slow_down}]).

%% Reachable operational sequences for the gauge-conservation and
%% gauge-matching properties: spawns happen only into an idle slot, transfers use
%% a fresh ref per spawn, and result/timeout/deadline/retry events reference a
%% task that is actually live. This is the envelope the core's gating keeps the
%% shell within; the gauges are only meaningful along it. Staleness is exercised
%% by events() above, not here.
ops() ->
    ?SIZED(Size, valid_ops(?M:init(), Size)).

valid_ops(_State, 0) ->
    [];
valid_ops(State, Fuel) ->
    ?LET(
        Ev,
        valid_event(State, Fuel),
        ?LET(Rest, valid_ops(element(1, ?M:step(Ev, State)), Fuel - 1), [Ev | Rest])
    ).

valid_event(State, Fuel) ->
    %% Fuel strictly decreases, so {r, Fuel}/{tok, Fuel} are unique per spawn.
    Ref = {r, Fuel},
    Tok = {tok, Fuel},
    Spawns =
        [{spawn_transfer, Ref, range(0, 1000), Tok}] ++
            [spawn_persist || ?M:persist_slot(State) =:= idle] ++
            [{spawn_group, group_kind()} || ?M:group_slot(State) =:= idle] ++
            [spawn_retention || ?M:retention_slot(State) =:= idle],
    Persist =
        case ?M:persist_slot(State) of
            {in_flight, G, _} -> [{persist_result, G, persist_result_value()}];
            idle -> []
        end,
    Group =
        case ?M:group_slot(State) of
            {in_flight, G2, _} -> [{group_result, G2, group_result_value()}];
            idle -> []
        end,
    Retention =
        case ?M:retention_slot(State) of
            {in_flight, G3, _} ->
                [{retention_result, G3, retention_result_value()}, {retention_timeout, G3}];
            idle ->
                []
        end,
    Transfers =
        lists:flatmap(
            fun({R, {_Size, T}}) ->
                [
                    {transfer_result, R, transfer_result_value()},
                    {transfer_deadline, R, T},
                    {retry_transfer, R}
                ]
            end,
            maps:to_list(?M:transfers(State))
        ),
    oneof([recover | Spawns ++ Persist ++ Group ++ Retention ++ Transfers]).
