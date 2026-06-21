%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(replica_reader_tasks_SUITE).
-moduledoc """
Property and falsification tests for the pure async-task-lifecycle model
`rabbitmq_stream_s3_replica_reader_tasks`.

The point of this suite is not to confirm the model is green. It is to show the
harness is strong enough to *falsify* the recovery-seam bug class: an async
result from a task that is no longer the live one must never be delivered to the
core. The soundness oracle (`is_legitimate/3`) re-derives, from the pre-state and
the event alone, whether a delivery is valid - independently of how `step/2`
decides. A `step` that delivers a stale result disagrees with the oracle and is
caught.

To prove the oracle has discriminating power, two faithful reproductions of the
historical correlation schemes this model replaces (`step_buggy_genonly/2`,
`step_buggy_slotonly/2`) are run through the same property: the property must
find a counterexample for each. The generation-only variant is the exact
timeout-race that was previously found by hand; the slot-only variant is the
original cross-recovery mis-attribution.
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
        cross_recovery_is_deterministically_caught
    ].

%% =========================================================================
%% Property cases
%% =========================================================================

%% The real step never delivers a stale result, for any interleaving.
correct_step_is_sound(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun() -> prop_sound(fun ?M:step/2) end, [], 5000
    ).

%% The real step keeps the structural invariants (generation monotonic and only
%% bumped by recover; recover abandons every in-flight task).
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
%% Deterministic pins for the two historical bugs
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
        {spawn_persist, 10},
        recover,
        {spawn_persist, 20},
        {persist_result, 0, {ok, 99}}
    ],
    ?assert(replay_sound(fun ?M:step/2, Seq)),
    ?assertNot(replay_sound(fun step_buggy_slotonly/2, Seq)).

%% =========================================================================
%% Properties
%% =========================================================================

prop_sound(StepFun) ->
    ?FORALL(Events, events(), replay_sound(StepFun, Events)).

prop_structural(StepFun) ->
    ?FORALL(Events, events(), replay_structural(StepFun, Events)).

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
%% Any other deliver (wrong completion for the event, or a deliver with no
%% matching live task) is illegitimate.
is_legitimate(_S, _Event, {deliver, _, _}) ->
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
        map_size(?M:transfers(Post)) =:= 0;
recover_abandons_all(_Event, _Post) ->
    true.

%% =========================================================================
%% Generators
%% =========================================================================

%% Generations and refs are drawn from small pools so that result/timeout events
%% frequently reference a task that is no longer (or was never) the live one -
%% the stale interleavings are where the bug class lives.
events() ->
    list(event()).

event() ->
    oneof([
        {spawn_persist, range(1, 1000)},
        {spawn_group, group_kind()},
        spawn_retention,
        {spawn_transfer, ref_id(), range(1, 1000)},
        {persist_result, gen(), persist_result_value()},
        {group_result, gen(), group_result_value()},
        {retention_result, gen(), retention_result_value()},
        {retention_timeout, gen()},
        {transfer_result, ref_id(), transfer_result_value()},
        recover
    ]).

gen() -> range(0, 3).

ref_id() -> oneof([r1, r2, r3, r4]).

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
