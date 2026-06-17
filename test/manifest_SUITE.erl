%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(manifest_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("include/rabbitmq_stream_s3.hrl").

%% Each test fragment covers 100 offsets and is 100 bytes of segment data.
-define(SZ, 100).

all() ->
    [
        partial_group_consumption_basic,
        partial_group_max_bytes_does_not_regress_first_offset,
        partial_group_max_age_no_negative_total_size,
        partial_then_full_consumption_removes_group
    ].

init_per_suite(Config) -> Config.
end_per_suite(Config) -> Config.

%% ------------------------------------------------------------------
%% Tests
%% ------------------------------------------------------------------

%% A single partial consumption behaves as expected: the oldest two children
%% are removed, total_size and first_offset advance to the first survivor.
%% This pins the first-cycle behavior the idempotency fix must not change.
partial_group_consumption_basic(_Config) ->
    {Manifest0, GetGroup} = grouped_manifest(),
    {Refs, M1} = run_cycle(Manifest0, [{max_bytes, 250}], 5000, GetGroup),
    ?assertEqual([0, 100], fragment_offsets(Refs)),
    ?assertEqual(200, M1#manifest.total_size),
    ?assertEqual(200, M1#manifest.first_offset),
    %% The group entry stays in the root (partial consumption).
    ?assertEqual(?ENTRY_B, byte_size(M1#manifest.entries)).

%% Regression for the primary finding: a second retention cycle against a
%% partially-consumed group must not re-process the already-removed prefix.
%% The immutable group object still lists F1/F2, so without the fix the second
%% cycle re-removes F1 and regresses first_offset below the real data floor.
partial_group_max_bytes_does_not_regress_first_offset(_Config) ->
    {Manifest0, GetGroup} = grouped_manifest(),
    %% Cycle 1: remove F1, F2. first_offset -> 200 (F3), total_size -> 200.
    {_Refs1, M1} = run_cycle(Manifest0, [{max_bytes, 250}], 5000, GetGroup),
    ?assertEqual(200, M1#manifest.first_offset),
    %% Cycle 2: a tighter cap removes only F3; first_offset advances to 300.
    {Refs2, M2} = run_cycle(M1, [{max_bytes, 150}], 6000, GetGroup),
    ?assertEqual([200], fragment_offsets(Refs2)),
    ?assertEqual(300, M2#manifest.first_offset),
    ?assertEqual(100, M2#manifest.total_size),
    %% first_offset must never regress below the previous cycle's floor.
    ?assert(M2#manifest.first_offset >= M1#manifest.first_offset).

%% Regression: re-processing an already-removed prefix double-subtracts its
%% size and would drive total_size negative (which crashes the persist when it
%% is serialized as an unsigned field). After the fix the second cycle sizes
%% only the surviving children.
partial_group_max_age_no_negative_total_size(_Config) ->
    {Manifest0, GetGroup} = grouped_manifest(),
    %% Cycle 1: max_bytes removes F1, F2. total_size -> 200, first_offset -> 200.
    {_Refs1, M1} = run_cycle(Manifest0, [{max_bytes, 250}], 5000, GetGroup),
    ?assertEqual(200, M1#manifest.total_size),
    %% Cycle 2: max_age expires F1, F2, F3 (last_ts 1000/2000/3000) but not F4
    %% (4000). Cutoff = Now - MaxAge = 9000 - 5500 = 3500.
    {Refs2, M2} = run_cycle(M1, [{max_age, 5500}], 9000, GetGroup),
    %% Only F3 is newly removable; F1/F2 are already gone.
    ?assertEqual([200], fragment_offsets(Refs2)),
    ?assertEqual(100, M2#manifest.total_size),
    ?assert(M2#manifest.total_size >= 0).

%% A partial consumption followed by a cycle that consumes the rest must fully
%% remove the group (entry + object) and account only for the survivors, not
%% re-list the already-removed prefix.
partial_then_full_consumption_removes_group(_Config) ->
    {Manifest0, GetGroup} = grouped_manifest(),
    {_Refs1, M1} = run_cycle(Manifest0, [{max_bytes, 250}], 5000, GetGroup),
    %% Cycle 2: a tiny cap consumes the remaining survivors F3, F4 entirely.
    {Refs2, M2} = run_cycle(M1, [{max_bytes, 50}], 6000, GetGroup),
    %% The group entry is gone from the root and total_size reaches zero.
    ?assertEqual(<<>>, M2#manifest.entries),
    ?assertEqual(0, M2#manifest.total_size),
    %% Only the survivors and the group object are deleted; F1/F2 are not
    %% re-deleted.
    ?assertEqual([200, 300], fragment_offsets(Refs2)),
    ?assertEqual(1, length(group_refs(Refs2))).

%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------

%% A manifest whose root is a single group of four fragments F1..F4 at offsets
%% 0/100/200/300, each 100 bytes, last_ts 1000/2000/3000/4000. build_manifest's
%% GetGroupFun always returns the full, immutable child list regardless of
%% edits applied to the root -- exactly how the real S3 group object behaves.
grouped_manifest() ->
    rabbitmq_stream_s3_test_helpers:build_manifest([
        {group, [
            {fragment, #{offset => 0, size => ?SZ, last_ts => 1000, uid => 1}},
            {fragment, #{offset => 100, size => ?SZ, last_ts => 2000, uid => 2}},
            {fragment, #{offset => 200, size => ?SZ, last_ts => 3000, uid => 3}},
            {fragment, #{offset => 300, size => ?SZ, last_ts => 4000, uid => 4}}
        ]}
    ]).

run_cycle(Manifest, Specs, Now, GetGroup) ->
    case rabbitmq_stream_s3_manifest:evaluate_remote_retention(Manifest, Specs, Now, GetGroup) of
        unchanged ->
            {[], Manifest};
        {Edit, Refs} ->
            {Refs, rabbitmq_stream_s3_manifest:apply_edit(Edit, Manifest)}
    end.

fragment_offsets(Refs) ->
    [O || #fragment_ref{offset = O} <- Refs].

group_refs(Refs) ->
    [R || #group_ref{} = R <- Refs].
