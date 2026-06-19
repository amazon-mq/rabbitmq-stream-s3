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
        partial_then_full_consumption_removes_group,
        kilo_group_max_age_partial,
        kilo_group_max_bytes,
        kilo_group_full_consumption,
        mega_group_retention,
        multi_tier_idempotent,
        apply_edit_appends_in_order,
        apply_edit_truncates_from_front,
        apply_edit_rejects_stale_append,
        apply_edit_rejects_gap_append,
        apply_edit_rejects_unaligned_pos,
        apply_edit_rejects_out_of_bounds_replace,
        apply_edit_rejects_nonzero_pos_truncate,
        apply_edit_rejects_negative_total_size
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

%% A kilo-group (a group of groups) where max_age expires the first inner group
%% wholly and the second inner group partially. Retention must descend two
%% levels: delete the wholly-consumed inner group object plus the expired leaf
%% fragments, leave the kilo-group entry in place, and advance first_offset to
%% the oldest survivor.
kilo_group_max_age_partial(_Config) ->
    {M0, GetGroup} = kilo_manifest(),
    %% last_ts are 1000..4000 for the kilo's fragments, 5000 for the trailing
    %% root fragment. Cutoff = 9000 - 5500 = 3500 expires F0..F2, keeps F3, F4.
    {Refs, M1} = run_cycle(M0, [{max_age, 5500}], 9000, GetGroup),
    ?assertEqual([0, 100, 200], fragment_offsets(Refs)),
    %% Exactly the first inner group object is deleted (it is wholly consumed).
    ?assertEqual(1, length(group_refs(Refs))),
    ?assertEqual(300, M1#manifest.first_offset),
    ?assertEqual(2 * ?SZ, M1#manifest.total_size),
    %% The kilo-group entry survives, so the two root entries remain.
    ?assertEqual(2 * ?ENTRY_B, byte_size(M1#manifest.entries)).

%% max_bytes across a kilo-group: the size to remove is computed by descending
%% to leaves (group entries carry no size).
kilo_group_max_bytes(_Config) ->
    {M0, GetGroup} = kilo_manifest(),
    %% total_size = 500. max_bytes 250 removes the oldest 250 bytes: F0, F1, F2.
    {Refs, M1} = run_cycle(M0, [{max_bytes, 250}], 1000, GetGroup),
    ?assertEqual([0, 100, 200], fragment_offsets(Refs)),
    ?assertEqual(1, length(group_refs(Refs))),
    ?assertEqual(300, M1#manifest.first_offset),
    ?assertEqual(2 * ?SZ, M1#manifest.total_size).

%% Expiring everything consumes the whole kilo-group and the trailing root
%% fragment: both root entries are spliced and every nested object is deleted.
kilo_group_full_consumption(_Config) ->
    {M0, GetGroup} = kilo_manifest(),
    {Refs, M1} = run_cycle(M0, [{max_age, 0}], 99999999, GetGroup),
    ?assertEqual([0, 100, 200, 300, 400], fragment_offsets(Refs)),
    %% The kilo-group object and both inner group objects are deleted.
    ?assertEqual(3, length(group_refs(Refs))),
    ?assertEqual(<<>>, M1#manifest.entries),
    ?assertEqual(0, M1#manifest.total_size),
    ?assertEqual(M0#manifest.next_offset, M1#manifest.first_offset).

%% A three-level tree (mega-group of kilo-groups of groups) is handled to leaf
%% granularity.
mega_group_retention(_Config) ->
    {M0, GetGroup} = mega_manifest(),
    %% Remove just the oldest fragment by bytes (total 800, keep 700).
    {Refs, M1} = run_cycle(M0, [{max_bytes, 700}], 1000, GetGroup),
    ?assertEqual([0], fragment_offsets(Refs)),
    ?assertEqual(100, M1#manifest.first_offset),
    ?assertEqual(7 * ?SZ, M1#manifest.total_size).

%% Multi-tier retention is idempotent across cycles: a second pass against the
%% partially-consumed kilo-group does not re-delete the objects the first pass
%% removed and keeps total_size and first_offset consistent.
multi_tier_idempotent(_Config) ->
    {M0, GetGroup} = kilo_manifest(),
    {Refs1, M1} = run_cycle(M0, [{max_bytes, 350}], 1000, GetGroup),
    ?assertEqual([0, 100], fragment_offsets(Refs1)),
    ?assertEqual(200, M1#manifest.first_offset),
    ?assertEqual(300, M1#manifest.total_size),
    %% Second cycle removes only the next fragment; F0/F1 are not revisited.
    {Refs2, M2} = run_cycle(M1, [{max_bytes, 250}], 1000, GetGroup),
    ?assertEqual([200], fragment_offsets(Refs2)),
    ?assertEqual(300, M2#manifest.first_offset),
    ?assertEqual(200, M2#manifest.total_size),
    ?assert(M2#manifest.first_offset >= M1#manifest.first_offset).

%% ------------------------------------------------------------------
%% apply_edit/2 trust-boundary hardening
%% ------------------------------------------------------------------
%%
%% apply_edit/2 raises on a structurally inconsistent edit so the replica cache
%% can catch it and resync rather than silently corrupt its entries array or
%% crash the per-node cache shared by every stream.

apply_edit_appends_in_order(_Config) ->
    M0 = mk_manifest([0], 100),
    Edit = append_edit(1, 100, byte_size(M0#manifest.entries)),
    M1 = rabbitmq_stream_s3_manifest:apply_edit(Edit, M0),
    ?assertEqual(2 * ?ENTRY_B, byte_size(M1#manifest.entries)),
    ?assertEqual(200, M1#manifest.total_size),
    ?assertEqual(2, M1#manifest.next_offset).

apply_edit_truncates_from_front(_Config) ->
    M0 = mk_manifest([0, 1], 200),
    Edit = #edit{
        first_offset = 1,
        first_timestamp = 10,
        first_last_timestamp = 11,
        next_offset = undefined,
        size = -100,
        entries = <<>>,
        pos = 0,
        len = ?ENTRY_B
    },
    M1 = rabbitmq_stream_s3_manifest:apply_edit(Edit, M0),
    ?assertEqual(?ENTRY_B, byte_size(M1#manifest.entries)),
    ?assertEqual(100, M1#manifest.total_size).

%% The double-apply shape: an append whose Pos is short of the array end
%% because this replica already applied it. Must be rejected, not spliced in.
apply_edit_rejects_stale_append(_Config) ->
    M0 = mk_manifest([0, 1], 200),
    Stale = append_edit(2, 100, ?ENTRY_B),
    ?assertError(_, rabbitmq_stream_s3_manifest:apply_edit(Stale, M0)).

%% An append whose Pos is past the array end (this replica missed edits).
apply_edit_rejects_gap_append(_Config) ->
    M0 = mk_manifest([0], 100),
    Ahead = append_edit(1, 100, 3 * ?ENTRY_B),
    ?assertError(_, rabbitmq_stream_s3_manifest:apply_edit(Ahead, M0)).

apply_edit_rejects_unaligned_pos(_Config) ->
    M0 = mk_manifest([0], 100),
    Edit = (append_edit(1, 100, 0))#edit{pos = 1},
    ?assertError(_, rabbitmq_stream_s3_manifest:apply_edit(Edit, M0)).

apply_edit_rejects_out_of_bounds_replace(_Config) ->
    M0 = mk_manifest([0], 100),
    Edit = #edit{
        first_offset = 0,
        first_timestamp = 0,
        first_last_timestamp = 1,
        next_offset = undefined,
        size = 0,
        entries = frag_entry(9),
        pos = 0,
        len = 2 * ?ENTRY_B
    },
    ?assertError(_, rabbitmq_stream_s3_manifest:apply_edit(Edit, M0)).

apply_edit_rejects_nonzero_pos_truncate(_Config) ->
    M0 = mk_manifest([0, 1], 200),
    Edit = #edit{
        first_offset = 0,
        first_timestamp = 0,
        first_last_timestamp = 1,
        next_offset = undefined,
        size = -100,
        entries = <<>>,
        pos = ?ENTRY_B,
        len = ?ENTRY_B
    },
    ?assertError(_, rabbitmq_stream_s3_manifest:apply_edit(Edit, M0)).

apply_edit_rejects_negative_total_size(_Config) ->
    M0 = mk_manifest([0], 50),
    Edit = #edit{
        first_offset = 1,
        first_timestamp = 10,
        first_last_timestamp = 11,
        next_offset = undefined,
        size = -100,
        entries = <<>>,
        pos = 0,
        len = ?ENTRY_B
    },
    ?assertError(_, rabbitmq_stream_s3_manifest:apply_edit(Edit, M0)).

%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------

%% A fragment entry (?ENTRY_B bytes) at the given offset, 100 bytes of data.
frag_entry(Offset) ->
    FirstTs = Offset * 10,
    LastTs = FirstTs + 1,
    Uid = Offset + 1,
    ?ENTRY(Offset, FirstTs, LastTs, ?MANIFEST_KIND_FRAGMENT, 100, Uid).

%% A flat manifest of fragment entries at the given offsets.
mk_manifest(Offsets, TotalSize) ->
    Entries = iolist_to_binary([frag_entry(O) || O <- Offsets]),
    #manifest{
        first_offset = hd(Offsets),
        first_timestamp = hd(Offsets) * 10,
        first_last_timestamp = hd(Offsets) * 10 + 1,
        next_offset = lists:last(Offsets) + 1,
        total_size = TotalSize,
        entries = Entries
    }.

%% An append edit adding one fragment at Offset with Size bytes, landing at Pos.
append_edit(Offset, Size, Pos) ->
    #edit{
        first_offset = 0,
        first_timestamp = 0,
        first_last_timestamp = 1,
        next_offset = Offset + 1,
        size = Size,
        entries = frag_entry(Offset),
        pos = Pos,
        len = 0
    }.

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

%% Root = [kilo-group(group[F0,F1], group[F2,F3]), F4]. Fragments at offsets
%% 0/100/200/300 nested two levels deep under the kilo-group, plus a trailing
%% root fragment F4 at 400. last_ts 1000..5000, each 100 bytes (total 500).
kilo_manifest() ->
    rabbitmq_stream_s3_test_helpers:build_manifest([
        {kilo_group, [
            {group, [
                {fragment, #{offset => 0, size => ?SZ, last_ts => 1000}},
                {fragment, #{offset => 100, size => ?SZ, last_ts => 2000}}
            ]},
            {group, [
                {fragment, #{offset => 200, size => ?SZ, last_ts => 3000}},
                {fragment, #{offset => 300, size => ?SZ, last_ts => 4000}}
            ]}
        ]},
        {fragment, #{offset => 400, size => ?SZ, last_ts => 5000}}
    ]).

%% Root = [mega-group(kilo-group(group[F0,F1], group[F2,F3]),
%%                     kilo-group(group[F4,F5], group[F6,F7]))]. Eight fragments
%% at offsets 0..700 nested three levels deep, each 100 bytes (total 800).
mega_manifest() ->
    Frag = fun(O) -> {fragment, #{offset => O, size => ?SZ}} end,
    rabbitmq_stream_s3_test_helpers:build_manifest([
        {mega_group, [
            {kilo_group, [
                {group, [Frag(0), Frag(100)]},
                {group, [Frag(200), Frag(300)]}
            ]},
            {kilo_group, [
                {group, [Frag(400), Frag(500)]},
                {group, [Frag(600), Frag(700)]}
            ]}
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
