%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(fragment_iterator_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("include/rabbitmq_stream_s3.hrl").

-import(rabbitmq_stream_s3_test_helpers, [build_manifest/1]).

all() ->
    [
        empty_manifest,
        single_fragment,
        multiple_fragments,
        descend_into_group,
        descend_into_kilo_group,
        descend_into_mega_group,
        end_of_manifest_after_last,
        init_at_offset_middle,
        init_at_offset_beyond_end,
        group_fetch_failed,
        size_field_correct,
        refresh_after_exhaustion,
        all_refs_empty,
        all_refs_fragments_only,
        all_refs_with_groups,
        descend_skips_retained_entries,
        descend_skips_retained_entries_kilo_group
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

%% ------------------------------------------------------------------
%% Tests
%% ------------------------------------------------------------------

empty_manifest(_Config) ->
    {Manifest, GetGroup} = build_manifest([]),
    It = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroup),
    ?assertEqual(end_of_manifest, rabbitmq_stream_s3_fragment_iterator:next(It)).

single_fragment(_Config) ->
    Uid = 16#aabbccdd,
    {Manifest, GetGroup} = build_manifest([
        {fragment, #{offset => 100, size => 64000, uid => Uid}}
    ]),
    It = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroup),
    {ok, #fragment_ref{offset = 100, uid = Uid, size = 64000}, It1} = rabbitmq_stream_s3_fragment_iterator:next(
        It
    ),
    ?assertEqual(end_of_manifest, rabbitmq_stream_s3_fragment_iterator:next(It1)).

multiple_fragments(_Config) ->
    {Manifest, GetGroup} = build_manifest([
        {fragment, #{offset => 0, size => 1000, uid => 1}},
        {fragment, #{offset => 50, size => 2000, uid => 2}},
        {fragment, #{offset => 100, size => 3000, uid => 3}}
    ]),
    It0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroup),
    {ok, #fragment_ref{offset = 0, uid = 1, size = 1000}, It1} = rabbitmq_stream_s3_fragment_iterator:next(
        It0
    ),
    {ok, #fragment_ref{offset = 50, uid = 2, size = 2000}, It2} = rabbitmq_stream_s3_fragment_iterator:next(
        It1
    ),
    {ok, #fragment_ref{offset = 100, uid = 3, size = 3000}, It3} = rabbitmq_stream_s3_fragment_iterator:next(
        It2
    ),
    ?assertEqual(end_of_manifest, rabbitmq_stream_s3_fragment_iterator:next(It3)).

descend_into_group(_Config) ->
    %% Root: [Group([F0, F1]), F2]
    {Manifest, GetGroup} = build_manifest([
        {group, [
            {fragment, #{offset => 0, uid => 16#aa}},
            {fragment, #{offset => 100, uid => 16#bb}}
        ]},
        {fragment, #{offset => 200, uid => 16#cc}}
    ]),
    It0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroup),
    {ok, #fragment_ref{offset = 0, uid = 16#aa, size = _}, It1} = rabbitmq_stream_s3_fragment_iterator:next(
        It0
    ),
    {ok, #fragment_ref{offset = 100, uid = 16#bb, size = _}, It2} = rabbitmq_stream_s3_fragment_iterator:next(
        It1
    ),
    {ok, #fragment_ref{offset = 200, uid = 16#cc, size = _}, It3} = rabbitmq_stream_s3_fragment_iterator:next(
        It2
    ),
    ?assertEqual(end_of_manifest, rabbitmq_stream_s3_fragment_iterator:next(It3)).

descend_into_kilo_group(_Config) ->
    %% Root: [KiloGroup([Group([F0, F1]), Group([F2, F3])]), F4]
    {Manifest, GetGroup} = build_manifest([
        {kilo_group, [
            {group, [
                {fragment, #{offset => 0, uid => 16#a0}},
                {fragment, #{offset => 100, uid => 16#a1}}
            ]},
            {group, [
                {fragment, #{offset => 200, uid => 16#a2}},
                {fragment, #{offset => 300, uid => 16#a3}}
            ]}
        ]},
        {fragment, #{offset => 400, uid => 16#a4}}
    ]),
    It0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroup),
    {ok, #fragment_ref{offset = 0, uid = 16#a0, size = _}, It1} = rabbitmq_stream_s3_fragment_iterator:next(
        It0
    ),
    {ok, #fragment_ref{offset = 100, uid = 16#a1, size = _}, It2} = rabbitmq_stream_s3_fragment_iterator:next(
        It1
    ),
    {ok, #fragment_ref{offset = 200, uid = 16#a2, size = _}, It3} = rabbitmq_stream_s3_fragment_iterator:next(
        It2
    ),
    {ok, #fragment_ref{offset = 300, uid = 16#a3, size = _}, It4} = rabbitmq_stream_s3_fragment_iterator:next(
        It3
    ),
    {ok, #fragment_ref{offset = 400, uid = 16#a4, size = _}, It5} = rabbitmq_stream_s3_fragment_iterator:next(
        It4
    ),
    ?assertEqual(end_of_manifest, rabbitmq_stream_s3_fragment_iterator:next(It5)).

descend_into_mega_group(_Config) ->
    %% Root: [MegaGroup([KiloGroup([Group([F0])])]), F1]
    {Manifest, GetGroup} = build_manifest([
        {mega_group, [
            {kilo_group, [
                {group, [
                    {fragment, #{offset => 0, uid => 16#ff}}
                ]}
            ]}
        ]},
        {fragment, #{offset => 800, uid => 16#ab}}
    ]),
    It0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroup),
    {ok, #fragment_ref{offset = 0, uid = 16#ff, size = _}, It1} = rabbitmq_stream_s3_fragment_iterator:next(
        It0
    ),
    {ok, #fragment_ref{offset = 800, uid = 16#ab, size = _}, It2} = rabbitmq_stream_s3_fragment_iterator:next(
        It1
    ),
    ?assertEqual(end_of_manifest, rabbitmq_stream_s3_fragment_iterator:next(It2)).

end_of_manifest_after_last(_Config) ->
    {Manifest, GetGroup} = build_manifest([
        {fragment, #{offset => 0}}
    ]),
    It0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroup),
    {ok, _, It1} = rabbitmq_stream_s3_fragment_iterator:next(It0),
    ?assertEqual(end_of_manifest, rabbitmq_stream_s3_fragment_iterator:next(It1)),
    %% Calling next again is idempotent.
    ?assertEqual(end_of_manifest, rabbitmq_stream_s3_fragment_iterator:next(It1)).

init_at_offset_middle(_Config) ->
    {Manifest, GetGroup} = build_manifest([
        {fragment, #{offset => 0, uid => 1}},
        {fragment, #{offset => 50, uid => 2}},
        {fragment, #{offset => 100, uid => 3}}
    ]),
    %% Start at offset 60 — positions at the fragment containing it (offset 50).
    It0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 60, GetGroup),
    {ok, #fragment_ref{offset = 50, uid = 2, size = _}, It1} = rabbitmq_stream_s3_fragment_iterator:next(
        It0
    ),
    {ok, #fragment_ref{offset = 100, uid = 3, size = _}, It2} = rabbitmq_stream_s3_fragment_iterator:next(
        It1
    ),
    ?assertEqual(end_of_manifest, rabbitmq_stream_s3_fragment_iterator:next(It2)).

init_at_offset_beyond_end(_Config) ->
    {Manifest, GetGroup} = build_manifest([
        {fragment, #{offset => 0, uid => 1}},
        {fragment, #{offset => 50, uid => 2}}
    ]),
    %% Start beyond all entries — positions at the last entry.
    It0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 9999, GetGroup),
    {ok, #fragment_ref{offset = 50, uid = 2, size = _}, It1} = rabbitmq_stream_s3_fragment_iterator:next(
        It0
    ),
    ?assertEqual(end_of_manifest, rabbitmq_stream_s3_fragment_iterator:next(It1)).

group_fetch_failed(_Config) ->
    %% Build a manifest with a group, but supply a failing get_group_fun.
    {Manifest, _} = build_manifest([
        {group, [
            {fragment, #{offset => 0}}
        ]}
    ]),
    FailingGetGroup = fun(#group_ref{}) -> {error, timeout} end,
    It0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, FailingGetGroup),
    ?assertMatch(
        {error, {group_fetch_failed, timeout}},
        rabbitmq_stream_s3_fragment_iterator:next(It0)
    ).

size_field_correct(_Config) ->
    Size = 67_108_864,
    {Manifest, GetGroup} = build_manifest([
        {fragment, #{offset => 0, size => Size, uid => 42}}
    ]),
    It0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroup),
    {ok, #fragment_ref{offset = 0, uid = 42, size = Size}, _} = rabbitmq_stream_s3_fragment_iterator:next(
        It0
    ).

refresh_after_exhaustion(_Config) ->
    %% Exhaust iterator, then get a fresh one with new entries appended.
    {Manifest1, GetGroup1} = build_manifest([
        {fragment, #{offset => 0, uid => 1}}
    ]),
    It0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest1, 0, GetGroup1),
    {ok, #fragment_ref{offset = 0, uid = 1, size = _}, It1} = rabbitmq_stream_s3_fragment_iterator:next(
        It0
    ),
    end_of_manifest = rabbitmq_stream_s3_fragment_iterator:next(It1),
    %% Simulate new manifest with appended fragment.
    {Manifest2, GetGroup2} = build_manifest([
        {fragment, #{offset => 0, uid => 1}},
        {fragment, #{offset => 50, uid => 2}}
    ]),
    It2 = rabbitmq_stream_s3_fragment_iterator:init(Manifest2, 50, GetGroup2),
    {ok, #fragment_ref{offset = 50, uid = 2, size = _}, It3} = rabbitmq_stream_s3_fragment_iterator:next(
        It2
    ),
    ?assertEqual(end_of_manifest, rabbitmq_stream_s3_fragment_iterator:next(It3)).

all_refs_empty(_Config) ->
    {Manifest, GetGroup} = build_manifest([]),
    ?assertEqual([], rabbitmq_stream_s3_fragment_iterator:all_refs(Manifest, GetGroup)).

all_refs_fragments_only(_Config) ->
    {Manifest, GetGroup} = build_manifest([
        {fragment, #{offset => 0, uid => 1, size => 1000}},
        {fragment, #{offset => 50, uid => 2, size => 2000}},
        {fragment, #{offset => 100, uid => 3, size => 3000}}
    ]),
    Refs = rabbitmq_stream_s3_fragment_iterator:all_refs(Manifest, GetGroup),
    ?assertEqual(3, length(Refs)),
    ?assertMatch(
        [
            #fragment_ref{offset = 0, uid = 1},
            #fragment_ref{offset = 50, uid = 2},
            #fragment_ref{offset = 100, uid = 3}
        ],
        Refs
    ).

all_refs_with_groups(_Config) ->
    {Manifest, GetGroup} = build_manifest([
        {group, [
            {fragment, #{offset => 0, uid => 16#a1}},
            {fragment, #{offset => 100, uid => 16#a2}}
        ]},
        {fragment, #{offset => 200, uid => 16#a3}}
    ]),
    Refs = rabbitmq_stream_s3_fragment_iterator:all_refs(Manifest, GetGroup),
    %% Should contain: the group ref, both fragments inside it, and the root fragment.
    ?assertEqual(4, length(Refs)),
    ?assertMatch(#group_ref{offset = 0}, hd(Refs)),
    FragOffsets = [O || #fragment_ref{offset = O} <- Refs],
    ?assertEqual([0, 100, 200], FragOffsets).

descend_skips_retained_entries(_Config) ->
    %% Root: [Group([F0, F100, F200, F300]), F400]
    %% Simulate partial retention: first_offset = 200 means F0 and F100
    %% have been deleted. The iterator should skip them when descending.
    {Manifest0, GetGroup} = build_manifest([
        {group, [
            {fragment, #{offset => 0, uid => 16#a0}},
            {fragment, #{offset => 100, uid => 16#a1}},
            {fragment, #{offset => 200, uid => 16#a2}},
            {fragment, #{offset => 300, uid => 16#a3}}
        ]},
        {fragment, #{offset => 400, uid => 16#a4}}
    ]),
    %% Advance first_offset to simulate retention having deleted F0 and F100.
    Manifest = Manifest0#manifest{first_offset = 200, first_timestamp = 200},
    It0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 200, GetGroup),
    %% First result should be F200, not F0.
    {ok, #fragment_ref{offset = 200, uid = 16#a2}, It1} =
        rabbitmq_stream_s3_fragment_iterator:next(It0),
    {ok, #fragment_ref{offset = 300, uid = 16#a3}, It2} =
        rabbitmq_stream_s3_fragment_iterator:next(It1),
    {ok, #fragment_ref{offset = 400, uid = 16#a4}, It3} =
        rabbitmq_stream_s3_fragment_iterator:next(It2),
    ?assertEqual(end_of_manifest, rabbitmq_stream_s3_fragment_iterator:next(It3)).

descend_skips_retained_entries_kilo_group(_Config) ->
    %% Root: [KiloGroup([Group([F0, F100]), Group([F200, F300])]), F400]
    %% first_offset = 200: the entire first group is skipped, and within
    %% the second group F200 is the first entry returned.
    {Manifest0, GetGroup} = build_manifest([
        {kilo_group, [
            {group, [
                {fragment, #{offset => 0, uid => 16#b0}},
                {fragment, #{offset => 100, uid => 16#b1}}
            ]},
            {group, [
                {fragment, #{offset => 200, uid => 16#b2}},
                {fragment, #{offset => 300, uid => 16#b3}}
            ]}
        ]},
        {fragment, #{offset => 400, uid => 16#b4}}
    ]),
    Manifest = Manifest0#manifest{first_offset = 200, first_timestamp = 200},
    It0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 200, GetGroup),
    %% Should skip the entire first group and start at F200 in the second.
    {ok, #fragment_ref{offset = 200, uid = 16#b2}, It1} =
        rabbitmq_stream_s3_fragment_iterator:next(It0),
    {ok, #fragment_ref{offset = 300, uid = 16#b3}, It2} =
        rabbitmq_stream_s3_fragment_iterator:next(It1),
    {ok, #fragment_ref{offset = 400, uid = 16#b4}, It3} =
        rabbitmq_stream_s3_fragment_iterator:next(It2),
    ?assertEqual(end_of_manifest, rabbitmq_stream_s3_fragment_iterator:next(It3)),

    %% first_offset = 300: skips the first group entirely AND F200 within
    %% the second group. First returned entry is F300.
    Manifest2 = Manifest0#manifest{first_offset = 300, first_timestamp = 300},
    It4 = rabbitmq_stream_s3_fragment_iterator:init(Manifest2, 300, GetGroup),
    {ok, #fragment_ref{offset = 300, uid = 16#b3}, It5} =
        rabbitmq_stream_s3_fragment_iterator:next(It4),
    {ok, #fragment_ref{offset = 400, uid = 16#b4}, It6} =
        rabbitmq_stream_s3_fragment_iterator:next(It5),
    ?assertEqual(end_of_manifest, rabbitmq_stream_s3_fragment_iterator:next(It6)).
