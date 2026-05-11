%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_test_helpers).
-moduledoc """
Shared test helpers for rabbitmq_stream_s3 CT suites.
""".

-include("include/rabbitmq_stream_s3.hrl").

-compile([export_all, nowarn_export_all]).

%% ------------------------------------------------------------------
%% Manifest tree builder
%% ------------------------------------------------------------------

-type fragment_spec() ::
    {fragment, #{
        offset := non_neg_integer(),
        size => non_neg_integer(),
        first_ts => integer(),
        last_ts => integer(),
        uid => non_neg_integer()
    }}.

-type tree_spec() ::
    fragment_spec()
    | {group, [tree_spec()]}
    | {kilo_group, [tree_spec()]}
    | {mega_group, [tree_spec()]}.

-doc """
Build a manifest and get_group_fun from a declarative tree spec.

Usage:

    {Manifest, GetGroupFun} = rabbitmq_stream_s3_test_helpers:build_manifest([
        {group, [
            {fragment, #{offset => 0}},
            {fragment, #{offset => 100}}
        ]},
        {fragment, #{offset => 200, size => 7000}}
    ])

Every node is `{Kind, ...}` where Kind is `fragment`, `group`,
`kilo_group`, or `mega_group`.

Fragments are `{fragment, Map}`. Only `offset` is required.
Optional keys: `size` (default 64000), `first_ts` (default Offset * 10),
`last_ts` (default (Offset + 1) * 10), `uid` (default random).

Groups are `{group, [Children]}`, `{kilo_group, [Children]}`,
`{mega_group, [Children]}`.
""".
-spec build_manifest([tree_spec()]) ->
    {#manifest{}, rabbitmq_stream_s3_fragment_iterator:get_group_fun()}.
build_manifest(Specs) ->
    {RootEntries, TotalSize, Groups} = build_children(Specs, #{}),
    Manifest =
        case RootEntries of
            <<>> ->
                #manifest{};
            _ ->
                ?ENTRY(FirstOffset, FirstTs, FirstLastTs, _, _, _, _) = RootEntries,
                #manifest{
                    first_offset = FirstOffset,
                    next_offset = next_offset(Specs),
                    first_timestamp = FirstTs,
                    first_last_timestamp = FirstLastTs,
                    total_size = TotalSize,
                    entries = RootEntries
                }
        end,
    GetGroupFun = fun(#group_ref{offset = O, kind = K, uid = U}) ->
        case Groups of
            #{{O, K, U} := Entries} ->
                {ok, Entries};
            #{} ->
                {error, not_found}
        end
    end,
    {Manifest, GetGroupFun}.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

build_children(Specs, Groups) ->
    lists:foldl(
        fun(Spec, {EntriesAcc, SizeAcc, GroupsAcc}) ->
            {Entry, Size, GroupsAcc1} = build_entry(Spec, GroupsAcc),
            {<<EntriesAcc/binary, Entry/binary>>, SizeAcc + Size, GroupsAcc1}
        end,
        {<<>>, 0, Groups},
        Specs
    ).

build_entry({fragment, Props}, Groups) ->
    Offset = maps:get(offset, Props),
    Size = maps:get(size, Props, 64000),
    FirstTs = maps:get(first_ts, Props, Offset * 10),
    LastTs = maps:get(last_ts, Props, (Offset + 1) * 10),
    Uid = maps:get(uid, Props, rabbitmq_stream_s3:uid()),
    Entry = ?ENTRY(Offset, FirstTs, LastTs, ?MANIFEST_KIND_FRAGMENT, Size, Uid),
    {Entry, Size, Groups};
build_entry({group, Children}, Groups) ->
    build_group_entry(?MANIFEST_KIND_GROUP, Children, Groups);
build_entry({kilo_group, Children}, Groups) ->
    build_group_entry(?MANIFEST_KIND_KILO_GROUP, Children, Groups);
build_entry({mega_group, Children}, Groups) ->
    build_group_entry(?MANIFEST_KIND_MEGA_GROUP, Children, Groups).

build_group_entry(Kind, Children, Groups0) ->
    {ChildEntries, TotalSize, Groups1} = build_children(Children, Groups0),
    ?ENTRY(Offset, FirstTs, _, _, _, _, _) = ChildEntries,
    ?ENTRY(_, _, LastTs, _, _, _) = rabbitmq_stream_s3_array:last(?ENTRY_B, ChildEntries),
    Uid = rabbitmq_stream_s3:uid(),
    Entry = ?ENTRY(Offset, FirstTs, LastTs, Kind, 0, Uid),
    Groups2 = Groups1#{{Offset, Kind, Uid} => ChildEntries},
    {Entry, TotalSize, Groups2}.

next_offset(Specs) ->
    next_offset_spec(lists:last(Specs)).

next_offset_spec({fragment, Props}) ->
    maps:get(offset, Props) + 1;
next_offset_spec({group, Children}) ->
    next_offset(Children);
next_offset_spec({kilo_group, Children}) ->
    next_offset(Children);
next_offset_spec({mega_group, Children}) ->
    next_offset(Children).
