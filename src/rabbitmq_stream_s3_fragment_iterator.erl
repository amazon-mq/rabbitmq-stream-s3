%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_fragment_iterator).
-moduledoc """
Forward iterator over the manifest tree's leaf entries (fragments).

Hides the tree structure from callers. Lazily downloads group objects
as it descends into branches.
""".

-include("include/rabbitmq_stream_s3.hrl").

-export([
    init/3,
    next/1,
    all_refs/2
]).

-export_type([
    iterator/0,
    get_group_fun/0
]).

-type get_group_fun() ::
    fun((#group_ref{}) -> {ok, rabbitmq_stream_s3:entries()} | {error, term()}).

-record(iterator, {
    entries :: binary(),
    index :: non_neg_integer(),
    stack :: [{binary(), non_neg_integer()}],
    get_group_fun :: get_group_fun(),
    %% The offset to position at, at every level of the tree: the caller's
    %% requested offset clamped up to the manifest's first_offset. The clamp
    %% serves two roles at once. It positions descent at the requested offset
    %% (so a read starting mid-group lands on the right child, not the group's
    %% first child), and the first_offset floor skips leading entries that
    %% retention has deleted (whose objects are gone). Reusing one value at
    %% every level is correct because the iterator only moves forward: a group
    %% entirely after this offset resolves to its first child (index 0).
    start_offset :: osiris:offset()
}).

-opaque iterator() :: #iterator{}.

-doc """
Create an iterator positioned at the entry containing or following `Offset`.
""".
-spec init(#manifest{}, osiris:offset(), get_group_fun()) -> iterator().
init(#manifest{entries = Entries, first_offset = FirstOffset}, Offset, GetGroupFun) ->
    StartOffset = max(Offset, FirstOffset),
    Idx = find_start_index(Entries, StartOffset),
    #iterator{
        entries = Entries,
        index = Idx,
        stack = [],
        get_group_fun = GetGroupFun,
        start_offset = StartOffset
    }.

-doc """
Return the next fragment entry and advance the iterator.
""".
-spec next(iterator()) ->
    {ok, #fragment_ref{}, iterator()}
    | end_of_manifest
    | {error, {group_fetch_failed, term()}}.
next(#iterator{entries = Entries, index = Idx} = It) ->
    case rabbitmq_stream_s3_array:try_at(Idx, ?ENTRY_B, Entries) of
        undefined ->
            ascend(It);
        ?ENTRY(Offset, _FTs, _LTs, ?MANIFEST_KIND_FRAGMENT, Size, Uid) ->
            It1 = It#iterator{index = Idx + 1},
            {ok, #fragment_ref{offset = Offset, uid = Uid, size = Size}, It1};
        ?ENTRY(Offset, _FTs, _LTs, Kind, _Size, Uid) ->
            descend(It, #group_ref{offset = Offset, kind = Kind, uid = Uid})
    end.

-doc """
Return all object references in the manifest (fragments and groups).

Traverses the entire tree, emitting both leaf entries (fragment refs) and
internal nodes (group refs). Used for bulk deletion when discarding a manifest.
""".
-spec all_refs(#manifest{}, get_group_fun()) -> [#fragment_ref{} | #group_ref{}].
all_refs(#manifest{entries = <<>>}, _GetGroupFun) ->
    [];
all_refs(#manifest{entries = Entries}, GetGroupFun) ->
    collect_refs(Entries, GetGroupFun).

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

-spec find_start_index(binary(), osiris:offset()) -> non_neg_integer().
find_start_index(<<>>, _Offset) ->
    0;
find_start_index(Entries, Offset) ->
    Idx = rabbitmq_stream_s3_array:partition_point(
        fun(?ENTRY(O, _FTs, _LTs, _K, _Sz, _Uid, _)) -> Offset >= O end,
        ?ENTRY_B,
        Entries
    ),
    saturating_decr(Idx).

-spec saturating_decr(non_neg_integer()) -> non_neg_integer().
saturating_decr(0) -> 0;
saturating_decr(N) -> N - 1.

-spec descend(iterator(), #group_ref{}) ->
    {ok, #fragment_ref{}, iterator()}
    | end_of_manifest
    | {error, {group_fetch_failed, term()}}.
descend(
    #iterator{
        entries = Entries,
        index = Idx,
        stack = Stack,
        get_group_fun = GetGroupFun,
        start_offset = StartOffset
    } = It,
    GroupRef
) ->
    case GetGroupFun(GroupRef) of
        {ok, ChildEntries} ->
            ChildIdx = find_start_index(ChildEntries, StartOffset),
            It1 = It#iterator{
                entries = ChildEntries,
                index = ChildIdx,
                stack = [{Entries, Idx + 1} | Stack]
            },
            next(It1);
        {error, not_found} ->
            %% Group deleted by retention. Skip past it and continue.
            next(It#iterator{index = Idx + 1});
        {error, Reason} ->
            {error, {group_fetch_failed, Reason}}
    end.

-spec ascend(iterator()) ->
    {ok, #fragment_ref{}, iterator()}
    | end_of_manifest.
ascend(#iterator{stack = []}) ->
    end_of_manifest;
ascend(#iterator{stack = [{ParentEntries, ParentIdx} | Rest]} = It) ->
    It1 = It#iterator{
        entries = ParentEntries,
        index = ParentIdx,
        stack = Rest
    },
    next(It1).

collect_refs(<<>>, _GetGroupFun) ->
    [];
collect_refs(?ENTRY(Offset, _FTs, _LTs, ?MANIFEST_KIND_FRAGMENT, Size, Uid, Rest), GetGroupFun) ->
    [#fragment_ref{offset = Offset, uid = Uid, size = Size} | collect_refs(Rest, GetGroupFun)];
collect_refs(?ENTRY(Offset, _FTs, _LTs, Kind, _Size, Uid, Rest), GetGroupFun) ->
    GroupRef = #group_ref{offset = Offset, kind = Kind, uid = Uid},
    ChildRefs =
        case GetGroupFun(GroupRef) of
            {ok, ChildEntries} -> collect_refs(ChildEntries, GetGroupFun);
            {error, _} -> []
        end,
    [GroupRef | ChildRefs] ++ collect_refs(Rest, GetGroupFun).
