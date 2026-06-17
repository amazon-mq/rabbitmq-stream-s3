%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_manifest).
-moduledoc """
Functions for working with the _manifest_ data structure.

The manifest is a tree structure represented by objects in the remote tier
which tracks metadata about a stream: total size, oldest data, and pointers to
fragments (via tree-branch-like "group" objects, for large enough streams).
""".

-include_lib("stdlib/include/assert.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-type t() :: #manifest{}.
-type edit() :: #edit{}.
-type get_group_fun() :: rabbitmq_stream_s3_fragment_iterator:get_group_fun().

-export_type([t/0, edit/0]).

-export([
    init/0,
    new_edit/1,
    apply_edit/2,
    get_group_fun/1,
    get_cached_group_fun/1,
    clear_group_cache/2,
    evict_group_cache/1,
    evaluate_remote_retention/3,
    evaluate_remote_retention/4
]).

%% Public ETS table caching the first group object per stream. Keyed by
%% stream_id(), value is {#group_ref{}, entries()}. Avoids re-downloading
%% the same immutable group object on every retention evaluation cycle.
-define(FIRST_GROUPS, rabbitmq_stream_s3_manifest_first_groups).

-doc "Create the first-group cache ETS table.".
-spec init() -> ok.
init() ->
    _ = ets:new(?FIRST_GROUPS, [named_table, public, set, {read_concurrency, true}]),
    ok.

%% ---------------------------------------------------------------------------

-doc "Creates a new blank edit from the current manifest metadata".
-spec new_edit(t()) -> edit().
new_edit(#manifest{
    first_offset = FirstOffset,
    first_timestamp = FirstTs,
    first_last_timestamp = FirstLastTs,
    next_offset = NextOffset
}) ->
    #edit{
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        next_offset = NextOffset
    }.

-doc """
Apply an edit to a manifest.

The edit type describes a few kinds of modifications to a manifest:

* Appends from new fragments being uploaded.
* Updates to first_offset / first_timestamp / etc from evaluating retention
  when the retention affected a group object. (The group object is not
  modified, just the top-level metadata in the manifest.)
* Truncation from fragments being deleted by retention from the beginning of
  the manifest's array.
""".
-spec apply_edit(edit(), t()) -> t().
apply_edit(
    #edit{
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        next_offset = EditNextOffset,
        size = Size,
        entries = EditEntries,
        pos = Pos,
        len = Len
    },
    #manifest{
        next_offset = ManifestNextOffset,
        total_size = TotalSize0,
        entries = Entries0
    } = Manifest0
) ->
    Entries =
        if
            %% Pure insertion (append)
            Pos =:= byte_size(Entries0) andalso Len =:= 0 ->
                <<Entries0/binary, EditEntries/binary>>;
            %% No-op (empty)
            Len =:= 0 andalso EditEntries =:= <<>> ->
                Entries0;
            %% Pure deletion (truncate)
            EditEntries =:= <<>> ->
                %% We only truncate from the beginning. No hole punching.
                ?assertEqual(0, Pos),
                binary:part(Entries0, Len, byte_size(Entries0) - Len);
            %% Replacement (for rebalancing)
            true ->
                <<
                    (binary:part(Entries0, 0, Pos))/binary,
                    EditEntries/binary,
                    (binary:part(Entries0, Pos + Len, byte_size(Entries0) - Pos - Len))/binary
                >>
        end,
    NextOffset =
        case EditNextOffset of
            undefined ->
                ManifestNextOffset;
            _ when is_integer(EditNextOffset) ->
                EditNextOffset
        end,
    Manifest0#manifest{
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        next_offset = NextOffset,
        total_size = TotalSize0 + Size,
        entries = Entries
    }.

-doc """
Returns a function that downloads group objects from S3 and returns their
entries array. Used by the fragment iterator and offset resolution.
Always downloads fresh from S3.
""".
-spec get_group_fun(stream_id()) -> rabbitmq_stream_s3_fragment_iterator:get_group_fun().
get_group_fun(StreamId) ->
    fun(#group_ref{kind = Kind} = GroupRef) ->
        fetch_group(StreamId, Kind, GroupRef)
    end.

-doc """
Like `get_group_fun/1` but caches the first plain group per stream in ETS.
Retention evaluates the same first group repeatedly, so caching avoids
redundant S3 round-trips. The cached entry is invalidated when the group
is deleted.
""".
-spec get_cached_group_fun(stream_id()) -> rabbitmq_stream_s3_fragment_iterator:get_group_fun().
get_cached_group_fun(StreamId) ->
    fun(#group_ref{kind = Kind} = GroupRef) ->
        case get_group_cached(StreamId, GroupRef) of
            {ok, _} = Hit ->
                Hit;
            miss ->
                case fetch_group(StreamId, Kind, GroupRef) of
                    {ok, Entries} = Ok ->
                        maybe_cache_group(StreamId, GroupRef, Entries),
                        Ok;
                    {error, _} = Err ->
                        Err
                end
        end
    end.

fetch_group(StreamId, Kind, #group_ref{} = GroupRef) ->
    fetch_group(StreamId, Kind, GroupRef, 2).

fetch_group(StreamId, Kind, #group_ref{} = GroupRef, Retries) ->
    Key = rabbitmq_stream_s3:group_key(StreamId, GroupRef),
    case rabbitmq_stream_s3_api:get(Key, #{}) of
        {ok, Data} ->
            HeaderSize = group_header_size(Kind),
            <<_Header:HeaderSize/binary, Entries/binary>> = Data,
            {ok, Entries};
        {error, not_found} = Err ->
            Err;
        {error, _} = Err when Retries =< 0 ->
            Err;
        {error, _} ->
            timer:sleep(100),
            fetch_group(StreamId, Kind, GroupRef, Retries - 1)
    end.

-doc """
Clear the cached first group for a stream. Called when retention deletes
a group object.
""".
-spec clear_group_cache(stream_id(), #group_ref{}) -> ok.
clear_group_cache(StreamId, #group_ref{} = GroupRef) ->
    _ = catch ets:match_delete(?FIRST_GROUPS, {StreamId, {GroupRef, '_'}}),
    ok.

-doc "Remove any cached group for a stream. Called on stream termination.".
-spec evict_group_cache(stream_id()) -> ok.
evict_group_cache(StreamId) ->
    _ = catch ets:delete(?FIRST_GROUPS, StreamId),
    ok.

%% Cache only plain groups (not kilo/mega). The first group is the hot path
%% for retention and is immutable once written.
maybe_cache_group(StreamId, #group_ref{kind = ?MANIFEST_KIND_GROUP} = GroupRef, Entries) ->
    _ = catch ets:insert(?FIRST_GROUPS, {StreamId, {GroupRef, Entries}}),
    ok;
maybe_cache_group(_, _, _) ->
    ok.

get_group_cached(StreamId, #group_ref{kind = ?MANIFEST_KIND_GROUP} = GroupRef) ->
    try ets:lookup(?FIRST_GROUPS, StreamId) of
        [{_, {GroupRef, Entries}}] -> {ok, Entries};
        _ -> miss
    catch
        error:badarg -> miss
    end;
get_group_cached(_, _) ->
    miss.

-doc """
Evaluate remote tier retention against the manifest.

Returns `unchanged` if no entries need to be removed, or
`{edit(), [#fragment_ref{} | #group_ref{}]}` with the edit to apply and the
references to delete from S3.

Evaluates fragment entries at the head of the root. If the root starts with
a group entry, downloads the group object via GetGroupFun and evaluates
retention against the fragments within. Fragments inside the group are
deleted individually. When all fragments in a group are expired, the group
entry is removed from the root and the group object is deleted.
""".
-spec evaluate_remote_retention(t(), [osiris:retention_spec()], integer()) ->
    unchanged | {edit(), [#fragment_ref{} | #group_ref{}]}.
evaluate_remote_retention(Manifest, Specs, Now) ->
    evaluate_remote_retention(Manifest, Specs, Now, undefined).

-spec evaluate_remote_retention(
    t(), [osiris:retention_spec()], integer(), get_group_fun() | undefined
) ->
    unchanged | {edit(), [#fragment_ref{} | #group_ref{}]}.
evaluate_remote_retention(#manifest{entries = <<>>}, _Specs, _Now, _GetGroupFun) ->
    unchanged;
evaluate_remote_retention(#manifest{}, [], _Now, _GetGroupFun) ->
    unchanged;
evaluate_remote_retention(#manifest{} = Manifest, Specs, Now, GetGroupFun) ->
    case eval_retention_specs(Manifest, Specs, Now) of
        0 ->
            %% No leading fragments to remove. Check if the first entry is a group.
            maybe_eval_group_retention(Manifest, Specs, Now, GetGroupFun);
        NumEntries ->
            build_retention_result(Manifest, NumEntries)
    end.

%% Returns the number of leading fragment entries to remove.
eval_retention_specs(Manifest, Specs, Now) ->
    lists:foldl(
        fun(Spec, Acc) ->
            max(Acc, entries_to_remove(Manifest, Spec, Now))
        end,
        0,
        Specs
    ).

entries_to_remove(
    #manifest{total_size = TotalSize, entries = Entries}, {max_bytes, MaxBytes}, _Now
) ->
    remove_for_max_bytes(Entries, TotalSize, MaxBytes, 0);
entries_to_remove(#manifest{entries = Entries}, {max_age, MaxAgeMs}, Now) ->
    Cutoff = Now - MaxAgeMs,
    remove_for_max_age(Entries, Cutoff, 0);
entries_to_remove(_, _, _) ->
    0.

remove_for_max_bytes(_Entries, TotalSize, MaxBytes, N) when TotalSize =< MaxBytes ->
    N;
remove_for_max_bytes(<<>>, _TotalSize, _MaxBytes, N) ->
    N;
remove_for_max_bytes(Entries, TotalSize, MaxBytes, N) ->
    <<_Offset:64, _FirstTs:64, _LastTs:64, Kind:8, Size:40, _Uid:32, Rest/binary>> = Entries,
    case Kind of
        ?MANIFEST_KIND_FRAGMENT ->
            remove_for_max_bytes(Rest, TotalSize - Size, MaxBytes, N + 1);
        _ ->
            %% Hit a group entry. Stop here (group retention handles it).
            N
    end.

remove_for_max_age(<<>>, _Cutoff, N) ->
    N;
remove_for_max_age(Entries, Cutoff, N) ->
    <<_Offset:64, _FirstTs:64/signed, LastTs:64/signed, Kind:8, _Size:40, _Uid:32, Rest/binary>> =
        Entries,
    case Kind of
        ?MANIFEST_KIND_FRAGMENT when LastTs < Cutoff ->
            remove_for_max_age(Rest, Cutoff, N + 1);
        _ ->
            N
    end.

build_retention_result(#manifest{entries = Entries} = Manifest, NumEntries) ->
    BytesToRemove = NumEntries * ?ENTRY_B,
    {Removed, Remaining} = {
        binary:part(Entries, 0, BytesToRemove),
        binary:part(Entries, BytesToRemove, byte_size(Entries) - BytesToRemove)
    },
    %% Collect fragment refs for deletion.
    Refs = collect_fragment_refs(Removed, []),
    %% Compute the size delta.
    SizeDelta = lists:foldl(fun(#fragment_ref{size = S}, Acc) -> Acc - S end, 0, Refs),
    %% Determine new first_offset and timestamps from the remaining entries.
    {NewFirstOffset, NewFirstTs, NewFirstLastTs} =
        case Remaining of
            <<>> ->
                {Manifest#manifest.next_offset, -1, -1};
            ?ENTRY(Offset, FirstTs, LastTs, _, _, _, _) ->
                {Offset, FirstTs, LastTs}
        end,
    Edit = #edit{
        first_offset = NewFirstOffset,
        first_timestamp = NewFirstTs,
        first_last_timestamp = NewFirstLastTs,
        next_offset = undefined,
        size = SizeDelta,
        entries = <<>>,
        pos = 0,
        len = BytesToRemove
    },
    {Edit, Refs}.

%% Evaluate retention within the first group entry, if present.
maybe_eval_group_retention(_Manifest, _Specs, _Now, undefined) ->
    unchanged;
maybe_eval_group_retention(#manifest{entries = Entries} = Manifest, Specs, Now, GetGroupFun) ->
    case Entries of
        <<Offset:64/unsigned, _FTs:64/signed, _LTs:64/signed, Kind:8/unsigned, _Size:40/unsigned,
            Uid:32/unsigned,
            _Rest/binary>> when Kind =/= ?MANIFEST_KIND_FRAGMENT ->
            GroupRef = #group_ref{offset = Offset, kind = Kind, uid = Uid},
            case GetGroupFun(GroupRef) of
                {ok, GroupEntries} ->
                    eval_group_entries(Manifest, GroupEntries, GroupRef, Specs, Now);
                {error, _} ->
                    unchanged
            end;
        _ ->
            unchanged
    end.

%% Evaluate retention on fragment entries within a group.
%%
%% A group object is immutable, so after an earlier cycle partially consumed
%% this group it still lists the children those cycles already removed. Skip
%% that already-consumed prefix (children below the manifest's first_offset)
%% before evaluating. Otherwise retention re-counts and re-deletes them and
%% double-subtracts their sizes from total_size on every subsequent cycle,
%% which drifts total_size below the true durable size (silently disabling
%% max_bytes retention) or negative (crashing the persist under max_age), and
%% regresses first_offset below the real data floor.
eval_group_entries(Manifest, GroupEntries0, GroupRef, Specs, Now) ->
    GroupEntries = drop_consumed_children(GroupEntries0, Manifest#manifest.first_offset),
    NumToRemove = eval_group_retention_specs(
        GroupEntries, Manifest#manifest.total_size, Specs, Now
    ),
    case NumToRemove of
        0 ->
            unchanged;
        _ ->
            NumGroupEntries = byte_size(GroupEntries) div ?ENTRY_B,
            build_group_retention_result(
                Manifest, GroupEntries, GroupRef, NumToRemove, NumGroupEntries
            )
    end.

%% Drop the leading group children whose offset is below first_offset: an
%% earlier retention cycle already removed them, but they remain in the
%% immutable group object.
drop_consumed_children(GroupEntries, FirstOffset) ->
    Skip = consumed_prefix_bytes(GroupEntries, FirstOffset, 0),
    binary:part(GroupEntries, Skip, byte_size(GroupEntries) - Skip).

consumed_prefix_bytes(
    <<Offset:64/unsigned, _FTs:64/signed, _LTs:64/signed, _K:8, _Sz:40, _Uid:32, Rest/binary>>,
    FirstOffset,
    Acc
) when Offset < FirstOffset ->
    consumed_prefix_bytes(Rest, FirstOffset, Acc + ?ENTRY_B);
consumed_prefix_bytes(_Entries, _FirstOffset, Acc) ->
    Acc.

eval_group_retention_specs(GroupEntries, ManifestTotalSize, Specs, Now) ->
    lists:foldl(
        fun(Spec, Acc) ->
            max(Acc, group_entries_to_remove(GroupEntries, ManifestTotalSize, Spec, Now))
        end,
        0,
        Specs
    ).

group_entries_to_remove(Entries, ManifestTotalSize, {max_bytes, MaxBytes}, _Now) ->
    remove_for_max_bytes_in_group(Entries, ManifestTotalSize, MaxBytes, 0);
group_entries_to_remove(Entries, _ManifestTotalSize, {max_age, MaxAgeMs}, Now) ->
    Cutoff = Now - MaxAgeMs,
    remove_for_max_age_in_group(Entries, Cutoff, 0);
group_entries_to_remove(_, _, _, _) ->
    0.

%% Like remove_for_max_bytes but allows removing all entries (groups can be
%% fully consumed).
remove_for_max_bytes_in_group(_Entries, TotalSize, MaxBytes, N) when TotalSize =< MaxBytes ->
    N;
remove_for_max_bytes_in_group(<<>>, _TotalSize, _MaxBytes, N) ->
    N;
remove_for_max_bytes_in_group(Entries, TotalSize, MaxBytes, N) ->
    <<_:64, _:64, _:64, _:8, Size:40, _:32, Rest/binary>> = Entries,
    remove_for_max_bytes_in_group(Rest, TotalSize - Size, MaxBytes, N + 1).

%% Like remove_for_max_age but doesn't stop at the last entry (groups can be
%% fully consumed).
remove_for_max_age_in_group(<<>>, _Cutoff, N) ->
    N;
remove_for_max_age_in_group(Entries, Cutoff, N) ->
    <<_Offset:64, _FirstTs:64/signed, LastTs:64/signed, _Kind:8, _Size:40, _Uid:32, Rest/binary>> =
        Entries,
    case LastTs < Cutoff of
        true -> remove_for_max_age_in_group(Rest, Cutoff, N + 1);
        false -> N
    end.

build_group_retention_result(Manifest, GroupEntries, GroupRef, NumToRemove, NumGroupEntries) ->
    BytesToRemove = NumToRemove * ?ENTRY_B,
    Removed = binary:part(GroupEntries, 0, BytesToRemove),
    FragRefs = collect_fragment_refs(Removed, []),
    SizeDelta = lists:foldl(fun(#fragment_ref{size = S}, Acc) -> Acc - S end, 0, FragRefs),
    case NumToRemove >= NumGroupEntries of
        true ->
            %% Entire group consumed. Remove the group entry from the root.
            Remaining = binary:part(
                Manifest#manifest.entries, ?ENTRY_B, byte_size(Manifest#manifest.entries) - ?ENTRY_B
            ),
            {NewFirstOffset, NewFirstTs, NewFirstLastTs} =
                case Remaining of
                    <<>> ->
                        {Manifest#manifest.next_offset, -1, -1};
                    ?ENTRY(Off, FTs, LTs, _, _, _, _) ->
                        {Off, FTs, LTs}
                end,
            Edit = #edit{
                first_offset = NewFirstOffset,
                first_timestamp = NewFirstTs,
                first_last_timestamp = NewFirstLastTs,
                next_offset = undefined,
                size = SizeDelta,
                entries = <<>>,
                pos = 0,
                len = ?ENTRY_B
            },
            {Edit, [GroupRef | FragRefs]};
        false ->
            %% Partial group consumption. Update root metadata only.
            %% The group entry stays. Determine new first_offset from the
            %% first surviving entry in the group.
            SurvivingStart = binary:part(GroupEntries, BytesToRemove, ?ENTRY_B),
            <<NewFirstOffset:64/unsigned, NewFirstTs:64/signed, NewFirstLastTs:64/signed, _:8, _:40,
                _:32>> = SurvivingStart,
            Edit = #edit{
                first_offset = NewFirstOffset,
                first_timestamp = NewFirstTs,
                first_last_timestamp = NewFirstLastTs,
                next_offset = undefined,
                size = SizeDelta,
                entries = <<>>,
                pos = 0,
                len = 0
            },
            {Edit, FragRefs}
    end.

collect_fragment_refs(<<>>, Acc) ->
    lists:reverse(Acc);
collect_fragment_refs(Entries, Acc) ->
    <<Offset:64/unsigned, _FirstTs:64/signed, _LastTs:64/signed, ?MANIFEST_KIND_FRAGMENT:8/unsigned,
        Size:40/unsigned, Uid:32/unsigned, Rest/binary>> = Entries,
    Ref = #fragment_ref{offset = Offset, uid = Uid, size = Size},
    collect_fragment_refs(Rest, [Ref | Acc]).

group_header_size(?MANIFEST_KIND_GROUP) -> ?MANIFEST_HEADER_SIZE;
group_header_size(?MANIFEST_KIND_KILO_GROUP) -> ?MANIFEST_HEADER_SIZE;
group_header_size(?MANIFEST_KIND_MEGA_GROUP) -> ?MANIFEST_HEADER_SIZE.
