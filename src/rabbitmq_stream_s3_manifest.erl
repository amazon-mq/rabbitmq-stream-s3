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
    is_well_formed/1,
    assert_well_formed/1,
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
    Size0 = byte_size(Entries0),
    %% apply_edit/2 is a trust boundary: on a replica it applies edits that
    %% arrived over the network, and on the writer it applies edits the core
    %% built. The branches below are not self-validating, so a malformed or
    %% stale edit could splice at the wrong position and silently corrupt the
    %% ordered entries array (the shape behind the #206/replica-divergence
    %% bugs). Assert the structural invariants up front and fail loudly
    %% instead: the replica caller (rabbitmq_stream_s3_manifest_replica)
    %% catches this and forces a full resync rather than serving a corrupt
    %% manifest, and on the writer it surfaces a bug rather than persisting
    %% corruption. Entries are fixed-size records, so every offset and length
    %% into the array must be entry-aligned and within bounds.
    assert_edit_well_formed(Pos, Len, byte_size(EditEntries), Size0),
    Entries =
        case {Len, EditEntries} of
            %% Metadata-only edit (e.g. retention adjusting a group's leading
            %% offsets): the entries array does not change.
            {0, <<>>} ->
                Entries0;
            %% Append. The new entries must land exactly at the end. A stale or
            %% duplicate append (this replica already applied it, so its array
            %% is longer) has Pos < Size0 and is rejected here - which is what
            %% turns the silent double-apply corruption into a caught
            %% inconsistency that triggers a resync.
            {0, _} ->
                ?assertEqual(Size0, Pos),
                <<Entries0/binary, EditEntries/binary>>;
            %% Pure deletion (truncate). We only truncate from the beginning;
            %% no hole punching.
            {_, <<>>} ->
                ?assertEqual(0, Pos),
                binary:part(Entries0, Len, Size0 - Len);
            %% Replacement (rebalancing): replace [Pos, Pos + Len) with the new
            %% sub-array.
            {_, _} ->
                <<
                    (binary:part(Entries0, 0, Pos))/binary,
                    EditEntries/binary,
                    (binary:part(Entries0, Pos + Len, Size0 - Pos - Len))/binary
                >>
        end,
    NextOffset =
        case EditNextOffset of
            undefined ->
                ManifestNextOffset;
            _ when is_integer(EditNextOffset) ->
                EditNextOffset
        end,
    TotalSize = TotalSize0 + Size,
    %% total_size is non-negative and serialised as a u70. A duplicate or
    %% mis-applied retention edit (whose size delta is negative) would drive it
    %% below zero; reject that here rather than let it crash serialisation on
    %% the writer or poison max-bytes retention / metrics on a replica.
    ?assert(TotalSize >= 0),
    Manifest = Manifest0#manifest{
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        next_offset = NextOffset,
        total_size = TotalSize,
        entries = Entries
    },
    %% Validate the post-state, not just the edit shape: a well-shaped but
    %% wrong edit (stale, mis-positioned, wrong size delta) can still produce a
    %% structurally invalid manifest. Failing here keeps corruption from being
    %% persisted (writer) or served (replica; the caller catches it and
    %% resyncs).
    assert_well_formed(Manifest),
    Manifest.

%% Structural invariants common to every edit shape: offsets and lengths into
%% the entries array are non-negative, entry-aligned, and within the array. A
%% violation means the edit does not match this manifest (a gap, a diverged
%% replica, or a malformed edit) and is raised so the caller can recover.
-spec assert_edit_well_formed(
    non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()
) -> ok.
assert_edit_well_formed(Pos, Len, EditLen, Size0) ->
    ?assert(Pos >= 0 andalso Len >= 0),
    ?assertEqual(0, Pos rem ?ENTRY_B),
    ?assertEqual(0, Len rem ?ENTRY_B),
    ?assertEqual(0, EditLen rem ?ENTRY_B),
    ?assert(Pos + Len =< Size0),
    ok.

-doc """
Value-level well-formedness predicate over a manifest. Decides, from the value
alone (no S3 access), the structural invariants that hold for any committed
manifest:

- `0 =< first_offset =< next_offset` and `total_size >= 0` (bounds);
- coverage is empty iff `first_offset == next_offset` (no entries);
- root entries are in strictly increasing offset order (ordering);
- the first entry starts at or before `first_offset` (no gap below the floor:
  it is `==` for a flat manifest and `=<` after intra-group retention, which
  advances `first_offset` inside the leading group entry);
- the last entry starts strictly below `next_offset`;
- for a flat manifest (no group entries) only, `total_size` equals the sum of
  the entry sizes. The check is skipped when a group entry is present: a group
  entry carries size 0 while `total_size` still counts its underlying fragment
  bytes, so verifying accounting would require fetching the group objects.

Durability-in-S3 and the temporal invariants (`next_offset`/`first_offset`
monotonicity across steps) are not value-level and are not checked here. See
docs/invariants.md (Durability: Coverage, Accounting; Manifest structure:
Ordering).
""".
-spec is_well_formed(t()) -> boolean().
is_well_formed(#manifest{first_offset = F, next_offset = N, total_size = TS, entries = Entries}) ->
    is_integer(F) andalso is_integer(N) andalso is_integer(TS) andalso
        F >= 0 andalso F =< N andalso TS >= 0 andalso
        entries_well_formed(F, N, TS, Entries).

entries_well_formed(F, N, _TS, <<>>) ->
    %% No entries: coverage [f, n) is empty, so f must equal n.
    F =:= N;
entries_well_formed(F, N, TS, Entries) ->
    #{first := First, last := Last, sum := Sum, has_group := HasGroup, ordered := Ordered} =
        rabbitmq_stream_s3_array:fold(fun wf_fold/2, init_wf_acc(), ?ENTRY_B, Entries),
    Ordered andalso
        First =< F andalso
        Last < N andalso
        (HasGroup orelse Sum =:= TS).

init_wf_acc() ->
    #{
        first => undefined,
        prev => undefined,
        last => undefined,
        sum => 0,
        has_group => false,
        ordered => true
    }.

wf_fold(
    ?ENTRY(Offset, _FirstTs, _LastTs, Kind, Size, _Uid),
    #{prev := Prev, sum := Sum, has_group := HasGroup, ordered := Ordered} = Acc
) ->
    Acc#{
        first :=
            case maps:get(first, Acc) of
                undefined -> Offset;
                F0 -> F0
            end,
        prev := Offset,
        last := Offset,
        sum := Sum + Size,
        has_group := HasGroup orelse Kind =/= ?MANIFEST_KIND_FRAGMENT,
        ordered := Ordered andalso (Prev =:= undefined orelse Offset > Prev)
    }.

-doc """
Assert a manifest is well-formed, raising `{manifest_not_well_formed, Summary}`
with a compact summary otherwise. The runtime guard at the manifest mutation
boundary (apply_edit/2): a malformed result is a bug (a mis-applied or stale
edit), and failing here turns silent corruption into a caught error rather than
persisting it (writer) or serving it (replica, whose caller resyncs).
""".
-spec assert_well_formed(t()) -> ok.
assert_well_formed(#manifest{} = M) ->
    case is_well_formed(M) of
        true ->
            ok;
        false ->
            erlang:error({manifest_not_well_formed, wf_summary(M)})
    end.

wf_summary(#manifest{first_offset = F, next_offset = N, total_size = TS, entries = Entries}) ->
    #{
        first_offset => F,
        next_offset => N,
        total_size => TS,
        num_entries => byte_size(Entries) div ?ENTRY_B
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

%% Evaluate retention when the oldest root entry is a group of any kind.
maybe_eval_group_retention(_Manifest, _Specs, _Now, undefined) ->
    unchanged;
maybe_eval_group_retention(#manifest{entries = Entries} = Manifest, Specs, Now, GetGroupFun) ->
    case Entries of
        <<_:64, _:64/signed, _:64/signed, Kind:8/unsigned, _:40, _:32, _/binary>> when
            Kind =/= ?MANIFEST_KIND_FRAGMENT
        ->
            group_retention_result(Manifest, Specs, Now, GetGroupFun);
        _ ->
            unchanged
    end.

%% Retention over a manifest whose oldest root entry is a group. The manifest
%% tree may be several levels deep (group, kilo-group, mega-group), so this
%% recurses to fragment granularity.
%%
%% The computation is in two phases. First it finds the new first_offset f': the
%% offset of the oldest fragment that survives the policy, found by descending
%% the tree. Then it deletes exactly the objects whose offset interval lies in
%% [f, f') -- the fragments below f' and every group object all of whose
%% descendants are below f' -- and splices out the leading root entries that are
%% wholly consumed. Objects below the current first_offset were removed by an
%% earlier cycle and are skipped, which keeps retention idempotent at every
%% level. Group objects are immutable and are never rewritten.
group_retention_result(
    #manifest{entries = Entries, first_offset = Lo, next_offset = N, total_size = TotalSize},
    Specs,
    Now,
    Get
) ->
    try new_first_offset(Lo, N, Entries, Specs, Now, TotalSize, Get) of
        unchanged ->
            unchanged;
        {Hi, NewFirstTs, NewFirstLastTs} ->
            case collect_deletions(Entries, Lo, Hi, Get) of
                {[], _, _} ->
                    unchanged;
                {Refs, Bytes, _} ->
                    NumWhole = count_whole_root(Entries, Hi, Get, 0),
                    Edit = #edit{
                        first_offset = Hi,
                        first_timestamp = NewFirstTs,
                        first_last_timestamp = NewFirstLastTs,
                        next_offset = undefined,
                        size = -Bytes,
                        entries = <<>>,
                        pos = 0,
                        len = NumWhole * ?ENTRY_B
                    },
                    {Edit, Refs}
            end
    catch
        throw:group_fetch_failed ->
            unchanged
    end.

%% Fetch a group object's child entries, aborting retention on a fetch error.
get_children(Get, GroupRef) ->
    case Get(GroupRef) of
        {ok, Children} -> Children;
        {error, _} -> throw(group_fetch_failed)
    end.

%% The new first_offset: the largest f' any policy in Specs requires, with the
%% timestamps of the fragment that becomes the new first. Returns unchanged when
%% no policy removes anything.
new_first_offset(Lo, N, Entries, Specs, Now, TotalSize, Get) ->
    Best = lists:foldl(
        fun(Spec, {BestHi, _, _} = Acc) ->
            {Hi, _, _} = Cand = spec_boundary(Lo, N, Entries, Spec, Now, TotalSize, Get),
            case Hi > BestHi of
                true -> Cand;
                false -> Acc
            end
        end,
        {Lo, -1, -1},
        Specs
    ),
    case Best of
        {Lo, _, _} -> unchanged;
        _ -> Best
    end.

%% The new first_offset a single policy requires, as {Offset, FirstTs, LastTs}.
%% A boundary equal to Lo means the policy removes nothing.
spec_boundary(Lo, N, Entries, {max_age, MaxAgeMs}, Now, _TotalSize, Get) ->
    Cutoff = Now - MaxAgeMs,
    case boundary_age(Entries, Lo, Cutoff, Get) of
        all -> {N, -1, -1};
        {_, _, _} = B -> B
    end;
spec_boundary(Lo, N, Entries, {max_bytes, MaxBytes}, _Now, TotalSize, Get) ->
    case TotalSize - MaxBytes of
        ToRemove when ToRemove > 0 ->
            case boundary_bytes(Entries, Lo, ToRemove, Get) of
                {all, _} -> {N, -1, -1};
                {boundary, Off, FTs, LTs} -> {Off, FTs, LTs}
            end;
        _ ->
            {Lo, -1, -1}
    end;
spec_boundary(Lo, _N, _Entries, _Spec, _Now, _TotalSize, _Get) ->
    {Lo, -1, -1}.

%% The oldest fragment at or after Lo whose last timestamp is at or after the
%% cutoff -- i.e. the oldest survivor. `all` if every fragment is expired.
%% Timestamps are monotonic across fragments, so the expired prefix is
%% contiguous and a group whose last timestamp predates the cutoff is wholly
%% expired and can be skipped without descending.
boundary_age(<<>>, _Lo, _Cutoff, _Get) ->
    all;
boundary_age(
    <<Offset:64/unsigned, FTs:64/signed, LTs:64/signed, Kind:8/unsigned, _:40, Uid:32/unsigned,
        Rest/binary>>,
    Lo,
    Cutoff,
    Get
) ->
    case Kind of
        ?MANIFEST_KIND_FRAGMENT when Offset >= Lo andalso LTs >= Cutoff ->
            {Offset, FTs, LTs};
        ?MANIFEST_KIND_FRAGMENT ->
            boundary_age(Rest, Lo, Cutoff, Get);
        _ when LTs < Cutoff ->
            boundary_age(Rest, Lo, Cutoff, Get);
        _ ->
            Children = get_children(Get, #group_ref{offset = Offset, kind = Kind, uid = Uid}),
            case boundary_age(Children, Lo, Cutoff, Get) of
                all -> boundary_age(Rest, Lo, Cutoff, Get);
                Found -> Found
            end
    end.

%% The fragment at which the oldest ToRemove bytes have been removed (removing
%% whole fragments oldest-first), as {boundary, Offset, FirstTs, LastTs}, or
%% {all, RemainingToRemove} if every fragment is removed. Group entries carry no
%% size, so groups are descended to reach their fragments.
boundary_bytes(<<>>, _Lo, ToRemove, _Get) ->
    {all, ToRemove};
boundary_bytes(
    <<Offset:64/unsigned, FTs:64/signed, LTs:64/signed, Kind:8/unsigned, Size:40/unsigned,
        Uid:32/unsigned, Rest/binary>>,
    Lo,
    ToRemove,
    Get
) ->
    case Kind of
        ?MANIFEST_KIND_FRAGMENT when Offset < Lo ->
            boundary_bytes(Rest, Lo, ToRemove, Get);
        ?MANIFEST_KIND_FRAGMENT when ToRemove =< 0 ->
            {boundary, Offset, FTs, LTs};
        ?MANIFEST_KIND_FRAGMENT ->
            boundary_bytes(Rest, Lo, ToRemove - Size, Get);
        _ ->
            Children = get_children(Get, #group_ref{offset = Offset, kind = Kind, uid = Uid}),
            case boundary_bytes(Children, Lo, ToRemove, Get) of
                {boundary, _, _, _} = B -> B;
                {all, ToRemove1} -> boundary_bytes(Rest, Lo, ToRemove1, Get)
            end
    end.

%% Collect the objects to delete -- fragments in [Lo, Hi) and groups all of
%% whose descendants are below Hi -- with the total fragment bytes removed.
%% Returns {Refs, Bytes, AllConsumed}; AllConsumed is true when every entry of
%% this node lies below Hi. Fragments below Lo were deleted by an earlier cycle
%% and are skipped; a group with no newly-deleted children is likewise already
%% gone and its object is not re-deleted.
collect_deletions(<<>>, _Lo, _Hi, _Get) ->
    {[], 0, true};
collect_deletions(
    <<Offset:64/unsigned, _:64/signed, _:64/signed, Kind:8/unsigned, Size:40/unsigned,
        Uid:32/unsigned, Rest/binary>>,
    Lo,
    Hi,
    Get
) ->
    case Offset >= Hi of
        true ->
            {[], 0, false};
        false ->
            case Kind of
                ?MANIFEST_KIND_FRAGMENT when Offset >= Lo ->
                    {RRefs, RBytes, RAll} = collect_deletions(Rest, Lo, Hi, Get),
                    Ref = #fragment_ref{offset = Offset, uid = Uid, size = Size},
                    {[Ref | RRefs], Size + RBytes, RAll};
                ?MANIFEST_KIND_FRAGMENT ->
                    collect_deletions(Rest, Lo, Hi, Get);
                _ ->
                    GroupRef = #group_ref{offset = Offset, kind = Kind, uid = Uid},
                    Children = get_children(Get, GroupRef),
                    {CRefs, CBytes, CAll} = collect_deletions(Children, Lo, Hi, Get),
                    {RRefs, RBytes, RAll} = collect_deletions(Rest, Lo, Hi, Get),
                    case CAll of
                        true ->
                            GroupDel =
                                case CRefs of
                                    [] -> [];
                                    _ -> [GroupRef]
                                end,
                            {GroupDel ++ CRefs ++ RRefs, CBytes + RBytes, RAll};
                        false ->
                            {CRefs ++ RRefs, CBytes + RBytes, false}
                    end
            end
    end.

%% Number of leading root entries lying wholly below Hi, which are spliced out
%% of the root array. A group is wholly consumed when it has no descendant at or
%% beyond Hi.
count_whole_root(<<>>, _Hi, _Get, N) ->
    N;
count_whole_root(
    <<Offset:64/unsigned, _:64/signed, _:64/signed, Kind:8/unsigned, _:40, Uid:32/unsigned,
        Rest/binary>>,
    Hi,
    Get,
    N
) ->
    case Offset >= Hi of
        true ->
            N;
        false ->
            case Kind of
                ?MANIFEST_KIND_FRAGMENT ->
                    count_whole_root(Rest, Hi, Get, N + 1);
                _ ->
                    Children = get_children(Get, #group_ref{offset = Offset, kind = Kind, uid = Uid}),
                    case has_offset_ge(Children, Hi, Get) of
                        true -> N;
                        false -> count_whole_root(Rest, Hi, Get, N + 1)
                    end
            end
    end.

%% Whether any fragment at or beyond Hi exists within these entries.
has_offset_ge(<<>>, _Hi, _Get) ->
    false;
has_offset_ge(
    <<Offset:64/unsigned, _:64/signed, _:64/signed, Kind:8/unsigned, _:40, Uid:32/unsigned,
        Rest/binary>>,
    Hi,
    Get
) ->
    case Offset >= Hi of
        true ->
            true;
        false ->
            case Kind of
                ?MANIFEST_KIND_FRAGMENT ->
                    has_offset_ge(Rest, Hi, Get);
                _ ->
                    Children = get_children(Get, #group_ref{offset = Offset, kind = Kind, uid = Uid}),
                    has_offset_ge(Children, Hi, Get) orelse has_offset_ge(Rest, Hi, Get)
            end
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

frag(O, Sz) ->
    FTs = O * 10,
    LTs = FTs + 9,
    Uid = O + 1,
    ?ENTRY(O, FTs, LTs, ?MANIFEST_KIND_FRAGMENT, Sz, Uid).
grp(O, Sz) ->
    FTs = O * 10,
    LTs = FTs + 9,
    Uid = O + 1,
    ?ENTRY(O, FTs, LTs, ?MANIFEST_KIND_GROUP, Sz, Uid).

%% An empty manifest is well-formed iff first_offset == next_offset.
is_well_formed_empty_test() ->
    ?assert(
        is_well_formed(#manifest{first_offset = 7, next_offset = 7, total_size = 0, entries = <<>>})
    ),
    ?assertNot(
        is_well_formed(#manifest{first_offset = 0, next_offset = 5, total_size = 0, entries = <<>>})
    ).

%% A flat manifest: ordered offsets, first == f, last < n, total_size == sum.
is_well_formed_flat_test() ->
    Entries = <<(frag(0, 100))/binary, (frag(10, 200))/binary>>,
    Good = #manifest{first_offset = 0, next_offset = 20, total_size = 300, entries = Entries},
    ?assert(is_well_formed(Good)),
    %% Accounting drift (total_size != sum of flat entry sizes).
    ?assertNot(is_well_formed(Good#manifest{total_size = 999})),
    %% first_offset > next_offset.
    ?assertNot(is_well_formed(Good#manifest{first_offset = 25, next_offset = 24})),
    %% Last entry offset (10) is not strictly below next_offset.
    ?assertNot(is_well_formed(Good#manifest{next_offset = 10, total_size = 300})).

%% Offsets must be strictly increasing.
is_well_formed_unordered_test() ->
    Entries = <<(frag(10, 100))/binary, (frag(0, 100))/binary>>,
    ?assertNot(
        is_well_formed(#manifest{
            first_offset = 0, next_offset = 30, total_size = 200, entries = Entries
        })
    ).

%% No gap below the floor: the first entry must start at or before first_offset.
is_well_formed_gap_below_floor_test() ->
    Entries = <<(frag(10, 100))/binary, (frag(20, 100))/binary>>,
    ?assertNot(
        is_well_formed(#manifest{
            first_offset = 0, next_offset = 30, total_size = 200, entries = Entries
        })
    ).

%% A group-led manifest: accounting is skipped (group hides bytes), and
%% first_offset may sit inside the leading group (intra-group retention).
is_well_formed_group_led_test() ->
    Entries = <<(grp(0, 0))/binary, (frag(100, 200))/binary>>,
    %% total_size intentionally not equal to the visible entry sizes; skipped.
    M = #manifest{first_offset = 50, next_offset = 200, total_size = 999, entries = Entries},
    ?assert(is_well_formed(M)),
    ?assert(is_well_formed(M#manifest{first_offset = 0})),
    %% Still rejects an out-of-order group-led manifest.
    Bad = <<(grp(100, 0))/binary, (frag(0, 200))/binary>>,
    ?assertNot(
        is_well_formed(#manifest{
            first_offset = 100, next_offset = 200, total_size = 999, entries = Bad
        })
    ).

assert_well_formed_raises_test() ->
    Bad = #manifest{
        first_offset = 0, next_offset = 20, total_size = 999, entries = <<(frag(0, 100))/binary>>
    },
    ?assertError({manifest_not_well_formed, _}, assert_well_formed(Bad)),
    ?assertEqual(
        ok,
        assert_well_formed(#manifest{
            first_offset = 0, next_offset = 0, total_size = 0, entries = <<>>
        })
    ).

-endif.
