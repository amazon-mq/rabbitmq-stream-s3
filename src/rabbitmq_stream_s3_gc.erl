%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_gc).
-moduledoc """
Garbage-collects dangling remote tier objects.

LISTs S3 objects under `rabbitmq/stream/` and identifies orphans by comparing
object keys against current state in Khepri (epoch) and the manifest replica
ETS cache (first_offset).

Safety relies on monotonicity: epoch only moves forward, and first_offset
normally only moves forward. Eventually-consistent reads can only return stale
(lower) values, which makes the GC more conservative (false negatives), never
less safe.

The one break in that monotonicity is a remote-tier-ahead reset, which rebuilds
the manifest from the local log floor after a timeline change and can *lower*
first_offset, re-tiering live fragments (with fresh UIDs) at offsets below a
floor a concurrent sweep already snapshotted. Two guards keep the sweep safe:

  1. Each candidate deletion is re-validated against the live first_offset
     immediately before the object is deleted (see still_dangling/1) and skipped
     if the floor has dropped to at or below its offset. The reset lowers the
     cached floor before it re-uploads, so a re-tiered fragment is always at or
     above the live floor by the time it exists.
  2. The cross-stream sweep, like the single-stream path, gates each stream on a
     strongly-consistent metadata read, so a partitioned or deposed node that
     cannot reach a quorum fails closed and skips rather than deleting on a stale
     local view.

Streams with no local manifest replica are skipped (cannot verify first_offset).
Unknown stream prefixes (stream not in Khepri) are skipped (no safe deletion
signal without Khepri restructuring).
""".

-include("include/rabbitmq_stream_s3.hrl").
-include("include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-export([run/0, run/1, run_stream/2, run_stream/3]).

-type mode() :: dry_run | delete.
-type config() :: #{mode => mode(), writer_epoch => non_neg_integer()}.
-type reason() :: below_first_offset | stale_epoch.
-type finding() :: #{stream_id := stream_id(), key := rabbitmq_stream_s3:key(), reason := reason()}.

-export_type([mode/0, config/0, finding/0]).

run() ->
    run(#{mode => dry_run}).

-doc """
Run garbage collection. In `dry_run` mode, identifies and logs orphans without
deleting. In `delete` mode, deletes identified orphans via the reaper.
""".
-spec run(config()) -> {ok, [finding()]}.
run(Config) when is_map(Config) ->
    Mode = maps:get(mode, Config, dry_run),
    {ok, Streams} = rabbitmq_stream_s3_db:list(),
    Lookup = build_lookup(Streams),
    Fun = make_handler(Mode),
    Findings = list_and_classify(<<"rabbitmq/stream/">>, start, Lookup, Fun, []),
    ?LOG_INFO("GC ~ts complete: ~b dangling object(s)", [Mode, length(Findings)]),
    {ok, Findings}.

-doc """
Run garbage collection scoped to a single stream. Only lists objects under the
stream's S3 prefix and classifies them against the stream's current epoch and
first_offset. Used by the replica reader after a manifest reset to reclaim
orphaned fragments without a full cross-stream sweep.
""".
-spec run_stream(stream_id(), config()) -> {ok, [finding()]}.
run_stream(StreamId, Config) when is_binary(StreamId), is_map(Config) ->
    Mode = maps:get(mode, Config, dry_run),
    case build_stream_lookup(StreamId, Config) of
        {ok, Lookup} ->
            Prefix = rabbitmq_stream_s3:stream_prefix(StreamId),
            Fun = make_handler(Mode),
            Findings = list_and_classify(Prefix, start, Lookup, Fun, []),
            ?LOG_INFO(
                "GC ~ts for stream ~ts complete: ~b dangling object(s)",
                [Mode, StreamId, length(Findings)]
            ),
            {ok, Findings};
        skip ->
            {ok, []}
    end.

-doc """
Run garbage collection scoped to a single stream identified by its vhost and
queue name. Resolves the stream id, then delegates to `run_stream/2`.
""".
-spec run_stream(rabbit_types:vhost(), binary(), config()) ->
    {ok, [finding()]} | {error, {not_found, binary()}}.
run_stream(VHost, QueueName, Config) when
    is_binary(VHost), is_binary(QueueName), is_map(Config)
->
    case rabbitmq_stream_s3_replica_reader:resolve_stream_id(VHost, QueueName) of
        {ok, StreamId} ->
            run_stream(StreamId, Config);
        {error, _} = Err ->
            Err
    end.

make_handler(dry_run) ->
    fun(Finding, Acc) ->
        log_finding(Finding),
        [Finding | Acc]
    end;
make_handler(delete) ->
    fun(#{stream_id := StreamId, key := Key} = Finding, Acc) ->
        case still_dangling(Finding) of
            true ->
                log_finding(Finding),
                rabbitmq_stream_s3_reaper:delete_objects(StreamId, [Key]),
                [Finding | Acc];
            false ->
                ?LOG_INFO(
                    "GC: not deleting ~ts (stream=~ts): it is no longer below the "
                    "live first offset; a concurrent manifest reset re-tiered into "
                    "this range",
                    [Key, StreamId]
                ),
                Acc
        end
    end.

%% Re-validate an offset-based finding against the live manifest immediately
%% before deleting it. The sweep's safety argument is that first_offset only
%% moves forward, so a snapshot floor is never above the live floor. A
%% remote-tier-ahead reset breaks that: it lowers first_offset and re-tiers live
%% fragments (with fresh UIDs) at offsets below the snapshot floor. The reset
%% lowers the cached floor before it re-uploads, so re-reading the live floor here
%% and skipping anything now at or above it restores the assumption; a genuine
%% orphan skipped by a transient drop is reclaimed by a later sweep. Epoch-based
%% (manifest) findings need no re-check: epoch is genuinely monotonic.
-spec still_dangling(finding()) -> boolean().
still_dangling(#{reason := stale_epoch}) ->
    true;
still_dangling(#{reason := below_first_offset, stream_id := StreamId, key := Key}) ->
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{first_offset = FirstOffset} ->
            case parse_key(Key) of
                {data, _StreamId, Offset} -> Offset < FirstOffset;
                {group, _StreamId, Offset} -> Offset < FirstOffset;
                _ -> false
            end;
        undefined ->
            %% No live manifest to compare against: do not delete (a later sweep
            %% reclaims a genuine orphan once a floor is known again).
            false
    end.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

build_lookup(Streams) ->
    maps:fold(
        fun(StreamId, _LocalEntry, Acc) ->
            %% Like the single-stream path (build_stream_lookup/2), gate each
            %% stream on a strongly-consistent metadata read so a partitioned or
            %% deposed node that cannot reach a quorum fails closed and skips,
            %% rather than deleting on a stale local view. The committed epoch
            %% from this read is also more authoritative than the local db:list/0
            %% snapshot. This makes a full sweep do one quorum read per stream,
            %% which is acceptable for an infrequent operator-driven sweep.
            case rabbitmq_stream_s3_db:get_consistent(StreamId) of
                {ok, #{epoch := Epoch}} ->
                    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
                        #manifest{entries = <<>>} ->
                            %% Empty manifest: nothing in the remote tier to
                            %% compare against (matches the prior get_range/1
                            %% `empty` skip).
                            Acc;
                        #manifest{first_offset = FirstOffset} = Manifest ->
                            Acc#{StreamId => lookup_entry(StreamId, Epoch, FirstOffset, Manifest)};
                        undefined ->
                            Acc
                    end;
                {error, Reason} ->
                    ?LOG_INFO(
                        "GC for stream ~ts skipped: could not read committed "
                        "metadata with quorum (~p); not sweeping",
                        [StreamId, Reason]
                    ),
                    Acc
            end
        end,
        #{},
        Streams
    ).

%% Build a per-stream lookup entry. Besides the epoch and first_offset used for
%% the offset/epoch heuristics, it records the leading group's object key (the
%% one group below first_offset that may still be referenced) and a skip-groups
%% flag for the conservative multi-level case. See leading_group_info/2.
-spec lookup_entry(stream_id(), osiris:epoch(), osiris:offset(), #manifest{}) -> map().
lookup_entry(StreamId, Epoch, FirstOffset, Manifest) ->
    {ReferencedGroupKey, SkipGroups} = leading_group_info(StreamId, Manifest),
    #{
        epoch => Epoch,
        first_offset => FirstOffset,
        referenced_group_key => ReferencedGroupKey,
        skip_groups => SkipGroups
    }.

%% Determine which group object, if any, must be protected from offset-based GC,
%% and whether group deletion must be skipped entirely for the stream.
%%
%% Only the first root entry can be a group that sits below first_offset while
%% still being referenced: retention advances first_offset into the leading
%% group on partial expiry, and every later root entry begins at or above that
%% group's coverage end. A leading level-1 group contains only fragments, so
%% protecting that single object is sufficient (its expired fragment children
%% are reclaimed correctly by the offset heuristic). A leading kilo-/mega-group
%% contains nested groups whose own leading child can also straddle the floor as
%% a separate object; identifying those requires descending the tree, so we
%% conservatively skip all group deletion for such streams. This is rare: a
%% kilo-group needs ~1024 groups (~1M fragments).
-spec leading_group_info(stream_id(), #manifest{}) ->
    {rabbitmq_stream_s3:key() | none, boolean()}.
leading_group_info(_StreamId, #manifest{entries = <<>>}) ->
    {none, false};
leading_group_info(StreamId, #manifest{entries = Entries}) ->
    ?ENTRY(Offset, _FirstTs, _LastTs, Kind, _Size, Uid, _Rest) = Entries,
    case Kind of
        ?MANIFEST_KIND_FRAGMENT ->
            {none, false};
        ?MANIFEST_KIND_GROUP ->
            Key = rabbitmq_stream_s3:group_key(
                StreamId, #group_ref{offset = Offset, kind = Kind, uid = Uid}
            ),
            {Key, false};
        _ ->
            %% Kilo-/mega-group: nested referenced leading groups cannot be
            %% cheaply identified. Conservatively skip group deletion.
            {none, true}
    end.

build_stream_lookup(StreamId, Config) ->
    %% Read the committed epoch with a strongly consistent (quorum-requiring)
    %% read, not the default low-latency local read. The sweep deletes data
    %% objects below a floor taken from the local just-reset manifest, which sits
    %% above the committed floor. A deposed writer that read stale local state
    %% would delete a successor's live fragments in that gap. A consistent read
    %% makes a deposed minority writer, which cannot reach a quorum, fail closed
    %% and skip. When the caller supplies its own writer epoch (the reset path),
    %% additionally require the committed epoch to equal it, so a deposed writer
    %% that can still reach a quorum also skips.
    case rabbitmq_stream_s3_db:get_consistent(StreamId) of
        {ok, #{epoch := CommittedEpoch}} ->
            case epoch_permits_sweep(CommittedEpoch, Config) of
                true ->
                    %% Read first_offset from the manifest record directly rather
                    %% than via get_range/1, which reports `empty` whenever the
                    %% entries array is empty. A manifest reset (the caller of
                    %% run_stream/2) installs exactly such an empty manifest with
                    %% first_offset at the local floor, and that floor is precisely
                    %% what the orphaned fragments sit below. Using get_range/1
                    %% here would skip the stream and reclaim nothing.
                    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
                        #manifest{first_offset = FirstOffset} = Manifest ->
                            {ok, #{
                                StreamId => lookup_entry(
                                    StreamId, CommittedEpoch, FirstOffset, Manifest
                                )
                            }};
                        undefined ->
                            skip
                    end;
                false ->
                    ?LOG_INFO(
                        "GC for stream ~ts skipped: writer epoch ~p is behind the "
                        "committed epoch ~p, this writer has been deposed",
                        [StreamId, maps:get(writer_epoch, Config, undefined), CommittedEpoch]
                    ),
                    skip
            end;
        {error, Reason} ->
            ?LOG_INFO(
                "GC for stream ~ts skipped: could not read committed metadata "
                "with quorum (~p); not sweeping",
                [StreamId, Reason]
            ),
            skip
    end.

%% On the reset path the caller pins its writer epoch, and the sweep is permitted
%% only when the committed epoch exactly equals it, confirming this writer is the
%% current committed one and has not been superseded. On the operator CLI path no
%% writer epoch is pinned, and the consistent read alone is the guard.
epoch_permits_sweep(CommittedEpoch, #{writer_epoch := WriterEpoch}) ->
    CommittedEpoch =:= WriterEpoch;
epoch_permits_sweep(_CommittedEpoch, _Config) ->
    true.

list_and_classify(_Prefix, done, _Lookup, _Fun, Acc) ->
    lists:reverse(Acc);
list_and_classify(Prefix, Continuation, Lookup, Fun, Acc) ->
    case rabbitmq_stream_s3_api:list(Prefix, Continuation) of
        {ok, [], done} ->
            lists:reverse(Acc);
        {ok, Keys, NextContinuation} ->
            Orphans = classify_page(Keys, Lookup),
            NewAcc = lists:foldl(Fun, Acc, Orphans),
            list_and_classify(Prefix, NextContinuation, Lookup, Fun, NewAcc);
        {error, Reason} ->
            ?LOG_WARNING("GC: failed to list objects under ~ts: ~p", [Prefix, Reason]),
            lists:reverse(Acc)
    end.

classify_page(Keys, Lookup) ->
    lists:filtermap(
        fun(Key) ->
            case classify(Key, Lookup) of
                {ok, Finding} -> {true, Finding};
                skip -> false
            end
        end,
        Keys
    ).

-spec classify(rabbitmq_stream_s3:key(), map()) -> {ok, finding()} | skip.
classify(Key, Lookup) ->
    case parse_key(Key) of
        {data, StreamId, Offset} ->
            case Lookup of
                #{StreamId := #{first_offset := FirstOffset}} when Offset < FirstOffset ->
                    {ok, #{stream_id => StreamId, key => Key, reason => below_first_offset}};
                _ ->
                    skip
            end;
        {group, StreamId, Offset} ->
            case Lookup of
                #{StreamId := #{first_offset := FirstOffset} = Info} when Offset < FirstOffset ->
                    classify_group(StreamId, Key, Info);
                _ ->
                    skip
            end;
        {manifest, StreamId, Epoch} ->
            case Lookup of
                #{StreamId := #{epoch := CurrentEpoch}} when Epoch < CurrentEpoch ->
                    {ok, #{stream_id => StreamId, key => Key, reason => stale_epoch}};
                _ ->
                    skip
            end;
        unknown ->
            %% Key belongs to a stream not in the lookup (either unknown to
            %% Khepri or missing a local manifest replica). Not safe to delete
            %% without a positive "stream deleted" signal. See #149 for the
            %% planned Khepri restructuring that would make stream-prefix
            %% sweep safe.
            skip
    end.

%% A group object below first_offset is an orphan and safe to delete unless it
%% is the manifest's referenced leading group, or the stream is in conservative
%% skip-groups mode (a leading kilo-/mega-group whose nested referenced groups
%% cannot be cheaply identified). See leading_group_info/2.
-spec classify_group(stream_id(), rabbitmq_stream_s3:key(), map()) ->
    {ok, finding()} | skip.
classify_group(_StreamId, _Key, #{skip_groups := true}) ->
    skip;
classify_group(_StreamId, Key, #{referenced_group_key := Key}) ->
    skip;
classify_group(StreamId, Key, _Info) ->
    {ok, #{stream_id => StreamId, key => Key, reason => below_first_offset}}.

%% Parse an S3 key into its components.
%%
%% Fragment keys: rabbitmq/stream/<StreamId>/data/<offset>.<uid>.fragment
%% Group keys:   rabbitmq/stream/<StreamId>/metadata/<offset>.<uid>.<kind>
%% Manifest keys: rabbitmq/stream/<StreamId>/metadata/root.<epoch>.<uid>.manifest
-spec parse_key(rabbitmq_stream_s3:key()) ->
    {data, stream_id(), osiris:offset()}
    | {group, stream_id(), osiris:offset()}
    | {manifest, stream_id(), osiris:epoch()}
    | unknown.
parse_key(<<"rabbitmq/stream/", Rest/binary>>) ->
    case binary:split(Rest, <<"/">>) of
        [StreamId, <<"data/", Filename/binary>>] ->
            parse_data_filename(StreamId, Filename);
        [StreamId, <<"metadata/", Filename/binary>>] ->
            parse_metadata_filename(StreamId, Filename);
        _ ->
            unknown
    end;
parse_key(_) ->
    unknown.

parse_data_filename(StreamId, Filename) ->
    %% <offset>.<uid>.fragment
    case binary:split(Filename, <<".">>, [global]) of
        [OffsetBin, _Uid, <<"fragment">>] ->
            {data, StreamId, binary_to_integer(OffsetBin)};
        _ ->
            unknown
    end.

parse_metadata_filename(StreamId, Filename) ->
    case binary:split(Filename, <<".">>, [global]) of
        [<<"root">>, EpochBin, _Uid, <<"manifest">>] ->
            {manifest, StreamId, binary_to_integer(EpochBin)};
        [OffsetBin, _Uid, Kind] when
            Kind =:= <<"group">> orelse Kind =:= <<"kgroup">> orelse Kind =:= <<"mgroup">>
        ->
            %% Unlike fragments, a group object spans a multi-fragment range, so
            %% offset-below-first_offset alone does not prove it is dead (a
            %% partially-expired leading group is still referenced). classify/2
            %% checks it against the referenced leading group; see
            %% leading_group_info/2.
            {group, StreamId, binary_to_integer(OffsetBin)};
        _ ->
            unknown
    end.

log_finding(#{stream_id := StreamId, key := Key, reason := Reason}) ->
    ?LOG_INFO("GC: dangling object ~ts (stream=~ts, reason=~p)", [Key, StreamId, Reason]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

parse_key_fragment_test() ->
    Key = <<"rabbitmq/stream/my_stream/data/00000000000000000100.deadbeef.fragment">>,
    ?assertEqual({data, <<"my_stream">>, 100}, parse_key(Key)).

parse_key_fragment_zero_test() ->
    Key = <<"rabbitmq/stream/s/data/00000000000000000000.00000001.fragment">>,
    ?assertEqual({data, <<"s">>, 0}, parse_key(Key)).

parse_key_group_test() ->
    Key = <<"rabbitmq/stream/s/metadata/00000000000000000500.aabbccdd.group">>,
    ?assertEqual({group, <<"s">>, 500}, parse_key(Key)).

parse_key_kilo_group_test() ->
    Key = <<"rabbitmq/stream/s/metadata/00000000000000001000.aabbccdd.kgroup">>,
    ?assertEqual({group, <<"s">>, 1000}, parse_key(Key)).

%% The operator CLI path supplies no writer epoch, so the consistent read is the
%% only guard and the sweep is permitted.
epoch_permits_sweep_no_writer_epoch_test() ->
    ?assert(epoch_permits_sweep(7, #{mode => delete})).

%% The reset path pins the writer epoch. The sweep proceeds only when the
%% committed epoch matches, i.e. this writer is still the committed authority.
epoch_permits_sweep_matching_epoch_test() ->
    ?assert(epoch_permits_sweep(7, #{mode => delete, writer_epoch => 7})).

%% A successor has committed a higher epoch: this writer is deposed, skip.
epoch_permits_sweep_deposed_writer_test() ->
    ?assertNot(epoch_permits_sweep(8, #{mode => delete, writer_epoch => 7})).

%% A writer that has not yet committed at its own epoch (committed epoch is the
%% predecessor's) also skips, since below its raised floor could be the
%% predecessor's committed data.
epoch_permits_sweep_uncommitted_writer_test() ->
    ?assertNot(epoch_permits_sweep(6, #{mode => delete, writer_epoch => 7})).

parse_key_mega_group_test() ->
    Key = <<"rabbitmq/stream/s/metadata/00000000000000005000.aabbccdd.mgroup">>,
    ?assertEqual({group, <<"s">>, 5000}, parse_key(Key)).

parse_key_manifest_test() ->
    Key = <<"rabbitmq/stream/my_stream/metadata/root.3.aabb0011.manifest">>,
    ?assertEqual({manifest, <<"my_stream">>, 3}, parse_key(Key)).

%% Helpers for the classify/leading-group tests.
group_entry(Offset, Kind, Uid) ->
    ?ENTRY(Offset, 0, 0, Kind, 0, Uid).

fragment_entry(Offset, Uid) ->
    ?ENTRY(Offset, 0, 0, ?MANIFEST_KIND_FRAGMENT, 0, Uid).

%% A fragment below first_offset is always dead and deletable (unchanged
%% behaviour).
classify_fragment_below_floor_deletes_test() ->
    Lookup = #{<<"s">> => #{first_offset => 100, epoch => 1}},
    Key = <<"rabbitmq/stream/s/data/00000000000000000050.0000002a.fragment">>,
    ?assertMatch({ok, #{reason := below_first_offset}}, classify(Key, Lookup)).

%% The leading group recorded as referenced must be protected even though its
%% offset is below first_offset (the partial-expiry case).
classify_referenced_leading_group_skipped_test() ->
    StreamId = <<"s">>,
    Uid = 16#aabbccdd,
    LeadingKey = rabbitmq_stream_s3:group_key(
        StreamId, #group_ref{offset = 50, kind = ?MANIFEST_KIND_GROUP, uid = Uid}
    ),
    Lookup = #{
        StreamId => #{
            first_offset => 100,
            epoch => 1,
            referenced_group_key => LeadingKey,
            skip_groups => false
        }
    },
    ?assertEqual(skip, classify(LeadingKey, Lookup)).

%% A different group below first_offset (not the referenced leading one) is an
%% orphan and deletable.
classify_other_below_floor_group_deletes_test() ->
    StreamId = <<"s">>,
    LeadingKey = rabbitmq_stream_s3:group_key(
        StreamId, #group_ref{offset = 50, kind = ?MANIFEST_KIND_GROUP, uid = 16#aabbccdd}
    ),
    Lookup = #{
        StreamId => #{
            first_offset => 100,
            epoch => 1,
            referenced_group_key => LeadingKey,
            skip_groups => false
        }
    },
    OtherKey = <<"rabbitmq/stream/s/metadata/00000000000000000010.00000099.group">>,
    ?assertMatch({ok, #{reason := below_first_offset}}, classify(OtherKey, Lookup)).

%% In conservative skip-groups mode (leading kilo-/mega-group) no group is
%% deleted, but fragments still are.
classify_skip_groups_mode_skips_all_groups_test() ->
    StreamId = <<"s">>,
    Lookup = #{
        StreamId => #{
            first_offset => 100,
            epoch => 1,
            referenced_group_key => none,
            skip_groups => true
        }
    },
    GroupKey = <<"rabbitmq/stream/s/metadata/00000000000000000010.00000099.group">>,
    ?assertEqual(skip, classify(GroupKey, Lookup)),
    FragKey = <<"rabbitmq/stream/s/data/00000000000000000010.00000099.fragment">>,
    ?assertMatch({ok, #{reason := below_first_offset}}, classify(FragKey, Lookup)).

%% leading_group_info: empty manifest and a leading fragment protect nothing.
leading_group_info_none_test() ->
    ?assertEqual({none, false}, leading_group_info(<<"s">>, #manifest{entries = <<>>})),
    Frag = fragment_entry(0, 1),
    ?assertEqual(
        {none, false}, leading_group_info(<<"s">>, #manifest{entries = Frag})
    ).

%% leading_group_info: a leading level-1 group is protected by key.
leading_group_info_group_test() ->
    StreamId = <<"s">>,
    Entries = group_entry(50, ?MANIFEST_KIND_GROUP, 16#aabbccdd),
    Expected = rabbitmq_stream_s3:group_key(
        StreamId, #group_ref{offset = 50, kind = ?MANIFEST_KIND_GROUP, uid = 16#aabbccdd}
    ),
    ?assertEqual({Expected, false}, leading_group_info(StreamId, #manifest{entries = Entries})).

%% leading_group_info: a leading kilo-group triggers conservative skip.
leading_group_info_kilo_group_skips_test() ->
    Entries = group_entry(50, ?MANIFEST_KIND_KILO_GROUP, 16#aabbccdd),
    ?assertEqual({none, true}, leading_group_info(<<"s">>, #manifest{entries = Entries})).

parse_key_manifest_epoch_zero_test() ->
    Key = <<"rabbitmq/stream/s/metadata/root.0.aabb0011.manifest">>,
    ?assertEqual({manifest, <<"s">>, 0}, parse_key(Key)).

parse_key_unknown_test() ->
    ?assertEqual(unknown, parse_key(<<"some/other/key">>)),
    ?assertEqual(unknown, parse_key(<<"rabbitmq/stream/s/other/file">>)).

%% Round-trip: keys generated by rabbitmq_stream_s3 are parseable.
parse_key_roundtrip_fragment_test() ->
    StreamId = <<"__vhost_stream_123">>,
    Key = rabbitmq_stream_s3:fragment_key(StreamId, 42, 16#deadbeef),
    ?assertEqual({data, StreamId, 42}, parse_key(Key)).

parse_key_roundtrip_manifest_test() ->
    StreamId = <<"__vhost_stream_123">>,
    Ref = #manifest_ref{epoch = 7, uid = 16#aabbccdd},
    Key = rabbitmq_stream_s3:manifest_key(StreamId, Ref),
    ?assertEqual({manifest, StreamId, 7}, parse_key(Key)).

parse_key_roundtrip_group_test() ->
    StreamId = <<"__vhost_stream_123">>,
    Ref = #group_ref{offset = 999, kind = ?MANIFEST_KIND_GROUP, uid = 16#11223344},
    Key = rabbitmq_stream_s3:group_key(StreamId, Ref),
    ?assertEqual({group, StreamId, 999}, parse_key(Key)).

classify_fragment_below_first_offset_test() ->
    Lookup = #{<<"s">> => #{epoch => 5, first_offset => 200}},
    Key = <<"rabbitmq/stream/s/data/00000000000000000100.deadbeef.fragment">>,
    ?assertEqual(
        {ok, #{stream_id => <<"s">>, key => Key, reason => below_first_offset}},
        classify(Key, Lookup)
    ).

classify_fragment_at_first_offset_test() ->
    %% At or above first_offset is NOT garbage.
    Lookup = #{<<"s">> => #{epoch => 5, first_offset => 100}},
    Key = <<"rabbitmq/stream/s/data/00000000000000000100.deadbeef.fragment">>,
    ?assertEqual(skip, classify(Key, Lookup)).

classify_manifest_stale_epoch_test() ->
    Lookup = #{<<"s">> => #{epoch => 5, first_offset => 0}},
    Key = <<"rabbitmq/stream/s/metadata/root.3.aabb0011.manifest">>,
    ?assertEqual(
        {ok, #{stream_id => <<"s">>, key => Key, reason => stale_epoch}},
        classify(Key, Lookup)
    ).

classify_manifest_current_epoch_test() ->
    %% Current epoch is NOT garbage.
    Lookup = #{<<"s">> => #{epoch => 3, first_offset => 0}},
    Key = <<"rabbitmq/stream/s/metadata/root.3.aabb0011.manifest">>,
    ?assertEqual(skip, classify(Key, Lookup)).

classify_unknown_stream_skipped_test() ->
    %% Stream not in lookup -> skip (not safe to delete).
    Lookup = #{},
    Key = <<"rabbitmq/stream/unknown/data/00000000000000000100.deadbeef.fragment">>,
    ?assertEqual(skip, classify(Key, Lookup)).

%% A candidate deletion is re-validated against the live first_offset just before
%% deleting. A remote-tier-ahead reset lowers first_offset and re-tiers live
%% fragments below a sweep's snapshot floor; the re-validation must skip them
%% while still reclaiming genuine orphans below the live floor. This is the
%% durability hole that an offset-only snapshot check would open.
still_dangling_respects_live_first_offset_test_() ->
    {setup,
        fun() ->
            {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
            unlink(Pid),
            Pid
        end,
        fun(Pid) -> gen_server:stop(Pid) end, fun(_) ->
            StreamId = <<"gc-revalidate-stream">>,
            %% Live, post-reset floor: first_offset lowered to 800.
            ok = rabbitmq_stream_s3_manifest_replica:put_manifest(
                StreamId, #manifest{first_offset = 800, next_offset = 800}
            ),
            Orphan = #{
                stream_id => StreamId,
                key =>
                    <<"rabbitmq/stream/gc-revalidate-stream/data/00000000000000000700.0000002a.fragment">>,
                reason => below_first_offset
            },
            ReTiered = #{
                stream_id => StreamId,
                key =>
                    <<"rabbitmq/stream/gc-revalidate-stream/data/00000000000000000850.0000002b.fragment">>,
                reason => below_first_offset
            },
            StaleManifest = #{
                stream_id => StreamId,
                key =>
                    <<"rabbitmq/stream/gc-revalidate-stream/metadata/root.1.0000aabb.manifest">>,
                reason => stale_epoch
            },
            [
                %% 700 < live floor 800: a genuine orphan, still deletable.
                ?_assert(still_dangling(Orphan)),
                %% 850 >= live floor 800: a reset re-tiered into this range. A
                %% sweep that snapshotted the old floor (e.g. 1000) would have
                %% marked it; the live floor protects it.
                ?_assertNot(still_dangling(ReTiered)),
                %% Epoch-based findings are not re-checked (epoch is monotonic).
                ?_assert(still_dangling(StaleManifest))
            ]
        end}.

%% With no live manifest (cache miss), a candidate is not deleted: there is no
%% floor to confirm it is still an orphan.
still_dangling_without_manifest_keeps_object_test_() ->
    {setup,
        fun() ->
            {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
            unlink(Pid),
            Pid
        end,
        fun(Pid) -> gen_server:stop(Pid) end, fun(_) ->
            Finding = #{
                stream_id => <<"gc-no-manifest-stream">>,
                key =>
                    <<"rabbitmq/stream/gc-no-manifest-stream/data/00000000000000000100.0000002a.fragment">>,
                reason => below_first_offset
            },
            [?_assertNot(still_dangling(Finding))]
        end}.

-endif.
