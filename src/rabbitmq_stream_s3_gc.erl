%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_gc).
-moduledoc """
Garbage-collects dangling remote tier objects.

LISTs S3 objects under `rabbitmq/stream/` and identifies orphans by comparing
object keys against current state in Khepri (epoch) and the manifest replica
ETS cache (first_offset).

Safety relies on monotonicity: epoch and first_offset only move forward.
Eventually-consistent reads from Khepri or ETS can only return stale (lower)
values, which makes the GC more conservative (false negatives), never less safe
(false positives are structurally impossible).

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
        log_finding(Finding),
        rabbitmq_stream_s3_reaper:delete_objects(StreamId, [Key]),
        [Finding | Acc]
    end.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

build_lookup(Streams) ->
    maps:fold(
        fun(StreamId, #{epoch := Epoch}, Acc) ->
            case rabbitmq_stream_s3_manifest_replica:get_range(StreamId) of
                {FirstOffset, _NextOffset} ->
                    Acc#{StreamId => #{epoch => Epoch, first_offset => FirstOffset}};
                empty ->
                    Acc
            end
        end,
        #{},
        Streams
    ).

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
                        #manifest{first_offset = FirstOffset} ->
                            {ok, #{
                                StreamId => #{epoch => CommittedEpoch, first_offset => FirstOffset}
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

%% Parse an S3 key into its components.
%%
%% Fragment keys: rabbitmq/stream/<StreamId>/data/<offset>.<uid>.fragment
%% Group keys:   rabbitmq/stream/<StreamId>/metadata/<offset>.<uid>.<kind>
%% Manifest keys: rabbitmq/stream/<StreamId>/metadata/root.<epoch>.<uid>.manifest
-spec parse_key(rabbitmq_stream_s3:key()) ->
    {data, stream_id(), osiris:offset()}
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
            %% Group objects have the same offset-based safety as fragments.
            {data, StreamId, binary_to_integer(OffsetBin)};
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
    ?assertEqual({data, <<"s">>, 500}, parse_key(Key)).

parse_key_kilo_group_test() ->
    Key = <<"rabbitmq/stream/s/metadata/00000000000000001000.aabbccdd.kgroup">>,
    ?assertEqual({data, <<"s">>, 1000}, parse_key(Key)).

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
    ?assertEqual({data, <<"s">>, 5000}, parse_key(Key)).

parse_key_manifest_test() ->
    Key = <<"rabbitmq/stream/my_stream/metadata/root.3.aabb0011.manifest">>,
    ?assertEqual({manifest, <<"my_stream">>, 3}, parse_key(Key)).

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
    ?assertEqual({data, StreamId, 999}, parse_key(Key)).

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

-endif.
