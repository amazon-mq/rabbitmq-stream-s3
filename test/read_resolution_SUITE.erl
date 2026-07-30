%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(read_resolution_SUITE).
-moduledoc """
Property and falsification tests for the read-tier-resolution decisions in
`rabbitmq_stream_s3_log_reader` (`resolve_first_lookup/1` and the routing in
`resolve_remote_location/2`).

The point of this suite is to falsify the bug class the fix is meant to make
impossible: a *transient* fragment-fetch error being collapsed into the
"remote tier is empty -> serve from local" branch, which silently skips the
remote range below the local floor.

The soundness oracle (`expected_class/1`) re-derives, from the lookup outcome
alone, which tier the read must resolve to - independently of how
`resolve_first_lookup/1` decides. A `{ok, ...}` lookup must resolve remote, an
`end_of_manifest` must resolve local, and a transient `group_fetch_failed` must
resolve to retry (never local).

To prove the oracle has discriminating power, `resolve_first_lookup_buggy/1` - a
faithful reproduction of the historical catch-all (`_ -> {local, first}`) that
left `resolve_first` exposed - is run through it and the property must find a
counterexample.
""".

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("include/rabbitmq_stream_s3.hrl").

-define(M, rabbitmq_stream_s3_log_reader).

all() ->
    [
        correct_resolution_is_sound,
        transient_error_never_resolves_local,
        ok_resolves_to_remote_location,
        end_of_manifest_resolves_local,
        property_catches_collapsed_transient_error,
        collapsed_transient_error_is_deterministically_caught,
        routing_offset_in_local_tier_resolves_local,
        routing_missing_row_below_floor_fails_closed,
        routing_pending_below_floor_fails_closed,
        routing_pending_spec_first_fails_closed,
        routing_empty_local_log_routes_remote,
        pending_row_reports_unavailable_at_callback_boundary
    ].

%% The routing properties drive the real `log_reader:resolve_remote_location/2`,
%% which reads the cached manifest through `manifest_replica`. The pure-core cases
%% above do not need it, but a singleton is cheap to keep for the whole suite.
init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(osiris),
    _ = seshat:new_group(rabbitmq_stream_s3),
    {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
    unlink(Pid),
    ok = rabbitmq_stream_s3_log_reader:init_counters(),
    [{manifest_replica, Pid} | Config].

end_per_suite(Config) ->
    catch gen_server:stop(?config(manifest_replica, Config)),
    Config.

%% =========================================================================
%% Soundness property
%% =========================================================================

%% The real decision agrees with the oracle for every lookup outcome.
correct_resolution_is_sound(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun() -> prop_sound(fun ?M:resolve_first_lookup/1) end, [], 5000
    ).

%% The no-silent-skip invariant in isolation: a transient fetch error must never
%% resolve to the local tier, for any error reason.
transient_error_never_resolves_local(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun() -> prop_transient_never_local() end, [], 5000
    ).

%% Falsification: the historical catch-all that collapses a transient error into
%% the local fallback must be caught.
property_catches_collapsed_transient_error(_Config) ->
    CE = proper:counterexample(
        prop_sound(fun resolve_first_lookup_buggy/1), [{numtests, 5000}, quiet]
    ),
    ct:pal("collapsed-transient-error counterexample: ~p", [CE]),
    ?assertNotEqual(true, CE).

%% =========================================================================
%% Deterministic pins
%% =========================================================================

%% A transient group fetch error resolves to retry under the real decision and to
%% the local tier under the buggy catch-all, which the oracle flags.
collapsed_transient_error_is_deterministically_caught(_Config) ->
    Lookup = {error, {group_fetch_failed, slow_down}},
    ?assertEqual(retry, classify(?M:resolve_first_lookup(Lookup))),
    ?assertEqual(local, classify(resolve_first_lookup_buggy(Lookup))).

%% An `ok` lookup builds a remote location addressed at the fragment.
ok_resolves_to_remote_location(_Config) ->
    FragRef = #fragment_ref{offset = 4711, uid = 7, size = 123},
    Iterator = an_iterator,
    {remote, Location} = ?M:resolve_first_lookup({ok, FragRef, Iterator}),
    ?assertEqual(4711, Location#remote_location.chunk_id),
    ?assertEqual(?SEGMENT_HEADER_B, Location#remote_location.position),
    ?assertEqual(FragRef, Location#remote_location.fragment_ref),
    ?assertEqual(Iterator, Location#remote_location.iterator).

%% A genuinely exhausted manifest resolves to the local first offset.
end_of_manifest_resolves_local(_Config) ->
    ?assertEqual({local, first}, ?M:resolve_first_lookup(end_of_manifest)).

%% =========================================================================
%% Routing invariant properties (light Step 2): drive the real
%% log_reader:resolve_remote_location/2 routing decision over generated local
%% floors and manifest shapes, generalizing the point cases in the log_reader
%% eunit tests. These lock the floor/shape guards; the transient-error-never-
%% local invariant is covered by the pure-core cases above.
%% =========================================================================

-define(LR, rabbitmq_stream_s3_log_reader).

%% An offset at or above a populated local floor is served locally, regardless of
%% the remote manifest (the floor branch returns before consulting it).
routing_offset_in_local_tier_resolves_local(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun() -> prop_offset_in_local_tier_local() end, [], 1000
    ).

prop_offset_in_local_tier_local() ->
    ?FORALL(
        {Floor, Delta},
        {range(0, 1_000_000), range(0, 1_000_000)},
        begin
            Shared = osiris_log_shared:new(),
            ok = osiris_log_shared:set_first_chunk_id(Shared, Floor),
            Offset = Floor + Delta,
            Config = #{name => <<"routing-local-tier">>, shared => Shared},
            ?LR:resolve_remote_location(Offset, Config) =:= {local, Offset}
        end
    ).

%% An offset below the local floor on a stream with no cache row at all must
%% fail closed, exactly like an explicitly pending row: tiering is
%% unconditional and plugin-wide, so a missing row is never a positive
%% "un-tiered stream" statement, and falling back to the local tier here would
%% silently skip the remote range below the local floor.
routing_missing_row_below_floor_fails_closed(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun() -> prop_missing_row_below_floor_fails_closed() end, [], 1000
    ).

prop_missing_row_below_floor_fails_closed() ->
    ?FORALL(
        {Floor, BelowDelta},
        {range(1, 1_000_000), range(1, 1_000_000)},
        begin
            Offset = max(0, Floor - BelowDelta),
            StreamId = <<"routing-missing-row-never-put">>,
            Shared = osiris_log_shared:new(),
            ok = osiris_log_shared:set_first_chunk_id(Shared, Floor),
            %% A stream id with no row ever written: get_manifest -> undefined.
            Config = #{name => StreamId, shared => Shared},
            ?LR:resolve_remote_location(Offset, Config) =:=
                {error, {manifest_not_resolved, StreamId}}
        end
    ).

%% An offset below the local floor while the cache row is pending (the plugin is
%% attached but the manifest is not yet resolved or synced) must fail closed
%% with a retryable error. The remote tier's extent is unknown at that moment;
%% falling back to the local tier would silently skip the remote range below
%% the local floor.
routing_pending_below_floor_fails_closed(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun() -> prop_pending_below_floor_fails_closed() end, [], 1000
    ).

prop_pending_below_floor_fails_closed() ->
    ?FORALL(
        {Floor, BelowDelta},
        {range(1, 1_000_000), range(1, 1_000_000)},
        begin
            Offset = max(0, Floor - BelowDelta),
            StreamId = <<"routing-pending-below-floor">>,
            ok = rabbitmq_stream_s3_manifest_replica:mark_pending(StreamId),
            Shared = osiris_log_shared:new(),
            ok = osiris_log_shared:set_first_chunk_id(Shared, Floor),
            Config = #{name => StreamId, shared => Shared},
            ?LR:resolve_remote_location(Offset, Config) =:=
                {error, {manifest_not_resolved, StreamId}}
        end
    ).

%% The 'first' spec while the cache row is pending must fail closed for any
%% local floor, populated or empty: 'first' means the beginning of the stream,
%% and where the stream begins is unknown until the manifest resolves.
routing_pending_spec_first_fails_closed(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun() -> prop_pending_spec_first_fails_closed() end, [], 1000
    ).

prop_pending_spec_first_fails_closed() ->
    ?FORALL(
        Floor,
        oneof([-1, range(0, 1_000_000)]),
        begin
            StreamId = <<"routing-pending-spec-first">>,
            ok = rabbitmq_stream_s3_manifest_replica:mark_pending(StreamId),
            Shared = osiris_log_shared:new(),
            ok = osiris_log_shared:set_first_chunk_id(Shared, Floor),
            Config = #{name => StreamId, shared => Shared},
            ?LR:resolve_remote_location(first, Config) =:=
                {error, {manifest_not_resolved, StreamId}}
        end
    ).

%% resolve_remote_location/2 (exercised above) reports a pending row with the
%% plugin's own internal reason, but init_offset_reader/2 and
%% resolve_offset_spec/2 are the two functions that actually implement the
%% osiris_log_reader callback contract: at that boundary the internal reason
%% must be translated to osiris_log_reader's transient_error/0 contract
%% ({error, unavailable}), not leaked out as a plugin-specific term.
pending_row_reports_unavailable_at_callback_boundary(_Config) ->
    StreamId = <<"pending-callback-boundary">>,
    ok = rabbitmq_stream_s3_manifest_replica:mark_pending(StreamId),
    Shared = osiris_log_shared:new(),
    ok = osiris_log_shared:set_first_chunk_id(Shared, 100),
    Cfg = #{name => StreamId, shared => Shared},
    ?assertEqual({error, unavailable}, ?LR:init_offset_reader(first, Cfg)),
    ?assertEqual({error, unavailable}, ?LR:resolve_offset_spec(first, Cfg)).

%% With the local log empty (first_chunk_id = -1) and a populated remote tier,
%% the beginning and any offset below the remote first resolve to the remote
%% tier, while an offset at or beyond the remote tail waits at the live tail.
routing_empty_local_log_routes_remote(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun() -> prop_empty_local_log_routes_remote() end, [], 1000
    ).

prop_empty_local_log_routes_remote() ->
    ?FORALL(
        {First, Span, BelowDelta, TailDelta},
        {range(1, 1000), range(1, 1000), range(1, 1000), range(0, 1000)},
        begin
            Next = First + Span,
            Below = max(0, First - BelowDelta),
            Tail = Next + TailDelta,
            StreamId = <<"routing-empty-local-log">>,
            Entries = ?ENTRY(First, 1000, 2000, ?MANIFEST_KIND_FRAGMENT, 200, 42),
            Manifest = #manifest{first_offset = First, next_offset = Next, entries = Entries},
            ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest),
            %% Fresh shared atomics: first_chunk_id defaults to -1 (empty local).
            Shared = osiris_log_shared:new(),
            Config = #{name => StreamId, shared => Shared},
            is_remote(?LR:resolve_remote_location(first, Config)) andalso
                is_remote(?LR:resolve_remote_location(Below, Config)) andalso
                ?LR:resolve_remote_location(Tail, Config) =:= {local, next}
        end
    ).

is_remote({ok, #remote_location{}}) -> true;
is_remote(_) -> false.

%% =========================================================================
%% Properties
%% =========================================================================

prop_sound(ResolveFun) ->
    ?FORALL(Lookup, lookup(), classify(ResolveFun(Lookup)) =:= expected_class(Lookup)).

prop_transient_never_local() ->
    ?FORALL(
        Reason,
        group_fetch_reason(),
        classify(?M:resolve_first_lookup({error, {group_fetch_failed, Reason}})) =/= local
    ).

%% =========================================================================
%% Soundness oracle: derives the required tier from the lookup outcome alone,
%% independently of resolve_first_lookup/1.
%% =========================================================================

expected_class({ok, #fragment_ref{}, _Iterator}) -> remote;
expected_class(end_of_manifest) -> local;
expected_class({error, {group_fetch_failed, _}}) -> retry.

classify({remote, #remote_location{}}) -> remote;
classify({local, first}) -> local;
classify({retry, {group_fetch_failed, _}}) -> retry.

%% =========================================================================
%% Faithful reproduction of the historical catch-all this module replaces. The
%% `_ -> {local, first}` clause collapses a transient error and a genuinely empty
%% manifest into the same local fallback; the property must find a counterexample.
%% =========================================================================

resolve_first_lookup_buggy({ok, #fragment_ref{offset = Offset} = FragRef, Iterator}) ->
    {remote, #remote_location{
        chunk_id = Offset,
        position = ?SEGMENT_HEADER_B,
        fragment_ref = FragRef,
        iterator = Iterator
    }};
resolve_first_lookup_buggy(_) ->
    {local, first}.

%% =========================================================================
%% Generators
%% =========================================================================

lookup() ->
    oneof([
        {ok, fragment_ref(), an_iterator},
        end_of_manifest,
        {error, {group_fetch_failed, group_fetch_reason()}}
    ]).

fragment_ref() ->
    ?LET(
        {Offset, Uid, Size},
        {range(0, 1_000_000), range(0, 1000), range(0, 1000)},
        #fragment_ref{offset = Offset, uid = Uid, size = Size}
    ).

group_fetch_reason() ->
    oneof([slow_down, timeout, {http, 503}, econnrefused, enoent]).
