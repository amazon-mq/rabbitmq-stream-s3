%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(manifest_replica_statem_SUITE).
-moduledoc """
Property-based (proper_statem) model of rabbitmq_stream_s3_manifest_replica's
per-stream state machine.

manifest_replica_SUITE pins the no-context-leak invariant with a handful of
hand-written examples (sync_without_context_dropped,
apply_edits_without_context_dropped, ...). This suite instead models the real
gen_server's abstract per-stream state -- whether a reader context is
registered, the recorded {seq, epoch, writer_node}, and whether a cache row
exists -- and drives the real running process with randomized interleavings of
register_replica_context/5, killing the pid backing a context (to trigger the
real 'DOWN' handling), sync/4, apply_edits/4, and forget/1.

The postconditions are the properties the no-context fix (see git history for
rabbitmq_stream_s3_manifest_replica.erl) exists to guarantee:

1. A stream with no registered context is a no-op target for sync and
   apply_edits: get_manifest/1 and get_manifest_and_epoch/1 never change.
2. A registered context that receives syncs/edits in a legal seq/epoch order
   has its cache reflect them exactly.
3. An edit whose seq does not match last_seq + 1 never corrupts the cached
   manifest -- the cache is left exactly as it was.
4. Killing the pid backing a context eventually (the release is asynchronous,
   via the member monitor) makes is_context_registered/1 false and
   get_manifest/1 undefined.
""".

-compile([export_all, nowarn_export_all]).

-behaviour(proper_statem).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("rabbitmq_stream_s3/include/rabbitmq_stream_s3.hrl").

-define(REPLICA, rabbitmq_stream_s3_manifest_replica).

all() ->
    [no_leak_or_corruption].

init_per_suite(Config) -> Config.
end_per_suite(Config) -> Config.

%% ------------------------------------------------------------------
%% Test case
%% ------------------------------------------------------------------

no_leak_or_corruption(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_no_leak_or_corruption/0, [], 200).

prop_no_leak_or_corruption() ->
    ?FORALL(
        Cmds,
        commands(?MODULE),
        begin
            {ok, Pid} = ?REPLICA:start_link(),
            unlink(Pid),
            {History, State, Result} = run_commands(?MODULE, Cmds),
            cleanup_member_pids(State),
            gen_server:stop(Pid),
            ?WHENFAIL(
                io:format(
                    "~n~nFailing command sequence~nHistory: ~p~nState: ~p~nResult: ~p~n",
                    [History, State, Result]
                ),
                Result =:= ok
            )
        end
    ).

%% Reap the dummy member pids the model registered so they don't leak past
%% this ?FORALL iteration; they only block on `receive stop -> ok end`, so
%% nothing but a monitor's DOWN or an explicit kill ever ends them.
cleanup_member_pids(State) ->
    maps:foreach(
        fun(_StreamId, SM) ->
            case maps:get(pid, SM) of
                undefined -> ok;
                P when is_pid(P) -> catch exit(P, kill)
            end
        end,
        State
    ).

%% ------------------------------------------------------------------
%% proper_statem callbacks
%% ------------------------------------------------------------------

stream_ids() ->
    [<<"statem-stream-a">>, <<"statem-stream-b">>, <<"statem-stream-c">>].

fresh_stream() ->
    #{registered => false, pid => undefined, seq_epoch => undefined, manifest => undefined}.

initial_state() ->
    #{Id => fresh_stream() || Id <- stream_ids()}.

command(S) ->
    ?LET(StreamId, elements(stream_ids()), command_for_stream(StreamId, maps:get(StreamId, S))).

command_for_stream(StreamId, SM) ->
    oneof([
        {call, ?MODULE, do_register, [StreamId]},
        {call, ?MODULE, do_kill, [StreamId, maps:get(pid, SM)]},
        {call, ?MODULE, do_forget, [StreamId]},
        ?LET(
            {Seq, Epoch},
            gen_seq_epoch(SM),
            {call, ?MODULE, do_sync, [StreamId, Seq, Epoch]}
        ),
        ?LET(
            {Seq, Epoch},
            gen_seq_epoch(SM),
            {call, ?MODULE, do_apply_edits, [
                StreamId, build_append_edit(maps:get(manifest, SM), Seq), Seq, Epoch
            ]}
        )
    ]).

%% Bias seq/epoch generation toward the cases that matter: the legal
%% next-in-sequence pair (exercises property 2), a fresh epoch bump as a
%% reconnecting writer after an election would send (also legal), and
%% uniform noise (exercises the no-context and gap/corruption properties, 1
%% and 3).
gen_seq_epoch(SM) ->
    Recorded = maps:get(seq_epoch, SM),
    ?LET(
        {Choice, RandSeq, RandEpoch},
        {frequency([{3, legal}, {1, bump}, {2, random}]), integer(0, 10), integer(0, 5)},
        case Choice of
            legal -> legal_next(Recorded);
            bump -> epoch_bump(Recorded);
            random -> {RandSeq, RandEpoch}
        end
    ).

legal_next(undefined) -> {0, 1};
legal_next({LastSeq, LastEpoch, _}) -> {LastSeq + 1, LastEpoch}.

epoch_bump(undefined) -> {0, 1};
epoch_bump({_LastSeq, LastEpoch, _}) -> {0, LastEpoch + 1}.

precondition(_S, _Call) ->
    true.

next_state(S, V, {call, _, do_register, [StreamId]}) ->
    SM = maps:get(StreamId, S),
    S#{StreamId => SM#{registered => true, pid => V}};
next_state(S, _V, {call, _, do_kill, [StreamId, _Pid]}) ->
    SM = maps:get(StreamId, S),
    case maps:get(pid, SM) of
        %% No live pid backs this stream's context: killing is a no-op.
        undefined -> S;
        %% release_stream/2 (the real DOWN handler) drops the context, the
        %% seqs entry, and the cached row together.
        _ -> S#{StreamId => fresh_stream()}
    end;
next_state(S, _V, {call, _, do_forget, [StreamId]}) ->
    %% forget/1 releases the same trio as a member DOWN, regardless of
    %% whether a context was registered.
    S#{StreamId => fresh_stream()};
next_state(S, _V, {call, _, do_sync, [StreamId, Seq, Epoch]}) ->
    SM = maps:get(StreamId, S),
    case maps:get(registered, SM) of
        false ->
            %% No context: dropped. Cache state must not change (property 1).
            S;
        true ->
            Recorded = maps:get(seq_epoch, SM),
            case ?REPLICA:is_stale_sync(Epoch, Seq, Recorded) of
                true ->
                    S;
                false ->
                    NewManifest = build_sync_manifest(Seq),
                    S#{
                        StreamId => SM#{
                            seq_epoch => {Seq, Epoch, node()}, manifest => NewManifest
                        }
                    }
            end
    end;
next_state(S, _V, {call, _, do_apply_edits, [StreamId, Edit, Seq, Epoch]}) ->
    SM = maps:get(StreamId, S),
    case maps:get(registered, SM) of
        false ->
            %% No context: dropped. Cache state must not change (property 1).
            S;
        true ->
            case maps:get(seq_epoch, SM) of
                {LastSeq, Epoch, _} when Seq =:= LastSeq + 1 ->
                    Base = manifest_or_default(SM),
                    NewManifest = rabbitmq_stream_s3_manifest:apply_edit(Edit, Base),
                    S#{
                        StreamId => SM#{
                            seq_epoch => {Seq, Epoch, node()}, manifest => NewManifest
                        }
                    };
                _ ->
                    %% Gap or epoch mismatch: must never corrupt the cache
                    %% (property 3). The manifest is left exactly as-is.
                    S
            end
    end.

postcondition(_S, {call, _, do_register, [StreamId]}, Pid) ->
    is_pid(Pid) andalso ?REPLICA:is_context_registered(StreamId);
postcondition(S, {call, _, do_kill, [StreamId, _Pid]}, _Res) ->
    SM = maps:get(StreamId, S),
    case maps:get(pid, SM) of
        undefined ->
            true;
        _ ->
            %% Property 4: the release is asynchronous (via the member
            %% monitor's DOWN), so poll rather than assert immediately.
            ok =:= await(fun() -> not ?REPLICA:is_context_registered(StreamId) end) andalso
                ok =:= await(fun() -> ?REPLICA:get_manifest(StreamId) =:= undefined end)
    end;
postcondition(_S, {call, _, do_forget, [StreamId]}, Res) ->
    Res =:= ok andalso
        not ?REPLICA:is_context_registered(StreamId) andalso
        ?REPLICA:get_manifest(StreamId) =:= undefined;
postcondition(S, {call, _, do_sync, [StreamId, Seq, Epoch]}, Res) ->
    SM = maps:get(StreamId, S),
    Res =:= ok andalso
        case maps:get(registered, SM) of
            false ->
                %% Property 1: no leaked row for a stream with no context.
                ?REPLICA:get_manifest(StreamId) =:= undefined andalso
                    ?REPLICA:get_manifest_and_epoch(StreamId) =:= undefined;
            true ->
                Recorded = maps:get(seq_epoch, SM),
                case ?REPLICA:is_stale_sync(Epoch, Seq, Recorded) of
                    true ->
                        %% Stale: dropped, cache unchanged.
                        ?REPLICA:get_manifest(StreamId) =:= maps:get(manifest, SM) andalso
                            ?REPLICA:get_manifest_and_epoch(StreamId) =:=
                                expected_manifest_and_epoch(SM);
                    false ->
                        %% Property 2: applied, cache reflects it exactly.
                        Expected = build_sync_manifest(Seq),
                        ?REPLICA:get_manifest(StreamId) =:= Expected andalso
                            ?REPLICA:get_manifest_and_epoch(StreamId) =:= {Expected, Epoch}
                end
        end;
postcondition(S, {call, _, do_apply_edits, [StreamId, Edit, Seq, Epoch]}, Res) ->
    SM = maps:get(StreamId, S),
    case maps:get(registered, SM) of
        false ->
            %% Property 1: no leaked row/seq for a stream with no context.
            %% write_manifest/3 stores the manifest and epoch together, so
            %% both accessors must be untouched, not just get_manifest/1.
            Res =:= {error, no_context} andalso
                ?REPLICA:get_manifest(StreamId) =:= undefined andalso
                ?REPLICA:get_manifest_and_epoch(StreamId) =:= expected_manifest_and_epoch(SM);
        true ->
            case maps:get(seq_epoch, SM) of
                {LastSeq, Epoch, _} when Seq =:= LastSeq + 1 ->
                    %% Property 2: in-sequence edit applied, cache reflects it.
                    Expected = rabbitmq_stream_s3_manifest:apply_edit(
                        Edit, manifest_or_default(SM)
                    ),
                    Res =:= ok andalso
                        ?REPLICA:get_manifest(StreamId) =:= Expected andalso
                        ?REPLICA:get_manifest_and_epoch(StreamId) =:= {Expected, Epoch};
                _ ->
                    %% Property 3: gap/epoch mismatch never corrupts the cache.
                    Res =:= {error, gap} andalso
                        ?REPLICA:get_manifest(StreamId) =:= maps:get(manifest, SM) andalso
                        ?REPLICA:get_manifest_and_epoch(StreamId) =:=
                            expected_manifest_and_epoch(SM)
            end
    end.

manifest_or_default(SM) ->
    case maps:get(manifest, SM) of
        undefined -> #manifest{};
        M -> M
    end.

%% The {Manifest, Epoch} pair get_manifest_and_epoch/1 must report for a
%% stream the model has not (yet) recorded a fresh accepted sync/edit for:
%% undefined if nothing was ever cached, otherwise the last cached manifest
%% paired with the epoch it was written at (write_manifest/3 always stores
%% both together).
expected_manifest_and_epoch(#{manifest := undefined}) ->
    undefined;
expected_manifest_and_epoch(#{manifest := M, seq_epoch := {_Seq, Epoch, _}}) ->
    {M, Epoch}.

%% ------------------------------------------------------------------
%% Command implementations (real calls against the running gen_server)
%% ------------------------------------------------------------------

do_register(StreamId) ->
    Shared = atomics:new(1, []),
    Counter = counters:new(5, []),
    %% A fresh dummy pid backs the context; it is monitored by the real
    %% gen_server and reaped either by do_kill or by cleanup_member_pids/1
    %% at the end of this ?FORALL iteration.
    Pid = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    ok = ?REPLICA:register_replica_context(StreamId, Pid, <<"/tmp/statem">>, Shared, Counter),
    Pid.

do_kill(_StreamId, undefined) ->
    no_pid;
do_kill(_StreamId, Pid) ->
    Ref = monitor(process, Pid),
    exit(Pid, kill),
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after 2000 -> timeout
    end.

do_sync(StreamId, Seq, Epoch) ->
    ?REPLICA:sync(StreamId, Seq, Epoch, build_sync_manifest(Seq)).

do_apply_edits(StreamId, Edit, Seq, Epoch) ->
    ?REPLICA:apply_edits(StreamId, [Edit], Seq, Epoch).

do_forget(StreamId) ->
    ?REPLICA:forget(StreamId).

%% ------------------------------------------------------------------
%% Deterministic manifest/edit builders (pure functions of Seq, so the model
%% and postconditions can recompute the expected value independently)
%% ------------------------------------------------------------------

%% A full-reset manifest whose content is a deterministic function of Seq, so
%% two syncs at different Seq are always structurally distinguishable.
build_sync_manifest(Seq) ->
    {Manifest, _GetGroupFun} = rabbitmq_stream_s3_test_helpers:build_manifest([
        {fragment, #{offset => 0, size => 1000 + Seq, first_ts => Seq * 1000, uid => Seq}}
    ]),
    Manifest.

%% An append edit for a new fragment onto the end of Base (defaulting to an
%% empty manifest), deterministic in Seq. Built from the model's own tracked
%% manifest, so when a legal seq/epoch is generated the edit is structurally
%% valid (its append position matches what the real cached row will hold).
build_append_edit(MaybeManifest, Seq) ->
    Base =
        case MaybeManifest of
            undefined -> #manifest{};
            M -> M
        end,
    #manifest{next_offset = NextOffset, entries = Entries} = Base,
    Ts = Seq * 1000,
    LastTs = Ts + 100,
    Size = 1000 + Seq,
    Entry = ?ENTRY(NextOffset, Ts, LastTs, ?MANIFEST_KIND_FRAGMENT, Size, Seq),
    %% The first append into an empty manifest (next_offset =:= 0) also
    %% establishes the floor fields; a later append preserves them.
    {FirstOffset, FirstTs, FirstLastTs} =
        case NextOffset of
            0 ->
                {NextOffset, Ts, LastTs};
            _ ->
                {
                    Base#manifest.first_offset,
                    Base#manifest.first_timestamp,
                    Base#manifest.first_last_timestamp
                }
        end,
    #edit{
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        next_offset = NextOffset + 1,
        size = Size,
        entries = Entry,
        pos = byte_size(Entries),
        len = 0
    }.

%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------

await(Fun) ->
    await(Fun, 2000).

await(_Fun, Remaining) when Remaining =< 0 ->
    {error, timeout};
await(Fun, Remaining) ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(20),
            await(Fun, Remaining - 20)
    end.
