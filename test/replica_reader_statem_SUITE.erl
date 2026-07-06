%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(replica_reader_statem_SUITE).
-moduledoc """
Property-based (proper_statem) model of rabbitmq_stream_s3_replica_reader's
per-stream upload lifecycle, driven against a real gen_server.

replica_reader_SUITE pins each recovery seam with one hand-written example at a
time (`killed_reader_re_registers_and_resumes`, `resumes_after_restart`,
`upload_path_recovers_from_trimmed_segment`, `stale_*_is_ignored`, ...). None of
them *combine* these events within a single run - e.g. publish, force a
fragment cut, kill the reader mid-upload, publish more, trigger retention, kill
again, in a randomized order and count. That combinatorial space is where a
`proper_statem` adds value here; the async-task correlation logic itself
(persist/group/retention/transfer staleness, generations, deliver-vs-drop) is
already exhaustively modelled against the pure
`rabbitmq_stream_s3_replica_reader_tasks` core in `replica_reader_tasks_SUITE`,
and `rabbitmq_stream_s3_replica_reader_core` is pointwise-property-tested in
`prop_SUITE`. This suite does not re-model either; it drives the real
gen_server (real `osiris_writer`, real local Khepri store, the `_api_fs`/
`_api_fault` local-filesystem S3 stand-in) through randomized command
sequences and checks properties that are only observable at that level.

## Commands

- `do_publish/1`: write N records to the real osiris writer and flush (a
  barrier ensuring the writer has processed the casts).
- `do_force_cut/0`: `force_fragment_cut/1`, cutting whatever is in the
  assembly immediately.
- `do_kill/0`: kill the live reader pid outright (untrappable, like an OOM
  kill) and let the per-stream supervisor restart it.
- `do_eval_local_retention/0`, `do_eval_remote_retention/0`:
  `evaluate_local_retention/1`, `evaluate_remote_retention/1`, invoked at
  arbitrary points in the lifecycle (including the window right after a kill,
  before the new incarnation has resolved its manifest) rather than only when
  the reader is known-healthy, as every hand-written call site does.
- `do_trigger_trimmed_segment/0`: reproduces issue #225 deterministically
  (rather than hoping a random interleaving races it) using the same recipe as
  `upload_path_recovers_from_trimmed_segment`: park the next fragment upload
  at `stream_put` (before it reads the local segment) via the fault-injecting
  API backend, write past the retention bound so the parked fragment's segment
  is trimmed, then release. Composed as one atomic command so it can appear
  anywhere in a random sequence - including immediately before or after a
  `do_kill` - without needing to model the parked state in between.

The stream is configured with a real (not synthetic) local retention bound
(`{max_bytes, ...}`) so the local-log-ahead recovery path is reachable both
deliberately (`do_trigger_trimmed_segment`) and organically (a backlog of
`do_publish`/`do_kill` interleavings outrunning the tiering pipeline), not only
as a scripted one-off.

## Properties checked after every command

1. **Range monotonicity.** `get_range/1` (the manifest's `{first, next}`
   range) never regresses across the whole run, including across a
   kill+restart and a local-log-ahead reset. This is what the writer/epoch
   fencing CAS (`p/writer-fencing/README.md`) exists to guarantee is
   externally observable as: the committed offset range this replica reader
   publishes must never move backward.
2. **No permanent stall after a kill.** After killing the reader, the
   per-stream supervisor must restart it under a fresh pid, and the new
   incarnation must eventually (bounded poll) resume tiering up to everything
   published before the kill (`await_offset/3`). A run that stays stalled
   forever is a liveness violation, not merely a slow one.
3. **Local retention never outruns the persisted floor
   (`p/trimmed-segment/README.md`, issue #206/#225).** The local tier's first
   surviving segment offset must never (durably) sit ahead of the persisted
   manifest's `next_offset`. Real user retention can transiently trim ahead of
   an in-flight upload (that is the trimmed-segment race itself); the check is
   a bounded poll for the recovery (reset-to-local-floor) to land, not an
   instantaneous assertion, mirroring the P model's liveness framing
   (`AwaitingResolution` hot state / `TransferEventuallyResolves`). This is
   also what a disabled `local_log_ahead` guard in `handle_transfer_failure`
   breaks: the upload loops forever on a segment that can never come back, so
   this check (and property 2's final convergence check) times out. Verified
   by temporarily forcing that guard's branch to never fire: the property
   failed reliably (first run, `Failed: After 14 test(s)`) with
   `Final convergence (await written=127): {error,timeout}` against a
   sequence ending in `do_trigger_trimmed_segment`, confirming this suite
   catches a regression of issue #225.

Properties 1 and 3 are cheap in the common case (an ETS read and a
`filelib:wildcard`, no polling needed unless a real divergence is in
progress), so checking them after every command does not dominate the
per-iteration cost. Property 2's bounded poll only runs on `do_kill`, plus once
more at the end of each run against the final published count.

## Iteration cost and sizing

Each `?FORALL` iteration starts a real `osiris_writer` (with tiering hooks,
hence a real replica reader), drives it through a random command sequence, and
tears both down (writer stop, remote object deletion, local directory
deletion, manifest cache eviction) plus resets the fault-backend control
table. Measured on this suite's harness (local filesystem `_api_fs`/
`_api_fault`, local Khepri, no network): a `do_kill` (which waits out a real
supervisor restart and manifest re-resolution) costs on the order of tens of
milliseconds when nothing is stalled; `do_trigger_trimmed_segment` costs a bit
more (100 writes plus a deliberate park/release) but is still comfortably
sub-100ms in the non-buggy case. This is much closer to the two hand-written
sibling suites' per-testcase cost than to the near-instant pure-model
`manifest_replica_statem_SUITE`/`api_aws_pool_statem_SUITE`, which drive fake
in-memory backends. `numtests` and the command sequence length are tuned down
accordingly from those suites' defaults (200/150) to keep the whole property
in the same ballpark as the rest of `ct-quick`; see the test case for the
exact numbers and the measurement this was based on.

Each stream uses a fresh, globally-unique stream id per iteration (matching
the hand-written suite's per-testcase-derived ids) rather than reusing one
across iterations: the metadata store (Khepri) is a real durable store for the
whole suite's lifetime (started once by the CT hook, not reset per test), and
reusing a stream id across iterations after the S3 object it references has
been deleted would resolve into the manifest-object-missing retry loop
(Local authority: a missing object at a committed revision is never
misclassified as an empty stream). A small, harmless per-iteration Khepri
container node is therefore left behind, exactly as the existing
`replica_reader_SUITE` already leaves one per test case; the runtime resources
this suite is careful to reclaim every iteration (the writer and reader
processes, local segment files, remote fs objects, the manifest cache row, the
fault-backend control table) are the ones that would otherwise accumulate
across dozens of iterations within a single test run.
""".

-compile([export_all, nowarn_export_all]).

-behaviour(proper_statem).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-define(READER, rabbitmq_stream_s3_replica_reader).
-define(HELPERS, rabbitmq_stream_s3_test_helpers).

-define(STREAM_KEY, {?MODULE, stream_id}).
-define(WRITER_KEY, {?MODULE, writer}).
-define(RANGE_KEY, {?MODULE, last_next_offset}).

%% Bounded-poll budgets. do_kill and the final run-end check both wait out a
%% real supervisor restart plus (possibly) a local-log-ahead reset; the local
%% floor check runs after every command but only polls when a real divergence
%% is in progress (see moduledoc).
-define(KILL_AWAIT_MS, 10_000).
-define(FINAL_AWAIT_MS, 15_000).
-define(LOCAL_FLOOR_AWAIT_MS, 5_000).
-define(POLL_INTERVAL_MS, 20).

%% Real user retention bound so the local-log-ahead recovery path (issue
%% #206/#225) is reachable, not just tiering-fun-safe reclaim. Small segments
%% so the bound is crossed after a modest number of records.
-define(MAX_SEGMENT_BYTES, 2_000).
-define(RETENTION_MAX_BYTES, 20_000).
-define(FRAGMENT_TARGET_SIZE, 1_000).
-define(RECORD, binary:copy(<<"x">>, 200)).

all() ->
    [no_stall_or_regression].

suite() ->
    [{ct_hooks, [rabbitmq_stream_s3_cth]}].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    application:set_env(rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fs),
    catch rabbitmq_stream_s3_api_fault:reset(),
    Config.

%% ------------------------------------------------------------------
%% Test case
%% ------------------------------------------------------------------

no_stall_or_regression(_Config) ->
    %% See moduledoc "Iteration cost and sizing": measured at well under
    %% 100ms/iteration on this harness (real writer+reader, local fs+Khepri),
    %% so 40 iterations of up to ~15 commands stays well within the rest of
    %% ct-quick's budget while covering a meaningfully large combination space.
    rabbit_ct_proper_helpers:run_proper(fun prop_no_stall_or_regression/0, [], 40).

prop_no_stall_or_regression() ->
    ?FORALL(
        Cmds,
        resize(15, commands(?MODULE)),
        begin
            StreamId = fresh_stream_id(),
            put(?STREAM_KEY, StreamId),
            erase(?RANGE_KEY),
            put(?RANGE_KEY, 0),
            ok = rabbitmq_stream_s3_api_fault:setup(),
            application:set_env(
                rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fault
            ),
            Writer = start_fresh_writer(StreamId),
            put(?WRITER_KEY, Writer),
            {History, State, Result} = run_commands(?MODULE, Cmds),
            FinalConverged = await_offset_safe(maps:get(written, State)),
            cleanup(StreamId, Writer),
            ?WHENFAIL(
                io:format(
                    "~n~nFailing command sequence~nHistory: ~p~nState: ~p~nResult: ~p~n"
                    "Final convergence (await written=~p): ~p~n",
                    [History, State, Result, maps:get(written, State), FinalConverged]
                ),
                Result =:= ok andalso FinalConverged =:= ok
            )
        end
    ).

fresh_stream_id() ->
    N = erlang:unique_integer([positive, monotonic]),
    iolist_to_binary(io_lib:format("statem_replica_reader_~b", [N])).

writer_cfg(StreamId) ->
    #{
        name => binary_to_list(StreamId),
        epoch => 1,
        replica_nodes => [],
        leader_node => node(),
        reference => StreamId,
        options => #{},
        max_segment_size_bytes => ?MAX_SEGMENT_BYTES,
        retention => [{max_bytes, ?RETENTION_MAX_BYTES}]
    }.

start_fresh_writer(StreamId) ->
    Cfg = (writer_cfg(StreamId))#{
        remote_config => #{
            persist_threshold => 1,
            fragment_target_size => ?FRAGMENT_TARGET_SIZE
        }
    },
    {ok, Writer} = osiris_writer:start(Cfg),
    ?HELPERS:flush_writer(Writer),
    Writer.

%% Reclaim everything this iteration created: the writer (cascades to the
%% reader via its writer-pid monitor), the remote fs objects, the local
%% segment directory, the manifest cache row, and the fault-backend control
%% table. The Khepri container node for StreamId is intentionally left behind
%% (see moduledoc "Iteration cost and sizing").
cleanup(StreamId, Writer) ->
    Cfg = writer_cfg(StreamId),
    catch osiris_writer:stop(Cfg),
    _ = rabbitmq_stream_s3_reaper:delete_stream(StreamId),
    catch osiris_log:delete_directory(Cfg),
    catch rabbitmq_stream_s3_manifest_replica:forget(StreamId),
    catch rabbitmq_stream_s3_api_fault:reset(),
    catch exit(Writer, kill),
    ok.

%% ------------------------------------------------------------------
%% proper_statem callbacks
%% ------------------------------------------------------------------

initial_state() ->
    #{written => 0, kills => 0}.

%% rabbitmq_stream_s3_stream_sup gives each stream its own restart budget
%% (intensity 5, period 10s - see that module). Exceeding it is a deliberate
%% safety valve (the per-stream supervisor parks and is not restarted by the
%% temporary-child factory), not the liveness bug this suite is after, and a
%% short random command sequence executes far faster than the 10s window, so
%% an unbounded number of do_kill calls would eventually just be exercising
%% that valve instead of the kill/resume path. Cap kills per run comfortably
%% under the budget so every do_kill in a run is exercising genuine
%% kill-then-resume, not supervisor-budget exhaustion.
-define(MAX_KILLS_PER_RUN, 3).

command(S) ->
    Base = [
        {5, {call, ?MODULE, do_publish, [publish_count()]}},
        {2, {call, ?MODULE, do_force_cut, []}},
        {2, {call, ?MODULE, do_eval_local_retention, []}},
        {1, {call, ?MODULE, do_eval_remote_retention, []}},
        {1, {call, ?MODULE, do_trigger_trimmed_segment, []}}
    ],
    KillOpt =
        case maps:get(kills, S) < ?MAX_KILLS_PER_RUN of
            true -> [{2, {call, ?MODULE, do_kill, []}}];
            false -> []
        end,
    frequency(Base ++ KillOpt).

publish_count() -> integer(1, 15).

precondition(_S, _Call) ->
    true.

next_state(S, _V, {call, _, do_publish, [N]}) ->
    S#{written => maps:get(written, S) + N};
next_state(S, _V, {call, _, do_kill, []}) ->
    S#{kills => maps:get(kills, S) + 1};
next_state(S, _V, {call, _, do_trigger_trimmed_segment, []}) ->
    %% Always writes exactly 100 records (see do_trigger_trimmed_segment/0),
    %% deterministically regardless of whether the block was actually hit -
    %% this keeps the model's `written` count independent of that runtime
    %% outcome, which matters because next_state also runs symbolically
    %% during command generation, before any real result exists.
    S#{written => maps:get(written, S) + 100};
next_state(S, _V, _Call) ->
    S.

postcondition(_S, {call, _, do_publish, [_N]}, Res) ->
    Res =:= ok andalso invariants_hold();
postcondition(_S, {call, _, do_force_cut, []}, Res) ->
    lists:member(Res, [
        ok, {error, no_assembly}, {error, empty_assembly}, {error, {not_found, stream_id()}}
    ]) andalso invariants_hold();
postcondition(_S, {call, _, do_eval_local_retention, []}, Res) ->
    %% evaluate_local_retention/1 falls back to
    %% manifest_replica:evaluate_local_retention/1 when the reader is not
    %% currently registered (e.g. the window right after a do_kill, before
    %% the new incarnation re-registers). That fallback is written for a
    %% replica node with a registered context; on this writer-only harness no
    %% context is ever registered, so the fallback's answer is always
    %% {error, {not_found, StreamId}} rather than {error,
    %% manifest_not_resolved} (which instead comes from the live reader
    %% itself, before its manifest has resolved).
    lists:member(Res, [
        ok, {error, manifest_not_resolved}, {error, {not_found, stream_id()}}
    ]) andalso invariants_hold();
postcondition(_S, {call, _, do_eval_remote_retention, []}, Res) ->
    valid_remote_retention_result(Res) andalso invariants_hold();
postcondition(_S, {call, _, do_trigger_trimmed_segment, []}, Res) ->
    Res =:= ok andalso invariants_hold();
postcondition(S, {call, _, do_kill, []}, Res) ->
    kill_ok(S, Res) andalso invariants_hold().

valid_remote_retention_result(ok) -> true;
valid_remote_retention_result({error, manifest_not_resolved}) -> true;
valid_remote_retention_result({error, {in_progress, _}}) -> true;
valid_remote_retention_result({error, {not_found, _}}) -> true;
valid_remote_retention_result(_) -> false.

%% Property 2: a killed reader is restarted by the per-stream supervisor under
%% a fresh pid and eventually resumes tiering up to everything published
%% before the kill. no_pid means the previous command already killed it and
%% the supervisor had not yet re-registered a replacement - nothing to check.
kill_ok(_S, no_pid) ->
    true;
kill_ok(S, OldPid) when is_pid(OldPid) ->
    Written = maps:get(written, S),
    ok =:=
        await(fun() ->
            case rabbitmq_stream_s3_registry:whereis_name({stream_id(), node()}) of
                undefined -> false;
                OldPid -> false;
                _NewPid -> true
            end
        end) andalso
        ok =:= await_offset_safe(Written).

%% ------------------------------------------------------------------
%% Global invariants (checked after every command)
%% ------------------------------------------------------------------

invariants_hold() ->
    range_monotonic() andalso local_floor_within_bound().

%% Property 1: get_range/1's {first, next} range never regresses.
range_monotonic() ->
    case rabbitmq_stream_s3_manifest_replica:get_range(stream_id()) of
        empty ->
            true;
        {_First, Next} ->
            Prev = get(?RANGE_KEY),
            put(?RANGE_KEY, max(Next, Prev)),
            Next >= Prev
    end.

%% Property 3: local retention must never durably leave the local tier's first
%% surviving segment ahead of the persisted manifest's next_offset. A real
%% trim-ahead race (do_trigger_trimmed_segment, or an organic backlog) is
%% legal transiently; the bounded poll is for the recovery reset to land, not
%% an instantaneous assertion. Times out (fails) if local_log_ahead recovery
%% is broken and the pipeline is wedged retrying a permanently-gone segment.
local_floor_within_bound() ->
    ok =:=
        await(
            fun() ->
                case rabbitmq_stream_s3_manifest_replica:get_range(stream_id()) of
                    empty ->
                        true;
                    {_First, Next} ->
                        case local_floor() of
                            undefined -> true;
                            LocalFloor -> LocalFloor =< Next
                        end
                end
            end,
            ?LOCAL_FLOOR_AWAIT_MS
        ).

local_floor() ->
    {ok, Dir} = application:get_env(osiris, data_dir),
    Pattern = filename:join([Dir, binary_to_list(stream_id()), "*.segment"]),
    case filelib:wildcard(Pattern) of
        [] ->
            undefined;
        Files ->
            lists:min([rabbitmq_stream_s3:segment_file_offset(list_to_binary(F)) || F <- Files])
    end.

%% ------------------------------------------------------------------
%% Command implementations (real calls against the running gen_server)
%% ------------------------------------------------------------------

do_publish(N) ->
    Writer = writer(),
    [osiris_writer:write(Writer, ?RECORD) || _ <- lists:seq(1, N)],
    ?HELPERS:flush_writer(Writer).

do_force_cut() ->
    ?READER:force_fragment_cut(stream_id()).

do_eval_local_retention() ->
    ?READER:evaluate_local_retention(stream_id()).

do_eval_remote_retention() ->
    ?READER:evaluate_remote_retention(stream_id()).

do_kill() ->
    case rabbitmq_stream_s3_registry:whereis_name({stream_id(), node()}) of
        undefined ->
            no_pid;
        Pid ->
            Ref = monitor(process, Pid),
            true = exit(Pid, kill),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 -> ok
            end,
            Pid
    end.

%% Reproduces issue #225 (the trimmed-segment upload race) deterministically:
%% park the next fragment upload at stream_put (before it reads the local
%% segment), write past the retention bound so the parked fragment's segment
%% is trimmed, then release. See upload_path_recovers_from_trimmed_segment in
%% replica_reader_SUITE for the scenario this generalizes into a composable
%% statem command. Always writes exactly 100 records regardless of whether the
%% block was hit, so the model's `written` count stays deterministic (see
%% next_state/3).
do_trigger_trimmed_segment() ->
    StreamId = stream_id(),
    Writer = writer(),
    WriteN = fun(N) ->
        [osiris_writer:write(Writer, ?RECORD) || _ <- lists:seq(1, N)],
        ?HELPERS:flush_writer(Writer)
    end,
    Ref = rabbitmq_stream_s3_api_fault:block_once(stream_put, StreamId),
    WriteN(20),
    case await_blocked_safe(Ref, 5000) of
        {ok, TaskPid} ->
            WriteN(80),
            ok = rabbitmq_stream_s3_api_fault:release(TaskPid, Ref);
        not_hit ->
            %% Nothing parked within the wait (e.g. everything was already
            %% tiered when this command ran). Disarm explicitly rather than
            %% leaving a one-shot block that could later park a task with
            %% nobody left to release it.
            ok = rabbitmq_stream_s3_api_fault:reset(),
            WriteN(80),
            %% Close a TOCTOU race found by this suite's own repeated-run
            %% validation: the upload task can reach maybe_block (delete the
            %% ets entry, committing to park, and send us
            %% {fault_blocked, Ref, TaskPid}) fractionally after our await
            %% above times out but before the reset/0 call lands. reset/0
            %% does not affect a task that already parked - it is only
            %% waiting on this exact {fault_release, Ref} message now, and if
            %% we do not send it, the task is stuck forever (an unmonitored
            %% governor task; nothing but its own 180s transfer deadline
            %% would ever notice, and even that never resubmits *this* task,
            %% only the fragment's Ref via a fresh one). Drain the mailbox for
            %% a late-arriving notification and release it too.
            drain_late_block(Ref)
    end,
    ok.

await_blocked_safe(Ref, Timeout) ->
    try rabbitmq_stream_s3_api_fault:await_blocked(Ref, Timeout) of
        TaskPid -> {ok, TaskPid}
    catch
        error:{fault_block_not_hit, Ref} -> not_hit
    end.

drain_late_block(Ref) ->
    receive
        {fault_blocked, Ref, TaskPid} ->
            ok = rabbitmq_stream_s3_api_fault:release(TaskPid, Ref)
    after 500 ->
        ok
    end.

%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------

stream_id() -> get(?STREAM_KEY).
writer() -> get(?WRITER_KEY).

await_offset_safe(Offset) ->
    await_offset_safe(Offset, ?FINAL_AWAIT_MS).

await_offset_safe(_Offset, Remaining) when Remaining =< 0 ->
    {error, timeout};
await_offset_safe(Offset, Remaining) ->
    %% Nudge a cut of whatever is sitting in the assembly on every poll. A
    %% fragment only cuts automatically once its accumulated bytes cross
    %% fragment_target_size (see docs/upload-path.md, "Non-guarantee": the
    %% plugin never promises a timely upload of an under-threshold tail), so a
    %% short publish_count() batch can otherwise leave data durably
    %% un-uploaded forever without an explicit (operator/CLI-equivalent)
    %% force_fragment_cut - exactly what that call exists for. A no-op
    %% ({error, empty_assembly} or {error, no_assembly}) is expected and
    %% harmless once there is nothing left to flush.
    _ = catch ?READER:force_fragment_cut(stream_id()),
    try
        ok = ?HELPERS:await_offset(stream_id(), Offset, 200),
        ok
    catch
        _:_ ->
            timer:sleep(?POLL_INTERVAL_MS),
            await_offset_safe(Offset, Remaining - 220)
    end.

await(Fun) ->
    await(Fun, ?KILL_AWAIT_MS).

await(_Fun, Remaining) when Remaining =< 0 ->
    {error, timeout};
await(Fun, Remaining) ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(?POLL_INTERVAL_MS),
            await(Fun, Remaining - ?POLL_INTERVAL_MS)
    end.
