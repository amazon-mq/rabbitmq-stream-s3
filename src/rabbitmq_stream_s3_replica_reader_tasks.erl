%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_replica_reader_tasks).
-moduledoc """
A pure, total state machine for the replica reader's async-task lifecycle.

The replica reader spawns four families of async tasks - persist (manifest
commit), group upload, remote retention evaluation, and fragment transfer - and
reacts to their results, timeouts and crashes. Historically this logic lived
inline across the gen_server's `handle_info` clauses, each family correlating
results to the live task with its own ad-hoc scheme (a monitor field, a
make_ref/0 token, map membership). Every recovery-seam bug we have fixed was a
missing or mis-coordinated case in that implicit, untyped machine.

This module makes that machine explicit, pure and total so its correctness is a
property we can falsify rather than a set of guards we hand-verify. The
gen_server's role shrinks to translating real messages into `event/0`s, running
`step/2`, and carrying out the returned `decision/0`s as I/O.

## Staleness by construction

The three single-in-flight families (persist, group, retention) each occupy a
`slot/1`: `idle | {in_flight, Generation, Data}`. The slot carries the
`generation/0` it was spawned in. A result tagged with a generation is applied
only when it matches the slot it claims to complete:

```
step({persist_result, G, R}, #tasks{persist = {in_flight, G, _}} = T) -> deliver;
step({persist_result, _G, _R}, T)                                     -> drop.
```

That single pattern subsumes two separate guards the previous design had to
coordinate by hand:

- a result that races its own timeout is dropped, because the timeout already
  set the slot to `idle` and `idle` does not match `{in_flight, G, _}`;
- a result from before a recovery is dropped, because `recover` bumped the
  generation and any new task in the slot carries the new one, so the stale
  result's generation does not match.

Transfers are different on purpose: many run at once, each identified by a
unique caller-minted `Ref` held in a map. A `Ref` is per-task identity that
needs no generation - `recover` clears the map, so a stale `Ref` is simply
absent. This is why transfers are not generation-tagged.

## Task crashes are failure results

A persist/group/retention task is monitored; its crash arrives as a `'DOWN'`
message. The gen_server's `'DOWN'` handler for each family does exactly what its
error-result handler does (the same core failure call, the same slot teardown,
the same metric). `demonitor(_, [flush])` on the result path removes the queued
`'DOWN'`, so a result and a crash are mutually exclusive; a `'DOWN'` from before
a recovery is flushed by `reset_for_recovery`. A crash is therefore just "a
failure of the live task", already modelled by a failure `*_result` at the
slot's generation: the slot's occupancy gives the same mutual exclusion that
`demonitor`/flush gives in the shell. The model has one failure transition per
family rather than two delivery channels for the same outcome.

## Derived gauges

The pipeline gauges are pure functions of this state, not independently mutated
counters (the historical source of gauge drift on task-kill and restart paths):

- `transfers_in_flight/1 = map_size(transfers)`
- `bytes_in_transfer/1   = sum of the outstanding transfer sizes`
- `bytes_in_persist/1    = persist_pending_bytes + the in-flight persist snapshot`

`bytes_in_persist` tracks bytes that have been transferred to S3 but are not yet
durable in a committed manifest. A transfer success moves its bytes into
`persist_pending_bytes`; `spawn_persist` snapshots the pending bytes into the
persist slot (the bytes this commit will cover) and zeroes pending; a successful
persist drops the snapshot; a failed persist returns the snapshot to pending so
the next persist covers them. The gauge is therefore conserved across
`spawn_persist` and persist failure, and decreases only on persist success.
""".

-export([
    init/0,
    generation/1,
    persist_slot/1,
    group_slot/1,
    retention_slot/1,
    transfers/1,
    persist_pending_bytes/1,
    transfers_in_flight/1,
    bytes_in_transfer/1,
    bytes_in_persist/1,
    step/2
]).

-export_type([tasks/0, event/0, decision/0, generation/0]).

-type generation() :: non_neg_integer().

%% A slot for a family that has at most one task in flight at a time. It carries
%% the generation the task was spawned in, so a result claiming to complete it
%% is matched on both occupancy (idle vs in_flight) and identity (generation) by
%% one pattern.
-type slot(Data) :: idle | {in_flight, generation(), Data}.

-record(tasks, {
    %% The current incarnation. Bumped by recover/0; tasks spawned afterwards
    %% carry the new generation, so a result from before the recovery cannot
    %% match the slot a new task now occupies.
    generation = 0 :: generation(),
    %% Data is the snapshot of persist_pending_bytes taken at spawn_persist:
    %% the bytes this commit will make durable. Returned to persist_pending_bytes
    %% on failure, dropped on success.
    persist = idle :: slot(non_neg_integer()),
    %% Data is the group kind being uploaded.
    group = idle :: slot(term()),
    %% No per-task data is needed to correlate a retention result.
    retention = idle :: slot(undefined),
    %% Transfers are keyed by a unique caller-minted Ref rather than a slot,
    %% because many run concurrently. Data is {Size, DeadlineToken}: the byte
    %% size (for the gauges) and the token of the currently-armed liveness
    %% deadline. The token is re-minted on every (re)submit so a fired-but-queued
    %% deadline message for a previous arming of the same Ref is rejected.
    transfers = #{} :: #{term() => {non_neg_integer(), term()}},
    %% Bytes transferred to S3 but not yet snapshotted into an in-flight persist.
    %% Accumulated by transfer successes, drained into the persist slot at
    %% spawn_persist, replenished from the slot when a persist fails.
    persist_pending_bytes = 0 :: non_neg_integer()
}).

-opaque tasks() :: #tasks{}.

%% Inputs to the machine. Spawn events record that the shell has started a task;
%% the shell stamps the spawned task's result with generation/1 at that moment.
%% Result/timeout events arrive later carrying the generation (or Ref/Token) the
%% task captured at spawn.
-type event() ::
    spawn_persist
    | {spawn_group, Kind :: term()}
    | spawn_retention
    | {spawn_transfer, Ref :: term(), Size :: non_neg_integer(), Token :: term()}
    | {persist_result, generation(), Result :: term()}
    | {group_result, generation(), Result :: term()}
    | {retention_result, generation(), Result :: term()}
    | {retention_timeout, generation()}
    | {transfer_result, Ref :: term(), Result :: term()}
    | {transfer_deadline, Ref :: term(), Token :: term()}
    | {retry_transfer, Ref :: term()}
    | recover.

%% Outputs of the machine: deliver a completion to the pure core, re-submit a
%% transfer upload, or drop a stale message. The shell turns a `deliver` into
%% the matching core call and a `resubmit` into the upload I/O.
-type completion() ::
    persist_complete
    | persist_failed
    | group_complete
    | group_failed
    | retention_complete
    | retention_failed
    | transfer_complete
    | transfer_failed.

-type decision() ::
    {deliver, completion(), Payload :: term()}
    | {resubmit, Ref :: term()}
    | {drop, Reason :: atom()}.

%%----------------------------------------------------------------------------
%% Accessors (the wiring layer and the property observer read state through
%% these rather than the record, keeping the representation private). The gauge
%% accessors derive their values rather than reading a stored counter.
%%----------------------------------------------------------------------------

-spec init() -> tasks().
init() -> #tasks{}.

-spec generation(tasks()) -> generation().
generation(#tasks{generation = G}) -> G.

-spec persist_slot(tasks()) -> slot(non_neg_integer()).
persist_slot(#tasks{persist = S}) -> S.

-spec group_slot(tasks()) -> slot(term()).
group_slot(#tasks{group = S}) -> S.

-spec retention_slot(tasks()) -> slot(undefined).
retention_slot(#tasks{retention = S}) -> S.

-spec transfers(tasks()) -> #{term() => {non_neg_integer(), term()}}.
transfers(#tasks{transfers = T}) -> T.

-spec persist_pending_bytes(tasks()) -> non_neg_integer().
persist_pending_bytes(#tasks{persist_pending_bytes = B}) -> B.

-spec transfers_in_flight(tasks()) -> non_neg_integer().
transfers_in_flight(#tasks{transfers = T}) -> map_size(T).

-spec bytes_in_transfer(tasks()) -> non_neg_integer().
bytes_in_transfer(#tasks{transfers = T}) ->
    maps:fold(fun(_Ref, {Size, _Token}, Acc) -> Acc + Size end, 0, T).

-spec bytes_in_persist(tasks()) -> non_neg_integer().
bytes_in_persist(#tasks{persist = Persist, persist_pending_bytes = Pending}) ->
    Pending + persisting_bytes(Persist).

persisting_bytes({in_flight, _G, Bytes}) -> Bytes;
persisting_bytes(idle) -> 0.

%%----------------------------------------------------------------------------
%% The state machine.
%%----------------------------------------------------------------------------

-spec step(event(), tasks()) -> {tasks(), [decision()]}.

%% --- spawns: record the new task at the current generation ---
%% spawn_persist snapshots the pending bytes (the bytes this commit will cover)
%% into the slot and zeroes pending; bytes_in_persist is unchanged.
step(spawn_persist, #tasks{generation = G, persist_pending_bytes = Pending} = T) ->
    {T#tasks{persist = {in_flight, G, Pending}, persist_pending_bytes = 0}, []};
step({spawn_group, Kind}, #tasks{generation = G} = T) ->
    {T#tasks{group = {in_flight, G, Kind}}, []};
step(spawn_retention, #tasks{generation = G} = T) ->
    {T#tasks{retention = {in_flight, G, undefined}}, []};
step({spawn_transfer, Ref, Size, Token}, #tasks{transfers = Tr} = T) ->
    {T#tasks{transfers = Tr#{Ref => {Size, Token}}}, []};
%% --- persist result: apply only to the matching in-flight persist ---
%% On success the snapshot is dropped; on failure it returns to pending so the
%% next persist covers those bytes (bytes_in_persist unchanged on failure).
step({persist_result, G, {ok, _} = Result}, #tasks{persist = {in_flight, G, _Bytes}} = T) ->
    {T#tasks{persist = idle}, [classify_persist(Result)]};
step(
    {persist_result, G, Result},
    #tasks{persist = {in_flight, G, Bytes}, persist_pending_bytes = Pending} = T
) ->
    {T#tasks{persist = idle, persist_pending_bytes = Pending + Bytes}, [classify_persist(Result)]};
step({persist_result, _G, _Result}, T) ->
    {T, [{drop, stale_persist_result}]};
%% --- group result ---
step({group_result, G, Result}, #tasks{group = {in_flight, G, Kind}} = T) ->
    {T#tasks{group = idle}, [classify_group(Kind, Result)]};
step({group_result, _G, _Result}, T) ->
    {T, [{drop, stale_group_result}]};
%% --- retention result ---
step({retention_result, G, Result}, #tasks{retention = {in_flight, G, _}} = T) ->
    {T#tasks{retention = idle}, [classify_retention(Result)]};
step({retention_result, _G, _Result}, T) ->
    {T, [{drop, stale_retention_result}]};
%% --- retention timeout: fail only the matching in-flight task ---
step({retention_timeout, G}, #tasks{retention = {in_flight, G, _}} = T) ->
    {T#tasks{retention = idle}, [{deliver, retention_failed, timeout}]};
step({retention_timeout, _G}, T) ->
    {T, [{drop, stale_retention_timeout}]};
%% --- transfer result: keyed by Ref membership, not a slot ---
step({transfer_result, Ref, Result}, #tasks{transfers = Tr} = T) when is_map_key(Ref, Tr) ->
    {Size, _Token} = maps:get(Ref, Tr),
    {Tasks, Decision} = classify_transfer(Ref, Size, Result, T),
    {Tasks#tasks{transfers = maps:remove(Ref, Tr)}, [Decision]};
step({transfer_result, _Ref, _Result}, T) ->
    {T, [{drop, unknown_transfer_result}]};
%% --- transfer deadline: fail the transfer only if this token is still armed ---
%% A fired-but-queued deadline for a previous arming of the same Ref carries an
%% old token and is dropped; the live arming carries the current token.
step({transfer_deadline, Ref, Token}, #tasks{transfers = Tr} = T) when
    is_map_key(Ref, Tr)
->
    case maps:get(Ref, Tr) of
        {_Size, Token} ->
            {T#tasks{transfers = maps:remove(Ref, Tr)}, [
                {deliver, transfer_failed, {Ref, transfer_deadline}}
            ]};
        _ ->
            {T, [{drop, stale_transfer_deadline}]}
    end;
step({transfer_deadline, _Ref, _Token}, T) ->
    {T, [{drop, stale_transfer_deadline}]};
%% --- retry transfer: re-upload only if the transfer is still outstanding ---
%% A retry scheduled before a recovery cleared the in-flight queue finds its Ref
%% absent and is dropped, rather than re-submitting a phantom upload.
step({retry_transfer, Ref}, #tasks{transfers = Tr} = T) when is_map_key(Ref, Tr) ->
    {T, [{resubmit, Ref}]};
step({retry_transfer, _Ref}, T) ->
    {T, [{drop, stale_retry_transfer}]};
%% --- recover: bump the generation and abandon every in-flight task ---
step(recover, #tasks{generation = G}) ->
    {#tasks{generation = G + 1}, []}.

%%----------------------------------------------------------------------------
%% Result classification. Mirrors the completion the pure core expects for each
%% raw async result; the shell turns a `deliver` into the corresponding core
%% call.
%%----------------------------------------------------------------------------

classify_persist({ok, Revision}) -> {deliver, persist_complete, Revision};
classify_persist({error, {conflict, _}}) -> {deliver, persist_failed, conflict};
classify_persist({error, Reason}) -> {deliver, persist_failed, Reason}.

%% The group kind is carried in the deliver so the shell can attribute the
%% per-kind "created" metric without re-reading the slot it just freed.
classify_group(Kind, {ok, Uid}) -> {deliver, group_complete, {Kind, Uid}};
classify_group(_Kind, {error, Reason}) -> {deliver, group_failed, Reason}.

classify_retention(unchanged) -> {deliver, retention_failed, unchanged};
classify_retention({failed, Reason}) -> {deliver, retention_failed, Reason};
classify_retention({Edit, Refs}) -> {deliver, retention_complete, {Edit, Refs}}.

%% A transfer success credits its bytes to persist_pending_bytes (stage 3 of the
%% pipeline): they are durable in S3 and now await a manifest persist. A failure
%% leaves the byte accounting unchanged (the bytes already left bytes_in_transfer
%% when the Ref was removed by the caller) and drives the core's retry path.
classify_transfer(Ref, Size, {ok, Uid}, #tasks{persist_pending_bytes = Pending} = T) ->
    {T#tasks{persist_pending_bytes = Pending + Size}, {deliver, transfer_complete, {Ref, Uid}}};
classify_transfer(Ref, _Size, {error, Reason}, T) ->
    {T, {deliver, transfer_failed, {Ref, Reason}}}.
