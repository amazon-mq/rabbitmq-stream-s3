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
""".

-export([
    init/0,
    generation/1,
    persist_slot/1,
    group_slot/1,
    retention_slot/1,
    transfers/1,
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
    %% Data is the byte count this persist will cover (carried for the gauges in
    %% the eventual wiring; opaque here).
    persist = idle :: slot(non_neg_integer()),
    %% Data is the group kind being uploaded.
    group = idle :: slot(term()),
    %% No per-task data is needed to correlate a retention result.
    retention = idle :: slot(undefined),
    %% Transfers are keyed by a unique caller-minted Ref rather than a slot,
    %% because many run concurrently. Data is the transfer's byte size.
    transfers = #{} :: #{term() => non_neg_integer()}
}).

-opaque tasks() :: #tasks{}.

%% Inputs to the machine. Spawn events record that the shell has started a task;
%% the shell stamps the spawned task's result with generation/1 at that moment.
%% Result/timeout events arrive later carrying the generation (or Ref) the task
%% captured at spawn.
-type event() ::
    {spawn_persist, Bytes :: non_neg_integer()}
    | {spawn_group, Kind :: term()}
    | spawn_retention
    | {spawn_transfer, Ref :: term(), Size :: non_neg_integer()}
    | {persist_result, generation(), Result :: term()}
    | {group_result, generation(), Result :: term()}
    | {retention_result, generation(), Result :: term()}
    | {retention_timeout, generation()}
    | {transfer_result, Ref :: term(), Result :: term()}
    | recover.

%% Outputs of the machine: deliver a completion to the pure core, or drop a
%% stale message. The shell turns a `deliver` into the matching core call.
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
    | {drop, Reason :: atom()}.

%%----------------------------------------------------------------------------
%% Accessors (the wiring layer and the property observer read state through
%% these rather than the record, keeping the representation private).
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

-spec transfers(tasks()) -> #{term() => non_neg_integer()}.
transfers(#tasks{transfers = T}) -> T.

%%----------------------------------------------------------------------------
%% The state machine.
%%----------------------------------------------------------------------------

-spec step(event(), tasks()) -> {tasks(), [decision()]}.

%% --- spawns: record the new task at the current generation ---
step({spawn_persist, Bytes}, #tasks{generation = G} = T) ->
    {T#tasks{persist = {in_flight, G, Bytes}}, []};
step({spawn_group, Kind}, #tasks{generation = G} = T) ->
    {T#tasks{group = {in_flight, G, Kind}}, []};
step(spawn_retention, #tasks{generation = G} = T) ->
    {T#tasks{retention = {in_flight, G, undefined}}, []};
step({spawn_transfer, Ref, Size}, #tasks{transfers = Tr} = T) ->
    {T#tasks{transfers = Tr#{Ref => Size}}, []};
%% --- persist result: apply only to the matching in-flight persist ---
step({persist_result, G, Result}, #tasks{persist = {in_flight, G, _Bytes}} = T) ->
    {T#tasks{persist = idle}, [classify_persist(Result)]};
step({persist_result, _G, _Result}, T) ->
    {T, [{drop, stale_persist_result}]};
%% --- group result ---
step({group_result, G, Result}, #tasks{group = {in_flight, G, _Kind}} = T) ->
    {T#tasks{group = idle}, [classify_group(Result)]};
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
    {T#tasks{transfers = maps:remove(Ref, Tr)}, [classify_transfer(Ref, Result)]};
step({transfer_result, _Ref, _Result}, T) ->
    {T, [{drop, unknown_transfer_result}]};
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

classify_group({ok, Uid}) -> {deliver, group_complete, Uid};
classify_group({error, Reason}) -> {deliver, group_failed, Reason}.

classify_retention(unchanged) -> {deliver, retention_failed, unchanged};
classify_retention({failed, Reason}) -> {deliver, retention_failed, Reason};
classify_retention({Edit, Refs}) -> {deliver, retention_complete, {Edit, Refs}}.

classify_transfer(Ref, {ok, Uid}) -> {deliver, transfer_complete, {Ref, Uid}};
classify_transfer(Ref, {error, Reason}) -> {deliver, transfer_failed, {Ref, Reason}}.
