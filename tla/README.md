# TLA+ Model

This directory contains a TLA+ specification that models the Osiris replication protocol extended with the remote tier (tiered storage).

## Origin

The base model is from [rabbitmq/osiris](https://github.com/rabbitmq/osiris/tree/main/tla) (`OsirisMsgPassing.tla`), dual-licensed under Apache-2.0 and MPL-2.0. It was imported in commit [`e15f58e`](https://github.com/amazon-mq/rabbitmq-stream-s3/commit/e15f58e) after adding retention modeling upstream (`7d3df32` in the osiris repository). The remote tier extensions were added in commit [`685fdfe`](https://github.com/amazon-mq/rabbitmq-stream-s3/commit/685fdfe).

## What the model covers

The base Osiris model specifies:
- Writer election via a coordinator (fencing, leader notification)
- Log replication from leader to followers
- Committed offset advancement (majority acknowledgment)
- Epoch-based truncation of uncommitted data on leader change
- Local retention (prefix deletion)

The remote tier extension adds:
- `remote_log`: the set of records uploaded to S3
- `remote_epoch`: epoch of the last successful manifest update (models the Khepri optimistic lock)
- `remote_first_offset`: advances on remote retention or truncation
- `replica_reader_pos`: per-replica tracking of the next offset to upload

Four new actions model the upload path:
- `ReplicaReaderUploads`: leader's replica reader uploads committed records, gated by the optimistic lock
- `ReplicaReaderJumpsForward`: replica reader accepts a gap when local retention deletes data it hasn't uploaded
- `ReplicaReaderTruncatesRemote`: new leader truncates remote tier when S3 is ahead of local (epoch change)
- `RemoteRetentionDeletesPrefix`: remote retention deletes old records from the remote tier

## Invariants

| Invariant | What it checks |
|-----------|----------------|
| `TypeOK` | All variables have expected types |
| `NoDivergence` | No follower has a record that conflicts with the leader at the same offset |
| `FollowerEqualOrLowerEpoch` | Followers never have a higher epoch than the leader |
| `NoLossOfConfirmedWrite` | A confirmed write exists on at least one replica or in the remote tier, unless every replica that had it has since retained past it |
| `LerMatchesLog` | Each replica's Log End Record matches its actual last log entry |
| `FollowerCommittedOffsetBounded` | Follower committed offsets never exceed the leader's |
| `RemoteNoDivergedData` | After cleanup (remote_epoch >= coord_epoch), the remote tier contains no records that contradict the leader's timeline |

## Running

Requires Java 21+ and [tla2tools.jar](https://github.com/tlaplus/tlaplus/releases) (v1.8.0 or later).

```
curl -fsSL -o tla2tools.jar https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
cd tla/
java -jar ../tla2tools.jar -simulate -depth 500 -workers auto -deadlock MC.tla
```

Simulation mode explores random traces for a bounded time rather than exhaustively checking all states. The CI workflow (`.github/workflows/tla-model-check.yaml`) runs simulation for 5 minutes on every push to `tla/` and weekly on Monday mornings.

For exhaustive model checking (slower, finds more bugs after model changes):

```
java -jar ../tla2tools.jar -workers auto -deadlock MC.tla
```

The `MC.cfg` constrains the state space to 3 replicas, 3 values, epochs < 4, and start/stop counter < 4.

## Key design decisions modeled

**Optimistic lock.** `ReplicaReaderUploads` requires `rep_epoch[r] >= remote_epoch`. A deposed leader with a stale epoch cannot update the manifest once the new leader has written.

**Local always wins.** `ReplicaReaderTruncatesRemote` removes remote records at or above the local first offset when the remote tier is ahead. This models the data directory reset and epoch change scenarios.

**Gaps are acceptable.** `ReplicaReaderJumpsForward` lets the replica reader skip past locally-retained data. The remote tier may have gaps, but no invariant is violated.

**Confirmed writes survive.** `NoLossOfConfirmedWrite` ensures data is not silently lost. A confirmed write must exist somewhere (local or remote) unless every replica that held it at confirmation time has since retained past it.
