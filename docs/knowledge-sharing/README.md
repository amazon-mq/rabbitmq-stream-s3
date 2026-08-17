# Tiered Storage: A Guided Introduction

This is a self-contained introduction to how the tiered storage plugin behaves, for anyone who wants to understand or operate it. It is an artifact of an internal tech talk, and is kept here as a reference and a hands-on walkthrough you can run yourself.

It is built around four promises the plugin makes: for each one it explains what the plugin guarantees, why you can trust it, and what you would see on a running broker. Sections 1 and 2 build a mental model, section 3 explains how the plugin works, section 4 covers the four promises, and section 5 is a hands-on walkthrough with commands you can copy and run.

If you know message queues but have not worked with streams, start at section 1. If you already know streams, skip to section 2.

## 1. If you know messaging but not streaming

If you come from message queues, streams share some vocabulary but behave differently in the ways that matter here.

| Queue concept | Stream equivalent | The shift |
|---------------|-------------------|-----------|
| Message | Record | Same idea: a unit of data a client publishes. |
| Consume removes the message | Consume reads at an offset | Reading does not delete. Many consumers read the same data independently. |
| Acknowledgement | Offset tracking | The broker remembers where a consumer is. The data stays put. |
| TTL or queue-length limit drops messages | Retention deletes whole segments from the old end | Streams age out ranges of the log, they do not drop individual records. |
| (no equivalent) | Offset specs | A consumer attaches at the beginning, the end, a specific offset, or a timestamp. |
| (no equivalent) | Tiered storage | Old data lives in S3. Consumers read it transparently when it is gone from disk. |

The one-sentence mental model: a stream is a tape that only moves forward. Publishers append to the right end. Consumers put a read head anywhere on the tape and move right. Retention trims from the left end. Reading is not consuming, so the same record can be read by many consumers and re-read later.

Without this plugin the whole tape lives on local disk, and when retention trims the left end that data is gone forever. The rest of this guide is about what changes when the tape gets a second tier.

## 2. The idea: tiered storage

![Tiers before and after](diagram-tiers-before-after.svg)

Tiered storage copies old data in a stream to S3 in the background. Redundant uploaded data is then deleted locally. Recent data is available locally and is served from disk. Historical data is served from S3. The consumer does not know or care which tier serves each byte.

The point for an end-user: "how much data should I keep" stops being a disk capacity question and becomes purely a question of time. Local disk holds a recent window. S3 holds the long tail cheaply. The readable window and the disk you provision are no longer coupled.

## 3. How it works

You can operate the plugin knowing only the four promises in the next section. This section is for readers who want to know what produces them. It introduces the moving parts, then follows data along the two paths it travels: out to S3, and back to a consumer.

### The moving parts

![The moving parts](diagram-moving-parts.svg)

Tiered storage adds a handful of components to a broker node. None of them sit on the path between a publisher and its confirm.

- The uploader, one per stream on the writer node, reads committed data from the local log, assembles it into fragments, and writes them to S3. It is the only thing that writes to the remote tier.
- The manifest is the index of what is in S3. It maps offsets to fragments so a reader can find any position. It lives in S3 as objects, with a small pointer and version held in Khepri so competing writers cannot corrupt it.
- The governor, one per node, paces uploads with a token bucket so a burst of fragments across many streams cannot saturate the node's network.
- The remote reader, one per consumer reading old data, fetches fragments from S3 with HTTP range requests and feeds them to the consumer.
- Retention and the reaper delete fragments from S3 when remote retention expires them, and clean up after a stream is deleted.

### The upload path

![Journey of a record](diagram-record-journey.svg)

A record is published, replicated to a majority, and committed entirely on local disk. The publisher's confirm depends on none of the tiering machinery. Then, in the background, the uploader reads the committed chunks, accumulates them until it has about 64 MiB, and cuts a fragment. The governor paces the upload, the fragment is written to S3 with a single PUT, and the manifest is updated to point at it. Local retention trims the segment later, once its data is safely uploaded.

Because every step after commit happens off to the side, an S3 outage can only make the upload fall behind. It cannot slow down or block publishing.

### The read path

A consumer subscribes with an offset spec: the beginning, the end, a specific offset, or a timestamp. The log reader resolves that to a position. If the position is still on local disk, it is an ordinary local stream read. If it is older than anything on disk, the log reader looks the offset up in the manifest, finds the fragment and the byte position within it, and hands off to a remote reader.

The remote reader fetches that fragment from S3 with a range request and delivers it to the consumer. It prefetches ahead of the consumer's position, growing the read window when the consumer reads quickly and shrinking it when the consumer slows. A steady reader approaches local-tier throughput, while a consumer that reads a little and disconnects wastes no bandwidth.

To move forward through the stream, the remote reader walks the manifest with a fragment iterator. The iterator yields fragments in offset order and hides the tree structure: the reader just asks for the next fragment and keeps going, whether the next entry sits in the root or deep in a branch. When the iterator runs out of remote fragments, the consumer has caught up to local disk, and the log reader switches to local reads with no gap. The remote reader exits.

### The manifest

The manifest is the index that makes any offset findable in S3. It maps an offset to the fragment that contains it and the byte position within that fragment.

It is stored as a shallow tree. Most entries point straight at fragments. When there are too many to keep in one object, groups of entries are factored out into separate objects, with groups of groups above them. The branching factor is high, 1024, so the tree stays shallow: even a petabyte-scale stream resolves any offset in at most three round trips to S3, and the part of the tree kept in memory stays a few KiB. The fragment iterator walks this tree in offset order and downloads group objects only as it reaches them, which is what lets a consumer read the whole stream linearly without ever holding the entire index in memory.

For the tree's on-disk layout and concurrency rules, see [../manifest.md](../manifest.md).

## 4. Four contracts

The most useful way to hold this plugin in your head is as four promises. For each one: what it promises, why you can trust it, and what you would see on a live broker.

### Contract 1: local streaming wins over remote

The promise: the write path is entirely local and never waits on S3. Publishing, replication, and reads of recent data are unaffected by S3 being slow or down. Uploads happen in a background process. And local retention always wins over upload progress, so the plugin can never pin segments on disk waiting for an upload and fill your disk.

Why you can trust it: the upload is a side branch off the committed log, as the upload path in section 3 shows. S3 is a soft dependency. If S3 is unavailable, uploads fall behind and that is all that happens to the write path.

What you would see: publish and confirm rates stay flat while uploads run. The upload pipeline metrics move, the write path metrics do not flinch.

### Contract 2: consumers see one continuous stream

The promise: a consumer reads across the seam between S3 and disk without interruption. When it asks for old data that is only in S3, it is served from S3. As it catches up to data that still exists locally, it transitions to disk reads with no gap. Prefetch adapts to the consumer's read rate, so reading from S3 approaches local throughput.

Why you can trust it: offset resolution and the remote-to-local transition are handled inside the broker. No client change is needed. The same consumer code works against either tier.

What you would see: a consumer starting at the beginning of a stream whose head is only in S3. It reads from the remote tier, then hands off to local reads as it reaches the recent window. The dashboard shows remote readers active, then offset resolution shifting from remote to local, then the remote reader going away.

### Contract 3: it stays small and predictable

The promise: the memory and lookup cost of the remote tier do not grow with how much data the stream holds. The in-memory manifest root stays a few KiB for an active stream, and at most around 35 KiB of fragment entries in the worst case. Finding any offset takes at most three round trips to S3 even at petabyte scale. Per-node transfer pacing bounds how much network the uploads use.

Why you can trust it: the manifest is a shallow tree with a high branching factor, so the part kept in memory stays tiny while the data behind it grows into the petabytes. A token-bucket governor paces transfers across all streams on a node.

What you would see: as a consumer reads, offset resolution stays fast no matter how large the stream is, because resolving any offset takes at most three round trips to S3. The `Avg offset resolution time` panel shows this. The in-memory manifest footprint itself is not exposed as a live counter, so this contract is best shown with the reasoning in scale.md rather than a single number. Note that the `manifest_total_size` status field is the remote tier's data volume, not the in-memory size, so it does grow with the stream. The bound is documented in [../scale.md](../scale.md).

### Contract 4: durability is best-effort within a window

![The durability window](diagram-durability-window.svg)

The promise: a record reaches S3 only if its fragment is uploaded before local retention trims the segment behind it. The plugin does not hold segments past the retention bound to wait for an upload to become durable. Local trimming always wins. The reason is deliberate: pinning segments during an S3 outage would let the local log grow without bound and risk filling the disk, which turns an object-store problem into a broker-availability problem. A bounded loss of the un-uploaded tail is preferred over an unbounded local resource leak.

The consequence is a durability window. The safe margin is the size of the local retention window measured against upload throughput. If S3 is unavailable, or uploads simply cannot keep up with ingress, for longer than the local window can buffer, the un-uploaded tail is lost from both tiers. A consumer that asks for a lost range is repositioned at the oldest available offset after it, the same behavior as data that aged out of a plain stream.

This contract is hard to demonstrate live, because it requires making S3 fail on demand. It is best understood from the diagram.

## 5. Hands-on walkthrough

Everything below is runnable. Replace `BROKER` with your stream endpoint host.

### 5.0 Prerequisites

The plugin is enabled cluster-wide and attaches to every stream through osiris hooks. There is no per-stream switch to flip. A stream is tiered simply because the plugin is enabled on the cluster.

```bash
# On every node: bucket and region in rabbitmq.conf
#   stream_s3.bucket = my-rabbitmq-streams-bucket
#   stream_s3.region = us-east-1

# Enable on every node (this also enables rabbitmq_prometheus on port 15692).
rabbitmq-plugins enable rabbitmq_stream_s3

# Get the load generator (Java 11+).
wget https://github.com/rabbitmq/rabbitmq-java-tools-binaries-dev/releases/download/v-stream-perf-test-latest/stream-perf-test-latest.jar -O stream-perf-test.jar
PERF="java -jar stream-perf-test.jar"
URI="rabbitmq-stream+tls://guest:guest@BROKER:5551"
```

For the demo we assume that the broker is deployed behind a load balancer and uses TLS for connections.

A note that matters for the demo: stream-perf-test defaults to a 20 GB max length, so nothing ages out during a short run and tiering stays invisible. The runs below set a smaller max length and push more than one segment of data through it, so the head of the stream is uploaded and then trimmed from disk. We leave the segment size at its default. Segment size is not something you tune in normal practice, and local retention always keeps at least one whole segment on disk (500 MB by default) no matter how small the max length is. That one-segment floor is why the demo produces a couple of GB rather than relying on a tiny byte limit.

### 5.1 Demo A: watch data move to the remote tier

Run a producer-only load and push more than one segment of data through a realistic local retention bound, so the head of the stream is uploaded and then trimmed off disk. Let this run for thirty to sixty seconds so a segment rolls off the local window.

```bash
$PERF --uris "$URI" --load-balancer \
  --streams demo --producers 1 --consumers 0 \
  --rate 20000 --size 4096 \
  --max-length-bytes 1gb
```

While it runs, scrape the plugin counters and watch the per-stream status.

```bash
# Plugin counters should appear and climb once a fragment is uploaded.
curl -s https://BROKER:15691/metrics | grep '^rabbitmq_stream_s3_'

# Per-stream status. Re-run it a few times and watch the offsets advance.
rabbitmq-streams stream_s3_status demo --vhost /
```

What to expect:
- `rabbitmq_stream_s3_transfers_completed` and `rabbitmq_stream_s3_persists_completed` climb.
- `rabbitmq_stream_s3_remote_bytes` and `rabbitmq_stream_s3_remote_messages` grow.
- In the status output, `manifest_next_offset` advances as uploads complete, and `manifest_total_size` grows to track the data volume now held in the remote tier.
- On the Grafana dashboard: `Transfers / s`, `Pipeline throughput`, `S3 bytes sent / s`, and `Remote tier size` all move. Publish and confirm rates stay flat. This is Contract 1, live.

### 5.2 Demo B: watch a consumer read from S3 and transition to local

The data at the start of the stream has now been trimmed from disk and lives only in S3. Start a consumer at the beginning, with a producer still running so there is a live tail to catch up to.

```bash
$PERF --uris "$URI" --load-balancer \
  --streams demo --producers 1 --consumers 1 \
  --offset first \
  --rate 20000 --size 4096 \
  --max-length-bytes 1gb
```

What to expect on the Grafana dashboard:
- `Active remote readers` becomes non-zero and `S3 bytes received / s` rises as the consumer reads old data from S3.
- `rabbitmq_stream_s3_get_range` climbs (range GETs against fragments).
- `Offset resolution by tier` and `Fragment transitions / s` show the consumer moving forward through the remote tier.
- As the consumer catches up to the recent window, reads transition to local and the remote reader goes away. This is Contract 2, live.
- Offset resolution time stays low on the `Avg offset resolution time` panel even though the consumer started at the very beginning of a large stream. This is the observable side of Contract 3.

The same `--max-length-bytes` is repeated so stream creation stays idempotent. stream-perf-test will reuse the existing `demo` stream. If you change the retention flag between runs it will warn and proceed.

### 5.3 Things to explore

Other behaviors worth poking at, and where to look:
- Restart the leader node mid-load. Watch delivery pause and resume, and watch `rabbitmq_stream_s3_persist_conflicts` and the recovery counters.
- Disable then re-enable the plugin under load. Confirm uploads resume from where the manifest left off.
- Delete the stream and confirm objects are reaped: watch `rabbitmq_stream_s3_objects_deleted`.

### 5.4 Cleanup

```bash
$PERF --uris "$URI" --load-balancer --streams demo --producers 0 --consumers 0 --delete-streams
```

## 6. Questions worth discussing

- Which of the four promises is most likely to surprise someone new to the plugin, and is the behavior documented clearly enough?
- What is missing from the dashboard for someone trying to triage "uploads are falling behind"?

## Appendix A: stream_s3_status fields

`rabbitmq-streams stream_s3_status <stream> [--vhost <vhost>]`

| Field | Meaning |
|-------|---------|
| `fragment_target_size` | Size a fragment reaches before it is cut and uploaded (default 64 MiB) |
| `log_next_offset` | Next offset in the local log (the write head) |
| `manifest_first_offset` | Oldest offset durable in the remote tier |
| `manifest_next_offset` | Next offset to upload (the upload edge) |
| `manifest_total_size` | Total bytes of stream data addressed by the manifest, that is the remote tier data volume. This grows with the stream. It is the sum of fragment sizes, not the in-memory manifest footprint |
| `persisted_next_offset` | Offset up to which the manifest is durably persisted |
| `transfers_in_flight` | Fragment uploads in progress for this stream |
| `transfers_pending_order` | Completed uploads buffered waiting for an earlier one to finish |
| `since_persist` | State of the durable-persist debounce |
| `persist_in_flight` | Whether a manifest persist is currently running |
| `assembly_payload_size` | Bytes accumulated in the fragment currently being assembled |
| `assembly_target_size` | Target size before the current fragment is cut |
| `assembly_num_chunks` | Chunks accumulated in the current fragment |

## Further reading

See the [main documentation](../README.md) for full guides through the plugin's behavior, operations and implementation details.
