# Tracking Data: Purpose and Resource Footprint

This investigation describes what tracking data is in Osiris, what it is used for, and what it costs. The writer holds tracking state in memory, but the more interesting cost is the tracking snapshots and deltas that Osiris embeds directly into the log. Because this plugin uploads log segments to S3 verbatim, every byte of tracking data the writer embeds is a byte we store and index in S3.

All references below are to the vendored Osiris source under `deps/osiris`.

## What tracking data is

Tracking data is a small key-value store that Osiris maintains per stream, alongside the user records. It exists to answer two questions that the raw record log cannot:

1. "Have I already seen this producer's message?" (deduplication)
2. "Where did this consumer get to?" (server-side offset storage)

Tracking state lives in `osiris_tracking` (`deps/osiris/src/osiris_tracking.erl`) as three maps:

| Map | Type | Purpose |
|-----|------|---------|
| `sequences` | `TrkId => {ChunkId, Sequence}` | Producer deduplication. Records the highest publishing sequence accepted for each producer reference. |
| `offsets` | `TrkId => Offset` | Server-side offset tracking. Records where a named consumer got to. |
| `timestamps` | `TrkId => Timestamp` | Timestamp-based tracking, same shape as offsets. |

`TrkId` is an arbitrary binary, capped at 256 bytes (`osiris_tracking:add/5`). In practice it is a producer reference or a consumer reference string.

### How each kind of entry is created

Sequence entries are created implicitly by every deduplicated write. When a producer publishes with a publishing id, the writer calls `put_writer/4`, which calls `osiris_tracking:add(WriterId, sequence, Corr, ChunkId, Trk)` (`deps/osiris/src/osiris_writer.erl`, `handle_command({cast, {write, ...}})`). So there is one pending sequence entry per distinct producer reference that wrote in a given batch.

Offset and timestamp entries are created explicitly by `osiris:write_tracking/3`, which the broker uses for server-side consumer offset storage.

## How tracking is embedded in the log

Osiris does not store tracking in a side file. It writes it into the same segment as the user records, using two of the three chunk types (`deps/osiris/src/osiris_log.erl`, chunk header documentation around line 200):

| Chunk type | Value | Contents |
|------------|-------|----------|
| `CHNK_USER` | 0x00 | User records. May carry tracking as a trailer. |
| `CHNK_TRK_DELTA` | 0x01 | Incremental tracking updates only, no user records. |
| `CHNK_TRK_SNAPSHOT` | 0x02 | Full snapshot of all live tracking state. |

There are three places tracking bytes land in the log.

### 1. Trailers on user chunks (the common case)

When a batch contains user writes and there are pending tracking updates, the writer appends the tracking updates as a trailer on the user chunk rather than writing a separate chunk (`osiris_writer.erl`, the `_ ->` branch that calls `osiris_log:write(Entries, ?CHNK_USER, Now, TrkData, Log1)`). The chunk header records the trailer length so readers can find it. This is the cheapest form: the chunk header and index record are already paid for by the user data, so the marginal cost is just the trailer bytes.

### 2. Tracking delta chunks

When there are pending tracking updates but no user writes in the batch (for example a `store_offset` arriving on an otherwise idle stream), the writer emits a standalone `CHNK_TRK_DELTA` chunk (`osiris_writer.erl`, the `[] when TrkData =/= [] ->` branch). This chunk carries only tracking data, but it still pays the full per-chunk overhead: a 48-byte header, a 4-byte simple-entry wrapper, and a 29-byte index record. There is a `TODO` in that branch noting that a timer could batch these to write fewer of them.

### 3. Tracking snapshots at segment boundaries

`evaluate_tracking_snapshot/2` runs before every write batch (`osiris_log.erl`). When the current segment has reached its maximum size and tracking is non-empty, it serializes the entire live tracking state with `osiris_tracking:snapshot/3` and writes it as a `CHNK_TRK_SNAPSHOT`. Because writing that chunk also crosses the segment-size threshold, the snapshot becomes the first chunk of the new segment. This is what lets recovery reconstruct tracking state by scanning a single segment: it starts from a full snapshot and then replays the deltas and trailers that follow it (`recover_tracking/3`).

## Wire format and per-entry sizes

Every tracking entry, in any of the three locations, is a self-describing block (`osiris_tracking:flush/1` for deltas and trailers, `osiris_tracking:snapshot/3` for snapshots):

```
| Trk type (1 byte) | ID size (1 byte) | ID (<=256 bytes) | type-specific data |
```

The type-specific data differs between deltas and snapshots, which makes deltas cheaper:

| Block | Layout | Bytes |
|-------|--------|-------|
| Delta / trailer entry (any type) | type + idsize + id + 8-byte value | `10 + len(ID)` |
| Snapshot sequence entry | type + idsize + id + 8-byte ChunkId + 8-byte sequence | `18 + len(ID)` |
| Snapshot offset entry | type + idsize + id + 8-byte offset | `10 + len(ID)` |
| Snapshot timestamp entry | type + idsize + id + 8-byte timestamp | `10 + len(ID)` |

The sequence delta is 8 bytes smaller than the sequence snapshot entry because the delta omits the ChunkId. On recovery, the ChunkId comes from the enclosing chunk's header (`recover_tracking/3` passes the chunk's `ChunkId` into `append_trailer/3`).

### Per-chunk overhead constants

From `deps/osiris/src/osiris.hrl`:

| Constant | Value | Notes |
|----------|-------|-------|
| `HEADER_SIZE_B` | 48 | Chunk header, paid by every chunk. |
| `INDEX_RECORD_SIZE_B` | 29 | One index record per chunk. |
| Simple-entry wrapper | 4 | `<<0:1, Length:31>>` prefix on the snapshot/delta body. |

Tracking chunks carry no bloom filter. The filter is built only from filter values on user entries, and tracking entries contribute none, so `osiris_bloom:to_binary/1` returns an empty binary for them (`deps/osiris/src/osiris_bloom.erl`).

### Standalone tracking chunks consume a stream offset

`write_chunk/6` advances the stream offset by the chunk's record count, and `make_chunk/7` assigns a count of 1 to the single simple entry that wraps a delta or snapshot body (`process_entry0/2`). So every standalone `CHNK_TRK_DELTA` and every `CHNK_TRK_SNAPSHOT` occupies one offset slot in the stream, even though it carries no user record. Trailers on user chunks do not have this effect, because they ride inside a chunk whose offset is already accounted for by its user records.

## Resource footprint

### Writer memory

The writer holds one `osiris_tracking` state per stream (`#?MODULE.tracking` in `osiris_writer.erl`). The three maps are bounded very differently, and only one of them is safe.

- `sequences` has a hard count cap of `MAX_SEQUENCES` (255). `trim_sequences/2` discards the entries with the lowest ChunkIds once the cap is exceeded, keeping the most recently active producers. This map cannot grow without bound no matter how producers behave.
- `offsets` and `timestamps` have no count cap. The only pruning is in `osiris_tracking:snapshot/3`, which keeps an entry as long as its stored value is at or above the stream's first offset (or first timestamp). There is no notion of client liveness anywhere in `osiris_tracking`: a disconnect leaves the entry behind, and `add/5` only guards `byte_size(TrkId) =< 256`, never the number of distinct IDs.

Each live entry costs roughly the size of its ID binary plus a small fixed value (a tuple or integer). For a stream with a fixed set of named consumers this is tens of kilobytes and irrelevant. The fixed-set assumption is what makes it "practically bounded".

### The degenerate case: churning unique tracking IDs

The practical bound disappears if clients use unique tracking IDs. Consider a client that connects, calls `store_offset` near the head of the stream with a fresh UUID reference, and disconnects, repeated indefinitely. Each store creates a new `offsets` entry. Because the stored offset is near the head, it stays at or above the stream's first offset for roughly a full retention window, so `snapshot/3` does not prune it. The disconnect does nothing, since tracking does not track clients. The result is unbounded growth of the `offsets` map, capped only by churn rate times retention window, which can be arbitrarily large.

Two things make this worse than a plain memory leak:

- Pruning only runs at segment-boundary snapshots, not continuously, so even entries that have become evictable linger in memory until the next rollover. Entries that point near the head are not evictable at all for a long time.
- Every accumulated dead entry is re-serialized into every subsequent snapshot (`osiris_tracking:snapshot/3` folds the entire `offsets` and `timestamps` maps). So the periodic snapshot grows monotonically with the count of dead UUIDs, and that growth is written into the segments this plugin uploads to S3. A leak that would otherwise be confined to writer memory becomes a steadily inflating per-segment cost in object storage and in the manifest index.

The same reasoning applies to `timestamps`. It does not apply to `sequences`, which is the one map with a hard cap, so a producer that churns UUID publishing references wastes only the 255 most recent slots and the deltas it writes, not unbounded snapshot space.

This is an Osiris-level behavior, not specific to this plugin, but tiered storage makes its consequences more visible: streams retained in S3 for a long time pay for every dead tracking entry in every snapshot for as long as the data near which it was stored is retained.

### In-log cost (the cost that reaches S3)

This is the cost that matters for this plugin. Tracking data is written into the segments we upload, so it occupies S3 space and produces index entries we transform on upload (see `docs/upload-path.md` and `docs/manifest.md`).

There are three components.

**Continuous: sequence trailers on user chunks.** If producers publish with publishing ids, every user chunk that contains writes from N distinct producers carries `N x (10 + len(ref))` trailer bytes. This is the dominant continuous cost because it scales with chunk rate and producer count. For a single producer with a 20-byte reference, that is 30 bytes per chunk. At a high chunk rate this is a steady, if small, tax on every uploaded segment. It does not add chunks or index records, only trailer bytes inside chunks that already exist.

**Bursty: standalone delta chunks.** Server-side offset storage on an otherwise idle stream produces standalone `CHNK_TRK_DELTA` chunks. Each one costs `48 (header) + 4 (wrapper) + (10 + len(ID)) (entry) + 29 (index record)`, roughly 90 bytes plus the ID, for a single tracking update. It also consumes one stream offset. If consumers store offsets frequently and the stream is not otherwise busy, these small chunks accumulate. The `TODO` in `osiris_writer.erl` about a delta timer exists precisely to coalesce these.

**Periodic: snapshots at segment boundaries.** Once per segment rollover the writer embeds a full snapshot. Its size is `48 + 4 + 29 + sum of all live entry blocks`. With the worst case of 255 sequences (each `18 + len(ref)`) plus some offsets and timestamps, a snapshot is on the order of single-digit kilobytes. Because it is amortized over an entire segment (default segment sizes are in the hundreds of MiB; see `docs/operations.md`), the snapshot is a rounding error per segment. Its real significance is correctness, not cost: it bounds how far recovery has to scan and lets a segment be interpreted on its own.

### Summary of cost per location

| Location | Per-occurrence cost | Frequency | Adds a chunk? | Adds an index record? | Consumes an offset? |
|----------|--------------------|-----------|---------------|----------------------|--------------------|
| Trailer on user chunk | `N x (10 + len(ID))` bytes | Every batch with pending tracking and user writes | No | No | No |
| Standalone delta chunk | `~83 + len(ID)` bytes | Batch with tracking but no user writes | Yes | Yes | Yes |
| Snapshot chunk | `~81 + sum(entry blocks)` bytes | Once per segment | Yes | Yes | Yes |

## Takeaways for this plugin

- The continuous cost is sequence trailers, and it is proportional to chunk count and producer count, not to data volume. High-throughput streams with small chunks and many deduplicated producers pay relatively more per byte of user data.
- The unbounded risk is offset and timestamp tracking with unique IDs. Unlike `sequences`, these maps have no count cap and no liveness eviction, so churning UUID references grows both writer memory and, more durably, every future snapshot we upload to S3. If this plugin is exposed to workloads that store offsets under non-reused references, this is the failure mode to watch, and it argues for either a cap or a liveness-based eviction in `osiris_tracking`.
- Standalone delta chunks are the least efficient form, because a single tracking update pays a full chunk plus index record plus an offset slot. Workloads that store consumer offsets often on quiet streams generate many of these. Coalescing them upstream in Osiris (the existing `TODO`) would reduce both S3 object content and index entries.
- Snapshots are cheap per segment when tracking state is small, and are worth their cost: they cap recovery scan distance and make each segment self-describing, which aligns with how this plugin uploads and reads segments independently. Their cost stops being negligible exactly when the degenerate case above is in play.
- None of these chunks carry a bloom filter, so they do not inflate the filter sections we upload.
