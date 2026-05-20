# Upload Durability: CRC Strategy

When the remote replica reader uploads a fragment to S3, we want to guarantee that the bytes stored in S3 are identical to what the writer originally wrote. This investigation explores how to achieve that guarantee with minimal CPU cost.

## Corruption points

Corruption can happen at two points in the upload path:

**Before the read.** The writer wrote correct bytes, but by the time the remote replica reader reads them back from the segment file, something changed. Causes include bit rot, kernel memory corruption, or hardware faults. The data is page-cache-hot in the common case (the writer wrote it moments ago), so this requires a fault in the brief window between write and upload read.

**In transit.** We read correct bytes from disk, but something flipped a bit on the network between our process and S3's storage layer. TCP checksums catch most of this, but they are not infallible.

## Existing checksums

Every chunk in the segment file has a 48-byte header containing a CRC32 of the chunk body. The writer computes this at write time. The CRC lives in the segment file alongside the body it protects.

S3 supports `x-amz-checksum-crc32`: the client sends a CRC32 of the entire PUT body, and S3 rejects the upload if the bytes it received produce a different checksum.

## The approach: crc32_combine

`erlang:crc32_combine(CrcA, CrcB, LenB)` produces the CRC of `A ++ B` given only `CrcA`, `CrcB`, and the length of B. It does not need the actual bytes of B. This is an O(log n) operation on the length, not O(n) on the data.

During upload, the remote replica reader builds a whole-object CRC incrementally:

```erlang
%% 8-byte fragment header (magic + version): computed directly
Crc0 = erlang:crc32(<<"OSIF", Version:32>>),

%% For each chunk in the fragment:
%%   Chunk header (48 bytes): computed directly
Crc1 = erlang:crc32(Crc0, ChunkHeaderBytes),
%%   Chunk body (variable, potentially megabytes): combined from header CRC
Crc2 = erlang:crc32_combine(Crc1, BodyCrcFromChunkHeader, BodyLength),

%% Index records (~20 KiB): computed directly
FinalCrc = erlang:crc32(CrcN, TransformedIndexRecords)
```

The body CRC is taken directly from each chunk's header (trusting it) and combined into the running object CRC. The remote replica reader never computes CRC over the body bytes itself.

The final `FinalCrc` is sent to S3 as `x-amz-checksum-crc32` alongside the PUT body (which contains the actual bytes read from disk).

## Why this catches disk corruption

Suppose chunk body #5 is corrupt on disk:

1. The remote replica reader reads the chunk header, which contains `BodyCrc5` (the correct CRC, written by the writer when the data was good).
2. It uses `crc32_combine` with `BodyCrc5` to build `ObjectCrc`. This reflects what the data *should* be.
3. It reads the corrupt body bytes and streams them to S3.
4. S3 receives the corrupt bytes. It computes CRC over what it received. This does not match `ObjectCrc` (which was built from the correct, pre-corruption CRC).
5. S3 rejects the PUT.

The mismatch between "CRC derived from trusted header values" and "CRC S3 computed from actual received bytes" catches the corruption. The remote replica reader never needed to verify the body itself.

## Why this catches transit corruption

If the bytes are correct on disk but corrupted in transit:

1. `ObjectCrc` is built from correct header CRCs (matching the correct body bytes that were read).
2. The bytes get corrupted on the wire.
3. S3 computes CRC over the corrupted bytes. Mismatch. S3 rejects the PUT.

Same mechanism, different corruption point.

## What this does not catch

If both the chunk header CRC field and the chunk body are corrupt in a way that they still match each other (the header CRC was rewritten to be the CRC of the corrupted body), then:

- `ObjectCrc` would be built from the corrupt-but-consistent CRC.
- The bytes S3 receives would match that CRC.
- S3 would accept the upload.

This requires a correlated double corruption: the header and body changed together in a way that preserves CRC consistency. This is not a realistic failure mode for any known hardware or software fault. Single-bit errors, multi-bit errors, and sector-level corruption all produce mismatches because the header CRC and body are stored at different positions (and often different disk sectors).

## Cost

| Component | Method | Per-byte cost |
|-----------|--------|---------------|
| Chunk bodies (bulk data, MiB) | `crc32_combine` | Zero. O(log n) on length, not on data. |
| Chunk headers (48 bytes each) | `erlang:crc32/2` direct | Negligible. ~48 KiB total for a typical fragment. |
| Fragment header (8 bytes) | `erlang:crc32/2` direct | Negligible. |
| Index records | `erlang:crc32/2` direct | Negligible. ~20 KiB for a typical fragment. |

For a 64 MiB fragment, the remote replica reader computes CRC directly over roughly 70 KiB of metadata (chunk headers, fragment header, index records). The remaining 64 MiB of chunk body data has zero per-byte CRC cost. The integrity of that bulk data is verified by S3 using the checksum derived from trusted header values.

## Comparison to the current design

The current implementation accumulates `erlang:crc32` over chunk body iodata inside the writer's `handle_event({chunk_written, ...})` callback on every chunk write. It then re-checks this CRC after the upload task reads the segment back. This adds CPU cost to the writer's hot path for every chunk, regardless of whether the data will be uploaded soon or much later.

Under the new design, the writer's hot path has zero checksum overhead from the plugin. All integrity verification happens at upload time, using checksums that already exist in the chunk headers, at near-zero marginal cost.
