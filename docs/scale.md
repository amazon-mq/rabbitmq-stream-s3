# Scale and Limits

How much data can a single stream hold? How long can it run? This doc explores the theoretical and practical limits of the design.

## The short answer

More data than you can produce. The theoretical limits are measured in exabytes and centuries. The practical limits are S3 request rates and local disk throughput, both of which are high.

## Offset space

Each record in a stream gets a 64-bit unsigned offset. The offset increments by one per record.

At 1 million records per second, the offset space lasts 584,942 years.
At 1 billion records per second, it lasts 584 years.

No stream will exhaust the offset space.

## Timestamp space

Timestamps are 64-bit signed integers in milliseconds since epoch. The maximum value represents the year 292,278,994. Not a concern.

## Data volume

### What S3 can hold

AWS documents no maximum bucket size: "The total volume of data and number of objects you can store in a bucket is virtually unlimited." Individual objects are capped at 5 TiB (though this was recently increased to 50 TiB). Fragments are far smaller (typically 64 MiB). S3 storage is not a practical constraint for any single stream.

### What the manifest can address

The manifest is a tree with branching factor `m = 1024`:

| Level | Entries addressed |
|-------|-------------------|
| Root only (no groups) | 1,024 fragments |
| One level of groups | 1,024 × 1,024 = ~1 million fragments |
| Kilo-groups | 1,024³ = ~1 billion fragments |
| Mega-groups | 1,024⁴ = ~1 trillion fragments |

At 64 MiB per fragment:

| Tree depth | Data addressed |
|------------|----------------|
| Root only | 64 GiB |
| Groups | 64 TiB |
| Kilo-groups | 64 PiB |
| Mega-groups | 64 EiB |

With a full set of 1024 mega-groups, the root covers ~70 EiB. Beyond that, the root continues to grow linearly (additional mega-group entries). The tree does not stop working, but the root object grows larger, using more memory and bandwidth per manifest upload. In practice, remote retention keeps the manifest bounded long before this matters.

### What it takes to produce that much data

| Throughput | Time to 1 PiB | Time to 1 EiB |
|------------|----------------|----------------|
| 100 MB/s | 4 months | 340 years |
| 1 GB/s | 12 days | 34 years |
| 10 GB/s | 29 hours | 3.4 years |

Even at very high sustained throughput, producing enough data to matter takes years.

## Manifest size in memory

The root is cached in memory. Its size depends on how many entries have not yet been factored into groups.

Worst case: the root has `m - 1 = 1023` fragment entries plus some number of group/kilo-group/mega-group entries. At 34 bytes per entry, the root is at most ~34 KiB of fragment entries (1023 * 34 = 34,782 bytes) plus one entry per group-level bucket. In practice the root is a few KiB for active streams.

Group objects are not all cached. They are downloaded on demand during offset resolution and fragment iteration, and may be cached for repeated access (retention evaluation, active consumers reading through a group's range).

## Practical limits

These are the things that actually constrain the system before any theoretical limit is reached.

### S3 request rates

S3 supports at least 3,500 PUT/COPY/POST/DELETE and 5,500 GET/HEAD requests per second per partitioned prefix. S3 automatically scales to higher request rates in response to sustained traffic.

**Uploads:** At 64 MiB per fragment, even very high throughput produces few PUTs per second. The upload path is unlikely to approach this limit.

**Downloads:** Each consumer's remote reader issues GET requests. A single consumer at full speed uses a few GETs/sec. Many consumers reading from S3 on the same stream increase the aggregate request rate. S3's automatic scaling handles sustained load, but a sudden burst of consumers on a previously idle stream may see brief throttling before scaling takes effect. When throttled, consumers see reduced throughput (slower delivery) but no errors or data loss. The broker retries automatically and throughput recovers as S3 scales.

### Local disk and page cache

Osiris does not flush (`fsync(2)`) writes. Data lives in the page cache and the OS writes it to disk in the background on its own schedule. In practice, reads of recently-written data (by consumers or by the upload path) are served entirely from page cache with no disk I/O.

This means the upload path is fast: the remote replica reader reads data that the writer just wrote, which is almost always page-cache-hot. The practical bottleneck is memory pressure, not disk throughput. If the system is under memory pressure and the OS evicts dirty pages, both the write path and the upload path slow down as they start hitting actual disk.

### Manifest upload frequency

The manifest root is uploaded after fragment uploads, debounced to avoid high-frequency updates. This keeps manifest PUTs infrequent and well within S3 limits.

## How long can a stream run?

Indefinitely. There is no time-based limit. The offset space, timestamp space, and manifest tree all support continuous operation for centuries. Remote retention can keep the manifest bounded regardless of how long the stream runs.

A stream with no remote retention grows the manifest linearly: one entry per fragment. At 1 PiB of accumulated data (with 64 MiB fragments), the manifest has around 16 million entries organized into 16,000 groups and 16 kilo-groups. The root stays small. Offset resolution requires at most 3 round-trips to S3 (kilo-group → group → fragment).

## Summary

| Resource | Theoretical limit | Practical concern? |
|----------|-------------------|-------------------|
| Offset space | 2⁶⁴ records (~584 years at 1B/sec) | No |
| Timestamp space | Year 292 million | No |
| Manifest tree depth | 70 EiB at 64 MiB fragments | No |
| S3 storage | No documented limit | No |
| S3 PUT rate | 3,500/sec/prefix | No (uploads are infrequent) |
| S3 GET rate | 5,500/sec/prefix (scales automatically) | Possible, at high consumer fan-out on a cold stream |
| Page cache pressure | Hardware-dependent (memory) | Yes, at high throughput with insufficient memory |
