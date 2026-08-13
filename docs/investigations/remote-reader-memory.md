# Remote reader memory behavior

> Status update (2026-08-13). The prefetch ceiling this doc cites, `read_size_max` at 64 MiB with a further next-fragment prefetch at the full window size, has been retired. The pipelined prefetch of [#349](https://github.com/amazon-mq/rabbitmq-stream-s3/issues/349) replaced it with a single window spanning both fragments, capped by `prefetch_window_max` (32 MiB), so the per-consumer ceiling is that plus one in-flight request rather than the ~128 MiB quoted below. The block-queue findings and every measurement below are unaffected and remain valid.

This doc is an investigation into the memory impact of remote tier reads.

Measurements were taken on a single development machine (Linux, Erlang/OTP 27, ERTS 15.2.7.6).

## Why uploads are cheaper than reads

The upload path never accumulates: `stream_put/stream_data` (`rabbitmq_stream_s3_api_aws.erl`) batches outgoing data to 1 MiB chunks and hands each to gun immediately, so a replica reader in steady state holds roughly one chunk of in-flight data per stream.

The read path must accumulate by design. When this investigation was made, the remote reader prefetched an AIMD-sized window ahead of the consumer (up to `read_size_max`, 64 MiB) and additionally prefetched the next fragment at the full window size, so tens of MiB of *intentional* buffer per remote consumer was the baseline. The waste investigated here was on top of that: the same bytes being copied repeatedly, and dead buffer memory surviving long after it was consumed.

## Mechanism: appending and slicing the same binary

The original buffer was a single binary grown per S3 delivery with `<<Buffer/binary, Data/binary>>`, and reads were served as `binary:part/3` sub-binaries replied to the log reader. Each half of that pattern is fine alone; together they degrade. In the OTP source:

- An append reuses the binary in place only while the underlying refc binary is still flagged writable. Otherwise `erts_bs_append_checked` takes the `not_writable` path and copies the entire buffer into a fresh writable binary (`erts/emulator/beam/erl_bits.c`, the `not_writable` label).
- `binary:part/3` over a writable binary yields a sub-binary marked *volatile* (`erts_build_sub_bitstring`). Results of 64 bytes or less are copied to the heap instead (`ERL_ONHEAP_BITS_LIMIT`) - but the smallest read on this path is the chunk-header over-read of `?CHUNK_HEADER_B + ?MAX_FILTER_SIZE` = 303 bytes, so every read produced a volatile sub-binary.
- The moment a volatile sub-binary is copied into a message (which `gen_server:reply` does) `erts_pin_writable_binary` clears the writable flag (`erts/emulator/beam/copy.c`).

The consumption pattern is read header, read chunk data, repeat, interleaved with ~1 MiB `{data, ...}` deliveries. So in steady state the buffer was sealed by the time each delivery arrived, and **every append copied the whole window**: up to 64 copies of an up-to-64 MiB buffer per window, with each superseded generation becoming binary-allocator garbage. Meanwhile each reply sub-binary (even a 303-byte header read) kept its entire window generation alive in the consumer connection's heap until that process happened to collect garbage.

## Measurement: the append/slice pattern in isolation

Simulating exactly the buffer pattern (64 deliveries of 1 MiB; 4 reads of 311 bytes served and replied to a sink process between deliveries):

| Scenario | Time |
|---|---|
| Pure appends, no reads | 30.1 ms |
| Appends + `binary:part` replies (the original pattern) | 1,192.7 ms |
| Same, but each reply `binary:copy`'d first | 43.2 ms |
| Segment/block queue (list of deliveries) | 0.3 ms |

The 40x gap between the first two rows is the seal-then-copy cycle. The pinning half was demonstrated directly: after serving one 311-byte read from a 64 MiB buffer, the receiving process's `process_info(_, binary)` showed it keeping the full **64 MiB** alive.

<details><summary>Measurement script (<code>buffer_bench.erl</code>, standalone escript)...</summary>

```erlang
-module(buffer_bench).
-export([main/1, run/3]).

-define(DELIVERY, 1 bsl 20).
-define(DELIVERIES, 64).
-define(READS_PER_DELIVERY, 4).
-define(READ_BYTES, 311).

main(_) ->
    Data = rand:bytes(?DELIVERY),
    Sink = spawn(fun sink/0),
    _ = run(pure, Data, Sink),
    lists:foreach(
        fun(Mode) ->
            {Time, GCs, Words} = measure(fun() -> run(Mode, Data, Sink) end),
            io:format("~-22s: ~8.1f ms   ~4b GCs   ~10b words GC-reclaimed~n",
                      [Mode, Time / 1000, GCs, Words])
        end,
        [pure, part_send, part_copy_send, segments]),
    Big = rand:bytes(64 bsl 20),
    Sink ! {ok, binary:part(Big, 1000, ?READ_BYTES)},
    Sink ! {report, self()},
    receive
        {binary_refs, Refs} ->
            Total = lists:sum([Sz || {_, Sz, _} <- Refs]),
            io:format("pinning: sink holds ~b bytes of reads, keeping ~b MiB of "
                      "buffer alive~n", [?READ_BYTES, Total div (1 bsl 20)])
    end,
    Sink ! stop,
    ok.

sink() ->
    receive
        stop -> ok;
        {report, From} ->
            {binary, Refs} = process_info(self(), binary),
            From ! {binary_refs, Refs},
            sink();
        _ -> sink()
    end.

measure(Fun) ->
    erlang:garbage_collect(),
    {garbage_collection, G0} = process_info(self(), garbage_collection),
    GCs0 = proplists:get_value(minor_gcs, G0),
    {_, Reclaimed0, _} = erlang:statistics(garbage_collection),
    T0 = erlang:monotonic_time(microsecond),
    Fun(),
    T1 = erlang:monotonic_time(microsecond),
    {garbage_collection, G1} = process_info(self(), garbage_collection),
    GCs1 = proplists:get_value(minor_gcs, G1),
    {_, Reclaimed1, _} = erlang:statistics(garbage_collection),
    {T1 - T0, GCs1 - GCs0, Reclaimed1 - Reclaimed0}.

run(pure, Data, _Sink) ->
    lists:foldl(fun(_, Buf0) -> <<Buf0/binary, Data/binary>> end, <<>>,
                lists:seq(1, ?DELIVERIES));
run(part_send, Data, Sink) ->
    lists:foldl(
        fun(_, Buf0) ->
            Buf = <<Buf0/binary, Data/binary>>,
            lists:foreach(
                fun(I) ->
                    Off = (I * 4096) rem (byte_size(Buf) - ?READ_BYTES),
                    Sink ! {ok, binary:part(Buf, Off, ?READ_BYTES)}
                end,
                lists:seq(1, ?READS_PER_DELIVERY)),
            Buf
        end, <<>>, lists:seq(1, ?DELIVERIES));
run(part_copy_send, Data, Sink) ->
    lists:foldl(
        fun(_, Buf0) ->
            Buf = <<Buf0/binary, Data/binary>>,
            lists:foreach(
                fun(I) ->
                    Off = (I * 4096) rem (byte_size(Buf) - ?READ_BYTES),
                    Sink ! {ok, binary:copy(binary:part(Buf, Off, ?READ_BYTES))}
                end,
                lists:seq(1, ?READS_PER_DELIVERY)),
            Buf
        end, <<>>, lists:seq(1, ?DELIVERIES));
run(segments, Data, Sink) ->
    lists:foldl(
        fun(_, Segs0) ->
            Segs = [Data | Segs0],
            lists:foreach(
                fun(_) ->
                    Sink ! {ok, binary:copy(binary:part(Data, 4096, ?READ_BYTES))}
                end,
                lists:seq(1, ?READS_PER_DELIVERY)),
            Segs
        end, [], lists:seq(1, ?DELIVERIES)).
```

</details>

## The fix: a block queue

`rabbitmq_stream_s3_read_buffer` stores the delivered binaries as-is, in offset order, addressed by absolute fragment byte positions (see [read-path.md](../read-path.md#buffer) for the design and invariants, and the module doc for the runtime rationale). The key properties:

- Appending retains the delivery as an immutable block: no binary is ever grown, so there is nothing for the runtime to seal and nothing to re-copy
- Consumed blocks are dropped whole as reads advance, freeing block-by-block
- A reply pins at most the blocks it overlaps: reads of at most 512 bytes are copied and pin nothing, larger single-block reads share a sub-binary of one ~1 MiB block, and block-spanning reads assemble a fresh binary of exactly the requested bytes

The API layer's existing 1 MiB batching (`?BUFFER_PENDING_DATA_BYTES`) becomes the block size, bounding both what a shared read can pin and the block count per window (64 at the AIMD ceiling).

### Benchmark

`gmake bench-read_buffer_bench` compares the shipped block queue against the flat-binary implementation it replaced, under the production access pattern: 1 MiB deliveries with the consumer trailing the newest delivered byte by a steady 8 MiB prefetch lag, interleaved 303-byte header and 32 KiB chunk reads replied to a sink process, consumed data discarded as the read floor advances. One iteration is a 16 MiB window.

The lag is load-bearing methodology, not decoration. The prefetch running ahead of the consumer is what the AIMD window exists for, and it is what keeps unconsumed bytes in the buffer. An early version of this benchmark had the consumer reading at the delivery edge; the old implementation's compaction then kept even the flat binary at ~1.25 MiB, and both the window-sized copying and the pinning largely vanished from the measurement (the designs looked about 160x apart, and consumer-side pinning looked near-identical). With the steady lag the buffer retains a realistic ~9 MiB unconsumed span:

The scenarios come in matched pairs so each design faces the same workload: header + chunk reads (`flat_binary` vs `block_queue`) and 2 MiB reads (`flat_binary_spanning` vs `block_queue_spanning`, where every block-queue read spans blocks and pays the assembly copy (the block queue's worst case)).

| Scenario | ips | average | p99 | GC/iter |
|---|---|---|---|---|
| `flat_binary` (regression baseline) | 25.2 | 39.6 ms | 41.4 ms | 12.0 |
| `flat_binary_spanning` | 51.1 | 19.6 ms | 22.0 ms | 3.8 |
| `block_queue` | 21.7 K | 46.2 µs | 270 µs | 1.9 |
| `block_queue_spanning` | 4.9 K | 204 µs | 295 µs | 4.0 |

~860x on the real pattern, and ~95x for the block queue's worst case against the flat binary's *best* case (2 MiB reads amortize flat's per-delivery window copy across more consumed bytes). The flat-binary scenarios are kept in the benchmark deliberately: if `block_queue` ever drifts toward them, window-sized copying has crept back into the read path.

### Memory: showing the pinning directly

Speed and GC counts are proxies; the benchmark's memory phase measures pinning itself, in the two places it hides. Deliveries are fresh 1 MiB copies here (the timing phase reuses one template binary since append cost is content-independent, but a memory measurement must allocate like production does).

**Consumer-side pinning, deterministic.** The sink retains its newest 8 replies - a consumer holding a delivery's worth of header and chunk reads in its socket send queue - then force-GCs itself and reports `process_info(self(), binary)` deduplicated per binary instance. The forced GC removes all timing dependence: what remains is exactly what those 8 live replies keep alive. Against the actual byte size of the replies this yields a pinning amplification factor:

| Scenario | Reply bytes held | Pinned | Amplification |
|---|---|---|---|
| `flat_binary` | 129.2 KiB | 9.0 MiB | 71.3x |
| `flat_binary_spanning` | 16.0 MiB | 72.0 MiB | 4.5x |
| `block_queue` | 129.2 KiB | 1.0 MiB | 7.9x |
| `block_queue_spanning` | 16.0 MiB | 16.0 MiB | 1.0x |

These numbers are exact and reproduce run over run. Reading them requires the amplification column, not the raw pinned column: `block_queue_spanning`'s 16 MiB is a consumer *choosing* to hold 16 MiB of chunk data and paying for exactly that (assembly produces fresh binaries, so nothing extra rides along), while `flat_binary`'s 9 MiB is 129 KiB of useful data dragging a full window behind it.

The two flat rows also expose an effect with real teeth: **generation multiplication**. Under the flat design every delivery produces a new buffer generation (the seal forces the copy), and replies from different read events are sub-binaries of *different* generations - overlapping content, distinct binaries, each pinned in full. `flat_binary`'s 8 retained replies all come from one read batch, so they dedup to a single 9 MiB generation; that is the flat design's *best possible* retention pattern. `flat_binary_spanning`'s 8 replies come from 8 read events, so they pin 8 distinct ~9 MiB generations: 72 MiB, more than 4x the live window, for the same 16 MiB of payload the block queue serves at 1.0x. Any consumer whose held replies span read events - which is the normal case - sits between those two rows under the flat design. Under the block queue there is nothing to multiply: blocks are shared, not superseded, so pinning is bounded by the blocks the replies overlap regardless of how many read events they span. Scaled to the AIMD ceiling, one flat generation is up to 64 MiB - per retained read event, per consumer.

### Supply x demand matrix

The scenarios above fix supply at 1 MiB blocks and demand at two read sizes. Chunks can be small or large, and the batching constant could have been chosen differently, so the benchmark also sweeps both axes: block size (what the S3 API layer's batching supplies) against read size (what the consumer demands), with a uniform-read consumer under the same 8 MiB lag and the flat binary as a baseline row. Reads ≤512 bytes are copied by `read/3`; reads larger than a block are assembled; everything between is served as a sub-binary of one or more blocks.

Average time per 16 MiB window:

| | read 303 B | read 32 KiB | read 256 KiB | read 2 MiB |
|---|---|---|---|---|
| flat, 1 MiB deliveries | 46.7 ms | 42.1 ms | 39.9 ms | 20.0 ms |
| queue, 256 KiB blocks | 17.4 ms | 131 µs | 21 µs | 207 µs |
| queue, 1 MiB blocks | 18.8 ms | 110 µs | 15 µs | 213 µs |
| queue, 4 MiB blocks | 16.0 ms | 93 µs | 12 µs | 2 µs |

Pinning amplification (sink retains its last 8 replies, then GCs):

| | read 303 B | read 32 KiB | read 256 KiB | read 2 MiB |
|---|---|---|---|---|
| flat, 1 MiB deliveries | 3893.2x | 36.0x | 9.0x | 4.5x |
| queue, 256 KiB blocks | 1.0x | 1.0x | 1.0x | 1.0x |
| queue, 1 MiB blocks | 1.0x | 4.0x | 1.0x | 1.0x |
| queue, 4 MiB blocks | 1.0x | 16.0x | 2.0x | 1.0x |

What the matrix shows:

- **Amplification has a closed form, and block size is its ceiling.** For shared reads it is the size of the blocks overlapped divided by the bytes held, so it peaks when a small retained set sits inside a big block (32 KiB reads: 4x in 1 MiB blocks, 16x in 4 MiB blocks) and falls to 1.0x when the retained set covers whole blocks. The copy threshold zeroes it for tiny reads (the 303 B column) and assembly zeroes it for block-spanning reads (the 2 MiB column). The flat row has no ceiling: its amplification is the retained window over the bytes held, which explodes as demand shrinks - **3893x** for header-sized reads
- **The 1 MiB batching constant is a reasonable middle.** 256 KiB blocks pin least but quadruple the per-window block count for slightly worse small-read timing; 4 MiB blocks buy microseconds while quadrupling the mid-size-read pinning ceiling to 16x. This is the measurement that makes `?BUFFER_PENDING_DATA_BYTES` an evidence-backed choice rather than an inherited one
- **The 303 B column is read-overhead-bound for every design** (thousands of copy+send operations per window); the block queue wins ~2.5x there, not hundreds of times. The real read path pairs each header read with a chunk-data read, which is why the headline scenarios interleave both. Everywhere else the buffer mechanics dominate and the gap is 100-4000x
- **Large reads from larger blocks are pure sharing**: 2 MiB reads out of 4 MiB blocks cost 2 µs per window and still pin at 1.0x - sharing beats assembly when the read fits a block, which is the argument for keeping blocks at least as large as common chunk sizes

**Whole-VM footprint, indicative.** A sampler process records peak `erlang:memory(binary)` over the run, against a baseline taken after garbage-collecting every process (without that quiescing step, leftovers from the previous scenario inflate the baseline and then deflate the apparent peak by dying mid-run). Peak-over-baseline captures the churn no per-process view shows - dead window generations awaiting collection plus the live working set. Representative readings: `flat_binary` ~90-130 MiB for a workload whose live window is 9 MiB, `flat_binary_spanning` ~240 MiB (generation churn plus multi-generation pinning), `block_queue` ~10-45 MiB, `block_queue_spanning` ~85-105 MiB (its 2 MiB assemblies are real allocations, freed by any GC and pinned by nothing). Unlike the pinning column this jitters run to run - it depends on where the collector happened to land - so treat it as context, not a regression gate.

## Process flags

The block queue removes the copying, but a second promptness question remained: block *references* live in the reader's gen_server state across many collections, get tenured to the old heap, and once `drop_before/2` discards them, minor GCs cannot reclaim them. An idle reader (consumer stalled, or parked at the tail) could pin its last prefetch window until a major GC that may never come. This is the same profile the connection pool already tunes for with `{receiver_spawn_opts, [{fullsweep_after, 0}]}` (`rabbitmq_stream_s3_api_aws_pool.erl`): a data pipe for large refc binaries whose own heap is small.

Measured on a reader-shaped process (200 deliveries of 1 MiB into a read buffer, a shared sub-binary read served from each, ~4-block window kept, then churn stops; "pinned" is what `process_info(Pid, binary)` shows the quiescent process still referencing, before anything forces a GC):

| Spawn flags | Pinned after churn | Loop time | GCs during loop |
|---|---|---|---|
| default | 6.0 MiB | 10.7 ms | 39 minor |
| `fullsweep_after 0` | 0.0 MiB | 5.4 ms | 0 minor (all full sweeps) |
| `fullsweep_after 10` | 6.0 MiB | 6.0 ms | 2 minor |
| `message_queue_data off_heap` alone | 6.0 MiB | 6.2 ms | 35 minor |
| `fullsweep_after 0` + `off_heap` | 0.0 MiB | 5.2 ms | 0 |

Findings:

- `fullsweep_after 0` releases everything promptly *and* runs the loop 2x faster: full sweeps of a near-empty heap are cheaper than minor collections that copy and tenure live block refs
- `fullsweep_after 10` is not a middle ground for promptness: the last major happens before the churn ends, and the process quiesces still pinning the dead window. Promptness needs every collection to be a full sweep
- `message_queue_data off_heap` is neutral in this simulation (its mailbox is empty) but is included on reasoning: the real reader's mailbox receives S3 response bodies as gun data frames with no flow control, and with every GC now a full sweep, an on-heap backlog would be scanned and copied on each one
- `min_bin_vheap_size` was considered and skipped: raising it delays collections (the opposite of what this process wants), and lowering it adds GC count with nothing to save once full sweeps are this cheap

Both flags are set at spawn in `rabbitmq_stream_s3_log_reader:init_remote_reader/2` (they are `spawn_opt`-only; `process_flag/2` cannot set `fullsweep_after`).

Consumer connection processes were left alone: the plugin does not own them, and the block queue already minimized their exposure (they pin at most one ~1 MiB block, and small reads pin nothing).

<details><summary>Measurement script (<code>flags_bench.erl</code>, run with <code>-pa ebin</code>)...</summary>

```erlang
-module(flags_bench).
-export([main/1]).

-define(BLOCK, 1 bsl 20).
-define(ITERATIONS, 200).
-define(WINDOW_BLOCKS, 4).

main(_) ->
    Template = binary:copy(<<0>>, ?BLOCK),
    Sink = spawn(fun sink/0),
    lists:foreach(
        fun({Name, SpawnOpts}) ->
            {Pinned, ElapsedUs, GCInfo} = run(Template, Sink, SpawnOpts),
            io:format(
                "~-24s pinned-after-churn: ~7.1f MiB   loop: ~6.1f ms   minor_gcs: ~p~n",
                [Name, Pinned / (1 bsl 20), ElapsedUs / 1000,
                 proplists:get_value(minor_gcs, GCInfo)]
            )
        end,
        [
            {"default flags", []},
            {"fullsweep_after=0", [{fullsweep_after, 0}]},
            {"fullsweep_after=10", [{fullsweep_after, 10}]},
            {"offheap mailbox", [{message_queue_data, off_heap}]},
            {"fullsweep0 + offheap", [{fullsweep_after, 0}, {message_queue_data, off_heap}]}
        ]
    ),
    Sink ! stop,
    ok.

sink() ->
    receive
        stop -> ok;
        _ -> sink()
    end.

run(Template, Sink, SpawnOpts) ->
    Parent = self(),
    Pid = spawn_opt(
        fun() ->
            T0 = erlang:monotonic_time(microsecond),
            Buf = churn(?ITERATIONS, rabbitmq_stream_s3_read_buffer:new(0), Template, Sink),
            _Empty = rabbitmq_stream_s3_read_buffer:drop_before(
                rabbitmq_stream_s3_read_buffer:end_pos(Buf), Buf
            ),
            T1 = erlang:monotonic_time(microsecond),
            {garbage_collection, GCInfo} = process_info(self(), garbage_collection),
            Parent ! {done, self(), T1 - T0, GCInfo},
            %% Stay alive, idle, WITHOUT a further GC, so the parent can see
            %% what a quiescent reader keeps pinned.
            receive release -> ok end
        end,
        [link | SpawnOpts]
    ),
    receive
        {done, Pid, Elapsed, GCInfo} ->
            {binary, Refs} = process_info(Pid, binary),
            Pinned = lists:sum([Sz || {_, Sz, _} <- Refs]),
            Pid ! release,
            {Pinned, Elapsed, GCInfo}
    end.

churn(0, Buf, _Template, _Sink) ->
    Buf;
churn(N, Buf0, Template, Sink) ->
    Block = binary:copy(Template),
    Buf1 = rabbitmq_stream_s3_read_buffer:append(Block, Buf0),
    EndPos = rabbitmq_stream_s3_read_buffer:end_pos(Buf1),
    Sink ! {ok, rabbitmq_stream_s3_read_buffer:read(EndPos - 65_536, 32_768, Buf1)},
    Buf =
        case rabbitmq_stream_s3_read_buffer:size(Buf1) > ?WINDOW_BLOCKS * ?BLOCK of
            true ->
                rabbitmq_stream_s3_read_buffer:drop_before(
                    EndPos - ?WINDOW_BLOCKS * ?BLOCK, Buf1
                );
            false ->
                Buf1
        end,
    churn(N - 1, Buf, Template, Sink).
```

</details>

## Follow-ups considered but not pursued here

- **iodata reads for the send path.** `read_iodata/3` exists, is exported, and is property-tested. Adopting it in `send_file` would make block-spanning chunks fully zero-copy (`gen_tcp:send`, `ssl:send`, and `erlang:crc32` all take iodata); `chunk_iterator` must keep flat reads for record parsing. Deferred: after the block queue, the remaining spanning-read copy is chunk-sized and rare (the benchmark's worst case is the `block_queue_spanning` row), so measure before changing the read contract
- **gun flow control.** The pool sets no `flow` option, so a 64 MiB range GET streams into the reader's mailbox without backpressure; a busy reader can accumulate the response in its message queue. `off_heap` makes this cheap for GC but does not bound the memory. Bounding it means per-stream `flow` plus `gun:update_flow/3` calls on consumption
- **AIMD ceiling and prefetch sizing.** `read_size_max` (64 MiB) plus a full-window next-fragment prefetch means up to ~128 MiB of intentional buffer per remote consumer. With the accidental overhead gone, this is now the dominant per-consumer memory term; a smaller initial prefetch for the *next* fragment, or a ceiling informed by consumer count, would flatten the worst case. Since addressed by the pipelined prefetch of [#349](https://github.com/amazon-mq/rabbitmq-stream-s3/issues/349): a single prefetch window now spans both fragments and is capped at `prefetch_window_max` (32 MiB, an application environment setting), so the per-consumer ceiling is that plus one request
- **Replica reader flags.** The upload path's replica reader is also binary-heavy, but it holds real heap state (fragment assembly); the fullsweep-0 trade-off needs its own measurement rather than assumption
