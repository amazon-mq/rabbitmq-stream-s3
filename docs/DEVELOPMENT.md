# Development

How to build, test, and format code in this plugin.

## Prerequisites

- Erlang/OTP 27 or later
- GNU Make 4 (on some systems this is `gmake`, on others `make`)

## Getting the source

This plugin lives inside the RabbitMQ server tree as a dependency. You do not clone it standalone.

```bash
git clone https://github.com/amazon-mq/upstream-to-rabbitmq-server.git
cd upstream-to-rabbitmq-server
```

The plugin is on the default branch, `main`, which is what the clone checks out.

The plugin source is at `deps/rabbitmq_stream_s3/`. All build and test commands run from that directory:

```bash
cd deps/rabbitmq_stream_s3
```

## Building

```bash
# Build everything including test dependencies
gmake test-build
```

Always use `gmake test-build` rather than compiling individual files with `erlc`. Manual compilation leaves stale beams in unexpected directories (e.g. `test/`) that shadow the real beams in `ebin/`, causing silent failures.

## Running tests

```bash
# Run all Common Test suites
gmake ct

# Run the fast suites (excludes broker_SUITE and other slow integration tests)
gmake ct-quick

# Run a specific suite
gmake ct-replica_reader

# Run a specific test case
gmake t=single_node:uploads_fragments ct-replica_reader

# Eunit (inline tests in source modules)
gmake eunit

# Dialyzer type check
gmake dialyze

# Cross-reference check for calls to undefined functions
gmake xref
```

### CT logs

After a test run, open the HTML report in your browser:

```bash
open logs/index.html
```

The logs contain the full truth: `ct:pal` output, stack traces, and process state. To search them from the command line:

```bash
# Find the most recent CT run directory:
find logs -name "ct_run*" -type d | sort | tail -1

# Search within it for diagnostic output:
grep -r 'your_diagnostic' <ct_run_dir>/ | sed 's/<[^>]*>//g'
```

## Coverage

```bash
# Run tests with coverage enabled
gmake ct COVER=1
gmake eunit COVER=1

# Or both together
gmake ct eunit COVER=1

# Generate HTML report from collected data
gmake cover-report
open cover/index.html
```

Coverage data files land in `cover/` (ct.coverdata, eunit.coverdata). The report shows per-module line coverage with annotated source.

## Benchmarks

Benchmarks live in `test/*_bench.erl` and run on the `rabbitmq_stream_s3_bench` harness (warmup, timed iterations in an isolated process, iterations-per-second and percentile report with a comparison against the fastest scenario).

```bash
# Run all benchmarks
gmake bench

# Run one benchmark module
gmake bench-read_buffer_bench
```

Benchmarks are for humans comparing before/after numbers on a quiet machine. They are deliberately not CI-gated and assert nothing: relative performance assertions are flaky on shared runners, and a benchmark that must pass gets tuned until it does. When a change touches a hot path (the read or upload path), run the relevant benchmark before and after and put both numbers in the commit message or PR.

To add one, create `test/<name>_bench.erl` exporting `run/0` that calls `rabbitmq_stream_s3_bench:run/1,2` with named scenario funs; `gmake bench` picks it up by filename. Keep a scenario's iteration meaningful (one window of work, not one tiny operation) and keep the default runtime in seconds, not minutes.

Wall time is the primary signal. In particular, do not trust `tprof`'s `call_memory` view for binary-heavy paths: it counts heap words and cannot see refc binary payloads, which is precisely what the plugin's hot paths move.

For memory questions the harness provides `sample_binary_memory/1` (peak VM binary memory over a run, baselined after garbage-collecting every process). Peak readings are GC-timing dependent and jitter run to run; for a deterministic number, measure what live references pin after a forced GC - see the memory phase of `read_buffer_bench` for the pattern (a sink retains the last N replies, GCs itself, and reports its deduplicated `process_info(_, binary)` total).

### The remote-reader S3 harness

`remote_reader_s3_bench` measures the remote read path against a real object store. It lives in `test/*_bench.erl` and `gmake bench` picks it up, but it does not use the `rabbitmq_stream_s3_bench` harness and does not measure the code's own speed. The real reader, `api_aws` client, connection pool and `gun` all run for real against MinIO, so connection reuse, pool growth, HTTP framing and byte accounting are executed rather than modelled. Only the wire is shaped.

```bash
gmake s3-bench-up                        # MinIO (needs podman or docker)
gmake bench-remote_reader_s3_bench       # one configuration
./scripts/s3-bench-sweep.sh depth 8 16 32
gmake s3-bench-down
```

With the store down it skips cleanly, so `gmake bench` stays green.

`gmake bench-remote_reader_s3_bench` dials the port MinIO publishes on the host, which is unshaped: `tc netem` applies on the bridge, and a run outside the network namespace does not cross it. Use the sweep for anything where latency is part of the question.

**One configuration per OS process.** A scenario sweeping in-process inherits the previous one's warm pool and grown prefetch window, so it reads faster for reasons unrelated to what it is measuring. `scripts/s3-bench-sweep.sh` restarts the wire and the VM for every point; parameters come from `S3B_*` environment variables.

**Give runs time to leave the ramp.** Three things start small: the prefetch window grows on misses, the pool starts at `min_size`, and with `S3B_AUTO_TUNE=1` the concurrency search starts at one request and doubles per sample. The same configuration measured 140.7 MiB/s over 0.9 s and 219.3 MiB/s over 9.3 s. Budget for ten seconds or more.

**Read every result against the substrate.** `S3B_SUBSTRATE=1` measures what the store delivers at a given concurrency with the reader taken out. A reader figure that tracks that line is measuring MinIO, not the prefetch policy.

**Validate before trusting it.** `gmake s3-bench-validate` runs the configurations that were actually stress-tested and prints each beside its measured result. Nothing there is fitted per scenario — only the broker's own configuration and each stream's manifest shape (derived from fragment count against `rebalance_threshold`). Two of the three scenarios reproduce within ~6%; the third is a known outlier.

**Latency comes from `tc netem`.** Nothing sits between the client and MinIO. Delaying packets in a kernel queue lets the transfer pipeline through it, so the delay lands once as time to first byte, which is what latency costs a range GET; delaying at the application layer would charge it per chunk and scale the cost with transfer size. Set `S3B_LATENCY_MS`; a sidecar applies the qdisc inside MinIO's network namespace.

**The client's own connection was often the limit.** `S3B_DRAIN_MIBS` caps the consumer's drain rate; the stress runs were bound by a single client TCP flow near 589 MiB/s, and scenarios that were client-bound cannot be reproduced without it.

**Latency needs socket buffers to match.** A connection sustains `window / rtt`, so 40 MB/s at 22 ms needs an 859 KB window. With the default `net.core.rmem_max` the window pins near 90 KB and every connection is window-limited — indistinguishable from a slow reader. `setsockopt` does not fail when it cannot honour the size asked for, so the only symptom is a number that looks like a slow reader. The harness reads `net.core.rmem_max`, and when the cap is below what `S3B_RECBUF_KB` asked for it prints a `WARNING` line with the ceiling that implies at the modelled latency. Raise the limits first, which is the same tuning production needed:

```bash
sudo sysctl -w net.core.rmem_max=16777216 net.core.wmem_max=16777216
```

## Formatting

All Erlang source is formatted with `erlfmt`:

```bash
# Check formatting (fails if anything needs reformatting)
gmake fmt-check

# Apply formatting
gmake fmt
```

If `erlfmt` is not on your PATH, set the `ERLFMT` variable:

```bash
gmake fmt ERLFMT=/path/to/erlfmt
```

Format after finishing a change, not during debugging.
