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

To add one, create `test/<name>_bench.erl` exporting `run/0` that calls `rabbitmq_stream_s3_bench:run/1,2` with named scenario funs; `make bench` picks it up by filename. Keep a scenario's iteration meaningful (one window of work, not one tiny operation) and keep the default runtime in seconds, not minutes.

Wall time is the primary signal. In particular, do not trust `tprof`'s `call_memory` view for binary-heavy paths: it counts heap words and cannot see refc binary payloads, which is precisely what the plugin's hot paths move.

For memory questions the harness provides `sample_binary_memory/1` (peak VM binary memory over a run, baselined after garbage-collecting every process). Peak readings are GC-timing dependent and jitter run to run; for a deterministic number, measure what live references pin after a forced GC - see the memory phase of `read_buffer_bench` for the pattern (a sink retains the last N replies, GCs itself, and reports its deduplicated `process_info(_, binary)` total).

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
