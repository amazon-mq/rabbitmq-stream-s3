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
git checkout streams-tiered-storage
```

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

## Formatting

All Erlang source is formatted with `erlfmt`:

```bash
# Check formatting (fails if anything needs reformatting)
erlfmt -c src/*.erl test/*.erl

# Apply formatting
erlfmt -w src/*.erl test/*.erl
```

Format after finishing a change, not during debugging.
