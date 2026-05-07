# Instructions for AI Agents

This file covers the `rabbitmq_stream_s3` plugin specifically. For general RabbitMQ conventions (comments, git, building, testing), see the parent repository's `AGENTS.md` at the root of [`rabbitmq/rabbitmq-server`](https://github.com/rabbitmq/rabbitmq-server).

## Before you start

Read the relevant documentation before modifying any module:

- `docs/README.md` — overview and reading guide
- `docs/DEVELOPMENT.md` — building, testing, formatting
- `docs/concepts.md` — streaming primitives and how the plugin extends them
- `docs/architecture.md` — how the pieces fit together
- `docs/conventions.md` — patterns and conventions for writing code here

## Building and Testing

See [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) for build commands, test invocation, CT log navigation, and formatting.

## Writing Tests

**Your goal is not to make tests pass.** Your goal is to write correct code that the tests validate. If a test fails, that is information. If a test hangs, that is information. Do not chase green. Follow the priorities below.

### Priorities (in order)

0. **Tests are evidence, not checkboxes.** Running a test suite is an investigative act. You are collecting evidence about whether the code is correct. You are not confirming a result. If you catch yourself re-running to "check if it passes," you are chasing green. Stop. Run once, preserve the output, read it.

1. **Investigate the root cause.** A root cause is the bedrock explanation. It fully explains the failure, irrefutably, and leaves no room to dive deeper. A timeout is not a root cause. A `badmatch` is not a root cause. "Function X is called with argument Y but expects Z because of W" is a root cause. Keep asking "why" until you reach a fact that makes the failure inevitable and obvious. Read source code. Read logs. If you cannot determine the root cause from available evidence, stop and ask.

2. **Improve the test framework.** Make the failure obvious from the error message so future investigators don't need to dig. Better diagnostics on timeout. Better assertion messages. Better helpers.

3. **Fix the code.** Only after you understand the root cause and its design implications. If the fix requires a design decision (e.g. when to fire an offset listener, what happens on recovery), stop and consult.

### Rules

- Never change test semantics to make a test green.
- Never change production semantics (e.g. `>=` to `>`) as a reaction to a test failure without understanding why.
- Never add error handling to production code to make a test pass.
- Never speculate about causes without evidence. If you don't have logs or a stack trace, get them first.
- Never run commands without timeouts. A hanging test is a symptom; killing it and retrying is not investigation.

### When to stop and ask

If you encounter any of the following, stop and present what you know:

- A failure whose root cause involves process lifecycle, message ordering, or barrier semantics that you cannot fully trace.
- A fix that would require changing how processes interact (hooks, listeners, registration).
- A design question about when state becomes visible, when side effects should fire, or what guarantees a barrier provides.
- Two failed attempts at the same problem. Step back, explain what you tried and what you observed, and ask for direction.

### Running tests

- Always use `timeout` when running make targets: `timeout 30 make ct-...`
- If a test hangs, that is a bug. Do not retry. Investigate.
- Always use `make test-build`, never `erlc` directly.
- Tee output to a file so you can grep without re-running:

```bash
timeout 120 make ct-replica_reader 2>&1 | tee /tmp/ct-out.txt
grep -E 'pass|fail|Error' /tmp/ct-out.txt
```

- See [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) for make targets, CT log navigation, and formatting.

### Deterministic test construction

See [docs/conventions.md](docs/conventions.md) for the test infrastructure (seed_log, build_manifest, barriers) and patterns for writing deterministic tests.

## Writing documentation

- Simple and direct sentences.
- No em dashes.
- No hard wrapping. Each paragraph is a single line.
- Derive style from the existing documentation in `docs/`.

## Making knowledge durable

Knowledge learned during implementation must be placed where it will survive:

- Implementation constraints and "why" explanations belong as comments in the code, next to the relevant line.
- Architecture and design decisions belong in `docs/`.
- Testing patterns and conventions belong in `docs/conventions.md`.
- If you learn something surprising (a framework quirk, a type mismatch, a non-obvious ordering requirement), capture the learning in a comment.
