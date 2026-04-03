# Prometheus Metrics

The `rabbitmq_stream_s3` plugin depends on `rabbitmq_prometheus`, which is
implicitly enabled when `rabbitmq_stream_s3` is enabled. Metrics are exposed
at the Prometheus endpoint `/metrics` (port 15692).

All metrics use the `rabbitmq_stream_s3_` prefix and carry a `module` label
identifying the source module.

## Counters

### Upload and Retention (`rabbitmq_stream_s3_server`)

| Metric | Type | Description |
|--------|------|-------------|
| `active_tasks` | gauge | Current number of in-flight tasks (uploads, deletions) |
| `total_tasks` | counter | Total tasks spawned |
| `task_failures` | counter | Task crashes (each failure triggers a retry with exponential backoff) |
| `fragments_created` | counter | Fragment objects uploaded to S3 |
| `groups_created` | counter | Group manifest objects created |
| `kilo_groups_created` | counter | Kilo-group manifest objects created |
| `mega_groups_created` | counter | Mega-group manifest objects created |
| `roots_created` | counter | Root manifest objects uploaded |
| `manifests_resolved` | counter | Non-empty manifest resolutions (on startup) |
| `manifests_resolved_empty` | counter | Empty manifest resolutions (on startup) |
| `fragments_deleted` | counter | Fragment objects deleted by retention (does not count stream deletion) |
| `groups_deleted` | counter | Group objects deleted by retention |
| `kilo_groups_deleted` | counter | Kilo-group objects deleted by retention |
| `mega_groups_deleted` | counter | Mega-group objects deleted by retention |
| `streams_deleted` | counter | Streams fully deleted from S3 |
| `local_tier_retention_evaluations` | counter | Local tier retention evaluations |
| `remote_tier_retention_evaluations` | counter | Remote tier retention evaluations |

### Metadata Store (`rabbitmq_stream_s3_db`)

| Metric | Type | Description |
|--------|------|-------------|
| `sproc_triggers` | counter | Khepri stored procedure triggers |
| `gets` | counter | Khepri get requests |
| `puts` | counter | Khepri put requests |
| `put_successes` | counter | Successful Khepri puts |
| `put_conflicts` | counter | Khepri put conflicts (concurrent writers) |
| `put_not_founds` | counter | Khepri put-not-found errors |
| `put_errors` | counter | Khepri put errors |

### Per-Operation (`rabbitmq_stream_s3_api`)

| Metric | Type | Description |
|--------|------|-------------|
| `get` | counter | Full-object GET requests |
| `get_range` | counter | Range GET requests (remote tier reads) |
| `put` | counter | PUT requests (fragment and manifest uploads) |
| `delete_many` | counter | Multi-object DELETE requests (retention) |
| `delete_one` | counter | Single-object DELETE requests |
| `list` | counter | LIST requests (`delete_prefix` counts as one regardless of pagination) |

### HTTP Transport (`rabbitmq_stream_s3_api_aws`)

| Metric | Type | Description |
|--------|------|-------------|
| `active_requests` | gauge | Current number of in-flight S3 requests |
| `total_requests` | counter | Total S3 API requests (includes internal pagination) |
| `response_500` | counter | HTTP 500 responses from S3 |
| `response_503` | counter | HTTP 503 responses from S3 |

## Histogram

### S3 Request Duration

```
rabbitmq_stream_s3_request_duration_seconds
```

A histogram of S3 API request duration in seconds with a `kind` label:
- `kind="read"` — GET requests (full-object and range)
- `kind="write"` — PUT, DELETE, and POST requests

Buckets: 10ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, +Inf
