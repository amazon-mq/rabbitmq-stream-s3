%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_config).
-moduledoc """
Accessors for the `rabbitmq_stream_s3` application environment.

Every application:get_env/2,3 call for the `rabbitmq_stream_s3` application
lives here. Callers use these functions instead of calling
`application:get_env/2,3` directly.
""".

-include("include/rabbitmq_stream_s3.hrl").

-export([
    api_backend/0,
    account_id/0,
    aws_access_key/0,
    aws_secret_key/0,
    aws_security_token/0,
    allow_static_credentials/0,
    aws_region/0,
    aws_region_endpoints/0,
    bucket/0,
    api_fs_data_dir/0,
    upload_pool_min_size/0,
    upload_pool_max_size/0,
    general_pool_min_size/0,
    general_pool_max_size/0,
    prefetch_request_size/0,
    prefetch_window_max/0,
    prefetch_max_depth/0,
    fragment_target_size/0,
    persist_threshold/0,
    persist_interval_ms/0,
    rebalance_threshold/0,
    membership_reconciliation_enabled/0,
    membership_reconciliation_interval/0,
    membership_reconciliation_trigger_interval/0,
    membership_reconciliation_target_group_size/0,
    membership_reconciliation_auto_remove/0,
    reconciliation_enabled/0,
    reconciliation_interval/0,
    gc_enabled/0,
    gc_interval/0,
    gc_mode/0,
    bucket_check_enabled/0,
    bucket_check_interval/0,
    task_retry_delay_max_ms/0,
    task_retry_delay_constant/0,
    task_retry_delay_exponent/0,
    upload_retry_delay_max_ms/0,
    verbose_logging/0,
    segment_upload_timeout/0,
    upload_retry_delay_ms/0,
    transfer_deadline_ms/0,
    retention_task_timeout/0,
    tick_timeout_milliseconds/0,
    max_transfer_bytes_per_sec/0,
    max_transfer_burst_bytes/0,
    verify_crc_on_read/0,
    kms_key_id/0
]).

-define(APP, rabbitmq_stream_s3).

%% The API backend module. Defaults to the AWS implementation.
-spec api_backend() -> module().
api_backend() ->
    application:get_env(?APP, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_aws).

%% AWS credentials. Return `undefined` when not configured (instance role is used).
-spec aws_access_key() -> binary() | undefined.
aws_access_key() ->
    case application:get_env(?APP, aws_access_key) of
        {ok, V} -> V;
        undefined -> undefined
    end.

-spec aws_secret_key() -> binary() | undefined.
aws_secret_key() ->
    case application:get_env(?APP, aws_secret_key) of
        {ok, V} -> V;
        undefined -> undefined
    end.

-spec aws_security_token() -> binary() | undefined.
aws_security_token() ->
    application:get_env(?APP, aws_security_token, undefined).

%% Whether static credentials from rabbitmq.conf are honored. Off by default:
%% they are long-lived, stored in plaintext on disk, and never rotated. When
%% this is false, configured static credentials are ignored and the plugin
%% falls back to container or EC2 instance credentials.
-spec allow_static_credentials() -> boolean().
allow_static_credentials() ->
    application:get_env(?APP, allow_static_credentials, false).

-spec aws_region() -> binary() | undefined.
aws_region() ->
    case application:get_env(?APP, aws_region) of
        {ok, V} -> V;
        undefined -> undefined
    end.

%% Overrides for the default region-to-TLD mapping.
-spec aws_region_endpoints() -> #{binary() => binary()}.
aws_region_endpoints() ->
    application:get_env(?APP, aws_region_endpoints, #{}).

%% Required. Crashes with badmatch if not configured.
-spec bucket() -> binary().
bucket() ->
    {ok, Bucket} = application:get_env(?APP, bucket),
    Bucket.

%% The AWS account ID that owns the data bucket. Optional: when set, it is sent
%% as x-amz-expected-bucket-owner on every S3 request so that S3 rejects the
%% request if the bucket is owned by another account. The header is omitted when
%% this returns `undefined`.
-spec account_id() -> binary() | undefined.
account_id() ->
    application:get_env(?APP, account_id, undefined).

-spec api_fs_data_dir() -> file:filename_all() | undefined.
api_fs_data_dir() ->
    application:get_env(?APP, api_fs_data_dir, undefined).

-spec upload_pool_min_size() -> non_neg_integer().
upload_pool_min_size() ->
    application:get_env(?APP, upload_pool_min_size, 0).

-spec upload_pool_max_size() -> pos_integer().
upload_pool_max_size() ->
    application:get_env(?APP, upload_pool_max_size, 20).

-spec general_pool_min_size() -> non_neg_integer().
general_pool_min_size() ->
    application:get_env(?APP, general_pool_min_size, 2).

%% The general pool also serves manifest, group and index GETs, and every
%% remote reader can hold `prefetch_max_depth` connections at once, so the
%% ceiling has to cover several lagging consumers per node. Connections are
%% opened on demand and closed when idle, so a high ceiling costs nothing at
%% rest; exhausting it degrades a reader's depth rather than failing its reads.
-spec general_pool_max_size() -> pos_integer().
general_pool_max_size() ->
    application:get_env(?APP, general_pool_max_size, 200).

%% ------------------------------------------------------------------
%% Remote read prefetch
%%
%% A single S3 connection transfers at roughly 40 MB/s whatever range size is
%% asked of it, so a remote reader's bandwidth is set by how many range GETs it
%% runs concurrently. Request size is fixed and the prefetch window is what
%% adapts; see rabbitmq_stream_s3_remote_reader_core.
%% ------------------------------------------------------------------

%% Bytes per range GET. Large enough to amortise time-to-first-byte over the
%% transfer, small enough that a window is several requests wide.
-spec prefetch_request_size() -> pos_integer().
prefetch_request_size() ->
    application:get_env(?APP, prefetch_request_size, 4_194_304).

%% Ceiling on how far ahead of the consumer a reader fetches, and so on its
%% memory: it holds or has outstanding at most this plus one request.
-spec prefetch_window_max() -> pos_integer().
prefetch_window_max() ->
    application:get_env(?APP, prefetch_window_max, 33_554_432).

%% Most range GETs one reader may have in flight. Also its share of the general
%% connection pool.
-spec prefetch_max_depth() -> pos_integer().
prefetch_max_depth() ->
    application:get_env(?APP, prefetch_max_depth, 8).

%% Target byte size at which the replica reader cuts a fragment for upload.
-spec fragment_target_size() -> pos_integer().
fragment_target_size() ->
    application:get_env(?APP, fragment_target_size, ?MAX_FRAGMENT_SIZE_B).

%% Number of applied fragments after which a manifest persist is triggered.
-spec persist_threshold() -> non_neg_integer().
persist_threshold() ->
    application:get_env(?APP, persist_threshold, 5).

%% Maximum time between manifest persists, in milliseconds. Bounds the persist
%% window when fragments arrive slowly.
-spec persist_interval_ms() -> non_neg_integer().
persist_interval_ms() ->
    application:get_env(?APP, persist_interval_ms, 2000).

%% Manifest-tree branching factor: the number of same-kind leading entries in
%% the root that triggers factoring them out into a group of the next-higher
%% kind. A smaller value reduces memory footprint but increases the number of
%% remote-tier requests during factoring and search; a larger value does the
%% reverse.
-spec rebalance_threshold() -> pos_integer().
rebalance_threshold() ->
    application:get_env(?APP, rebalance_threshold, 1024).

-spec membership_reconciliation_enabled() -> boolean().
membership_reconciliation_enabled() ->
    application:get_env(?APP, membership_reconciliation_enabled, false).

-spec membership_reconciliation_interval() -> non_neg_integer().
membership_reconciliation_interval() ->
    application:get_env(?APP, membership_reconciliation_interval, 60_000 * 60).

-spec membership_reconciliation_trigger_interval() -> non_neg_integer().
membership_reconciliation_trigger_interval() ->
    application:get_env(?APP, membership_reconciliation_trigger_interval, 10_000).

-spec membership_reconciliation_target_group_size() -> pos_integer() | undefined.
membership_reconciliation_target_group_size() ->
    application:get_env(?APP, membership_reconciliation_target_group_size, undefined).

-spec membership_reconciliation_auto_remove() -> boolean().
membership_reconciliation_auto_remove() ->
    application:get_env(?APP, membership_reconciliation_auto_remove, false).

%% Periodic reconciliation of the plugin's attachment to local osiris
%% processes: a replica reader bound to each live writer, and a registered
%% manifest_replica context for each live replica. Recovers a stream left
%% un-tiered by a writer-restart race, a parked reader, or a manifest_replica
%% restart that lost its in-memory contexts and cache.
-spec reconciliation_enabled() -> boolean().
reconciliation_enabled() ->
    application:get_env(?APP, reconciliation_enabled, true).

-spec reconciliation_interval() -> non_neg_integer().
reconciliation_interval() ->
    application:get_env(?APP, reconciliation_interval, 60_000).

%% Periodic probe of the configured bucket's accessibility (it exists and the
%% credentials may use it). A misconfigured or unreachable bucket does not stop
%% the stream working on local disk, so this only surfaces the condition via
%% logs, the `bucket_accessible` metric, and the status CLI; it never blocks
%% publishers.
-spec bucket_check_enabled() -> boolean().
bucket_check_enabled() ->
    application:get_env(?APP, bucket_check_enabled, true).

-spec bucket_check_interval() -> non_neg_integer().
bucket_check_interval() ->
    application:get_env(?APP, bucket_check_interval, 300_000).

%% Whether a delete-mode GC sweep runs automatically on an interval. A sweep is
%% a bucket-wide LIST plus one strongly-consistent metadata read per stream, so
%% it is off by default; orphan GC via the CLI remains available on demand.
-spec gc_enabled() -> boolean().
gc_enabled() ->
    application:get_env(?APP, gc_enabled, false).

%% Interval between automatic GC sweeps, in milliseconds. Defaults to 24 hours:
%% stragglers and tombstones are not urgent, and each sweep is comparatively
%% heavy.
-spec gc_interval() -> non_neg_integer().
gc_interval() ->
    application:get_env(?APP, gc_interval, 86_400_000).

%% Mode an automatic GC sweep runs in. `delete` reclaims dangling objects;
%% `dry_run` only identifies and logs them. Defaults to `delete`.
-spec gc_mode() -> rabbitmq_stream_s3_gc:mode().
gc_mode() ->
    application:get_env(?APP, gc_mode, delete).

-spec task_retry_delay_max_ms() -> non_neg_integer().
task_retry_delay_max_ms() ->
    application:get_env(?APP, task_retry_delay_max_ms, 5_000).

-spec task_retry_delay_constant() -> non_neg_integer().
task_retry_delay_constant() ->
    application:get_env(?APP, task_retry_delay_constant, 10).

-spec task_retry_delay_exponent() -> non_neg_integer().
task_retry_delay_exponent() ->
    application:get_env(?APP, task_retry_delay_exponent, 2).

-spec verbose_logging() -> boolean().
verbose_logging() ->
    application:get_env(?APP, verbose_logging, false).

-spec segment_upload_timeout() -> non_neg_integer().
segment_upload_timeout() ->
    application:get_env(?APP, segment_upload_timeout, 45_000).

%% Base delay before retrying a fragment upload that failed with a non-transient
%% error. The upload pipeline stalls at the failed offset until the retry
%% succeeds. Successive non-transient retries back off exponentially from this
%% base (using task_retry_delay_exponent) up to upload_retry_delay_max_ms, so a
%% persistently failing upload is not retried tightly.
-spec upload_retry_delay_ms() -> non_neg_integer().
upload_retry_delay_ms() ->
    application:get_env(?APP, upload_retry_delay_ms, 1000).

%% Ceiling for the non-transient upload-retry backoff. A confirmed-fatal error
%% (a checksum mismatch, an unexpected 4xx) is unlikely to clear on a tight
%% retry, so the backoff is allowed to grow well beyond the transient ceiling.
-spec upload_retry_delay_max_ms() -> non_neg_integer().
upload_retry_delay_max_ms() ->
    application:get_env(?APP, upload_retry_delay_max_ms, 30_000).

%% Deadline for a submitted fragment transfer to report a result back to the
%% replica reader. The reader submits each transfer to the per-node governor
%% and waits for a `{transfer_result, Ref, _}` message. That message can fail
%% to arrive: the governor can crash while the submission sits in its pending
%% queue (only reachable under a finite rate limit), the spawned task can be
%% killed externally (e.g. by the OOM killer) before it replies, or the
%% message can otherwise be lost. None of those produce a result, so without a
%% deadline the in-flight queue head never drains, `next_offset` is pinned, and
%% the stream's uploads stall silently and permanently. On expiry the reader
%% resubmits the transfer under the same reference (see the transfer_deadline
%% handling in rabbitmq_stream_s3_replica_reader).
%%
%% The default is a generous multiple of segment_upload_timeout so a healthy
%% but slow upload (including time spent queued behind the governor's token
%% bucket) is never resubmitted spuriously. A spurious early resubmit is safe
%% regardless: the resubmit reuses the same reference, the core accounts the
%% first result to arrive and drops the duplicate, and the losing upload's S3
%% object becomes an orphan that GC reclaims. Correctness does not depend on
%% the value; only efficiency does.
-spec transfer_deadline_ms() -> non_neg_integer().
transfer_deadline_ms() ->
    application:get_env(?APP, transfer_deadline_ms, segment_upload_timeout() * 4).

-spec retention_task_timeout() -> non_neg_integer().
retention_task_timeout() ->
    application:get_env(?APP, retention_task_timeout, 60_000).

-spec tick_timeout_milliseconds() -> non_neg_integer().
tick_timeout_milliseconds() ->
    application:get_env(?APP, tick_timeout_milliseconds, 5000).

-spec max_transfer_bytes_per_sec() -> pos_integer() | unlimited.
max_transfer_bytes_per_sec() ->
    application:get_env(?APP, max_transfer_bytes_per_sec, unlimited).

-spec max_transfer_burst_bytes() -> pos_integer() | undefined.
max_transfer_burst_bytes() ->
    application:get_env(?APP, max_transfer_burst_bytes, undefined).

-spec verify_crc_on_read() -> boolean().
verify_crc_on_read() ->
    application:get_env(?APP, verify_crc_on_read, false).

-spec kms_key_id() -> binary() | undefined.
kms_key_id() ->
    application:get_env(?APP, kms_key_id, undefined).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

defaults_test_() ->
    [
        ?_assertEqual(rabbitmq_stream_s3_api_aws, api_backend()),
        ?_assertEqual(undefined, aws_access_key()),
        ?_assertEqual(undefined, aws_secret_key()),
        ?_assertEqual(undefined, aws_security_token()),
        ?_assertEqual(false, allow_static_credentials()),
        ?_assertEqual(undefined, aws_region()),
        ?_assertEqual(undefined, account_id()),
        ?_assertEqual(#{}, aws_region_endpoints()),
        ?_assertEqual(undefined, api_fs_data_dir()),
        ?_assertEqual(0, upload_pool_min_size()),
        ?_assertEqual(20, upload_pool_max_size()),
        ?_assertEqual(2, general_pool_min_size()),
        ?_assertEqual(200, general_pool_max_size()),
        ?_assertEqual(4_194_304, prefetch_request_size()),
        ?_assertEqual(33_554_432, prefetch_window_max()),
        ?_assertEqual(8, prefetch_max_depth()),
        ?_assertEqual(?MAX_FRAGMENT_SIZE_B, fragment_target_size()),
        ?_assertEqual(5, persist_threshold()),
        ?_assertEqual(2000, persist_interval_ms()),
        ?_assertEqual(1024, rebalance_threshold()),
        ?_assertEqual(false, membership_reconciliation_enabled()),
        ?_assertEqual(3_600_000, membership_reconciliation_interval()),
        ?_assertEqual(10_000, membership_reconciliation_trigger_interval()),
        ?_assertEqual(undefined, membership_reconciliation_target_group_size()),
        ?_assertEqual(false, membership_reconciliation_auto_remove()),
        ?_assertEqual(true, reconciliation_enabled()),
        ?_assertEqual(60_000, reconciliation_interval()),
        ?_assertEqual(false, gc_enabled()),
        ?_assertEqual(86_400_000, gc_interval()),
        ?_assertEqual(delete, gc_mode()),
        ?_assertEqual(true, bucket_check_enabled()),
        ?_assertEqual(300_000, bucket_check_interval()),
        ?_assertEqual(5_000, task_retry_delay_max_ms()),
        ?_assertEqual(10, task_retry_delay_constant()),
        ?_assertEqual(2, task_retry_delay_exponent()),
        ?_assertEqual(false, verbose_logging()),
        ?_assertEqual(45_000, segment_upload_timeout()),
        ?_assertEqual(1000, upload_retry_delay_ms()),
        ?_assertEqual(30_000, upload_retry_delay_max_ms()),
        ?_assertEqual(180_000, transfer_deadline_ms()),
        ?_assertEqual(60_000, retention_task_timeout()),
        ?_assertEqual(5000, tick_timeout_milliseconds()),
        ?_assertEqual(false, verify_crc_on_read()),
        ?_assertEqual(undefined, kms_key_id())
    ].

configured_test_() ->
    {foreach, fun() -> ok end, fun(_) -> application:unset_env(rabbitmq_stream_s3, bucket) end, [
        fun(_) ->
            application:set_env(rabbitmq_stream_s3, bucket, <<"my-bucket">>),
            ?_assertEqual(<<"my-bucket">>, bucket())
        end
    ]}.

-endif.
