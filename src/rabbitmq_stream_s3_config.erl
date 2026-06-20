%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_config).
-moduledoc """
Accessors for the `rabbitmq_stream_s3` application environment.

Every application:get_env/2,3 call for the `rabbitmq_stream_s3` application
lives here. Callers use these functions instead of calling
`application:get_env/2,3` directly.
""".

-export([
    api_backend/0,
    aws_access_key/0,
    aws_secret_key/0,
    aws_security_token/0,
    aws_region/0,
    aws_region_endpoints/0,
    bucket/0,
    api_fs_data_dir/0,
    upload_pool_min_size/0,
    upload_pool_max_size/0,
    general_pool_min_size/0,
    general_pool_max_size/0,
    manifest_rebalance_factor/0,
    manifest_debounce_modifications/0,
    manifest_debounce_milliseconds/0,
    membership_reconciliation_enabled/0,
    membership_reconciliation_interval/0,
    membership_reconciliation_trigger_interval/0,
    membership_reconciliation_target_group_size/0,
    membership_reconciliation_auto_remove/0,
    reconciliation_enabled/0,
    reconciliation_interval/0,
    task_retry_delay_max_ms/0,
    task_retry_delay_constant/0,
    task_retry_delay_exponent/0,
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

-spec general_pool_max_size() -> pos_integer().
general_pool_max_size() ->
    application:get_env(?APP, general_pool_max_size, 50).

-spec manifest_rebalance_factor() -> pos_integer().
manifest_rebalance_factor() ->
    application:get_env(?APP, manifest_rebalance_factor, 1024).

-spec manifest_debounce_modifications() -> non_neg_integer().
manifest_debounce_modifications() ->
    application:get_env(?APP, manifest_debounce_modifications, 10).

-spec manifest_debounce_milliseconds() -> non_neg_integer().
manifest_debounce_milliseconds() ->
    application:get_env(?APP, manifest_debounce_milliseconds, 5000).

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

%% Delay before retrying a fragment upload that failed with a non-transient
%% error. The upload pipeline stalls at the failed offset until the retry
%% succeeds, so this bounds how often a persistently failing upload is retried.
-spec upload_retry_delay_ms() -> non_neg_integer().
upload_retry_delay_ms() ->
    application:get_env(?APP, upload_retry_delay_ms, 1000).

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
        ?_assertEqual(undefined, aws_region()),
        ?_assertEqual(#{}, aws_region_endpoints()),
        ?_assertEqual(undefined, api_fs_data_dir()),
        ?_assertEqual(0, upload_pool_min_size()),
        ?_assertEqual(20, upload_pool_max_size()),
        ?_assertEqual(2, general_pool_min_size()),
        ?_assertEqual(50, general_pool_max_size()),
        ?_assertEqual(1024, manifest_rebalance_factor()),
        ?_assertEqual(10, manifest_debounce_modifications()),
        ?_assertEqual(5000, manifest_debounce_milliseconds()),
        ?_assertEqual(false, membership_reconciliation_enabled()),
        ?_assertEqual(3_600_000, membership_reconciliation_interval()),
        ?_assertEqual(10_000, membership_reconciliation_trigger_interval()),
        ?_assertEqual(undefined, membership_reconciliation_target_group_size()),
        ?_assertEqual(false, membership_reconciliation_auto_remove()),
        ?_assertEqual(true, reconciliation_enabled()),
        ?_assertEqual(60_000, reconciliation_interval()),
        ?_assertEqual(5_000, task_retry_delay_max_ms()),
        ?_assertEqual(10, task_retry_delay_constant()),
        ?_assertEqual(2, task_retry_delay_exponent()),
        ?_assertEqual(false, verbose_logging()),
        ?_assertEqual(45_000, segment_upload_timeout()),
        ?_assertEqual(1000, upload_retry_delay_ms()),
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
