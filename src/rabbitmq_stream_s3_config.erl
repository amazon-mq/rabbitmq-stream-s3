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
    get_region_attempts/0,
    get_credentials_attempts/0,
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
    task_retry_delay_max_ms/0,
    task_retry_delay_constant/0,
    task_retry_delay_exponent/0,
    verbose_logging/0,
    segment_upload_timeout/0,
    retention_task_timeout/0,
    tick_timeout_milliseconds/0,
    max_transfer_bytes_per_sec/0,
    max_transfer_burst_bytes/0,
    verify_crc_on_read/0
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

-spec get_region_attempts() -> pos_integer().
get_region_attempts() ->
    application:get_env(?APP, get_region_attempts, 10).

-spec get_credentials_attempts() -> pos_integer().
get_credentials_attempts() ->
    application:get_env(?APP, get_credentials_attempts, 10).

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
    application:get_env(?APP, general_pool_min_size, 0).

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
        ?_assertEqual(10, get_region_attempts()),
        ?_assertEqual(10, get_credentials_attempts()),
        ?_assertEqual(undefined, api_fs_data_dir()),
        ?_assertEqual(0, upload_pool_min_size()),
        ?_assertEqual(20, upload_pool_max_size()),
        ?_assertEqual(0, general_pool_min_size()),
        ?_assertEqual(50, general_pool_max_size()),
        ?_assertEqual(1024, manifest_rebalance_factor()),
        ?_assertEqual(10, manifest_debounce_modifications()),
        ?_assertEqual(5000, manifest_debounce_milliseconds()),
        ?_assertEqual(false, membership_reconciliation_enabled()),
        ?_assertEqual(3_600_000, membership_reconciliation_interval()),
        ?_assertEqual(10_000, membership_reconciliation_trigger_interval()),
        ?_assertEqual(undefined, membership_reconciliation_target_group_size()),
        ?_assertEqual(false, membership_reconciliation_auto_remove()),
        ?_assertEqual(5_000, task_retry_delay_max_ms()),
        ?_assertEqual(10, task_retry_delay_constant()),
        ?_assertEqual(2, task_retry_delay_exponent()),
        ?_assertEqual(false, verbose_logging()),
        ?_assertEqual(45_000, segment_upload_timeout()),
        ?_assertEqual(60_000, retention_task_timeout()),
        ?_assertEqual(5000, tick_timeout_milliseconds()),
        ?_assertEqual(false, verify_crc_on_read())
    ].

configured_test_() ->
    {foreach, fun() -> ok end, fun(_) -> application:unset_env(rabbitmq_stream_s3, bucket) end, [
        fun(_) ->
            application:set_env(rabbitmq_stream_s3, bucket, <<"my-bucket">>),
            ?_assertEqual(<<"my-bucket">>, bucket())
        end
    ]}.

-endif.
