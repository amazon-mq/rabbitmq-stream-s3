%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_api_aws_SUITE).
-moduledoc """
Integration tests against an actual Amazon S3 bucket.

This integration suite is meant to be run by GitHub Actions where these
credentials are already set.

## Manual testing

If you wish you may run this suite by creating resources in AWS and setting
environment variables to give the suite access.

1. Open the AWS console
2. Go to S3 > Create bucket
3. Set a bucket name and leave everything else as defaults. I chose
   `rabbitmq-stream-s3-ci-ee4cd43b` since bucket names are globally unique.
4. Go to IAM > Policies > Create policy
5. Use the visual editor to set the JSON according to your bucket name:
    ```
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::rabbitmq-stream-s3-ci-ee4cd43b",
                    "arn:aws:s3:::rabbitmq-stream-s3-ci-ee4cd43b/*"
                ]
            }
        ]
    }
    ```
6. Set a name and description. I chose `rabbitmq-stream-s3-ci` for my policy
   name.
7. Go to IAM > Users > Create user
8. Set a user name (I chose `rabbitmq-stream-s3-ci`) > Next
9. Select "Attach policies directly" and add the `rabbitmq-stream-s3-ci` policy
   we just created in step 6 > Next > Create user
10. Go to IAM > Users > `rabbitmq-stream-s3-ci` > Security credentials > Create
    access key
11. Access key best practices & alternatives: Other > Next > Set a
    description > Create access key
12. Set environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
    to the Access key and Secret access key. Set `AWS_S3_BUCKET` to the bucket
    name and `AWS_REGION` to the region of the bucket. Then
    `make -C deps/rabbitmq_stream_s3 ct` will execute this suite with those
    credentials and bucket.
""".

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        kick_the_tires
    ].

init_per_suite(Config) ->
    Skip =
        {skip,
            "AWS access credentials are not set. Skipping this suite is OK! See the moduledoc for more information."},
    Cfg = {
        os:getenv("AWS_ACCESS_KEY_ID"),
        os:getenv("AWS_SECRET_ACCESS_KEY"),
        os:getenv("AWS_REGION"),
        os:getenv("AWS_S3_BUCKET")
    },
    case Cfg of
        {false, _, _, _} ->
            Skip;
        {_, false, _, _} ->
            Skip;
        {_, _, false, _} ->
            Skip;
        {_, _, _, false} ->
            Skip;
        {AccessKey, SecretKey, Region, Bucket} ->
            application:ensure_all_started(gun),
            ok = application:set_env(rabbitmq_stream_s3, aws_access_key, list_to_binary(AccessKey)),
            ok = application:set_env(rabbitmq_stream_s3, aws_secret_key, list_to_binary(SecretKey)),
            ok = application:set_env(rabbitmq_stream_s3, aws_region, list_to_binary(Region)),
            ok = application:set_env(rabbitmq_stream_s3, bucket, list_to_binary(Bucket)),
            Config
    end.

end_per_suite(Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
    ok = rabbitmq_stream_s3_api_aws:init(),
    Config.

end_per_testcase(_Testcase, Config) ->
    Config.

%%----------------------------------------------------------------------------

kick_the_tires(_Config) ->
    Keys =
        [K1, K2, K3] = [
            <<(atom_to_binary(?FUNCTION_NAME))/binary, "-", (nonce())/binary>>
         || _ <- lists:seq(1, 3)
        ],
    {ok, Conn} = rabbitmq_stream_s3_api_aws:open(),

    try
        ok = rabbitmq_stream_s3_api_aws:put(Conn, K1, <<"Hello, S3!">>, #{}),
        {ok, <<"Hello, S3!">>} = rabbitmq_stream_s3_api_aws:get(Conn, K1, #{}),

        {ok, <<"Hello,">>} = rabbitmq_stream_s3_api_aws:get_range(Conn, K1, {0, 5}, #{}),
        {ok, <<"S3!">>} = rabbitmq_stream_s3_api_aws:get_range(Conn, K1, -3, #{}),
        {ok, <<"lo, S3!">>} = rabbitmq_stream_s3_api_aws:get_range(Conn, K1, {3, undefined}, #{}),

        ok = rabbitmq_stream_s3_api_aws:put(Conn, K2, <<"Object 2">>, #{}),
        ok = rabbitmq_stream_s3_api_aws:put(Conn, K3, <<"Object 3">>, #{}),

        ok = rabbitmq_stream_s3_api_aws:delete(Conn, K1, #{}),
        {error, not_found} = rabbitmq_stream_s3_api_aws:get(Conn, K1, #{}),

        ok = rabbitmq_stream_s3_api_aws:delete(Conn, [K2, K3], #{}),
        {error, not_found} = rabbitmq_stream_s3_api_aws:get(Conn, K2, #{}),
        {error, not_found} = rabbitmq_stream_s3_api_aws:get(Conn, K3, #{}),

        ok
    after
        _ = rabbitmq_stream_s3_api_aws:delete(Conn, Keys, #{}),
        ok = rabbitmq_stream_s3_api_aws:close(Conn)
    end.

%%----------------------------------------------------------------------------

-spec nonce() -> binary().
nonce() ->
    binary:encode_hex(crypto:strong_rand_bytes(4), lowercase).
