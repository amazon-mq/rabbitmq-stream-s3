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
        {group, integration},
        container_credentials
    ].

groups() ->
    [{integration, [], [kick_the_tires]}].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(integration, Config) ->
    Cfg = {
        os:getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI"),
        os:getenv("AWS_ACCESS_KEY_ID"),
        os:getenv("AWS_SECRET_ACCESS_KEY"),
        os:getenv("AWS_SESSION_TOKEN"),
        os:getenv("AWS_REGION"),
        os:getenv("AWS_S3_BUCKET")
    },
    Skip =
        {skip,
            "AWS access credentials are not set. Skipping this group is OK! See the moduledoc for more information."},
    case Cfg of
        {AwsCredentialsUri, _, _, _, _, Bucket0} when
            AwsCredentialsUri =/= false andalso Bucket0 =/= false
        ->
            {ok, _} = application:ensure_all_started(gun),
            ok = application:set_env(rabbitmq_stream_s3, bucket, list_to_binary(Bucket0)),
            Config;
        {_, false, _, _, _, _} ->
            Skip;
        {_, _, false, _, _, _} ->
            Skip;
        {_, _, _, false, _, _} ->
            Skip;
        {_, _, _, _, false, _} ->
            Skip;
        {_, _, _, _, _, false} ->
            Skip;
        {_, AccessKey, SecretKey, SecurityToken, Region, Bucket1} ->
            {ok, _} = application:ensure_all_started(gun),
            ok = application:set_env(rabbitmq_stream_s3, aws_access_key, list_to_binary(AccessKey)),
            ok = application:set_env(rabbitmq_stream_s3, aws_secret_key, list_to_binary(SecretKey)),
            ok = application:set_env(
                rabbitmq_stream_s3,
                aws_security_token,
                list_to_binary(SecurityToken)
            ),
            ok = application:set_env(rabbitmq_stream_s3, aws_region, list_to_binary(Region)),
            ok = application:set_env(rabbitmq_stream_s3, bucket, list_to_binary(Bucket1)),
            Config
    end.

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
    application:ensure_all_started(gun),
    ok = rabbitmq_stream_s3:setup(),
    ok = rabbitmq_stream_s3_api_aws:init(),
    Config.

end_per_testcase(_Testcase, Config) ->
    catch ets:delete(rabbitmq_stream_s3_api_aws),
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

        Keys1 = [
            <<"prefix/", (atom_to_binary(?FUNCTION_NAME))/binary, "-",
                (integer_to_binary(N))/binary, "-",
                (nonce())/binary>>
         || N <- lists:seq(1, 10)
        ],
        NKeys = length(Keys1),
        Payload = <<"Object data">>,
        [ok = rabbitmq_stream_s3_api_aws:put(Conn, K, Payload, #{}) || K <- Keys1],
        {ok, {ListKeys, TotalSize, undefined}} = rabbitmq_stream_s3_api_aws:list(
            Conn,
            <<"prefix">>,
            undefined,
            #{}
        ),
        ?assertEqual(lists:sort(Keys1), lists:sort(ListKeys)),
        ?assertEqual(byte_size(Payload) * length(Keys1), TotalSize),
        {ok, #{pages := 1, objects := NKeys, total_size := TotalSize}} = rabbitmq_stream_s3_api_aws:delete_prefix(
            Conn,
            <<"prefix">>,
            #{}
        ),
        {ok, {[], 0, undefined}} = rabbitmq_stream_s3_api_aws:list(
            Conn,
            <<"prefix">>,
            undefined,
            #{}
        ),

        ok
    after
        _ = rabbitmq_stream_s3_api_aws:delete(Conn, Keys, #{}),
        _ = rabbitmq_stream_s3_api_aws:delete_prefix(Conn, <<"prefix">>, #{}),
        ok = rabbitmq_stream_s3_api_aws:close(Conn)
    end.

container_credentials(_Config) ->
    %% Start a TCP listener that serves a single fake container credentials response,
    %% mimicking the AWS_CONTAINER_CREDENTIALS_FULL_URI endpoint format.
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(ListenSock),
    Body = <<
        "{\"AccessKeyId\":\"AKIAIOSFODNN7EXAMPLE\","
        "\"SecretAccessKey\":\"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\","
        "\"Token\":\"test-session-token\","
        "\"Expiration\":\"2099-01-01T00:00:00Z\"}"
    >>,
    Resp = iolist_to_binary([
        "HTTP/1.1 200 OK\r\n",
        "Content-Type: application/json\r\n",
        "Content-Length: ",
        integer_to_binary(byte_size(Body)),
        "\r\n",
        "\r\n",
        Body
    ]),
    %% Serve the response in a separate process so the test doesn't block.
    spawn(fun() ->
        {ok, Sock} = gen_tcp:accept(ListenSock),
        %% Drain the HTTP request before responding. Without this, gun's send
        %% may block waiting for the socket buffer to drain, causing a timeout.
        drain_request(Sock),
        ok = gen_tcp:send(Sock, Resp),
        gen_tcp:close(Sock),
        gen_tcp:close(ListenSock)
    end),
    URI = "http://127.0.0.1:" ++ integer_to_list(Port),
    true = os:putenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", URI),
    try
        %% Remove any cached credentials so get_credentials/0 fetches fresh ones.
        ets:delete(rabbitmq_stream_s3_api_aws, credentials),
        {ok, <<"AKIAIOSFODNN7EXAMPLE">>, <<"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY">>,
            <<"test-session-token">>} = rabbitmq_stream_s3_api_aws:get_credentials()
    after
        os:unsetenv("AWS_CONTAINER_CREDENTIALS_FULL_URI")
    end.

%%----------------------------------------------------------------------------

nonce() ->
    binary:encode_hex(crypto:strong_rand_bytes(4), lowercase).

%% Reads from the socket until the HTTP request headers end (double CRLF).
drain_request(Sock) ->
    drain_request(Sock, <<>>).

drain_request(Sock, Acc) ->
    {ok, Data} = gen_tcp:recv(Sock, 0, 5000),
    Buf = <<Acc/binary, Data/binary>>,
    case binary:match(Buf, <<"\r\n\r\n">>) of
        nomatch -> drain_request(Sock, Buf);
        _ -> ok
    end.
