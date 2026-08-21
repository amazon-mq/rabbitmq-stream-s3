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
            ok = set_account_id(),
            ok = application:set_env(
                rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_aws
            ),
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
            ok = application:set_env(rabbitmq_stream_s3, allow_static_credentials, true),
            ok = application:set_env(
                rabbitmq_stream_s3,
                aws_security_token,
                list_to_binary(SecurityToken)
            ),
            ok = application:set_env(rabbitmq_stream_s3, aws_region, list_to_binary(Region)),
            ok = application:set_env(rabbitmq_stream_s3, bucket, list_to_binary(Bucket1)),
            ok = set_account_id(),
            ok = application:set_env(
                rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_aws
            ),
            Config
    end.

%% The account ID is optional. When AWS_ACCOUNT_ID is set the group also
%% exercises the x-amz-expected-bucket-owner header; otherwise the header is
%% omitted, so make sure no value leaked in from another suite.
set_account_id() ->
    case os:getenv("AWS_ACCOUNT_ID") of
        false ->
            application:unset_env(rabbitmq_stream_s3, account_id);
        AccountId ->
            application:set_env(rabbitmq_stream_s3, account_id, list_to_binary(AccountId))
    end.

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(_Testcase, Config) ->
    application:ensure_all_started(gun),
    application:ensure_all_started(seshat),
    _ = seshat:new_group(rabbitmq_stream_s3),
    %% Establish our own precondition: rabbitmq_stream_s3_api_aws:init/1 only
    %% starts (returns {ok, _}) when the configured backend is the AWS one,
    %% otherwise it returns `ignore`. This suite does not use
    %% rabbitmq_stream_s3_cth, but other suites that do leave the shared CT
    %% node's app env set to the FS backend (see rabbitmq_stream_s3_cth:
    %% pre_init_per_suite/3). In a full `make ct` run that leaked value would
    %% make start_link/0 return `ignore`. Set the backend explicitly so the
    %% suite is self-contained regardless of run order.
    ok = application:set_env(
        rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_aws
    ),
    {ok, Pid} = rabbitmq_stream_s3_api_aws:start_link(),
    [{api_aws_pid, Pid} | Config].

end_per_testcase(_Testcase, Config) ->
    case proplists:get_value(api_aws_pid, Config) of
        Pid when is_pid(Pid) ->
            gen_server:stop(Pid);
        _ ->
            ok
    end,
    Config.

%%----------------------------------------------------------------------------

kick_the_tires(_Config) ->
    {ok, _} = rabbitmq_stream_s3_api_aws_pool:start_link(
        rabbitmq_stream_s3_upload_pool,
        #{name => rabbitmq_stream_s3_upload_pool, min_size => 1, max_size => 3}
    ),
    {ok, _} = rabbitmq_stream_s3_api_aws_pool:start_link(
        rabbitmq_stream_s3_general_pool,
        #{name => rabbitmq_stream_s3_general_pool, min_size => 1, max_size => 3}
    ),
    Keys =
        [K1, K2, K3] = [
            <<(atom_to_binary(?FUNCTION_NAME))/binary, "-", (nonce())/binary>>
         || _ <- lists:seq(1, 3)
        ],
    try
        ok = rabbitmq_stream_s3_api_aws:put(K1, <<"Hello, S3!">>, #{}),
        {ok, <<"Hello, S3!">>} = rabbitmq_stream_s3_api_aws:get(K1, #{}),

        {ok, <<"Hello,">>} = rabbitmq_stream_s3_api_aws:get_range(K1, {0, 5}, #{}),
        {ok, <<"S3!">>} = rabbitmq_stream_s3_api_aws:get_range(K1, -3, #{}),
        {ok, <<"lo, S3!">>} = rabbitmq_stream_s3_api_aws:get_range(K1, {3, undefined}, #{}),

        ok = rabbitmq_stream_s3_api_aws:put(K2, <<"Object 2">>, #{}),
        ok = rabbitmq_stream_s3_api_aws:put(K3, <<"Object 3">>, #{}),

        %% Streaming upload: send data in two chunks and verify the result.
        StreamKey = <<(atom_to_binary(?FUNCTION_NAME))/binary, "-stream-", (nonce())/binary>>,
        Chunk1 = <<"Hello, ">>,
        Chunk2 = <<"streaming S3!">>,
        StreamData = <<Chunk1/binary, Chunk2/binary>>,
        Crc = erlang:crc32([Chunk1, Chunk2]),
        {ok, Stream0} = rabbitmq_stream_s3_api_aws:stream_put(
            StreamKey, byte_size(StreamData), #{}
        ),
        Stream1 = rabbitmq_stream_s3_api_aws:stream_data(Stream0, Chunk1),
        Stream2 = rabbitmq_stream_s3_api_aws:stream_data(Stream1, Chunk2),
        ok = rabbitmq_stream_s3_api_aws:stream_finish(Stream2, Crc),
        {ok, StreamData} = rabbitmq_stream_s3_api_aws:get(StreamKey, #{}),
        ok = rabbitmq_stream_s3_api_aws:delete(StreamKey, #{}),

        ok = rabbitmq_stream_s3_api_aws:delete(K1, #{}),
        {error, not_found} = rabbitmq_stream_s3_api_aws:get(K1, #{}),

        ok = rabbitmq_stream_s3_api_aws:delete([K2, K3], #{}),
        {error, not_found} = rabbitmq_stream_s3_api_aws:get(K2, #{}),
        {error, not_found} = rabbitmq_stream_s3_api_aws:get(K3, #{}),

        Keys1 = [
            <<"prefix/", (atom_to_binary(?FUNCTION_NAME))/binary, "-",
                (integer_to_binary(N))/binary, "-",
                (nonce())/binary>>
         || N <- lists:seq(1, 10)
        ],
        Payload = <<"Object data">>,
        [ok = rabbitmq_stream_s3_api_aws:put(K, Payload, #{}) || K <- Keys1],
        {ok, ListKeys, done} = rabbitmq_stream_s3_api_aws:list(
            <<"prefix">>,
            start,
            #{}
        ),
        ?assertEqual(lists:sort(Keys1), lists:sort(ListKeys)),
        ok = rabbitmq_stream_s3_api_aws:delete(ListKeys, #{}),
        {ok, [], done} = rabbitmq_stream_s3_api_aws:list(<<"prefix">>, start, #{}),

        ok
    after
        _ = rabbitmq_stream_s3_api_aws:delete(Keys, #{}),
        {ok, PrefixKeys, _} = rabbitmq_stream_s3_api_aws:list(<<"prefix">>, start, #{}),
        _ = rabbitmq_stream_s3_api_aws:delete(PrefixKeys, #{}),
        ok
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
        %% The credential server selects its source (static/imds/container) when
        %% it reads config, not per get_credentials/0 call. It was started in
        %% init_per_testcase before this env var was set, so reload_config/0 is
        %% needed to pick up the container credentials source. (The ETS table is
        %% protected and owned by the server, so the test cannot poke it directly.)
        ok = rabbitmq_stream_s3_api_aws:reload_config(),
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
