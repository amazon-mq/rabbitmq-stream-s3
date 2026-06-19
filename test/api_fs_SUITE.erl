%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(api_fs_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() -> [].

all() ->
    [
        put_and_get,
        get_not_found,
        delete_key,
        get_range_variants,
        delete_missing_key_is_idempotent,
        list_keys,
        stream_put
    ].

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    DataDir = filename:join(PrivDir, "s3"),
    ok = filelib:ensure_path(DataDir),
    rabbitmq_stream_s3_api_fs:set_data_dir(DataDir),
    Config.

end_per_suite(_Config) ->
    ok.

put_and_get(_Config) ->
    Key = <<"test/put_and_get.bin">>,
    ok = rabbitmq_stream_s3_api_fs:put(Key, <<"hello">>, #{}),
    ?assertEqual({ok, <<"hello">>}, rabbitmq_stream_s3_api_fs:get(Key, #{})).

get_not_found(_Config) ->
    ?assertEqual({error, not_found}, rabbitmq_stream_s3_api_fs:get(<<"no/such/key">>, #{})).

get_range_variants(_Config) ->
    Key = <<"test/get_range.bin">>,
    %% 10 bytes: 0123456789
    ok = rabbitmq_stream_s3_api_fs:put(Key, <<"0123456789">>, #{}),
    %% Explicit [start, end] inclusive.
    ?assertEqual({ok, <<"234">>}, rabbitmq_stream_s3_api_fs:get_range(Key, {2, 4}, #{})),
    %% Open-ended [start, eof).
    ?assertEqual({ok, <<"789">>}, rabbitmq_stream_s3_api_fs:get_range(Key, {7, undefined}, #{})),
    %% Suffix: last N bytes.
    ?assertEqual({ok, <<"89">>}, rabbitmq_stream_s3_api_fs:get_range(Key, -2, #{})),
    %% Suffix larger than the object returns the whole object (S3 semantics),
    %% not a crash on a negative offset.
    ?assertEqual({ok, <<"0123456789">>}, rabbitmq_stream_s3_api_fs:get_range(Key, -100, #{})),
    %% A range starting at or beyond the object size is unsatisfiable: 416,
    %% returned promptly rather than crashing the worker.
    ?assertMatch(
        {error, #{status := 416}}, rabbitmq_stream_s3_api_fs:get_range(Key, {10, 20}, #{})
    ),
    ?assertMatch(
        {error, #{status := 416}}, rabbitmq_stream_s3_api_fs:get_range(Key, {99, undefined}, #{})
    ).

delete_key(_Config) ->
    Key = <<"test/delete_me.bin">>,
    ok = rabbitmq_stream_s3_api_fs:put(Key, <<"data">>, #{}),
    ?assertEqual({ok, <<"data">>}, rabbitmq_stream_s3_api_fs:get(Key, #{})),
    ok = rabbitmq_stream_s3_api_fs:delete([Key], #{}),
    ?assertEqual({error, not_found}, rabbitmq_stream_s3_api_fs:get(Key, #{})).

delete_missing_key_is_idempotent(_Config) ->
    %% S3 DeleteObject is idempotent (204 for an absent key). The FS backend
    %% must match so it does not report a spurious error for a key that was
    %% never written or was already deleted.
    ?assertEqual(ok, rabbitmq_stream_s3_api_fs:delete([<<"no/such/key.bin">>], #{})),
    %% A mix of present and absent keys still succeeds and removes the present one.
    Key = <<"test/delete_mixed.bin">>,
    ok = rabbitmq_stream_s3_api_fs:put(Key, <<"data">>, #{}),
    ok = rabbitmq_stream_s3_api_fs:delete([<<"no/such/key2.bin">>, Key], #{}),
    ?assertEqual({error, not_found}, rabbitmq_stream_s3_api_fs:get(Key, #{})).

list_keys(_Config) ->
    Prefix = <<"test/list_prefix/">>,
    ok = rabbitmq_stream_s3_api_fs:put(<<Prefix/binary, "a.txt">>, <<"a">>, #{}),
    ok = rabbitmq_stream_s3_api_fs:put(<<Prefix/binary, "b.txt">>, <<"b">>, #{}),
    {ok, Keys, done} = rabbitmq_stream_s3_api_fs:list(Prefix, start, #{}),
    ?assertEqual(2, length(Keys)),
    ?assert(lists:member(<<Prefix/binary, "a.txt">>, Keys)),
    ?assert(lists:member(<<Prefix/binary, "b.txt">>, Keys)).

stream_put(_Config) ->
    Key = <<"test/streamed.bin">>,
    ContentLength = 11,
    {ok, Stream0} = rabbitmq_stream_s3_api_fs:stream_put(Key, ContentLength, #{}),
    Stream1 = rabbitmq_stream_s3_api_fs:stream_data(Stream0, <<"hello">>),
    Stream2 = rabbitmq_stream_s3_api_fs:stream_data(Stream1, <<" world">>),
    Crc = erlang:crc32(<<"hello world">>),
    ok = rabbitmq_stream_s3_api_fs:stream_finish(Stream2, Crc),
    ?assertEqual({ok, <<"hello world">>}, rabbitmq_stream_s3_api_fs:get(Key, #{})).
