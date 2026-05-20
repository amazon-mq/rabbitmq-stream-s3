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

delete_key(_Config) ->
    Key = <<"test/delete_me.bin">>,
    ok = rabbitmq_stream_s3_api_fs:put(Key, <<"data">>, #{}),
    ?assertEqual({ok, <<"data">>}, rabbitmq_stream_s3_api_fs:get(Key, #{})),
    ok = rabbitmq_stream_s3_api_fs:delete([Key], #{}),
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
