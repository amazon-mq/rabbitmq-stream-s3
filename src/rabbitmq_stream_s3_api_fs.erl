%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_api_fs).
-moduledoc """
A file system based implementation of the S3 API for testing purposes.

Each connection has an associated folder and each key for the connection has an
associated file in that folder.
""".

-include_lib("stdlib/include/assert.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("kernel/include/file.hrl").

-export([
    get/2,
    get_range/3,
    get_range_async/3,
    put/3,
    stream_put/3,
    stream_data/2,
    stream_finish/2,
    delete/2,
    list/3,
    match_async/3,
    handle_async/3,
    cancel_async/2
]).

% Auxiliary function for testing
-export([
    get_stream_data/1,
    list_fragments/1,
    clear/0,
    set_data_dir/1
]).

-type async_req() :: reference().
-type async_state() :: term().

-ifdef(TEST).
-export([range_spec_to_location_number/2]).
-endif.

-behaviour(rabbitmq_stream_s3_api).

-type key() :: rabbitmq_stream_s3_api:key().

-spec get(key(), rabbitmq_stream_s3_api:request_opts()) ->
    {ok, binary()} | {error, any()}.
get(Key, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    with_timeout(Timeout, fun() ->
        ?LOG_INFO("Trying to find file ~p in : ~p", [Key, data_dir()]),
        FilePath = key_to_path(Key),
        case file:read_file(FilePath) of
            {ok, _} = Result ->
                Result;
            {error, enoent} ->
                {error, not_found}
        end
    end).

-spec get_range(
    key(),
    rabbitmq_stream_s3_api:range_spec(),
    rabbitmq_stream_s3_api:request_opts()
) ->
    {ok, binary()} | {error, any()}.
get_range(Key, RangeSpec, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    with_timeout(Timeout, fun() ->
        FilePath = key_to_path(Key),
        case file:read_file_info(FilePath) of
            {ok, #file_info{size = FileSize}} ->
                {ok, Fd} = file:open(FilePath, [read, binary]),
                {Location, Number} = range_spec_to_location_number(FileSize, RangeSpec),
                Result =
                    case file:pread(Fd, Location, Number) of
                        {ok, Data} ->
                            {ok, Data};
                        eof ->
                            {ok, <<>>}
                    end,
                ok = file:close(Fd),
                Result;
            {error, enoent} ->
                {error, not_found}
        end
    end).

-doc """
Gets a range of bytes "asynchronously".

Since the FS backend is just for testing, this is not truly async. The caller
process performs the read itself and then sends itself a message with the
result.
""".
-spec get_range_async(
    key(),
    rabbitmq_stream_s3_api:range_spec(),
    rabbitmq_stream_s3_api:request_opts()
) ->
    {ok, async_req(), async_state()} | {error, any()}.
get_range_async(Key, RangeSpec, _Opts) ->
    FilePath = key_to_path(Key),
    Reply =
        case file:read_file_info(FilePath) of
            {ok, #file_info{size = FileSize}} ->
                {ok, Fd} = file:open(FilePath, [read, binary]),
                {Location, Number} = range_spec_to_location_number(FileSize, RangeSpec),
                Data =
                    case file:pread(Fd, Location, Number) of
                        {ok, D} -> D;
                        eof -> <<>>
                    end,
                ok = file:close(Fd),
                {data, Data, done};
            {error, enoent} ->
                {done, {error, not_found}}
        end,
    Req = erlang:make_ref(),
    self() ! {'$async', Req, Reply},
    {ok, Req, undefined}.

-spec put(key(), iodata(), rabbitmq_stream_s3_api:request_opts()) ->
    ok | {error, any()}.
put(Key, Data, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    with_timeout(Timeout, fun() ->
        ?LOG_INFO("Writing file ~p in : ~p", [Key, data_dir()]),
        FilePath = key_to_path(Key),
        ok = filelib:ensure_path(filename:dirname(FilePath)),
        Result = file:write_file(FilePath, Data),
        ?LOG_INFO("Write result: ~p", [Result]),
        Result
    end).

-spec delete(key() | [key()], rabbitmq_stream_s3_api:request_opts()) ->
    ok | {error, any()}.
delete(Key, Opts) when is_binary(Key) andalso is_map(Opts) ->
    delete([Key], Opts);
delete(Keys, Opts) when is_list(Keys) andalso is_map(Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    with_timeout(Timeout, fun() ->
        Result = lists:filtermap(
            fun(K) ->
                case file:delete(key_to_path(K)) of
                    ok -> false;
                    Error -> {true, {K, Error}}
                end
            end,
            Keys
        ),
        case Result of
            [] -> ok;
            _ -> {error, Result}
        end
    end).

list(Prefix, _Continuation, _Opts) when is_binary(Prefix) ->
    Path = key_to_path(Prefix),
    Keys = [
        path_to_key(list_to_binary(F))
     || F <- filelib:wildcard(binary_to_list(filename:join(Path, "**/*"))),
        not filelib:is_dir(F)
    ],
    {ok, Keys, done}.

-spec match_async(
    Msg :: term(),
    Reqs :: #{async_req() := async_state()},
    CancelledReqs :: #{async_req() => _}
) ->
    {ok, async_req()} | {cancelled, async_req(), final | more} | error.
match_async({'$async', Req, _Msg}, Reqs, _CancelledReqs) when is_map_key(Req, Reqs) ->
    {ok, Req};
match_async({'$async', Req, _Msg}, _Reqs, CancelledReqs) when is_map_key(Req, CancelledReqs) ->
    {cancelled, Req, final};
match_async(_Msg, _Reqs, _CancelledReqs) ->
    error.

-spec handle_async(Msg :: term(), async_req(), async_state()) ->
    {continue, async_state()}
    | {data, binary(), async_state() | done}
    | {done, ok | {error, any()}}.
handle_async({'$async', Req, Msg}, Req, undefined) ->
    Msg.

-spec cancel_async(async_req(), async_state()) -> ok.
cancel_async(_Req, _State) ->
    ok.

-spec stream_put(key(), pos_integer(), rabbitmq_stream_s3_api:request_opts()) ->
    {ok, async_state()} | {error, any()}.
stream_put(Key, _ContentLength, _Opts) ->
    FilePath = key_to_path(Key),
    ok = filelib:ensure_path(filename:dirname(FilePath)),
    {ok, Fd} = file:open(FilePath, [write, raw, binary]),
    {ok, {Fd, 0}}.

-spec stream_data(async_state(), iodata()) -> async_state().
stream_data({Fd, Crc0}, Data) ->
    ok = file:write(Fd, Data),
    {Fd, erlang:crc32(Crc0, Data)}.

-spec stream_finish(async_state(), non_neg_integer()) -> ok | {error, any()}.
stream_finish({Fd, Crc}, Crc32) ->
    ok = file:close(Fd),
    case Crc of
        Crc32 -> ok;
        _ -> {error, {checksum_mismatch, #{expected => Crc32, actual => Crc}}}
    end.

-spec get_stream_data(StreamName) ->
    {ok, Manifest, [FragmentFile]} | {error, not_found}
when
    StreamName :: binary(),
    Manifest :: file:filename() | undefined,
    FragmentFile :: file:filename().
get_stream_data(StreamName0) ->
    DataDir = binary_to_list(data_dir()),
    StreamNameWildcard = binary_to_list(<<"*", StreamName0/binary, "*">>),
    case filelib:wildcard(filename:join([DataDir, "**", StreamNameWildcard])) of
        [] ->
            {error, not_found};
        [StreamDir | _] ->
            Manifest =
                case filelib:wildcard(filename:join([StreamDir, "**", "*manifest"])) of
                    [] -> undefined;
                    [ManifestFile | _] -> ManifestFile
                end,
            Fragments = filelib:wildcard(
                filename:join(
                    [
                        DataDir,
                        "**",
                        StreamNameWildcard,
                        "**",
                        "*.fragment"
                    ]
                )
            ),
            {ok, Manifest, Fragments}
    end.

-doc "Returns the sorted first offsets of fragment files stored for the given stream.".
-spec list_fragments(StreamName :: binary()) -> [osiris:offset()].
list_fragments(StreamName) ->
    case get_stream_data(StreamName) of
        {ok, _Manifest, Fragments} ->
            lists:sort([
                list_to_integer(filename:basename(F, ".fragment"))
             || F <- Fragments
            ]);
        _ ->
            []
    end.

-spec clear() -> ok | {error, any()}.
clear() ->
    file:del_dir_r(data_dir()).

-spec set_data_dir(string()) -> ok.
set_data_dir(DataDir) ->
    application:set_env(rabbitmq_stream_s3, api_fs_data_dir, DataDir).

-doc """
Returns the data directory as a binary. `filelib:wildcard/1` only accepts
strings, so callers that list files must convert with `binary_to_list/1`.
`path_to_key/1` expects binary input.
""".
-spec data_dir() -> file:filename_all().
data_dir() ->
    Dir = rabbitmq_stream_s3_config:api_fs_data_dir(),
    ?assertNotEqual(undefined, Dir),
    iolist_to_binary(Dir).

-spec key_to_path(rabbitmq_stream_s3_api:key()) -> binary().
key_to_path(Key) ->
    filename:join(data_dir(), Key).

path_to_key(Path) ->
    DataDir = data_dir(),
    Len = byte_size(DataDir) + 1,
    binary:part(iolist_to_binary(Path), Len, byte_size(iolist_to_binary(Path)) - Len).

with_timeout(Timeout, Fun) ->
    Self = self(),
    Pid = spawn(fun() -> Self ! {self(), Fun()} end),
    receive
        {Pid, Result} -> Result
    after Timeout ->
        ?LOG_INFO(?MODULE_STRING ": operation timeouted"),
        exit(Pid, kill),
        {error, timeout}
    end.

range_spec_to_location_number(FileSize, SuffixRange) when
    is_integer(SuffixRange), SuffixRange < 0
->
    Location = FileSize + SuffixRange,
    {Location, -SuffixRange};
range_spec_to_location_number(FileSize, SuffixRange) when is_integer(SuffixRange) ->
    Location = 0,
    Number = min(SuffixRange, FileSize),
    {Location, Number};
range_spec_to_location_number(FileSize, {StartByte, EndByte}) ->
    Number =
        case EndByte of
            undefined -> FileSize - StartByte;
            _ -> EndByte - StartByte + 1
        end,
    {StartByte, Number}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

match_async_test() ->
    Ref = make_ref(),
    ?assertEqual({ok, Ref}, match_async({'$async', Ref, data}, #{Ref => state}, #{})),
    ?assertEqual({cancelled, Ref, final}, match_async({'$async', Ref, data}, #{}, #{Ref => ok})),
    ?assertEqual(error, match_async({'$async', Ref, data}, #{}, #{})),
    ?assertEqual(error, match_async(other_message, #{}, #{})),
    ok.

-endif.
