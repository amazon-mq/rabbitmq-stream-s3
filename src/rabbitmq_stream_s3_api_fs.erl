%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_api_fs).
-moduledoc """
A file system based implementation of the S3 API for testing purposes.

Each connection has an associated folder and each key for the connection has an
associated file in that folder.
""".

-include_lib("kernel/include/logger.hrl").
-include_lib("kernel/include/file.hrl").

-export([
    init/0,
    get/2,
    get_range/3,
    put/3,
    delete/2,
    delete_prefix/2
]).

% Auxiliary function for testing
-export([
    get_stream_data/1,
    clear/0,
    set_data_dir/1
]).

-ifdef(TEST).
-export([range_spec_to_location_number/2]).
-endif.

-behaviour(rabbitmq_stream_s3_api).

-type key() :: rabbitmq_stream_s3_api:key().

-spec init() -> ok.
init() ->
    ?LOG_INFO(?MODULE_STRING ": initializing"),
    ok.

-spec get(key(), rabbitmq_stream_s3_api:request_opts()) ->
    {ok, binary()} | {error, any()}.
get(Key, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    with_timeout(Timeout, fun() ->
        ?LOG_INFO("Trying to find file ~p in : ~p", [Key, data_dir()]),
        case key_to_path(Key) of
            {error, path_not_set} = E ->
                E;
            FilePath ->
                case file:read_file(binary_to_list(FilePath)) of
                    {ok, _} = Result ->
                        Result;
                    {error, enoent} ->
                        {error, not_found}
                end
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
        case key_to_path(Key) of
            {error, path_not_set} = E ->
                E;
            FilePath ->
                FilePathBin = binary_to_list(FilePath),
                case file:read_file_info(FilePathBin) of
                    {ok, #file_info{size = FileSize}} ->
                        {ok, Fd} = file:open(FilePathBin, [read, binary]),
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
        end
    end).

-spec put(key(), iodata(), rabbitmq_stream_s3_api:request_opts()) ->
    ok | {error, any()}.
put(Key, Data, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    with_timeout(Timeout, fun() ->
        ?LOG_INFO("Writing file ~p in : ~p", [Key, data_dir()]),
        case key_to_path(Key) of
            {error, path_not_set} = E ->
                E;
            FilePath ->
                ok = filelib:ensure_path(filename:dirname(FilePath)),
                Result = file:write_file(FilePath, Data),
                ?LOG_INFO("Write result: ~p", [Result]),
                Result
        end
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
                case key_to_path(K) of
                    {error, path_not_set} = E ->
                        {true, {K, E}};
                    FilePath ->
                        case file:delete(FilePath) of
                            ok -> false;
                            Error -> {true, {K, Error}}
                        end
                end
            end,
            Keys
        ),
        case Result of
            [] -> ok;
            _ -> {error, Result}
        end
    end).

-spec delete_prefix(key(), rabbitmq_stream_s3_api:request_opts()) ->
    {ok, map()} | {error, any()}.
delete_prefix(Prefix, Opts) when is_binary(Prefix) andalso is_map(Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    with_timeout(Timeout, fun() ->
        case key_to_path(Prefix) of
            {error, path_not_set} = Err ->
                Err;
            Path ->
                case file:del_dir_r(Path) of
                    ok ->
                        {ok, #{}};
                    {error, _} = Err ->
                        Err
                end
        end
    end).

-spec get_stream_data(StreamName) ->
    {ok, Manifest, [FragmentFile]} | {error, not_found | path_not_set}
when
    StreamName :: binary(),
    Manifest :: Path | undefined,
    FragmentFile :: Path,
    Path :: binary().
get_stream_data(StreamName0) ->
    case data_dir() of
        undefined ->
            {error, path_not_set};
        DataDir ->
            StreamNameWildcard = binary_to_list(<<"*", StreamName0/binary, "*">>),
            case filelib:wildcard(string:join([DataDir, "**", StreamNameWildcard], "/")) of
                [] ->
                    {error, not_found};
                [StreamDir | _] ->
                    Manifest =
                        case filelib:wildcard(string:join([StreamDir, "**", "*manifest"], "/")) of
                            [] -> undefined;
                            [ManifestFile | _] -> ManifestFile
                        end,
                    Fragments = filelib:wildcard(
                        string:join(
                            [
                                DataDir,
                                "**",
                                StreamNameWildcard,
                                "**",
                                "*.fragment"
                            ],
                            "/"
                        )
                    ),
                    {ok, Manifest, Fragments}
            end
    end.

-spec clear() -> ok | {error, any()}.
clear() ->
    file:del_dir_r(data_dir()).

-spec set_data_dir(string()) -> ok.
set_data_dir(DataDir) ->
    application:set_env(rabbitmq_stream_s3, api_fs_data_dir, DataDir).

-spec data_dir() -> binary() | undefined.
data_dir() ->
    application:get_env(rabbitmq_stream_s3, api_fs_data_dir, undefined).

-spec key_to_path(rabbitmq_stream_s3_api:key()) -> binary() | {error, path_not_set}.
key_to_path(Key) ->
    case data_dir() of
        undefined -> {error, path_not_set};
        DataDir -> filename:join(DataDir, Key)
    end.

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
