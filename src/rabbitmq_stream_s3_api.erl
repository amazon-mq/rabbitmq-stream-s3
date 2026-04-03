%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_api).
-moduledoc """
A high-level API for interacting with the remote tier.

This is built as a behaviour and the functions in this module call out to the
behaviour implementations in the configured back end. This is meant to make
testing easier, and it also might help us support multiple kinds of object
stores in the long run.

TODO: make another back end that uses the CommonTest priv directory and
file-system operations. Use that in non-unit tests.
""".
-export([
    init/0,
    open/0,
    close/1,
    get/2,
    get/3,
    get_range/3,
    get_range/4,
    put/3,
    put/4,
    delete/2,
    delete/3,
    delete_prefix/2,
    delete_prefix/3
]).

%% Up to the backend exactly what this is. Could be a pid for an HTTP
%% connection or a file descriptor for a local file.
-type connection() :: any().
-type key() :: rabbitmq_stream_s3:key().
-export_type([key/0]).
-type range_spec() ::
    {StartByte :: non_neg_integer(), EndByte :: non_neg_integer() | undefined}
    | SuffixRange :: integer().
-type request_opts() :: #{
    timeout => integer() | infinity,
    crc32 => integer(),
    unsigned_payload => boolean()
}.

-export_type([connection/0, range_spec/0, request_opts/0]).

-callback init() -> ok.
-callback open() -> {ok, connection()} | {error, any()}.
-callback close(connection()) -> ok.
-callback get(connection(), key(), request_opts()) -> {ok, binary()} | {error, any()}.
-callback get_range(connection(), key(), range_spec(), request_opts()) ->
    {ok, binary()} | {error, any()}.
-callback put(connection(), key(), iodata(), request_opts()) -> ok | {error, any()}.
-doc """
Delete the given key or all of the listed keys from the remote tier.

This operation is not expected to be atomic when passed multiple keys - it is
only allowed for the sake of reducing requests where possible. It would not be
atomic in a file system and S3 only supports deleting 1000 keys at a time, for
examples.
""".
-callback delete(connection(), key() | [key()], request_opts()) -> ok | {error, any()}.
-callback delete_prefix(connection(), key(), request_opts()) -> {ok, map()} | {error, any()}.

-define(C_GET, 1).
-define(C_GET_RANGE, 2).
-define(C_PUT, 3).
-define(C_DELETE_MANY, 4).
-define(C_DELETE_ONE, 5).
-define(C_LIST, 6).
-define(COUNTERS, [
    {get, ?C_GET, counter, "Number of full-object GET requests"},
    {get_range, ?C_GET_RANGE, counter, "Number of range GET requests"},
    {put, ?C_PUT, counter, "Number of PUT requests"},
    {delete_many, ?C_DELETE_MANY, counter, "Number of multi-object DELETE requests"},
    {delete_one, ?C_DELETE_ONE, counter, "Number of single-object DELETE requests"},
    {list, ?C_LIST, counter, "Number of LIST requests"}
]).
-define(COUNTER_KEY, {?MODULE, counter}).

backend() ->
    application:get_env(rabbitmq_stream_s3, ?MODULE, rabbitmq_stream_s3_api_aws).

counter() ->
    persistent_term:get(?COUNTER_KEY).

-spec init() -> ok.
init() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?COUNTER_KEY, Cnt),
    ok = rabbitmq_stream_s3_request_metrics:init(),
    (backend()):init().

-spec open() -> {ok, connection()} | {error, any()}.
open() ->
    (backend()):open().

-spec close(connection()) -> ok.
close(Conn) ->
    (backend()):close(Conn).

-doc #{equiv => get(Conn, Key, #{})}.
-spec get(connection(), key()) -> {ok, binary()} | {error, any()}.
get(Conn, Key) when is_binary(Key) ->
    get(Conn, Key, #{}).

-spec get(connection(), key(), request_opts()) -> {ok, binary()} | {error, any()}.
get(Conn, Key, Opts) when is_binary(Key) andalso is_map(Opts) ->
    counters:add(counter(), ?C_GET, 1),
    observe(read, fun() -> (backend()):get(Conn, Key, Opts) end).

-doc #{equiv => get_range(Conn, Key, Range, #{})}.
-spec get_range(connection(), key(), range_spec()) -> {ok, binary()} | {error, any()}.
get_range(Conn, Key, Range) when is_binary(Key) ->
    get_range(Conn, Key, Range, #{}).

-spec get_range(connection(), key(), range_spec(), request_opts()) ->
    {ok, binary()} | {error, any()}.
get_range(Conn, Key, Range, Opts) when is_binary(Key) andalso is_map(Opts) ->
    counters:add(counter(), ?C_GET_RANGE, 1),
    observe(read, fun() -> (backend()):get_range(Conn, Key, Range, Opts) end).

-doc #{equiv => put(Conn, Key, Data, #{})}.
-spec put(connection(), key(), iodata()) -> ok | {error, any()}.
put(Conn, Key, Data) when is_binary(Key) ->
    put(Conn, Key, Data, #{}).

-spec put(connection(), key(), iodata(), request_opts()) -> ok | {error, any()}.
put(Conn, Key, Data, Opts) when is_binary(Key) andalso is_map(Opts) ->
    counters:add(counter(), ?C_PUT, 1),
    observe(write, fun() -> (backend()):put(Conn, Key, Data, Opts) end).

-doc #{equiv => delete(Conn, Keys, #{})}.
-spec delete(connection(), key() | [key()]) -> ok | {error, any()}.
delete(Conn, Keys) when is_binary(Keys) orelse is_list(Keys) ->
    delete(Conn, Keys, #{}).

-spec delete(connection(), key() | [key()], request_opts()) ->
    ok | {error, any()}.
delete(Conn, Keys, Opts) when is_list(Keys) andalso is_map(Opts) ->
    counters:add(counter(), ?C_DELETE_MANY, 1),
    observe(write, fun() -> (backend()):delete(Conn, Keys, Opts) end);
delete(Conn, Key, Opts) when is_binary(Key) andalso is_map(Opts) ->
    counters:add(counter(), ?C_DELETE_ONE, 1),
    observe(write, fun() -> (backend()):delete(Conn, Key, Opts) end).

-spec delete_prefix(connection(), key()) -> {ok, map()} | {error, any()}.
delete_prefix(Conn, Prefix) ->
    delete_prefix(Conn, Prefix, #{}).

-spec delete_prefix(connection(), key(), request_opts()) -> {ok, map()} | {error, any()}.
delete_prefix(Conn, Prefix, Opts) when is_binary(Prefix) andalso is_map(Opts) ->
    counters:add(counter(), ?C_LIST, 1),
    observe(write, fun() -> (backend()):delete_prefix(Conn, Prefix, Opts) end).

observe(Kind, Fun) ->
    T0 = erlang:monotonic_time(millisecond),
    try
        Fun()
    after
        DurationMs = erlang:monotonic_time(millisecond) - T0,
        rabbitmq_stream_s3_request_metrics:observe(Kind, DurationMs)
    end.
