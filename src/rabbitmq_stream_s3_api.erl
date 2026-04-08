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
    get/1,
    get/2,
    get_range/2,
    get_range/3,
    put/2,
    put/3,
    delete/1,
    delete/2,
    delete_prefix/1,
    delete_prefix/2
]).

-export([backend/0]).

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

-export_type([range_spec/0, request_opts/0]).

-callback init() -> ok.
-callback get(key(), request_opts()) -> {ok, binary()} | {error, any()}.
-callback get_range(key(), range_spec(), request_opts()) ->
    {ok, binary()} | {error, any()}.
-callback put(key(), iodata(), request_opts()) -> ok | {error, any()}.
-doc """
Delete the given key or all of the listed keys from the remote tier.

This operation is not expected to be atomic when passed multiple keys - it is
only allowed for the sake of reducing requests where possible. It would not be
atomic in a file system and S3 only supports deleting 1000 keys at a time, for
examples.
""".
-callback delete(key() | [key()], request_opts()) -> ok | {error, any()}.
-callback delete_prefix(key(), request_opts()) -> {ok, map()} | {error, any()}.

-define(C_GET, 1).
-define(C_GET_RANGE, 2).
-define(C_PUT, 3).
-define(C_DELETE_MANY, 4).
-define(C_DELETE_ONE, 5).
-define(C_LIST, 6).
-define(C_BYTES_RECEIVED, 7).
-define(C_BYTES_SENT, 8).
-define(COUNTERS, [
    {get, ?C_GET, counter, "Number of full-object GET requests"},
    {get_range, ?C_GET_RANGE, counter, "Number of range GET requests"},
    {put, ?C_PUT, counter, "Number of PUT requests"},
    {delete_many, ?C_DELETE_MANY, counter, "Number of multi-object DELETE requests"},
    {delete_one, ?C_DELETE_ONE, counter, "Number of single-object DELETE requests"},
    {list, ?C_LIST, counter, "Number of LIST requests"},
    {bytes_received, ?C_BYTES_RECEIVED, counter, "Total bytes received from S3"},
    {bytes_sent, ?C_BYTES_SENT, counter, "Total bytes sent to S3"}
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

-doc #{equiv => get(Key, #{})}.
-spec get(key()) -> {ok, binary()} | {error, any()}.
get(Key) when is_binary(Key) ->
    get(Key, #{}).

-spec get(key(), request_opts()) -> {ok, binary()} | {error, any()}.
get(Key, Opts) when is_binary(Key) andalso is_map(Opts) ->
    counters:add(counter(), ?C_GET, 1),
    Result = observe(read, fun() -> (backend()):get(Key, Opts) end),
    case Result of
        {ok, Data} -> counters:add(counter(), ?C_BYTES_RECEIVED, byte_size(Data));
        _ -> ok
    end,
    Result.

-doc #{equiv => get_range(Key, Range, #{})}.
-spec get_range(key(), range_spec()) -> {ok, binary()} | {error, any()}.
get_range(Key, Range) when is_binary(Key) ->
    get_range(Key, Range, #{}).

-spec get_range(key(), range_spec(), request_opts()) ->
    {ok, binary()} | {error, any()}.
get_range(Key, Range, Opts) when is_binary(Key) andalso is_map(Opts) ->
    counters:add(counter(), ?C_GET_RANGE, 1),
    Result = observe(read, fun() -> (backend()):get_range(Key, Range, Opts) end),
    case Result of
        {ok, Data} -> counters:add(counter(), ?C_BYTES_RECEIVED, byte_size(Data));
        _ -> ok
    end,
    Result.

-doc #{equiv => put(Key, Data, #{})}.
-spec put(key(), iodata()) -> ok | {error, any()}.
put(Key, Data) when is_binary(Key) ->
    put(Key, Data, #{}).

-spec put(key(), iodata(), request_opts()) -> ok | {error, any()}.
put(Key, Data, Opts) when is_binary(Key) andalso is_map(Opts) ->
    counters:add(counter(), ?C_PUT, 1),
    counters:add(counter(), ?C_BYTES_SENT, iolist_size(Data)),
    observe(write, fun() -> (backend()):put(Key, Data, Opts) end).

-doc #{equiv => delete(Keys, #{})}.
-spec delete(key() | [key()]) -> ok | {error, any()}.
delete(Keys) when is_binary(Keys) orelse is_list(Keys) ->
    delete(Keys, #{}).

-spec delete(key() | [key()], request_opts()) ->
    ok | {error, any()}.
delete(Keys, Opts) when is_list(Keys) andalso is_map(Opts) ->
    counters:add(counter(), ?C_DELETE_MANY, 1),
    observe(write, fun() -> (backend()):delete(Keys, Opts) end);
delete(Key, Opts) when is_binary(Key) andalso is_map(Opts) ->
    counters:add(counter(), ?C_DELETE_ONE, 1),
    observe(write, fun() -> (backend()):delete(Key, Opts) end).

-spec delete_prefix(key()) -> {ok, map()} | {error, any()}.
delete_prefix(Prefix) ->
    delete_prefix(Prefix, #{}).

-spec delete_prefix(key(), request_opts()) -> {ok, map()} | {error, any()}.
delete_prefix(Prefix, Opts) when is_binary(Prefix) andalso is_map(Opts) ->
    counters:add(counter(), ?C_LIST, 1),
    observe(write, fun() -> (backend()):delete_prefix(Prefix, Opts) end).

observe(Kind, Fun) ->
    T0 = erlang:monotonic_time(),
    try
        Fun()
    after
        DurationMs = erlang:convert_time_unit(
            erlang:monotonic_time() - T0, native, millisecond
        ),
        rabbitmq_stream_s3_request_metrics:observe(Kind, DurationMs)
    end.
