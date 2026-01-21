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
    open/0,
    close/1,
    get/2,
    get/3,
    get_range/3,
    get_range/4,
    put/3,
    put/4
]).

%% Up to the backend exactly what this is. Could be a pid for an HTTP
%% connection or a file descriptor for a local file.
-type connection() :: any().
-doc """
A key within a bucket.

This identifies an object. Typically keys look like Unix paths, for example
`<<"rabbitmq/stream/data/__sq_12346786783/00000000000000000000.fragment">>`.
""".
-type key() :: binary().
-type range_spec() ::
    {StartByte :: non_neg_integer(), EndByte :: non_neg_integer() | undefined}
    | SuffixRange :: integer().
-type request_opts() :: #{
    timeout => integer() | infinity,
    crc32 => integer(),
    unsigned_payload => boolean()
}.

-export_type([connection/0, key/0, range_spec/0, request_opts/0]).

-callback open() -> {ok, connection()} | {error, any()}.
-callback close(connection()) -> ok.
-callback get(connection(), key(), request_opts()) -> {ok, binary()} | {error, any()}.
-callback get_range(connection(), key(), range_spec(), request_opts()) ->
    {ok, binary()} | {error, any()}.
-callback put(connection(), key(), iodata(), request_opts()) -> ok | {error, any()}.

backend() ->
    application:get_env(rabbitmq_stream_s3, ?MODULE, rabbitmq_stream_s3_api_aws).

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
    (backend()):get(Conn, Key, Opts).

-doc #{equiv => get_range(Conn, Key, Range, #{})}.
-spec get_range(connection(), key(), range_spec()) -> {ok, binary()} | {error, any()}.
get_range(Conn, Key, Range) when is_binary(Key) ->
    get_range(Conn, Key, Range, #{}).

-spec get_range(connection(), key(), range_spec(), request_opts()) ->
    {ok, binary()} | {error, any()}.
get_range(Conn, Key, Range, Opts) when is_binary(Key) andalso is_map(Opts) ->
    (backend()):get_range(Conn, Key, Range, Opts).

-doc #{equiv => put(Conn, Key, Data, #{})}.
-spec put(connection(), key(), iodata()) -> ok | {error, any()}.
put(Conn, Key, Data) when is_binary(Key) ->
    put(Conn, Key, Data, #{}).

-spec put(connection(), key(), iodata(), request_opts()) -> ok | {error, any()}.
put(Conn, Key, Data, Opts) when is_binary(Key) andalso is_map(Opts) ->
    (backend()):put(Conn, Key, Data, Opts).
