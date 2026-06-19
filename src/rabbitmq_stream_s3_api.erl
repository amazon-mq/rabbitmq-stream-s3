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
    get_range_async/2,
    get_range_async/3,
    put/2,
    put/3,
    stream_put/3,
    stream_data/2,
    stream_finish/2,
    stream_abort/1,
    delete/1,
    delete/2,
    list/1,
    list/2,
    match_async/3,
    handle_async/3,
    cancel_async/2,
    request_duration_prometheus_format/0
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
-type async_state() :: term().
-type async_req() :: reference().
-type list_continuation() :: start | done | term().

-export_type([
    range_spec/0,
    request_opts/0,
    async_state/0,
    async_req/0,
    list_continuation/0
]).

-callback get(key(), request_opts()) -> {ok, binary()} | {error, any()}.
-callback get_range(key(), range_spec(), request_opts()) ->
    {ok, binary()} | {error, any()}.
-callback get_range_async(key(), range_spec(), request_opts()) ->
    {ok, async_req(), async_state()} | {error, any()}.
-callback put(key(), iodata(), request_opts()) -> ok | {error, any()}.
-callback stream_put(key(), pos_integer(), request_opts()) ->
    {ok, async_state()} | {error, any()}.
-callback stream_data(async_state(), iodata()) -> async_state().
-callback stream_finish(async_state(), non_neg_integer()) -> ok | {error, any()}.
-doc """
Abandon an in-progress streaming PUT that will not be finished.

Called when the caller fails between stream_put/3 and a successful
stream_finish/2 (for example a source read error while streaming the body).
stream_finish/2 releases the request's resources on its own paths; this
releases them when finish is never reached. The partially-sent upload does not
become a committed object, so it leaves at most an orphan for GC.
""".
-callback stream_abort(async_state()) -> ok.
-doc """
Delete the given key or all of the listed keys from the remote tier.

This operation is not expected to be atomic when passed multiple keys - it is
only allowed for the sake of reducing requests where possible. It would not be
atomic in a file system and S3 only supports deleting 1000 keys at a time, for
examples.
""".
-callback delete(key() | [key()], request_opts()) -> ok | {error, any()}.
-callback list(key(), list_continuation(), request_opts()) ->
    {ok, [key()], list_continuation()} | {error, any()}.
-callback match_async(
    Msg :: term(),
    Reqs :: #{async_req() := async_state()},
    CancelledReqs :: #{async_req() => _}
) ->
    {ok, async_req()}
    | {cancelled, async_req(), final | more}
    | error.
-callback handle_async(Msg :: term(), async_req(), async_state()) ->
    {continue, async_state()}
    | {data, binary(), async_state() | done}
    | {done, ok | {error, any()}}
    | {done_cancel, {error, any()}}
    | ignore.
-callback cancel_async(async_req(), async_state()) -> ok.

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

-define(REQUEST_DURATION_BUCKETS, [10, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, infinity]).

backend() ->
    rabbitmq_stream_s3_config:api_backend().

counter() ->
    persistent_term:get(?COUNTER_KEY).

-spec init() -> ok.
init() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?COUNTER_KEY, Cnt),
    ok = rabbitmq_stream_s3_remote_reader:init_counters(),
    ok = rabbitmq_stream_s3_log_reader:init_counters(),
    ok = rabbitmq_stream_s3_governor:init_counters(),
    ok = rabbitmq_stream_s3_reaper:init_counters(),
    ok = rabbitmq_stream_s3_replica_reader:init_counters(),
    ok = rabbitmq_stream_s3_manifest_replica:init_counters(),
    lists:foreach(
        fun(Kind) ->
            rabbitmq_stream_s3_histogram:new(
                {?MODULE, request_duration, Kind}, ?REQUEST_DURATION_BUCKETS
            )
        end,
        [read, write]
    ),
    ok.

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

-doc #{equiv => get_range(Key, Range, #{})}.
-spec get_range_async(key(), range_spec()) ->
    {ok, async_req(), async_state()} | {error, any()}.
get_range_async(Key, Range) when is_binary(Key) ->
    get_range_async(Key, Range, #{}).

-spec get_range_async(key(), range_spec(), request_opts()) ->
    {ok, async_req(), async_state()} | {error, any()}.
get_range_async(Key, Range, Opts) when is_binary(Key) andalso is_map(Opts) ->
    counters:add(counter(), ?C_GET_RANGE, 1),
    StartTs = erlang:monotonic_time(),
    case (backend()):get_range_async(Key, Range, Opts) of
        {ok, Req, BackendState} ->
            {ok, Req, {StartTs, BackendState}};
        {error, _} = Err ->
            Err
    end.

-doc #{equiv => put(Key, Data, #{})}.
-spec put(key(), iodata()) -> ok | {error, any()}.
put(Key, Data) when is_binary(Key) ->
    put(Key, Data, #{}).

-spec put(key(), iodata(), request_opts()) -> ok | {error, any()}.
put(Key, Data, Opts) when is_binary(Key) andalso is_map(Opts) ->
    counters:add(counter(), ?C_PUT, 1),
    counters:add(counter(), ?C_BYTES_SENT, iolist_size(Data)),
    observe(write, fun() -> (backend()):put(Key, Data, Opts) end).

-spec stream_put(key(), pos_integer(), request_opts()) -> {ok, async_state()} | {error, any()}.
stream_put(Key, ContentLength, Opts) when
    is_binary(Key) andalso is_integer(ContentLength) andalso is_map(Opts)
->
    counters:add(counter(), ?C_PUT, 1),
    StartTs = erlang:monotonic_time(),
    case (backend()):stream_put(Key, ContentLength, Opts) of
        {ok, BackendState} ->
            {ok, {StartTs, BackendState}};
        {error, _} = Err ->
            Err
    end.

-spec stream_data(async_state(), iodata()) -> async_state().
stream_data({StartTs, BackendState}, Data) ->
    counters:add(counter(), ?C_BYTES_SENT, iolist_size(Data)),
    {StartTs, (backend()):stream_data(BackendState, Data)}.

-spec stream_finish(async_state(), non_neg_integer()) -> ok | {error, any()}.
stream_finish({StartTs, BackendState}, Crc32) ->
    Result = (backend()):stream_finish(BackendState, Crc32),
    Ms = rabbitmq_stream_s3_util:elapsed_ms(StartTs),
    rabbitmq_stream_s3_histogram:observe({?MODULE, request_duration, write}, Ms),
    Result.

-spec stream_abort(async_state()) -> ok.
stream_abort({StartTs, BackendState}) ->
    ok = (backend()):stream_abort(BackendState),
    Ms = rabbitmq_stream_s3_util:elapsed_ms(StartTs),
    rabbitmq_stream_s3_histogram:observe({?MODULE, request_duration, write}, Ms),
    ok.

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

-doc #{equiv => list(Prefix, start)}.
-spec list(key()) -> {ok, [key()], list_continuation()} | {error, any()}.
list(Prefix) ->
    list(Prefix, start).

-doc "List keys under Prefix. Pass the returned continuation to get the next page.".
-spec list(key(), list_continuation()) ->
    {ok, [key()], list_continuation()} | {error, any()}.
list(Prefix, Continuation) when is_binary(Prefix) ->
    counters:add(counter(), ?C_LIST, 1),
    (backend()):list(Prefix, Continuation, #{}).

-spec match_async(
    Msg :: term(),
    Reqs :: #{async_req() := async_state()},
    CancelledReqs :: #{async_req() => _}
) ->
    {ok, async_req()} | {cancelled, async_req(), final | more} | error.
match_async(Msg, Reqs, CancelledReqs) ->
    (backend()):match_async(Msg, Reqs, CancelledReqs).

-spec handle_async(Msg :: term(), async_req(), AsyncState :: term()) ->
    {continue, AsyncState :: term()}
    | {data, binary(), async_state()}
    | {done, ok | {error, any()}}
    | {done_cancel, {error, any()}}
    | ignore.
handle_async(Msg, Req, {StartTs, BackendState0}) ->
    case (backend()):handle_async(Msg, Req, BackendState0) of
        {continue, BackendState} ->
            {continue, {StartTs, BackendState}};
        {data, Data, done} ->
            counters:add(counter(), ?C_BYTES_RECEIVED, byte_size(Data)),
            ok = finish_async(StartTs),
            {data, Data, done};
        {data, Data, BackendState} ->
            counters:add(counter(), ?C_BYTES_RECEIVED, byte_size(Data)),
            {data, Data, {StartTs, BackendState}};
        {done, _} = Done ->
            ok = finish_async(StartTs),
            Done;
        {done_cancel, _} = DoneCancel ->
            ok = finish_async(StartTs),
            DoneCancel;
        ignore ->
            ignore
    end.

-spec cancel_async(async_req(), async_state()) -> ok.
cancel_async(Req, {StartTs, BackendState}) ->
    (backend()):cancel_async(Req, BackendState),
    ok = finish_async(StartTs).

finish_async(StartTs) ->
    Ms = rabbitmq_stream_s3_util:elapsed_ms(StartTs),
    rabbitmq_stream_s3_histogram:observe({?MODULE, request_duration, read}, Ms),
    ok.

observe(Kind, Fun) ->
    T0 = erlang:monotonic_time(),
    try
        Fun()
    after
        DurationMs = rabbitmq_stream_s3_util:elapsed_ms(T0),
        rabbitmq_stream_s3_histogram:observe({?MODULE, request_duration, Kind}, DurationMs)
    end.

-spec request_duration_prometheus_format() -> map().
request_duration_prometheus_format() ->
    Values = [
        begin
            {Buckets, Count, Sum} = rabbitmq_stream_s3_histogram:prometheus_format(
                {?MODULE, request_duration, Kind},
                fun(Ms) -> Ms / 1000 end,
                ?REQUEST_DURATION_BUCKETS
            ),
            {[{kind, Kind}], Buckets, Count, Sum}
        end
     || Kind <- [read, write]
    ],
    #{
        request_duration_seconds => #{
            type => histogram,
            help => <<"Duration of S3 API requests in seconds">>,
            values => Values
        }
    }.
