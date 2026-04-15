%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_api_aws).
-moduledoc """
A wrapper around the AWS S3 HTTP API.
""".

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-define(MiB, 1048576).

%% API:
-export([
    init/0,
    reload_config/0,
    get/2,
    get_range/3,
    get_range_async/3,
    put/3,
    stream_put/3,
    stream_data/2,
    stream_finish/2,
    delete/2,
    delete_prefix/2,
    list/3,
    match_async/2,
    handle_async/3,
    cancel_async/2
]).

%% For apply/3:
-export([get_credentials/0, get_credentials/2]).

%% For the pool. Not to be called by anyone else.
-export([hostname/0]).

-define(ALGORITHM, "AWS4-HMAC-SHA256").
-define(ISOFORMAT_BASIC, "~4.10.0b~2.10.0b~2.10.0bT~2.10.0b~2.10.0b~2.10.0bZ").
-define(TABLE, ?MODULE).
-define(METADATA_TOKEN_TTL_SECONDS, 60).
%% A margin to add to a TTL. Subtracting a few seconds reduces the chances that
%% we use a token just as it expires.
-define(TTL_SECONDS_BUFFER, 5).
-define(REGION_KEY, rabbitmq_stream_s3_api_aws_region).
-define(GENERAL_POOL, rabbitmq_stream_s3_general_pool).
-define(UPLOAD_POOL, rabbitmq_stream_s3_upload_pool).
%% Amount of data to buffer in async state before giving it to the remote
%% reader process. See the async_state() type.
%% 1024^2 (1 MiB).
-define(BUFFER_PENDING_DATA_BYTES, 1_048_576).

-define(C_ACTIVE_REQUESTS, 1).
-define(C_TOTAL_REQUESTS, 2).
-define(C_RESPONSE_500, 3).
-define(C_RESPONSE_503, 4).
-define(COUNTERS, [
    {active_requests, ?C_ACTIVE_REQUESTS, gauge, "Current number of requests to S3"},
    {total_requests, ?C_TOTAL_REQUESTS, counter, "Total number of requests to S3"},
    {response_500, ?C_RESPONSE_500, counter, "Number of HTTP 500 responses"},
    {response_503, ?C_RESPONSE_503, counter, "Number of HTTP 503 responses"}
]).
-define(COUNTER_KEY, {?MODULE, counter}).

-record(container_creds_req, {host, port, path, conn, stream_ref}).

-behaviour(rabbitmq_stream_s3_api).

-type key() :: rabbitmq_stream_s3:key().
-type request_opts() ::
    rabbitmq_stream_s3_api:request_opts()
    | #{
        %% uri_string:compose_query/1's QueryList parameter:
        query => [{binary(), binary() | true}]
    }.
-doc """
Uppercase HTTP method name, as a binary.

Called "HTTP Verb" in S3 docs. "GET", "PUT", "HEAD", "POST", "DELETE", etc..
""".
-type http_method() :: binary().
-type http_response() :: #{
    status := pos_integer(),
    %% TODO: why is gun:resp_headers() not exported?
    headers := [{binary(), binary()}],
    body => binary()
}.
%% Map keys must be lowercase.
-type req_headers() :: #{binary() => binary()}.
-type continuation_token() :: binary().
-type objects_metadata() :: #{
    objects := non_neg_integer(),
    total_size := non_neg_integer(),
    pages := non_neg_integer()
}.
-type async_state() :: #{
    pool := ?GENERAL_POOL | ?UPLOAD_POOL,
    conn := pid(),
    stream_ref := gun:stream_ref(),
    %% PERF: The remote tier sends relatively small binaries when reading with
    %% chunked transfer-encoding. Appending these binaries to the remote
    %% reader's buffer individually creates a lot of binary garbage, which
    %% results in long GC times for the reader process. This very significantly
    %% impacts consumption throughput and memory overhead.
    %%
    %% To avoid the garbage overhead, we prepend data sent in gun_data messages
    %% to this list and reverse and concatenate the binaries into one large
    %% binary when a fairly large amount of data has been collected.
    data => [binary()],
    pending_bytes => non_neg_integer(),
    timeout => timeout(),
    %% Timer reference for request timeout. Set when a `timeout` is given in
    %% request opts. Cancelled and flushed in `finish_async/1`.
    timer_ref => reference()
}.
%% Re-use the gun stream ref since it's already a reference.
-type async_req() :: gun:stream_ref().

-spec init() -> ok.
init() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?COUNTER_KEY, Cnt),
    _ = ets:new(?TABLE, [public, named_table]),
    reload_config().

-spec reload_config() -> ok.
reload_config() ->
    AccessKey0 = application:get_env(rabbitmq_stream_s3, aws_access_key),
    SecretKey0 = application:get_env(rabbitmq_stream_s3, aws_secret_key),
    case {AccessKey0, SecretKey0} of
        {undefined, undefined} ->
            ok;
        {{ok, AccessKey}, {ok, SecretKey}} ->
            _ = ets:insert(?TABLE, {
                credentials,
                AccessKey,
                SecretKey,
                application:get_env(rabbitmq_stream_s3, aws_security_token, undefined),
                undefined
            }),
            ok
        %% TODO: helpful error message when only one of these keys is set...
    end,
    case application:get_env(rabbitmq_stream_s3, aws_region) of
        {ok, Region} ->
            persistent_term:put(?REGION_KEY, Region),
            ok;
        undefined ->
            ok
    end,
    ok.

-doc "Gets the body of an object at key `Key`".
-spec get(key(), request_opts()) -> {ok, binary()} | {error, any()}.
get(Key, Opts) when is_binary(Key) andalso is_map(Opts) ->
    case request(<<"GET">>, key_to_path(Key), #{}, <<>>, Opts) of
        {ok, #{status := 200, body := Data}} ->
            {ok, Data};
        {ok, #{status := 404}} ->
            {error, not_found};
        {ok, #{status := Status} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Status, Key),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-doc """
Gets the given range `Range` of bytes of the object at `Key`.

See the `range_spec()` type: this can be used to read starting at a given byte
number, read a number of bytes from end of the object, or read an absolute
range.
""".
-spec get_range(key(), rabbitmq_stream_s3_api:range_spec(), request_opts()) ->
    {ok, binary()} | {error, any()}.
get_range(Key, Range, Opts) when is_binary(Key) andalso is_map(Opts) ->
    Headers = #{<<"range">> => range_specifier(Range)},
    case request(<<"GET">>, key_to_path(Key), Headers, <<>>, Opts) of
        %% HTTP Range requests must return 206 if only a partial range is served,
        %% according to the RFC.
        {ok, #{status := Status, body := Data}} when Status =:= 200 orelse Status =:= 206 ->
            {ok, Data};
        {ok, #{status := 404}} ->
            {error, not_found};
        {ok, #{status := Status} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Status, Key),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-spec get_range_async(key(), rabbitmq_stream_s3_api:range_spec(), request_opts()) ->
    {ok, async_req(), async_state()} | {error, any()}.
get_range_async(Key, Range, Opts) when is_binary(Key) andalso is_map(Opts) ->
    Headers = #{<<"range">> => range_specifier(Range)},
    request_async(<<"GET">>, key_to_path(Key), Headers, <<>>, Opts).

-doc "Uploads the given `Data` as an object at key `Key`".
-spec put(key(), iodata(), request_opts()) -> ok | {error, any()}.
put(Key, Data, Opts) when is_binary(Key) andalso is_map(Opts) ->
    Headers =
        case Opts of
            #{crc32 := Checksum} ->
                #{<<"x-amz-checksum-crc32">> => base64:encode(<<Checksum:32/unsigned>>)};
            _ ->
                #{}
        end,
    case request(<<"PUT">>, key_to_path(Key), Headers, Data, Opts) of
        {ok, #{status := 200}} ->
            ok;
        {ok, #{status := Status} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Status, Key),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-spec stream_put(key(), pos_integer(), request_opts()) -> {ok, async_state()} | {error, any()}.
stream_put(Key, ContentLength, Opts0) when is_binary(Key) andalso is_map(Opts0) ->
    Method = <<"PUT">>,
    Path = key_to_path(Key),
    EncodedLength = aws_chunked_encoded_length(ContentLength),
    Headers0 = #{
        <<"content-length">> => integer_to_binary(EncodedLength),
        <<"content-encoding">> => <<"aws-chunked">>,
        <<"x-amz-decoded-content-length">> => integer_to_binary(ContentLength),
        <<"x-amz-trailer">> => <<"x-amz-checksum-crc32">>
    },
    case get_credentials() of
        {ok, AccessKey, SecretKey, SecurityToken} ->
            Headers = sign_headers(
                Headers0,
                AccessKey,
                SecretKey,
                SecurityToken,
                Method,
                Path,
                no_body,
                Opts0#{stream_payload => true}
            ),
            Cnt = counter(),
            counters:add(Cnt, ?C_ACTIVE_REQUESTS, 1),
            counters:add(Cnt, ?C_TOTAL_REQUESTS, 1),
            Pool = ?UPLOAD_POOL,
            Conn = rabbitmq_stream_s3_api_aws_pool:checkout(Pool, 10_000),
            StreamRef = gun:headers(Conn, Method, Path, Headers),
            State = #{
                pool => Pool,
                conn => Conn,
                stream_ref => StreamRef,
                data => [],
                pending_bytes => 0,
                timeout => maps:get(timeout, Opts0, 60_000)
            },
            {ok, State};
        {error, _} = Err ->
            Err
    end.

-spec stream_data(async_state(), iodata()) -> async_state().
stream_data(#{data := PendingData0, pending_bytes := PendingBytes0} = State0, Data) ->
    PendingData = [PendingData0, Data],
    PendingBytes = PendingBytes0 + iolist_size(Data),
    State = State0#{data := PendingData, pending_bytes := PendingBytes},
    flush_chunks(State).

flush_chunks(
    #{
        conn := Conn,
        stream_ref := StreamRef,
        data := PendingData,
        pending_bytes := PendingBytes
    } = State0
) when PendingBytes >= ?MiB ->
    Flat = iolist_to_binary(PendingData),
    <<Chunk:?MiB/binary, Rest/binary>> = Flat,
    send_chunk(Conn, StreamRef, Chunk),
    flush_chunks(State0#{data := [Rest], pending_bytes := byte_size(Rest)});
flush_chunks(State) ->
    State.

-spec stream_finish(async_state(), non_neg_integer()) -> ok | {error, any()}.
stream_finish(
    #{
        conn := Conn,
        stream_ref := StreamRef,
        data := PendingData,
        pending_bytes := PendingBytes,
        timeout := Timeout
    } = State,
    Crc32
) ->
    %% Flush remaining buffer as the last data chunk (may be smaller than ?MiB).
    case PendingBytes of
        0 -> ok;
        _ -> send_chunk(Conn, StreamRef, iolist_to_binary(PendingData))
    end,
    Checksum = base64:encode(<<Crc32:32/unsigned>>),
    %% Final aws-chunked terminator + trailer + CRLF, sent as the last body chunk.
    ok = gun:data(
        Conn,
        StreamRef,
        fin,
        <<"0\r\nx-amz-checksum-crc32:", Checksum/binary, "\r\n\r\n">>
    ),
    try await_response(Conn, StreamRef, Timeout) of
        {ok, #{status := 200}} ->
            ok;
        {ok, #{status := Status} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Status),
            {error, Other};
        {error, _} = Err ->
            Err
    after
        finish_async(State)
    end.

send_chunk(Conn, StreamRef, Chunk) when is_binary(Chunk) ->
    Size = byte_size(Chunk),
    gun:data(Conn, StreamRef, nofin, [integer_to_binary(Size, 16), <<"\r\n">>, Chunk, <<"\r\n">>]).

-doc "Deletes the given key or list of keys".
-spec delete(key() | [key()], request_opts()) ->
    ok | {error, any()}.
delete(Keys, Opts) when is_list(Keys) andalso is_map(Opts) ->
    %% <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html>
    ?assert(length(Keys) =< 1000),
    %% Though not documented, S3 will reject the request if the list of keys
    %% is empty.
    ?assertNotEqual([], Keys),
    Data = delete_many_body(Keys),
    Headers = #{
        %% A checksum header seems to be required on this endpoint...
        <<"x-amz-checksum-crc32">> => base64:encode(<<(erlang:crc32(Data)):32/unsigned>>)
    },
    case request(<<"POST">>, <<"/?delete=">>, Headers, Data, Opts) of
        {ok, #{status := 200}} ->
            ok;
        {ok, #{status := Status} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Status),
            {error, Other};
        {error, _} = Err ->
            Err
    end;
delete(Key, Opts) when is_binary(Key) andalso is_map(Opts) ->
    %% <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html>.
    case request(<<"DELETE">>, key_to_path(Key), #{}, <<>>, Opts) of
        {ok, #{status := 204}} ->
            ok;
        {ok, #{status := Status} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Status, Key),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-doc """
Deletes all objects which are prefixed by the given key.

The S3 API doesn't provide an API for this directly so we first list a page of
objects under the prefix and then delete the page. ListObjects returns a max of
1000 keys which is also the max amount of keys we can pass to DeleteObjects.

Because of the pagination this function is quite slow on prefixes where many
many keys exist. So this function should only be used in the background - not
ever blocking any other operation.
""".
-spec delete_prefix(key(), request_opts()) ->
    {ok, objects_metadata()} | {error, any()}.
delete_prefix(Prefix, Opts) when is_binary(Prefix) andalso is_map(Opts) ->
    delete_prefix(Prefix, Opts, undefined, 0, 0, 0).

delete_prefix(Prefix, Opts, Token, Objects0, TotalSize0, Pages0) ->
    case list(Prefix, Token, Opts) of
        {ok, {[], 0, undefined}} ->
            Meta = #{
                objects => Objects0,
                total_size => TotalSize0,
                pages => Pages0
            },
            {ok, Meta};
        {ok, {PageKeys, PageSize, NextToken}} ->
            case delete(PageKeys, Opts) of
                ok ->
                    Objects = Objects0 + length(PageKeys),
                    TotalSize = TotalSize0 + PageSize,
                    Pages = Pages0 + 1,
                    case NextToken of
                        undefined ->
                            Meta = #{
                                objects => Objects,
                                total_size => TotalSize,
                                pages => Pages
                            },
                            {ok, Meta};
                        _ ->
                            delete_prefix(
                                Prefix,
                                Opts,
                                NextToken,
                                Objects,
                                TotalSize,
                                Pages
                            )
                    end;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

list(Prefix, ContinuationToken, Opts) ->
    Params0 = [{<<"list-type">>, <<"2">>}, {<<"prefix">>, Prefix}],
    Params1 =
        case ContinuationToken of
            undefined ->
                Params0;
            _ ->
                [{<<"continuation-token">>, ContinuationToken} | Params0]
        end,
    Params = uri_string:compose_query(Params1),
    case request(<<"GET">>, <<"/?", Params/binary>>, #{}, <<>>, Opts) of
        {ok, #{status := 200, body := Body}} ->
            {ok, decode_list_bucket_result(Body)};
        {ok, #{status := Status} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Status),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-spec decode_list_bucket_result(binary()) ->
    {[key()], TotalSize :: non_neg_integer(), continuation_token() | undefined}.
decode_list_bucket_result(Data) ->
    {#xmlElement{name = 'ListBucketResult', content = Result}, []} = xmerl_scan:string(
        binary_to_list(Data)
    ),
    lists:foldl(
        fun
            (
                #xmlElement{name = 'NextContinuationToken', content = [#xmlText{value = T}]},
                {Keys, TotalSize, _NextToken}
            ) ->
                {Keys, TotalSize, list_to_binary(T)};
            (#xmlElement{name = 'Contents', content = Contents}, {Keys0, TotalSize0, Token}) ->
                #xmlElement{name = 'Key', content = [#xmlText{value = Key}]} = lists:keyfind(
                    'Key',
                    #xmlElement.name,
                    Contents
                ),
                #xmlElement{name = 'Size', content = [#xmlText{value = Size}]} = lists:keyfind(
                    'Size',
                    #xmlElement.name,
                    Contents
                ),
                TotalSize = TotalSize0 + list_to_integer(Size),
                {[list_to_binary(Key) | Keys0], TotalSize, Token};
            (#xmlElement{}, Acc) ->
                Acc
        end,
        {[], 0, undefined},
        Result
    ).

log_unexpected_status(Function, Status) ->
    ?LOG_DEBUG("~ts unexpected HTTP status ~b", [Function, Status]).

log_unexpected_status(Function, Status, Key) ->
    ?LOG_DEBUG("~ts unexpected HTTP status ~b for key ~ts", [Function, Status, Key]).

-spec match_async(Msg :: term(), #{async_req() := async_state()}) ->
    {ok, async_req()} | error.
match_async({gun_error, Conn, _Reason}, Reqs) ->
    maps:fold(
        fun
            (Req, #{conn := C}, error) when C =:= Conn -> {ok, Req};
            (_, _, Acc) -> Acc
        end,
        error,
        Reqs
    );
match_async(Msg, Reqs) ->
    Req =
        case Msg of
            {gun_error, _, StreamRef, _} -> StreamRef;
            {gun_response, _, StreamRef, _, _, _} -> StreamRef;
            {gun_data, _, StreamRef, _, _} -> StreamRef;
            {request_timeout, StreamRef} -> StreamRef;
            _ -> undefined
        end,
    case Reqs of
        #{Req := _} -> {ok, Req};
        _ -> error
    end.

-spec handle_async(Msg :: term(), async_req(), async_state()) ->
    {continue, async_state()}
    | {data, binary(), async_state()}
    | {done, ok | {error, any()}}
    | ignore.
handle_async(
    {gun_error, Conn, StreamRef, Reason},
    StreamRef,
    #{conn := Conn, stream_ref := StreamRef} = State
) ->
    ?LOG_DEBUG("Received stream error on ~tw/~tw from gun: ~0p", [Conn, StreamRef, Reason]),
    finish_async(State),
    {done, {error, stream_error}};
handle_async(
    {gun_error, Conn, Reason},
    _StreamRef,
    #{conn := Conn} = State
) ->
    ?LOG_DEBUG("Received connection error on ~tw from gun: ~0p", [Conn, Reason]),
    finish_async(State),
    {done, {error, connection_error}};
handle_async(
    {gun_response, Conn, StreamRef, fin, Status, Headers},
    StreamRef,
    #{conn := Conn, stream_ref := StreamRef} = State
) ->
    Result =
        case Status of
            200 ->
                ok;
            404 ->
                {error, not_found};
            500 ->
                {error, internal_error};
            503 ->
                {error, slow_down};
            _ ->
                {error, #{status => Status, headers => Headers}}
        end,
    finish_async(State),
    {done, Result};
handle_async(
    {gun_response, Conn, StreamRef, nofin, Status, Headers},
    StreamRef,
    #{conn := Conn, stream_ref := StreamRef} = State0
) ->
    case Status of
        200 ->
            State = State0#{data => [], pending_bytes => 0},
            {continue, State};
        206 ->
            State = State0#{data => [], pending_bytes => 0},
            {continue, State};
        404 ->
            finish_async(State0),
            {done, {error, not_found}};
        500 ->
            finish_async(State0),
            {done, {error, internal_error}};
        503 ->
            finish_async(State0),
            {done, {error, slow_down}};
        _ ->
            finish_async(State0),
            {done, {error, #{status => Status, headers => Headers}}}
    end;
handle_async(
    {gun_data, Conn, StreamRef, nofin, Data},
    StreamRef,
    #{
        conn := Conn,
        stream_ref := StreamRef,
        pending_bytes := PendingBytes0,
        data := PendingData0
    } = State0
) ->
    case PendingBytes0 > ?BUFFER_PENDING_DATA_BYTES of
        true ->
            AllData = iolist_to_binary(lists:reverse(PendingData0, [Data])),
            State = State0#{data := [], pending_bytes := 0},
            {data, AllData, State};
        false ->
            State = State0#{
                data := [Data | PendingData0],
                pending_bytes := PendingBytes0 + byte_size(Data)
            },
            {continue, State}
    end;
handle_async(
    {gun_data, Conn, StreamRef, fin, Data},
    StreamRef,
    #{conn := Conn, stream_ref := StreamRef, data := Data0} = State
) ->
    finish_async(State),
    PendingData = iolist_to_binary(lists:reverse(Data0, [Data])),
    {data, PendingData, done};
handle_async(
    {request_timeout, StreamRef},
    StreamRef,
    #{conn := Conn, stream_ref := StreamRef} = State
) ->
    ?LOG_WARNING("S3 request timed out on ~tw/~tw", [Conn, StreamRef]),
    gun:cancel(Conn, StreamRef),
    finish_async(State),
    {done, {error, timeout}}.

-spec cancel_async(async_req(), async_state()) -> ok.
cancel_async(StreamRef, #{conn := Conn, stream_ref := StreamRef} = State) ->
    gun:cancel(Conn, StreamRef),
    ok = finish_async(State).

finish_async(#{conn := Conn, pool := Pool} = State) ->
    case State of
        #{timer_ref := TimerRef, stream_ref := StreamRef} ->
            case erlang:cancel_timer(TimerRef) of
                false ->
                    receive
                        {request_timeout, StreamRef} -> ok
                    after 0 -> ok
                    end;
                _ ->
                    ok
            end;
        _ ->
            ok
    end,
    counters:sub(counter(), ?C_ACTIVE_REQUESTS, 1),
    ok = rabbitmq_stream_s3_api_aws_pool:checkin(Pool, Conn).

-spec request(http_method(), key(), req_headers(), iodata(), request_opts()) ->
    {ok, http_response()}
    | {error, any()}.
request(Method, Path, Headers0, Body, Opts) when
    is_binary(Method) andalso
        is_binary(Path) andalso
        is_map(Headers0) andalso
        is_map(Opts)
->
    %% TODO: pass timeout through get_credentials/0?
    case get_credentials() of
        {ok, AccessKey, SecretKey, SecurityToken} ->
            Headers = sign_headers(
                Headers0,
                AccessKey,
                SecretKey,
                SecurityToken,
                Method,
                Path,
                Body,
                Opts
            ),
            Cnt = counter(),
            counters:add(Cnt, ?C_ACTIVE_REQUESTS, 1),
            counters:add(Cnt, ?C_TOTAL_REQUESTS, 1),
            try
                request0(Method, Path, Headers, Body, Opts)
            after
                counters:sub(Cnt, ?C_ACTIVE_REQUESTS, 1)
            end;
        {error, _} = Err ->
            Err
    end.

request0(Method, Path, Headers, Body, Opts) ->
    request0(Method, Path, Headers, Body, Opts, 2).

request0(Method, Path, Headers, Body, Opts, 0) ->
    request1(Method, Path, Headers, Body, Opts);
request0(Method, Path, Headers, Body, Opts, Retries) ->
    case request1(Method, Path, Headers, Body, Opts) of
        {error, {down, normal}} ->
            %% Retry if we get a connection that closes upon request.
            request0(Method, Path, Headers, Body, Opts, Retries - 1);
        Other ->
            Other
    end.

request1(Method, Path, Headers, Body, Opts) ->
    Pool =
        case Method of
            <<"PUT">> -> ?UPLOAD_POOL;
            _ -> ?GENERAL_POOL
        end,
    Timeout = maps:get(timeout, Opts, 5_000),
    T1 = start_timeout_window(Timeout),
    rabbitmq_stream_s3_api_aws_pool:with(Pool, Timeout, fun(Conn) ->
        StreamRef = gun:request(Conn, Method, Path, Headers, Body),
        await_response(Conn, StreamRef, end_timeout_window(Timeout, T1))
    end).

await_response(Conn, StreamRef, Timeout) ->
    T1 = start_timeout_window(Timeout),
    case gun:await(Conn, StreamRef, Timeout) of
        {response, fin, Status, RespHeaders} ->
            Response = #{status => Status, headers => RespHeaders},
            postprocess_response(Response),
            {ok, Response};
        {response, nofin, Status, RespHeaders} ->
            case gun:await_body(Conn, StreamRef, end_timeout_window(Timeout, T1)) of
                {ok, RespBody} ->
                    Response = #{
                        status => Status,
                        headers => RespHeaders,
                        body => RespBody
                    },
                    postprocess_response(Response),
                    {ok, Response};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

postprocess_response(#{status := 503}) ->
    counters:add(counter(), ?C_RESPONSE_503, 1);
postprocess_response(#{status := 500}) ->
    counters:add(counter(), ?C_RESPONSE_500, 1);
postprocess_response(_) ->
    ok.

request_async(Method, Path, Headers0, Body, Opts) ->
    case get_credentials() of
        {ok, AccessKey, SecretKey, SecurityToken} ->
            Headers = sign_headers(
                Headers0,
                AccessKey,
                SecretKey,
                SecurityToken,
                Method,
                Path,
                Body,
                Opts
            ),
            Pool = ?GENERAL_POOL,
            start_async_request(Pool, Method, Path, Headers, Body, Opts);
        {error, _} = Err ->
            Err
    end.

start_async_request(Pool, Method, Path, Headers, Body, Opts) ->
    case rabbitmq_stream_s3_api_aws_pool:try_checkout(Pool) of
        {ok, Conn} ->
            Cnt = counter(),
            counters:add(Cnt, ?C_ACTIVE_REQUESTS, 1),
            counters:add(Cnt, ?C_TOTAL_REQUESTS, 1),
            %% NOTE: no need to wrap this in try/catch and checkin the conn
            %% since gun:request/5 cannot exit/error/throw.
            StreamRef = gun:request(Conn, Method, Path, Headers, Body),
            State = #{pool => Pool, conn => Conn, stream_ref => StreamRef},
            {ok, StreamRef, maybe_set_timer(Opts, StreamRef, State)};
        busy ->
            {error, pool_busy}
    end.

maybe_set_timer(#{timeout := Timeout}, StreamRef, State) ->
    TimerRef = erlang:send_after(Timeout, self(), {request_timeout, StreamRef}),
    State#{timer_ref => TimerRef};
maybe_set_timer(_Opts, _StreamRef, State) ->
    State.

-spec hostname() -> binary().
hostname() ->
    hostname(region()).

-spec hostname(Region :: binary()) -> binary().
hostname(Region) ->
    <<"s3.", Region/binary, $., (tld(Region))/binary>>.

-spec region() -> binary().
region() ->
    Attempts = application:get_env(rabbitmq_stream_s3, get_region_attempts, 10),
    region(Attempts).

region(Retries) ->
    case persistent_term:get(?REGION_KEY, undefined) of
        undefined ->
            get_region_from_instance_metadata(Retries);
        Region ->
            Region
    end.

get_region_from_instance_metadata(0) ->
    {error, cannot_acquire_region_lock};
get_region_from_instance_metadata(Retries) ->
    LockId = {?REGION_KEY, erlang:make_ref()},
    case global:set_lock(LockId, [node()], 0) of
        true ->
            %% If we get the lock with no retries then we are the first to try,
            %% and we are in charge of the request.
            try
                request_region_from_instance_metadata_locked()
            after
                global:del_lock(LockId, [node()])
            end;
        false ->
            %% Another process is performing the refresh. Please wait...
            timer:sleep(100),
            region(Retries - 1)
    end.

request_region_from_instance_metadata_locked() ->
    {ok, R} = with_instance_metadata_conn(fun(Conn) ->
        {ok, #{status := 200, body := Body}} = get_instance_metadata(
            Conn,
            <<"GET">>,
            <<"/latest/meta-data/placement/availability-zone">>,
            #{<<"x-aws-ec2-metadata-token">> => metadata_token()}
        ),
        %% Strip trailing availability zone character, e.g. us-east-2c -> us-east-2
        Region = binary:part(Body, 0, byte_size(Body) - 1),
        persistent_term:put(?REGION_KEY, Region),
        {ok, Region}
    end),
    R.

-spec tld(Region :: binary()) -> binary().
tld(Region) ->
    Mapping = maps:merge(
        #{
            <<"cn-north-1">> => <<"amazonaws.com.cn">>,
            <<"cn-northwest-1">> => <<"amazonaws.com.cn">>,
            <<"us-iso-east-1">> => <<"c2s.ic.gov">>,
            <<"us-iso-west-1">> => <<"c2s.ic.gov">>,
            <<"us-isob-east-1">> => <<"sc2s.sgov.gov">>,
            <<"us-isof-east-1">> => <<"csp.hci.ic.gov">>,
            <<"us-isof-south-1">> => <<"csp.hci.ic.gov">>,
            <<"eusc-de-east-1">> => <<"amazonaws.eu">>
        },
        application:get_env(rabbitmq_stream_s3, region_endpoints, #{})
    ),
    maps:get(Region, Mapping, <<"amazonaws.com">>).

-spec get_credentials() ->
    {ok, AccessKey :: binary(), SecretKey :: binary(), SecurityToken :: binary() | undefined}
    | {error, any()}.
get_credentials() ->
    case get_credentials_cached() of
        {ok, _, _, _} = Ok -> Ok;
        error -> fetch_credentials(os:getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI"))
    end.

fetch_credentials(false) ->
    ?LOG_INFO(
        ?MODULE_STRING
        ": no AWS credentials available, requesting from EC2 instance metadata"
    ),
    Attempts = application:get_env(rabbitmq_stream_s3, get_credentials_attempts, 10),
    {Msec, Result} = timer:tc(?MODULE, get_credentials, [Attempts, imds], millisecond),
    log_credentials_result(Result, Msec, "EC2 instance metadata service"),
    Result;
fetch_credentials(URI) ->
    ?LOG_INFO(
        ?MODULE_STRING
        ": no AWS credentials available, requesting from container credentials endpoint"
    ),
    Attempts = application:get_env(rabbitmq_stream_s3, get_credentials_attempts, 10),
    {Msec, Result} = timer:tc(?MODULE, get_credentials, [Attempts, {container, URI}], millisecond),
    log_credentials_result(Result, Msec, "container credentials endpoint"),
    Result.

log_credentials_result({ok, _, _, _}, Msec, Source) ->
    ?LOG_INFO(
        "Successfully acquired credentials from ~ts in ~bms",
        [Source, Msec]
    );
log_credentials_result({error, _}, Msec, Source) ->
    ?LOG_ERROR(
        "Failed to acquire credentials from ~ts in ~bms",
        [Source, Msec]
    ).

get_credentials_cached() ->
    case ets:lookup(?TABLE, credentials) of
        [{credentials, AccessKey, SecretKey, SecurityToken, Expiration}] ->
            case is_expired(Expiration) of
                true ->
                    error;
                false ->
                    {ok, AccessKey, SecretKey, SecurityToken}
            end;
        [] ->
            error
    end.

is_expired(Expiration) when is_integer(Expiration) ->
    Now = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
    Now + ?TTL_SECONDS_BUFFER > Expiration;
is_expired(undefined) ->
    false.

get_credentials(Retries, Source) ->
    case get_credentials_cached() of
        {ok, _, _, _} = Ok ->
            Ok;
        error ->
            get_credentials_with_lock(Retries, Source)
    end.

get_credentials_with_lock(0, _Source) ->
    {error, cannot_acquire_credential_lock};
get_credentials_with_lock(Retries, Source) ->
    %% NOTE: lock ID `global:id()` is a tuple where the second element is the
    %% requester. We don't want any other process or even code path within the
    %% current process to attempt to join this lock request, so we use a
    %% random reference for uniqueness.
    LockId = {{?MODULE, credentials}, erlang:make_ref()},
    case global:set_lock(LockId, [node()], 0) of
        true ->
            %% If we get the lock with no retries then we are the first to try,
            %% and we are in charge of the request.
            try
                get_credentials_locked(Source)
            after
                global:del_lock(LockId, [node()])
            end;
        false ->
            %% Another process is performing the refresh. Please wait...
            timer:sleep(100),
            get_credentials(Retries - 1, Source)
    end.

get_credentials_locked(imds) ->
    request_credentials_from_instance_metadata_locked();
get_credentials_locked({container, URI}) ->
    request_credentials_from_container_endpoint(URI).

request_credentials_from_instance_metadata_locked() ->
    %% <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-metadata-security-credentials.html>
    with_instance_metadata_conn(fun(Conn) ->
        maybe
            {ok, RoleResp} ?=
                get_instance_metadata(
                    Conn,
                    <<"GET">>,
                    <<"/latest/meta-data/iam/security-credentials">>,
                    #{<<"x-aws-ec2-metadata-token">> => metadata_token()}
                ),
            %% TODO: more error handling...
            #{status := 200, body := Role} = RoleResp,
            {ok, CredsResp} ?=
                get_instance_metadata(
                    Conn,
                    <<"GET">>,
                    <<"/latest/meta-data/iam/security-credentials/", Role/binary>>,
                    #{<<"x-aws-ec2-metadata-token">> => metadata_token()}
                ),
            #{status := 200, body := Creds} = CredsResp,
            #{
                <<"AccessKeyId">> := AccessKey,
                <<"SecretAccessKey">> := SecretKey,
                <<"Token">> := SecurityToken,
                <<"Expiration">> := ExpirationIso8601
            } = json:decode(Creds),
            Expiration = parse_iso8601(ExpirationIso8601),
            _ = ets:insert(?TABLE, {
                credentials,
                AccessKey,
                SecretKey,
                SecurityToken,
                Expiration
            }),
            {ok, AccessKey, SecretKey, SecurityToken}
        end
    end).

request_credentials_from_container_endpoint(URI) ->
    %% <https://docs.aws.amazon.com/sdkref/latest/guide/feature-container-credentials.html>
    #{host := Host, port := Port, path := Path0} = uri_string:parse(URI),
    PortInt =
        case Port of
            undefined -> 80;
            _ -> Port
        end,
    Path =
        case Path0 of
            [] -> "/";
            _ -> Path0
        end,
    Req = #container_creds_req{host = Host, port = PortInt, path = Path},
    do_request_credentials_from_container_endpoint(
        open, Req, gun:open(Host, PortInt, #{transport => tcp, protocols => [http]})
    ).

do_request_credentials_from_container_endpoint(open, _Req, {error, _} = Err) ->
    Err;
do_request_credentials_from_container_endpoint(open, Req, {ok, Conn}) ->
    do_request_credentials_from_container_endpoint(
        await_up, Req#container_creds_req{conn = Conn}, gun:await_up(Conn, 7_000)
    );
do_request_credentials_from_container_endpoint(
    await_up, #container_creds_req{conn = Conn}, {error, _} = Err
) ->
    ok = gun:close(Conn),
    Err;
do_request_credentials_from_container_endpoint(
    await_up, #container_creds_req{path = Path, conn = Conn} = Req, {ok, _}
) ->
    StreamRef = gun:get(Conn, Path),
    try
        do_request_credentials_from_container_endpoint(
            await_response,
            Req#container_creds_req{stream_ref = StreamRef},
            gun:await(Conn, StreamRef, 13_000)
        )
    after
        gun:close(Conn)
    end;
do_request_credentials_from_container_endpoint(await_response, _Req, {error, _} = Err) ->
    Err;
do_request_credentials_from_container_endpoint(await_response, _Req, {response, _, Status, _}) when
    Status =/= 200
->
    {error, {unexpected_status, Status}};
do_request_credentials_from_container_endpoint(
    await_response,
    #container_creds_req{conn = Conn, stream_ref = StreamRef},
    {response, nofin, 200, _}
) ->
    {ok, Body} = gun:await_body(Conn, StreamRef, 6_000),
    #{
        <<"AccessKeyId">> := AccessKey,
        <<"SecretAccessKey">> := SecretKey,
        <<"Token">> := SecurityToken,
        <<"Expiration">> := ExpirationIso8601
    } = json:decode(Body),
    Expiration = parse_iso8601(ExpirationIso8601),
    _ = ets:insert(?TABLE, {credentials, AccessKey, SecretKey, SecurityToken, Expiration}),
    {ok, AccessKey, SecretKey, SecurityToken}.

-spec parse_iso8601(binary()) -> GregorianSeconds :: non_neg_integer().
parse_iso8601(<<
    Year:4/binary,
    $-,
    Month:2/binary,
    $-,
    Day:2/binary,
    $T,
    Hour:2/binary,
    $:,
    Minute:2/binary,
    $:,
    Second:2/binary,
    $Z
>>) ->
    calendar:datetime_to_gregorian_seconds(
        {
            {binary_to_integer(Year), binary_to_integer(Month), binary_to_integer(Day)},
            {binary_to_integer(Hour), binary_to_integer(Minute), binary_to_integer(Second)}
        }
    ).

-spec get_instance_metadata(pid(), http_method(), binary(), req_headers()) ->
    {ok, http_response()} | {error, any()}.
get_instance_metadata(Conn, Method, Path, Headers) ->
    StreamRef = gun:request(Conn, Method, Path, Headers, <<>>),
    case gun:await(Conn, StreamRef, 13_000) of
        {response, fin, Status, RespHeaders} ->
            {ok, #{status => Status, headers => RespHeaders}};
        {response, nofin, Status, RespHeaders} ->
            {ok, RespBody} = gun:await_body(Conn, StreamRef, 6_000),
            {ok, #{status => Status, headers => RespHeaders, body => RespBody}};
        {error, _} = Err ->
            Err
    end.

-spec metadata_token() -> binary().
metadata_token() ->
    case ets:lookup(?TABLE, metadata_token) of
        [{metadata_token, Token, Expiration}] ->
            case is_expired(Expiration) of
                true ->
                    get_metadata_token();
                false ->
                    Token
            end;
        [] ->
            get_metadata_token()
    end.

get_metadata_token() ->
    {ok, T} = with_instance_metadata_conn(fun(Conn) ->
        {ok, #{status := 200, body := Token}} = get_instance_metadata(
            Conn,
            <<"PUT">>,
            <<"/latest/api/token">>,
            #{
                <<"x-aws-ec2-metadata-token-ttl-seconds">> => integer_to_binary(
                    ?METADATA_TOKEN_TTL_SECONDS
                )
            }
        ),
        Expiration =
            calendar:datetime_to_gregorian_seconds(calendar:universal_time()) +
                ?METADATA_TOKEN_TTL_SECONDS,
        _ = ets:insert(?TABLE, {metadata_token, Token, Expiration}),
        {ok, Token}
    end),
    T.

with_instance_metadata_conn(Fun) when is_function(Fun, 1) ->
    %% TODO: determine what we should be logging at info level here.
    ?LOG_DEBUG(?MODULE_STRING ": connecting to EC2 instance metadata service"),
    % <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html>
    Host =
        case proplists:get_value(inet6, inet:get_rc(), false) of
            true -> "fd00:ec2::254";
            false -> "169.254.169.254"
        end,
    case gun:open(Host, 80, #{transport => tcp, protocols => [http]}) of
        {ok, Conn} ->
            case gun:await_up(Conn, 7_000) of
                {ok, _Protocol} ->
                    try
                        Fun(Conn)
                    after
                        gun:close(Conn)
                    end;
                {error, _} = Err ->
                    ok = gun:close(Conn),
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

sign_headers(Headers, AccessKey, SecretKey, SecurityToken, Method, Path, Body, Opts) ->
    {ok, Bucket} = application:get_env(rabbitmq_stream_s3, bucket),
    Region = region(),
    Host = <<Bucket/binary, $., (hostname(Region))/binary>>,
    sign_headers(
        calendar:universal_time(),
        Host,
        region(),
        Headers,
        AccessKey,
        SecretKey,
        SecurityToken,
        Method,
        Path,
        Body,
        Opts
    ).

sign_headers(
    {{Y, M, D}, {HH, MM, SS}} = _UniversalTimestamp,
    Host,
    Region,
    Headers0,
    AccessKey,
    SecretKey,
    SecurityToken,
    Method,
    Path,
    Body,
    Opts
) ->
    %% See <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>.
    %% The signature mainly mitigates replay attacks where an attacker
    %% man-in-the-middle's you, reusing the authorization header from a
    %% legitimate request to perform their own operation. It does this by
    %% hashing enough information about your request like HTTP method and
    %% bucket key with high entropy secrets (your credentials). With enough
    %% request details, the signature prevents an attacker from, for example,
    %% overwriting your object to a malicious one when they reuse a prerecorded
    %% authorization header.
    %% YYYYMMDD is 8 bytes.
    <<Date:8/binary, _/binary>> =
        RequestTimestamp = iolist_to_binary(io_lib:format(?ISOFORMAT_BASIC, [Y, M, D, HH, MM, SS])),
    PayloadHash =
        case Opts of
            #{unsigned_payload := true} ->
                <<"UNSIGNED-PAYLOAD">>;
            #{stream_payload := true} ->
                <<"STREAMING-UNSIGNED-PAYLOAD-TRAILER">>;
            _ ->
                hex(sha256hash(Body))
        end,
    DefaultHeaders0 = #{
        <<"host">> => Host,
        <<"x-amz-date">> => RequestTimestamp,
        <<"x-amz-content-sha256">> => PayloadHash
    },
    DefaultHeaders1 =
        case Opts of
            #{stream_payload := true} ->
                %% content-length (encoded size) is provided in Headers0 and will be
                %% merged in below. Do not add it here from iolist_size(Body).
                DefaultHeaders0;
            _ ->
                DefaultHeaders0#{<<"content-length">> => integer_to_binary(iolist_size(Body))}
        end,
    DefaultHeaders =
        case SecurityToken of
            undefined ->
                DefaultHeaders1;
            _ ->
                DefaultHeaders1#{<<"x-amz-security-token">> => SecurityToken}
        end,
    Headers1 = maps:merge(DefaultHeaders, Headers0),
    URIMap = uri_string:parse(Path),
    CanonicalRequest0 = <<
        %% <HTTPMethod>\n
        Method/binary,
        $\n,
        %% <CanonicalURI>\n
        (maps:get(path, URIMap))/binary,
        $\n,
        %% <CanonicalQueryString>\n
        (maps:get(query, URIMap, <<>>))/binary,
        $\n
    >>,
    %% Signed headers must be in order.
    {CanonicalRequest1, SignedHeaders} = maps:fold(
        fun(Head, Value, {Req0, Heads0}) ->
            case is_canonical_header(Head) of
                true ->
                    Req = <<Req0/binary, Head/binary, $:, Value/binary, $\n>>,
                    Heads =
                        case Heads0 of
                            <<>> ->
                                Head;
                            _ ->
                                <<Heads0/binary, $;, Head/binary>>
                        end,
                    {Req, Heads};
                false ->
                    {Req0, Heads0}
            end
        end,
        {CanonicalRequest0, <<>>},
        maps:iterator(Headers1, ordered)
    ),
    CanonicalRequest = <<
        %% <CanonicalHeaders>\n
        %%   Lowercase(<HeaderName1>) + ":" + Trim(<value>") + "\n" ...
        CanonicalRequest1/binary,
        %% \n from <CanonicalHeaders>\n
        $\n,
        %% <SignedHeaders>\n
        SignedHeaders/binary,
        $\n,
        %% <HashedPayload>
        PayloadHash/binary
    >>,
    StringToSign = <<
        %% "AWS4-HMAC-SHA256" + "\n" +
        ?ALGORITHM "\n",
        %% timeStampISO8601Format + "\n"
        RequestTimestamp/binary,
        $\n,
        %% <Scope> + "\n"
        Date/binary,
        $/,
        Region/binary,
        "/s3/aws4_request\n",
        %% Hex(Sha256Hash(<CanonicalRequest>))
        (hex(sha256hash(CanonicalRequest)))/binary
    >>,
    %% DateKey = HMAC-SHA256("AWS4"+"<SecretAccessKey>", "<YYYYMMDD>")
    DateKey = hmac_sha256(<<"AWS4", SecretKey/binary>>, Date),
    %% DateRegionKey = HMAC-SHA256(<DateKey>, "<aws-region>")
    DateRegionKey = hmac_sha256(DateKey, Region),
    %% DateRegionServiceKey = HMAC-SHA256(<DateRegionKey>, "<aws-service>")
    DateRegionServiceKey = hmac_sha256(DateRegionKey, <<"s3">>),
    %% SigningKey = HMAC-SHA256(<DateRegionServiceKey>, "aws4_request")
    SigningKey = hmac_sha256(DateRegionServiceKey, <<"aws4_request">>),
    %% HMAC-SHA256(SigningKey, StringToSign)
    Signature = hex(hmac_sha256(SigningKey, StringToSign)),
    Authorization = <<
        ?ALGORITHM " Credential=",
        AccessKey/binary,
        $/,
        Date/binary,
        $/,
        Region/binary,
        "/s3/aws4_request,SignedHeaders=",
        SignedHeaders/binary,
        ",Signature=",
        Signature/binary
    >>,
    Headers1#{<<"authorization">> => Authorization}.

-spec sha256hash(iodata()) -> <<_:256>>.
sha256hash(Data) ->
    crypto:hash(sha256, Data).

-spec hex(<<_:_*8>>) -> <<_:_*16>>.
hex(Data) when is_binary(Data) ->
    binary:encode_hex(Data, lowercase).

-spec hmac_sha256(iodata(), iodata()) -> binary().
hmac_sha256(Key, Message) ->
    crypto:mac(hmac, sha256, Key, Message).

%% The `CanonicalHeaders` list must include the following:
%% * HTTP `host` header
%% * If the `Content-MD5` header is present in the request, you must add it to
%%   the `CanonicalHeaders` list.
%% * Any `x-amz-*` headers that you plan to include in your request must also
%%   be added...
%% We also include `range` and `date` since the AWS documentation does too.
is_canonical_header(<<"host">>) -> true;
is_canonical_header(<<"content-encoding">>) -> true;
is_canonical_header(<<"Content-MD5">>) -> true;
is_canonical_header(<<"x-amz-", _/binary>>) -> true;
is_canonical_header(<<"range">>) -> true;
is_canonical_header(<<"date">>) -> true;
is_canonical_header(_) -> false.

%% https://www.rfc-editor.org/rfc/rfc9110.html#rule.ranges-specifier
-spec range_specifier(rabbitmq_stream_s3_api:range_spec()) -> binary().
range_specifier({StartByte, undefined}) ->
    <<"bytes=", (integer_to_binary(StartByte))/binary, "-">>;
range_specifier({StartByte, EndByte}) ->
    <<"bytes=", (integer_to_binary(StartByte))/binary, "-", (integer_to_binary(EndByte))/binary>>;
range_specifier(SuffixLen) when is_integer(SuffixLen) andalso SuffixLen < 0 ->
    %% integer_to_binary/1 will format the '-' for us.
    <<"bytes=", (integer_to_binary(SuffixLen))/binary>>.

%% See <https://github.com/rabbitmq/khepri/blob/0ebcf6918248729a9a975969afdde15b4ff98493/src/khepri_utils.erl#L50-L69>
-spec start_timeout_window(Timeout) -> Timestamp | none when
    Timeout :: timeout(),
    Timestamp :: integer().
start_timeout_window(infinity) ->
    none;
start_timeout_window(_Timeout) ->
    erlang:monotonic_time().
-spec end_timeout_window(Timeout, Timestamp | none) -> Timeout when
    Timeout :: timeout(),
    Timestamp :: integer().
end_timeout_window(infinity = Timeout, none) ->
    Timeout;
end_timeout_window(Timeout, T0) ->
    T1 = erlang:monotonic_time(),
    TDiff = erlang:convert_time_unit(T1 - T0, native, millisecond),
    Remaining = Timeout - TDiff,
    erlang:max(Remaining, 0).

delete_many_body(Keys) when is_list(Keys) ->
    Objects = [
        #xmlElement{
            name = 'Object',
            content = [#xmlElement{name = 'Key', content = [#xmlText{value = Key}]}]
        }
     || Key <- Keys
    ],
    Delete = #xmlElement{name = 'Delete', content = Objects},
    iolist_to_binary(xmerl:export_simple([Delete], xmerl_xml, [])).

-spec key_to_path(rabbitmq_stream_s3:key()) -> binary().
key_to_path(Key) ->
    <<$/, (uri_string:quote(Key, "/"))/binary>>.

%% Computes the aws-chunked framing overhead for a single chunk of DataSize bytes.
%% Each chunk is framed as: <hex-size>\r\n<data>\r\n
-spec aws_chunked_chunk_length(non_neg_integer()) -> non_neg_integer().
aws_chunked_chunk_length(DataSize) ->
    hex_digits(DataSize) + 2 + DataSize + 2.

-spec hex_digits(non_neg_integer()) -> pos_integer().
hex_digits(0) -> 1;
hex_digits(N) -> hex_digits(N, 0).

hex_digits(0, Acc) -> Acc;
hex_digits(N, Acc) -> hex_digits(N bsr 4, Acc + 1).

-doc """
Computes the total encoded content-length for an aws-chunked body with a CRC32 trailer,
given the decoded content length. All chunks except the last are exactly `?MiB` bytes.

The final terminator + trailer is:

    0\\r\\nx-amz-checksum-crc32:<8-char-base64>\\r\\n\\r\\n
""".
-spec aws_chunked_encoded_length(non_neg_integer()) -> non_neg_integer().
aws_chunked_encoded_length(ContentLength) ->
    FullChunks = ContentLength div ?MiB,
    Remainder = ContentLength rem ?MiB,
    %% "0\r\n" (3) + "x-amz-checksum-crc32:" (21) + base64(4 bytes)=8 + "\r\n" (2) + "\r\n" (2)
    TrailerLength = 3 + 21 + 8 + 2 + 2,
    FullChunks * aws_chunked_chunk_length(?MiB) +
        case Remainder of
            0 -> 0;
            _ -> aws_chunked_chunk_length(Remainder)
        end +
        TrailerLength.

counter() ->
    persistent_term:get(?COUNTER_KEY).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

hex_digits_test() ->
    ?assertEqual(1, hex_digits(0)),
    ?assertEqual(1, hex_digits(1)),
    ?assertEqual(1, hex_digits(15)),
    ?assertEqual(2, hex_digits(16)),
    ?assertEqual(2, hex_digits(255)),
    ?assertEqual(3, hex_digits(256)),
    %% 1 MiB = 0x100000 = 6 hex digits
    ?assertEqual(6, hex_digits(1048576)),
    ok.

aws_chunked_chunk_length_test() ->
    %% "3a\r\n<58 bytes>\r\n" = 2+2+58+2 = 64
    ?assertEqual(64, aws_chunked_chunk_length(58)),
    %% "100000\r\n<1048576 bytes>\r\n" = 6+2+1048576+2 = 1048586
    ?assertEqual(1048586, aws_chunked_chunk_length(1048576)),
    ok.

parse_iso8601_test() ->
    ?assertEqual(
        calendar:datetime_to_gregorian_seconds({{2026, 1, 21}, {1, 47, 0}}),
        parse_iso8601(<<"2026-01-21T01:47:00Z">>)
    ),
    ok.

range_spec_test() ->
    ?assertEqual(<<"bytes=-5">>, range_specifier(-5)),
    ?assertEqual(<<"bytes=10-20">>, range_specifier({10, 20})),
    ?assertEqual(<<"bytes=100-">>, range_specifier({100, undefined})),
    ok.

delete_many_body_test() ->
    ?assertEqual(
        <<"<?xml version=\"1.0\"?><Delete><Object><Key>sample1.txt</Key></Object><Object><Key>sample2.txt</Key></Object></Delete>">>,
        delete_many_body([<<"sample1.txt">>, <<"sample2.txt">>])
    ),
    ?assertEqual(
        <<"<?xml version=\"1.0\"?><Delete><Object><Key>foo&amp;bar.txt</Key></Object></Delete>">>,
        delete_many_body([<<"foo&bar.txt">>])
    ),
    ok.

sign_test() ->
    %% Examples from <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>:
    AccessKey = <<"AKIAIOSFODNN7EXAMPLE">>,
    SecretKey = <<"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY">>,

    %% Example: GET Object
    %% ==
    %% GET /test.txt HTTP/1.1
    %% Host: examplebucket.s3.amazonaws.com
    %% Authorization: SignatureToBeCalculated
    %% Range: bytes=0-9
    %% x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    %% x-amz-date: 20130524T000000Z
    #{<<"authorization">> := Authorization0} = sign_headers(
        {{2013, 5, 24}, {0, 0, 0}},
        <<"examplebucket.s3.amazonaws.com">>,
        <<"us-east-1">>,
        #{<<"range">> => <<"bytes=0-9">>, <<"x-amz-date">> => <<"20130524T000000Z">>},
        AccessKey,
        SecretKey,
        undefined,
        <<"GET">>,
        <<"/test.txt">>,
        <<>>,
        #{}
    ),
    ?assertEqual(
        <<"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41">>,
        Authorization0
    ),

    %% Example: PUT Object
    %% ==
    %% PUT test$file.text HTTP/1.1
    %% Host: examplebucket.s3.amazonaws.com
    %% Date: Fri, 24 May 2013 00:00:00 GMT
    %% Authorization: SignatureToBeCalculated
    %% x-amz-date: 20130524T000000Z
    %% x-amz-storage-class: REDUCED_REDUNDANCY
    %% x-amz-content-sha256: 44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072
    %%
    %% <Payload>
    %%
    %% Where `<Payload>` is "Welcome to Amazon S3."
    #{<<"authorization">> := Authorization1} = sign_headers(
        {{2013, 5, 24}, {0, 0, 0}},
        <<"examplebucket.s3.amazonaws.com">>,
        <<"us-east-1">>,
        #{
            <<"date">> => <<"Fri, 24 May 2013 00:00:00 GMT">>,
            <<"x-amz-date">> => <<"20130524T000000Z">>,
            <<"x-amz-storage-class">> => <<"REDUCED_REDUNDANCY">>
        },
        AccessKey,
        SecretKey,
        undefined,
        <<"PUT">>,
        uri_string:quote(<<"/test$file.text">>, "/"),
        <<"Welcome to Amazon S3.">>,
        #{}
    ),
    ?assertEqual(
        <<"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd">>,
        Authorization1
    ),

    %% Example: GET Bucket Lifecycle
    %% ==
    %% GET ?lifecycle HTTP/1.1
    %% Host: examplebucket.s3.amazonaws.com
    %% Authorization: SignatureToBeCalculated
    %% x-amz-date: 20130524T000000Z
    %% x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    #{<<"authorization">> := Authorization2} = sign_headers(
        {{2013, 5, 24}, {0, 0, 0}},
        <<"examplebucket.s3.amazonaws.com">>,
        <<"us-east-1">>,
        #{<<"x-amz-date">> => <<"20130524T000000Z">>},
        AccessKey,
        SecretKey,
        undefined,
        <<"GET">>,
        <<"/?lifecycle=">>,
        <<"">>,
        #{}
    ),
    ?assertEqual(
        <<"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543">>,
        Authorization2
    ),

    %% Example: Get Bucket (List Objects)
    %% ==
    %% GET ?max-keys=2&prefix=J HTTP/1.1
    %% Host: examplebucket.s3.amazonaws.com
    %% Authorization: SignatureToBeCalculated
    %% x-amz-date: 20130524T000000Z
    %% x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    #{<<"authorization">> := Authorization3} = sign_headers(
        {{2013, 5, 24}, {0, 0, 0}},
        <<"examplebucket.s3.amazonaws.com">>,
        <<"us-east-1">>,
        #{<<"x-amz-date">> => <<"20130524T000000Z">>},
        AccessKey,
        SecretKey,
        undefined,
        <<"GET">>,
        <<"/?max-keys=2&prefix=J">>,
        <<"">>,
        #{}
    ),
    ?assertEqual(
        <<"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7">>,
        Authorization3
    ),

    ok.

-endif.
