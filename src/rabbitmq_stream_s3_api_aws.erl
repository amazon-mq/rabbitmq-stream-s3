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
    start_link/0,
    reload_config/0,
    get/2,
    get_range/3,
    get_range_async/3,
    put/3,
    stream_put/3,
    stream_data/2,
    stream_finish/2,
    stream_abort/1,
    delete/2,
    list/3,
    check_bucket/1,
    match_async/3,
    handle_async/3,
    cancel_async/2
]).

%% For apply/3:
-export([get_credentials/0]).

%% For the pool. Not to be called by anyone else.
-export([hostname/0, note_request_started/0, note_request_finished/0]).

-ifdef(TEST).
%% For tests that need to observe the counters this module owns, including the
%% pool's, since the pool moves active_requests but does not own the counter.
-export([with_counter/1]).
-endif.

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, format_status/1]).

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
%% How long the read path waits for a pooled connection before returning
%% pool_busy. Observed same-region TLS handshakes complete in 5-34ms; 100ms
%% gives ~3x headroom while keeping reads responsive.
-define(READ_CHECKOUT_TIMEOUT_MS, 100).
%% Amount of data to buffer in async state before giving it to the remote
%% reader process. See the async_state() type.
%%
%% This also sets the remote reader's buffer granularity: each batch becomes
%% one immutable block in its rabbitmq_stream_s3_read_buffer, so this constant
%% caps how much memory a shared sub-binary read can pin (one block) and keeps
%% the block count per prefetch window low.
%% 1024^2 (1 MiB).
-define(BUFFER_PENDING_DATA_BYTES, 1_048_576).

-define(C_ACTIVE_REQUESTS, 1).
-define(C_TOTAL_REQUESTS, 2).
-define(C_RESPONSE_403, 3).
-define(C_RESPONSE_500, 4).
-define(C_RESPONSE_503, 5).
-define(C_REQUEST_TIMEOUTS, 6).
-define(COUNTERS, [
    {active_requests, ?C_ACTIVE_REQUESTS, gauge, "Current number of requests to S3"},
    {total_requests, ?C_TOTAL_REQUESTS, counter, "Total number of requests to S3"},
    {response_403, ?C_RESPONSE_403, counter, "Number of HTTP 403 responses"},
    {response_500, ?C_RESPONSE_500, counter, "Number of HTTP 500 responses"},
    {response_503, ?C_RESPONSE_503, counter, "Number of HTTP 503 responses"},
    {request_timeouts, ?C_REQUEST_TIMEOUTS, counter, "Number of S3 requests that timed out"}
]).
-define(COUNTER_KEY, {?MODULE, counter}).

-record(container_creds_req, {host, port, path, conn, stream_ref}).

-behaviour(gen_server).
-behaviour(rabbitmq_stream_s3_api).

-record(state, {
    metadata_token :: {Token :: binary(), Expiration :: non_neg_integer()} | undefined,
    refresh_timer :: reference() | undefined,
    source :: static | imds | {container, string()} | undefined
}).

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
    %% Present for get_range_async/3: the byte range the request asked for. Kept
    %% so a non-conformant 200 (full object) response can be sliced down to the
    %% requested range, mirroring the synchronous get_range/3 recovery.
    range => rabbitmq_stream_s3_api:range_spec(),
    %% Set when a Range request was answered with 200 (full object). The body is
    %% buffered in full and sliced to `range` at fin instead of being forwarded
    %% incrementally (forwarding would deliver bytes at the wrong offset).
    slice_full => rabbitmq_stream_s3_api:range_spec(),
    %% Timer reference for request timeout. Set when a `timeout` is given in
    %% request opts. Cancelled and flushed in `finish_async/1`.
    timer_ref => reference()
}.
%% Re-use the gun stream ref since it's already a reference.
-type async_req() :: gun:stream_ref().

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    case rabbitmq_stream_s3_api:backend() of
        rabbitmq_stream_s3_api_aws ->
            Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
            persistent_term:put(?COUNTER_KEY, Cnt),
            _ = ets:new(?TABLE, [protected, named_table, {read_concurrency, true}]),
            State = do_reload_config(#state{metadata_token = undefined, refresh_timer = undefined}),
            {ok, State};
        _ ->
            ignore
    end.

-spec reload_config() -> ok.
reload_config() ->
    gen_server:call(?MODULE, reload_config).

%% -------------------------------------------------------------------------
%% gen_server callbacks
%% -------------------------------------------------------------------------

handle_call(reload_config, _From, State0) ->
    State = do_reload_config(State0),
    {reply, ok, State};
handle_call(refresh_credentials, _From, State0) ->
    %% Re-check ETS to collapse concurrent callers (thundering herd).
    case get_credentials_cached() of
        {ok, _, _, _} = Ok ->
            {reply, Ok, State0};
        error ->
            {Reply, State} = do_refresh_credentials(State0),
            {reply, Reply, State}
    end;
handle_call(refresh_region, _From, State0) ->
    %% Re-check persistent_term to collapse concurrent callers.
    case persistent_term:get(?REGION_KEY, undefined) of
        undefined ->
            {Result, State} = request_region_from_instance_metadata(State0),
            {reply, Result, State};
        Region ->
            {reply, {ok, Region}, State0}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(refresh_credentials, State0) ->
    {_Result, State} = do_refresh_credentials(State0),
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

format_status(#{state := #state{source = Source, refresh_timer = TRef}} = Status) ->
    Status#{state := #{source => Source, refresh_timer => TRef}}.

%% -------------------------------------------------------------------------
%% Internal: config and credential refresh
%% -------------------------------------------------------------------------

do_reload_config(State0) ->
    Source =
        case resolve_credentials_source() of
            {static, AccessKey, SecretKey} ->
                _ = ets:insert(?TABLE, {
                    credentials,
                    AccessKey,
                    SecretKey,
                    rabbitmq_stream_s3_config:aws_security_token(),
                    undefined
                }),
                static;
            OtherSource ->
                %% Evict any static credentials row a previous config left
                %% behind. Without this, flipping allow_static_credentials off
                %% and reloading would leave the old row in ETS, and
                %% get_credentials_cached/0 would keep serving it (a static row
                %% stores `undefined` expiry, so is_expired/1 never evicts it).
                _ = ets:delete(?TABLE, credentials),
                OtherSource
        end,
    case rabbitmq_stream_s3_config:aws_region() of
        undefined -> ok;
        Region -> persistent_term:put(?REGION_KEY, Region)
    end,
    State0#state{source = Source}.

resolve_credentials_source() ->
    AccessKey0 = rabbitmq_stream_s3_config:aws_access_key(),
    SecretKey0 = rabbitmq_stream_s3_config:aws_secret_key(),
    case {AccessKey0, SecretKey0} of
        {undefined, undefined} ->
            managed_credentials_source();
        {AccessKey, SecretKey} when is_binary(AccessKey) andalso is_binary(SecretKey) ->
            case rabbitmq_stream_s3_config:allow_static_credentials() of
                true ->
                    ?LOG_WARNING(
                        ?MODULE_STRING
                        ": using static AWS credentials from "
                        "stream_s3.access_key_id and stream_s3.secret_key. These are "
                        "long-lived, stored in plaintext on disk, and are not rotated. "
                        "Prefer an EC2 instance IAM role or container credentials in "
                        "production."
                    ),
                    {static, AccessKey, SecretKey};
                false ->
                    ?LOG_WARNING(
                        ?MODULE_STRING
                        ": static AWS credentials are configured but "
                        "stream_s3.allow_static_credentials is not set to true. Ignoring "
                        "them and falling back to container or EC2 instance credentials."
                    ),
                    managed_credentials_source()
            end;
        _ ->
            %% Exactly one of access_key_id / secret_key is set. This is a
            %% misconfiguration: static credentials need both. Warn and fall
            %% back to managed credentials rather than crashing the credential
            %% gen_server at init with a case_clause (which would restart-loop
            %% instead of using IMDS or container credentials).
            ?LOG_WARNING(
                ?MODULE_STRING
                ": only one of stream_s3.access_key_id and stream_s3.secret_key "
                "is set. Static credentials require both; ignoring the partial "
                "configuration and falling back to container or EC2 instance "
                "credentials."
            ),
            managed_credentials_source()
    end.

managed_credentials_source() ->
    case os:getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI") of
        false -> imds;
        URI -> {container, URI}
    end.

do_refresh_credentials(#state{source = static} = State) ->
    {{error, no_credentials}, State};
do_refresh_credentials(#state{source = imds} = State0) ->
    ?LOG_INFO(?MODULE_STRING ": refreshing credentials from EC2 instance metadata"),
    {Msec, {Result, State1}} = timer:tc(
        fun() -> request_credentials_from_instance_metadata(State0) end, millisecond
    ),
    log_credentials_result(Result, Msec, "EC2 instance metadata service"),
    State = schedule_refresh(Result, State1),
    {Result, State};
do_refresh_credentials(#state{source = {container, URI}} = State0) ->
    ?LOG_INFO(?MODULE_STRING ": refreshing credentials from container credentials endpoint"),
    {Msec, Result} = timer:tc(
        fun() -> request_credentials_from_container_endpoint(URI) end, millisecond
    ),
    log_credentials_result(Result, Msec, "container credentials endpoint"),
    State = schedule_refresh(Result, State0),
    {Result, State}.

log_credentials_result({ok, _, _, _}, Msec, Source) ->
    ?LOG_INFO("Successfully acquired credentials from ~ts in ~bms", [Source, Msec]);
log_credentials_result({error, _}, Msec, Source) ->
    ?LOG_ERROR("Failed to acquire credentials from ~ts in ~bms", [Source, Msec]).

%% Schedule a proactive refresh before credentials expire.
schedule_refresh({ok, _, _, _}, #state{refresh_timer = OldTimer} = State) ->
    _ = cancel_timer(OldTimer),
    RefreshIn =
        case ets:lookup(?TABLE, credentials) of
            [{credentials, _, _, _, Expiration}] when is_integer(Expiration) ->
                Now = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
                max((Expiration - Now - 30) * 1000, 5_000);
            _ ->
                60_000
        end,
    TRef = erlang:send_after(RefreshIn, self(), refresh_credentials),
    State#state{refresh_timer = TRef};
schedule_refresh({error, _}, #state{refresh_timer = OldTimer} = State) ->
    _ = cancel_timer(OldTimer),
    TRef = erlang:send_after(5_000, self(), refresh_credentials),
    State#state{refresh_timer = TRef}.

cancel_timer(undefined) -> ok;
cancel_timer(TRef) -> erlang:cancel_timer(TRef, [{async, true}, {info, false}]).

-doc "Gets the body of an object at key `Key`".
-spec get(key(), request_opts()) -> {ok, binary()} | {error, any()}.
get(Key, Opts) when is_binary(Key) andalso is_map(Opts) ->
    case request(<<"GET">>, key_to_path(Key), #{}, <<>>, Opts) of
        {ok, #{status := 200, body := Data}} ->
            {ok, Data};
        {ok, #{status := 404}} ->
            {error, not_found};
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other, Key),
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
        {ok, #{status := 206, body := Data}} ->
            {ok, Data};
        {ok, #{status := 200, body := Data}} ->
            %% A Range request must be answered with 206 Partial Content. An
            %% intermediary or a non-conformant store can ignore the Range
            %% header and answer 200 with the full object. Returning that whole
            %% body as the requested range would feed the caller bytes from the
            %% wrong offset, so slice the range out of the full object here.
            ?LOG_DEBUG(
                "~ts received 200 for a Range request, slicing ~p from the full object for key ~ts",
                [?FUNCTION_NAME, Range, Key]
            ),
            slice_range(Range, Data);
        {ok, #{status := 404}} ->
            {error, not_found};
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other, Key),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-spec get_range_async(key(), rabbitmq_stream_s3_api:range_spec(), request_opts()) ->
    {ok, async_req(), async_state()} | {error, any()}.
get_range_async(Key, Range, Opts) when is_binary(Key) andalso is_map(Opts) ->
    Headers = #{<<"range">> => range_specifier(Range)},
    case request_async(<<"GET">>, key_to_path(Key), Headers, <<>>, Opts) of
        {ok, StreamRef, State} ->
            %% Keep the requested range so handle_async/3 can recover if the
            %% store ignores the Range header and answers 200 (full object).
            {ok, StreamRef, State#{range => Range}};
        {error, _} = Err ->
            Err
    end.

-doc "Uploads the given `Data` as an object at key `Key`".
-spec put(key(), iodata(), request_opts()) -> ok | {error, any()}.
put(Key, Data, Opts) when is_binary(Key) andalso is_map(Opts) ->
    Headers0 = sse_headers(Key),
    Headers =
        case Opts of
            #{crc32 := Checksum} ->
                Headers0#{<<"x-amz-checksum-crc32">> => base64:encode(<<Checksum:32/unsigned>>)};
            _ ->
                Headers0
        end,
    case request(<<"PUT">>, key_to_path(Key), Headers, Data, Opts) of
        {ok, #{status := 200}} ->
            ok;
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other, Key),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-spec stream_put(key(), pos_integer(), request_opts()) -> {ok, async_state()} | {error, any()}.
stream_put(Key, ContentLength, Opts0) when is_binary(Key) andalso is_map(Opts0) ->
    Method = <<"PUT">>,
    Path = key_to_path(Key),
    EncodedLength = aws_chunked_encoded_length(ContentLength),
    Headers0 = (sse_headers(Key))#{
        <<"content-length">> => integer_to_binary(EncodedLength),
        <<"content-encoding">> => <<"aws-chunked">>,
        <<"x-amz-decoded-content-length">> => integer_to_binary(ContentLength),
        <<"x-amz-trailer">> => <<"x-amz-checksum-crc32">>
    },
    case get_credentials() of
        {ok, AccessKey, SecretKey, SecurityToken} ->
            case
                sign_headers(
                    Headers0,
                    AccessKey,
                    SecretKey,
                    SecurityToken,
                    Method,
                    Path,
                    no_body,
                    Opts0#{stream_payload => true}
                )
            of
                {ok, Headers} ->
                    Pool = ?UPLOAD_POOL,
                    %% active_requests is owned by the pool and moved on the
                    %% checkout itself, so a failed checkout cannot leak it and a
                    %% caller killed mid-request is still balanced by the pool.
                    case rabbitmq_stream_s3_api_aws_pool:checkout(Pool, 10_000) of
                        {ok, Conn} ->
                            inc(?C_TOTAL_REQUESTS, 1),
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
                        {error, pool_busy} = Err ->
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
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
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other),
            {error, Other};
        {error, _} = Err ->
            normalize_transport_error(Err)
    after
        finish_async(State)
    end.

send_chunk(Conn, StreamRef, Chunk) when is_binary(Chunk) ->
    Size = byte_size(Chunk),
    gun:data(Conn, StreamRef, nofin, [integer_to_binary(Size, 16), <<"\r\n">>, Chunk, <<"\r\n">>]).

-spec stream_abort(async_state()) -> ok.
stream_abort(#{conn := Conn} = State) ->
    %% The streaming PUT body was only partially sent, so this HTTP/1.1
    %% connection is mid-request and cannot be reused. Close it without checking
    %% it back in; the pool's 'DOWN' handler removes it, accounts the checkout
    %% end (active_requests), and opens a replacement. Mirrors the timeout/cancel
    %% path; the half-sent PUT never reaches S3 as an object, so nothing is left
    %% behind except an orphan at most.
    gun:close(Conn),
    finish_async_close(State).

-doc "Deletes the given key or list of keys".
-spec delete(key() | [key()], request_opts()) ->
    ok | {error, any()}.
delete([], _Opts) ->
    ok;
delete(Keys, Opts) when is_list(Keys) andalso is_map(Opts) ->
    %% <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html>
    ?assert(length(Keys) =< 1000),
    Data = delete_many_body(Keys),
    Headers = #{
        %% A checksum header seems to be required on this endpoint...
        <<"x-amz-checksum-crc32">> => base64:encode(<<(erlang:crc32(Data)):32/unsigned>>)
    },
    case request(<<"POST">>, <<"/?delete=">>, Headers, Data, Opts) of
        {ok, #{status := 200, body := Body}} ->
            %% A DeleteObjects request can return 200 while reporting per-key
            %% failures in the body. Treating that as a clean success leaks the
            %% objects silently, so surface any per-key errors to the caller.
            case decode_delete_errors(Body) of
                [] ->
                    ok;
                Errors ->
                    {error, {delete_errors, Errors}}
            end;
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other),
            {error, Other};
        {error, _} = Err ->
            Err
    end;
delete(Key, Opts) when is_binary(Key) andalso is_map(Opts) ->
    %% <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html>.
    case request(<<"DELETE">>, key_to_path(Key), #{}, <<>>, Opts) of
        {ok, #{status := 204}} ->
            ok;
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other, Key),
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
list(Prefix, Continuation, Opts) ->
    ContinuationToken =
        case Continuation of
            start -> undefined;
            _ -> Continuation
        end,
    Params0 = [{<<"list-type">>, <<"2">>}, {<<"prefix">>, Prefix}],
    Params1 =
        case ContinuationToken of
            undefined ->
                Params0;
            _ ->
                [{<<"continuation-token">>, ContinuationToken} | Params0]
        end,
    Params = uri_string:compose_query(lists:keysort(1, Params1)),
    case request(<<"GET">>, <<"/?", Params/binary>>, #{}, <<>>, Opts) of
        {ok, #{status := 200, body := Body}} ->
            {Keys, _TotalSize, NextToken} = decode_list_bucket_result(Body),
            Next =
                case NextToken of
                    undefined -> done;
                    _ -> NextToken
                end,
            {ok, Keys, Next};
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-doc """
Probe the bucket with a HeadBucket request.

See <https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html>. A
HEAD on the bucket root returns 200 when the bucket exists and the credentials
may access it, 404 when it does not exist, and 403 when access is denied. A
missing-credentials or transient error is returned verbatim so the caller can
distinguish it from a definitive misconfiguration.

Note that a nonexistent bucket can surface as 403 rather than 404 when the
credentials lack `s3:ListBucket` on it, so `access_denied` does not strictly
imply the bucket exists. Both are definitive "not usable" outcomes with the
same operator remedy (fix the bucket name, region, or IAM permissions), so the
monitor treats them the same way and only the reported reason differs.
""".
-spec check_bucket(request_opts()) ->
    ok | {error, no_such_bucket | access_denied | term()}.
check_bucket(Opts) when is_map(Opts) ->
    case request(<<"HEAD">>, <<"/">>, #{}, <<>>, Opts) of
        {ok, #{status := 200}} ->
            ok;
        {ok, #{status := 404}} ->
            {error, no_such_bucket};
        {ok, #{status := 403}} ->
            %% May be a genuine permission denial or a nonexistent bucket the
            %% credentials cannot list; both are definitive "not usable" with
            %% the same remedy (see the moduledoc above).
            {error, access_denied};
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-spec decode_list_bucket_result(binary()) ->
    {[key()], TotalSize :: non_neg_integer(), continuation_token() | undefined}.
decode_list_bucket_result(Data) ->
    {#xmlElement{name = 'ListBucketResult', content = Result}, []} = xmerl_scan:string(
        binary_to_list(Data), [{allow_entities, false}]
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

%% Extracts the per-key `<Error>` entries from a DeleteObjects 200 response.
%% Returns `[{Key, Code}]`, empty when every key was deleted. A body that does
%% not parse as a DeleteResult is treated as a clean success, matching the prior
%% behaviour of accepting any 200, rather than reporting phantom failures.
-spec decode_delete_errors(binary()) -> [{key(), binary()}].
decode_delete_errors(Body) ->
    try xmerl_scan:string(binary_to_list(Body), [{allow_entities, false}]) of
        {#xmlElement{name = 'DeleteResult', content = Content}, _} ->
            [
                {child_text('Key', ErrContent), child_text('Code', ErrContent)}
             || #xmlElement{name = 'Error', content = ErrContent} <- Content
            ];
        _ ->
            []
    catch
        _:_ ->
            []
    end.

-spec child_text(atom(), [term()]) -> binary().
child_text(Name, Content) ->
    case lists:keyfind(Name, #xmlElement.name, Content) of
        #xmlElement{content = [#xmlText{value = Value}]} ->
            list_to_binary(Value);
        _ ->
            <<>>
    end.

-spec transient_status(non_neg_integer()) -> boolean().
transient_status(429) -> true;
transient_status(Status) when Status >= 500, Status =/= 501 -> true;
transient_status(_) -> false.

log_unexpected_status(Function, Response) ->
    log_unexpected_status(Function, Response, undefined).

log_unexpected_status(Function, #{status := Status} = Response, Key) ->
    case transient_status(Status) of
        true ->
            ?LOG_DEBUG(
                "~ts: S3 returned transient HTTP status ~b~ts; will retry",
                [Function, Status, format_key_suffix(Key)]
            );
        false ->
            ?LOG_WARNING(
                "~ts: S3 returned unexpected HTTP status ~b~ts "
                "(maybe a configuration or compatibility problem). "
                "Response body: ~ts",
                [
                    Function,
                    Status,
                    format_key_suffix(Key),
                    truncate_body(maps:get(body, Response, <<>>))
                ]
            )
    end.

format_key_suffix(undefined) -> <<>>;
format_key_suffix(Key) -> <<" for key ", Key/binary>>.

truncate_body(Body) when byte_size(Body) =< 1024 -> Body;
truncate_body(Body) -> <<(binary:part(Body, 0, 1024))/binary, "...">>.

-spec match_async(
    Msg :: term(),
    Reqs :: #{async_req() := async_state()},
    CancelledReqs :: #{async_req() => _}
) ->
    {ok, async_req()} | {cancelled, async_req(), final | more} | error.
match_async({gun_error, Conn, _Reason}, Reqs, _CancelledReqs) ->
    %% Connection-level error: match any active request on this connection.
    %% We intentionally ignore cancelled requests - a dying connection only
    %% needs to notify live requests.
    maps:fold(
        fun
            (Req, #{conn := C}, error) when C =:= Conn -> {ok, Req};
            (_, _, Acc) -> Acc
        end,
        error,
        Reqs
    );
match_async(Msg, Reqs, CancelledReqs) ->
    {Req, Final} =
        case Msg of
            {gun_error, _, StreamRef, _} -> {StreamRef, final};
            {gun_response, _, StreamRef, fin, _, _} -> {StreamRef, final};
            {gun_response, _, StreamRef, nofin, _, _} -> {StreamRef, more};
            {gun_data, _, StreamRef, fin, _} -> {StreamRef, final};
            {gun_data, _, StreamRef, nofin, _} -> {StreamRef, more};
            {request_timeout, StreamRef} -> {StreamRef, final};
            _ -> {undefined, final}
        end,
    case Reqs of
        #{Req := _} ->
            {ok, Req};
        _ ->
            case CancelledReqs of
                #{Req := _} -> {cancelled, Req, Final};
                _ -> error
            end
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
            case State0 of
                #{range := Range} ->
                    %% A Range request answered with 200 means the store (or an
                    %% intermediary) ignored the Range header and is sending the
                    %% full object. The body cannot be forwarded incrementally as
                    %% the requested range: the bytes would land at the wrong
                    %% offset in the caller's buffer. Buffer the whole object and
                    %% slice the range out at fin, mirroring get_range/3. The
                    %% extra buffering only applies to this non-conformant case.
                    ?LOG_DEBUG(
                        "~ts received 200 for a Range request; buffering the full "
                        "object to slice ~p",
                        [?FUNCTION_NAME, Range]
                    ),
                    State = State0#{
                        data => [],
                        pending_bytes => 0,
                        slice_full => Range,
                        headers_at => rabbitmq_stream_s3_util:now()
                    },
                    {continue, State};
                _ ->
                    State = State0#{
                        data => [],
                        pending_bytes => 0,
                        headers_at => rabbitmq_stream_s3_util:now()
                    },
                    {continue, State}
            end;
        206 ->
            State = State0#{
                data => [], pending_bytes => 0, headers_at => rabbitmq_stream_s3_util:now()
            },
            {continue, State};
        _ ->
            %% Non-success response with a body. Cancel the request timer
            %% and drain the body before reporting the error so that the
            %% remaining gun_data frames do not orphan in the caller's
            %% mailbox.
            Reason =
                case Status of
                    404 -> not_found;
                    500 -> internal_error;
                    503 -> slow_down;
                    _ -> #{status => Status, headers => Headers}
                end,
            State = cancel_request_timer(State0),
            {continue, State#{draining => Reason}}
    end;
handle_async(
    {gun_data, Conn, StreamRef, nofin, _Data},
    StreamRef,
    #{conn := Conn, stream_ref := StreamRef, draining := _}
) ->
    %% Discard body data from a non-success response being drained.
    ignore;
handle_async(
    {gun_data, Conn, StreamRef, fin, _Data},
    StreamRef,
    #{conn := Conn, stream_ref := StreamRef, draining := Reason} = State
) ->
    finish_async(State),
    {done, {error, Reason}};
handle_async(
    {gun_data, Conn, StreamRef, nofin, Data},
    StreamRef,
    #{
        conn := Conn,
        stream_ref := StreamRef,
        slice_full := _,
        pending_bytes := PendingBytes0,
        data := PendingData0
    } = State0
) ->
    %% Slicing a full-object 200 response: buffer every frame and never forward
    %% partial data (the bytes would land at the wrong offset). The buffer is
    %% sliced to the requested range at fin.
    State = State0#{
        data := [Data | PendingData0],
        pending_bytes := PendingBytes0 + byte_size(Data)
    },
    {continue, State};
handle_async(
    {gun_data, Conn, StreamRef, fin, Data},
    StreamRef,
    #{conn := Conn, stream_ref := StreamRef, slice_full := Range, data := Data0} = State
) ->
    finish_async(State),
    FullObject = iolist_to_binary(lists:reverse(Data0, [Data])),
    case slice_range(Range, FullObject) of
        {ok, Sliced} ->
            {data, Sliced, done};
        {error, _} = Err ->
            {done, Err}
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
    State1 = mark_first_data(State0, byte_size(Data)),
    case PendingBytes0 > ?BUFFER_PENDING_DATA_BYTES of
        true ->
            AllData = iolist_to_binary(lists:reverse(PendingData0, [Data])),
            State = State1#{data := [], pending_bytes := 0},
            {data, AllData, State};
        false ->
            State = State1#{
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
    %% Report which phase the request stalled in so the log distinguishes a
    %% connection-level failure (no response at all) from a slow S3 transfer.
    ?LOG_WARNING(
        "S3 request timed out on ~tw/~tw: ~ts", [Conn, StreamRef, describe_stall(State)]
    ),
    inc(?C_REQUEST_TIMEOUTS, 1),
    %% gun cannot cancel an HTTP/1.1 stream on the wire - it only stops
    %% forwarding events to the owner. The cancelled stream continues to
    %% occupy the connection, blocking subsequent requests until its full
    %% response is received. Close the whole connection instead; the pool
    %% observes the 'DOWN' and opens a fresh replacement.
    gun:close(Conn),
    finish_async_close(State),
    {done_cancel, {error, timeout}}.

%% Record the time and running byte count of the first body frame. Runs on
%% every body frame (a couple of map operations); enriches `describe_stall/1`
%% on a timeout. Revisit if profiling flags the per-frame cost.
mark_first_data(#{first_data_at := undefined, bytes_received := Rx} = State, N) ->
    State#{first_data_at := rabbitmq_stream_s3_util:now(), bytes_received := Rx + N};
mark_first_data(#{bytes_received := Rx} = State, N) ->
    State#{bytes_received := Rx + N};
mark_first_data(State, _N) ->
    State.

%% Summarize how far a timed-out request progressed: whether response headers
%% arrived, whether any body arrived, the elapsed time to each, and bytes
%% received. Tolerates a state map without the diagnostic keys.
describe_stall(#{req_started_at := Start} = State) when is_integer(Start) ->
    Now = rabbitmq_stream_s3_util:now(),
    HeadersAt = maps:get(headers_at, State, undefined),
    FirstDataAt = maps:get(first_data_at, State, undefined),
    Rx = maps:get(bytes_received, State, 0),
    Phase =
        case {HeadersAt, FirstDataAt} of
            {undefined, _} -> "no_response (no headers received)";
            {_, undefined} -> "headers_only (headers received, no body)";
            {_, _} -> "mid_body (headers and partial body received)"
        end,
    ElapsedMs = rabbitmq_stream_s3_util:elapsed_ms(Now, Start),
    HeadersMs = elapsed_or_undef(Start, HeadersAt),
    FirstDataMs = elapsed_or_undef(Start, FirstDataAt),
    lists:flatten(
        io_lib:format(
            "phase=~ts elapsed=~bms headers_after=~ts first_data_after=~ts bytes=~b",
            [Phase, ElapsedMs, HeadersMs, FirstDataMs, Rx]
        )
    );
describe_stall(_State) ->
    "phase=unknown (no diagnostic state)".

elapsed_or_undef(_Start, undefined) ->
    "n/a";
elapsed_or_undef(Start, At) ->
    integer_to_list(rabbitmq_stream_s3_util:elapsed_ms(At, Start)) ++ "ms".

-spec cancel_async(async_req(), async_state()) -> ok.
cancel_async(StreamRef, #{conn := Conn, stream_ref := StreamRef} = State) ->
    %% On HTTP/1.1, gun:cancel only marks the stream dead; the response body
    %% continues draining on the wire, blocking subsequent pipelined requests.
    %% Close the connection so the pool replaces it with a fresh one.
    gun:close(Conn),
    ok = finish_async_close(State).

%% Cancel the request timer without checking in the connection or
%% decrementing the active-request counter. Used when the response
%% body still needs to be drained before the request is complete.
cancel_request_timer(State) ->
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
            end,
            maps:remove(timer_ref, State);
        _ ->
            State
    end.

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
    %% The pool decrements active_requests on the checkin below (see
    %% note_request_finished/0).
    ok = rabbitmq_stream_s3_api_aws_pool:checkin(Pool, Conn).

%% Finish an async request when the connection is being closed (e.g. on
%% timeout). Same bookkeeping as `finish_async` but omits the pool checkin -
%% the caller has already closed the connection, so the pool's 'DOWN' handler
%% removes the conn, accounts the checkout end (active_requests), and grows a
%% replacement.
finish_async_close(State) ->
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
    ok.

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
            case
                sign_headers(
                    Headers0,
                    AccessKey,
                    SecretKey,
                    SecurityToken,
                    Method,
                    Path,
                    Body,
                    Opts
                )
            of
                {ok, Headers} ->
                    %% active_requests is owned by the pool, tied to the
                    %% checkout that request1/5 does below (see
                    %% note_request_started/0).
                    inc(?C_TOTAL_REQUESTS, 1),
                    normalize_transport_error(
                        request0(Method, Path, Headers, Body, Opts)
                    );
                {error, _} = Err ->
                    Err
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
                {error, timeout} = Err ->
                    inc(?C_REQUEST_TIMEOUTS, 1),
                    Err;
                {error, _} = Err ->
                    Err
            end;
        {error, timeout} = Err ->
            inc(?C_REQUEST_TIMEOUTS, 1),
            Err;
        {error, _} = Err ->
            Err
    end.

-spec normalize_transport_error(Result) -> Result when Result :: term().
normalize_transport_error({error, {stream_error, _}}) ->
    {error, stream_error};
normalize_transport_error({error, {connection_error, _}}) ->
    {error, connection_error};
normalize_transport_error({error, {down, _}}) ->
    {error, connection_error};
normalize_transport_error(Result) ->
    Result.

postprocess_response(#{status := 403}) ->
    inc(?C_RESPONSE_403, 1);
postprocess_response(#{status := 503}) ->
    inc(?C_RESPONSE_503, 1);
postprocess_response(#{status := 500}) ->
    inc(?C_RESPONSE_500, 1);
postprocess_response(_) ->
    ok.

request_async(Method, Path, Headers0, Body, Opts) ->
    case get_credentials() of
        {ok, AccessKey, SecretKey, SecurityToken} ->
            case
                sign_headers(
                    Headers0,
                    AccessKey,
                    SecretKey,
                    SecurityToken,
                    Method,
                    Path,
                    Body,
                    Opts
                )
            of
                {ok, Headers} ->
                    Pool = ?GENERAL_POOL,
                    start_async_request(Pool, Method, Path, Headers, Body, Opts);
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

start_async_request(Pool, Method, Path, Headers, Body, Opts) ->
    case rabbitmq_stream_s3_api_aws_pool:checkout(Pool, ?READ_CHECKOUT_TIMEOUT_MS) of
        {ok, Conn} ->
            %% active_requests is owned by the pool, tied to this checkout.
            inc(?C_TOTAL_REQUESTS, 1),
            %% NOTE: no need to wrap this in try/catch and checkin the conn
            %% since gun:request/5 cannot exit/error/throw.
            StreamRef = gun:request(Conn, Method, Path, Headers, Body),
            %% Phase markers for timeout reporting (see `describe_stall/1`).
            State = #{
                pool => Pool,
                conn => Conn,
                stream_ref => StreamRef,
                req_started_at => rabbitmq_stream_s3_util:now(),
                headers_at => undefined,
                first_data_at => undefined,
                bytes_received => 0
            },
            {ok, StreamRef, maybe_set_timer(Opts, StreamRef, State)};
        {error, pool_busy} = Err ->
            Err
    end.

maybe_set_timer(#{timeout := Timeout}, StreamRef, State) ->
    TimerRef = erlang:send_after(Timeout, self(), {request_timeout, StreamRef}),
    State#{timer_ref => TimerRef};
maybe_set_timer(_Opts, _StreamRef, State) ->
    State.

-spec hostname() -> {ok, binary()} | {error, any()}.
hostname() ->
    case region() of
        {ok, Region} -> {ok, hostname(Region)};
        {error, _} = Err -> Err
    end.

-spec hostname(Region :: binary()) -> binary().
hostname(Region) ->
    <<"s3.", Region/binary, $., (tld(Region))/binary>>.

%% Region is required to build the request host and to sign requests, so a
%% failure here cannot be papered over: callers must surface it. We return a
%% tagged tuple (rather than the bare binary) so the failure propagates as a
%% clean {error, _} through hostname/0 and sign_headers/8 instead of crashing
%% the calling worker with a badarg on binary construction. Once a region is
%% known (from config or a successful IMDS lookup) it is cached in
%% persistent_term and never expires, so this only ever fails transiently before
%% the first successful lookup.
-spec region() -> {ok, binary()} | {error, any()}.
region() ->
    case persistent_term:get(?REGION_KEY, undefined) of
        undefined ->
            safe_call(refresh_region, 15_000);
        Region ->
            {ok, Region}
    end.

%% gen_server:call/3 exits the *calling* process on timeout, and likewise if the
%% server is down (noproc) or crashes mid-call. Our callers here are pool workers
%% and osiris readers/uploaders signing S3 requests; a refresh does blocking IMDS
%% or container HTTP I/O inside the server, and a slow or unreachable endpoint can
%% push that past the call timeout. An exit in those callers is a crash, not a
%% handleable error. Convert any such exit into an {error, _} tuple so the request
%% path returns cleanly: get/put/request all already handle {error, _} from
%% get_credentials/0 and region/0. We deliberately do not log here: the refresh
%% failure is already logged once per attempt inside the server (every ~5s under a
%% sustained outage), whereas logging per caller could flood under load.
-spec safe_call(term(), timeout()) -> term().
safe_call(Request, Timeout) ->
    try
        gen_server:call(?MODULE, Request, Timeout)
    catch
        exit:{Reason, {gen_server, call, _}} ->
            {error, {credential_server, Reason}}
    end.

request_region_from_instance_metadata(State0) ->
    case ensure_metadata_token(State0) of
        {error, Reason, State1} ->
            {{error, Reason}, State1};
        {ok, Token, State1} ->
            Result = with_instance_metadata_conn(fun(Conn) ->
                case
                    get_instance_metadata(
                        Conn,
                        <<"GET">>,
                        <<"/latest/meta-data/placement/availability-zone">>,
                        #{<<"x-aws-ec2-metadata-token">> => Token}
                    )
                of
                    {ok, #{status := 200, body := Body}} ->
                        %% Strip trailing availability zone character, e.g. us-east-2c -> us-east-2
                        Region = binary:part(Body, 0, byte_size(Body) - 1),
                        persistent_term:put(?REGION_KEY, Region),
                        {ok, Region};
                    {ok, #{status := Status}} ->
                        {error, {unexpected_status, Status}};
                    {error, _} = Err ->
                        Err
                end
            end),
            {Result, State1}
    end.

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
        rabbitmq_stream_s3_config:aws_region_endpoints()
    ),
    maps:get(Region, Mapping, <<"amazonaws.com">>).

-spec get_credentials() ->
    {ok, AccessKey :: binary(), SecretKey :: binary(), SecurityToken :: binary() | undefined}
    | {error, any()}.
get_credentials() ->
    case get_credentials_cached() of
        {ok, _, _, _} = Ok -> Ok;
        error -> safe_call(refresh_credentials, 15_000)
    end.

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

request_credentials_from_instance_metadata(State0) ->
    %% <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-metadata-security-credentials.html>
    case ensure_metadata_token(State0) of
        {error, Reason, State1} ->
            {{error, Reason}, State1};
        {ok, Token, State1} ->
            Result = with_instance_metadata_conn(fun(Conn) ->
                maybe
                    {ok, RoleResp} ?=
                        get_instance_metadata(
                            Conn,
                            <<"GET">>,
                            <<"/latest/meta-data/iam/security-credentials">>,
                            #{<<"x-aws-ec2-metadata-token">> => Token}
                        ),
                    {ok, Role} ?= expect_200(RoleResp),
                    {ok, CredsResp} ?=
                        get_instance_metadata(
                            Conn,
                            <<"GET">>,
                            <<"/latest/meta-data/iam/security-credentials/", Role/binary>>,
                            #{<<"x-aws-ec2-metadata-token">> => Token}
                        ),
                    {ok, Creds} ?= expect_200(CredsResp),
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
            end),
            {Result, State1}
    end.

%% IMDS responds with a non-200 status on throttling or transient errors. Return
%% an error tuple instead of letting a badmatch crash the gen_server: a crash
%% combined with the supervisor's restart intensity could escalate and take down
%% the whole tree on a flaky metadata endpoint.
expect_200(#{status := 200, body := Body}) -> {ok, Body};
expect_200(#{status := Status}) -> {error, {unexpected_status, Status}}.

request_credentials_from_container_endpoint(URI) ->
    %% <https://docs.aws.amazon.com/sdkref/latest/guide/feature-container-credentials.html>
    Parsed = uri_string:parse(URI),
    Scheme = maps:get(scheme, Parsed, "http"),
    Host = maps:get(host, Parsed),
    Port = maps:get(port, Parsed, undefined),
    Path0 = maps:get(path, Parsed, "/"),
    {Transport, DefaultPort} =
        case Scheme of
            "https" -> {tls, 443};
            _ -> {tcp, 80}
        end,
    PortInt =
        case Port of
            undefined -> DefaultPort;
            _ -> Port
        end,
    Path =
        case Path0 of
            [] -> "/";
            _ -> Path0
        end,
    Req = #container_creds_req{host = Host, port = PortInt, path = Path},
    do_request_credentials_from_container_endpoint(
        open, Req, gun:open(Host, PortInt, #{transport => Transport, protocols => [http]})
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

%% Returns a valid IMDS session token and the (possibly updated) state. Runs in
%% the gen_server process, so the token cached in State is the single source of
%% truth. A fresh token is fetched only when the cached one is missing or within
%% ?TTL_SECONDS_BUFFER of expiry. Threading the token back through State is what
%% makes the cache work: a previous version discarded the fetched token, so every
%% IMDS request paid for a fresh token fetch.
-spec ensure_metadata_token(#state{}) ->
    {ok, binary(), #state{}} | {error, any(), #state{}}.
ensure_metadata_token(#state{metadata_token = {Token, Expiration}} = State) ->
    case is_expired(Expiration) of
        false -> {ok, Token, State};
        true -> fetch_and_cache_metadata_token(State)
    end;
ensure_metadata_token(#state{metadata_token = undefined} = State) ->
    fetch_and_cache_metadata_token(State).

fetch_and_cache_metadata_token(State) ->
    case fetch_metadata_token() of
        {ok, {Token, Expiration}} ->
            {ok, Token, State#state{metadata_token = {Token, Expiration}}};
        {error, Reason} ->
            {error, Reason, State}
    end.

-spec fetch_metadata_token() ->
    {ok, {binary(), non_neg_integer()}} | {error, any()}.
fetch_metadata_token() ->
    with_instance_metadata_conn(fun(Conn) ->
        case
            get_instance_metadata(
                Conn,
                <<"PUT">>,
                <<"/latest/api/token">>,
                #{
                    <<"x-aws-ec2-metadata-token-ttl-seconds">> => integer_to_binary(
                        ?METADATA_TOKEN_TTL_SECONDS
                    )
                }
            )
        of
            {ok, #{status := 200, body := Token}} ->
                Expiration =
                    calendar:datetime_to_gregorian_seconds(calendar:universal_time()) +
                        ?METADATA_TOKEN_TTL_SECONDS,
                {ok, {Token, Expiration}};
            {ok, #{status := Status}} ->
                {error, {unexpected_status, Status}};
            {error, _} = Err ->
                Err
        end
    end).

with_instance_metadata_conn(Fun) when is_function(Fun, 1) ->
    ?LOG_DEBUG(?MODULE_STRING ": connecting to EC2 instance metadata service"),
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
    case region() of
        {ok, Region} ->
            Bucket = rabbitmq_stream_s3_config:bucket(),
            Host = <<Bucket/binary, $., (hostname(Region))/binary>>,
            {ok,
                sign_headers(
                    calendar:universal_time(),
                    Host,
                    Region,
                    Headers,
                    AccessKey,
                    SecretKey,
                    SecurityToken,
                    Method,
                    Path,
                    Body,
                    Opts
                )};
        {error, _} = Err ->
            Err
    end.

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
    MergedHeaders = maps:merge(DefaultHeaders, Headers0),
    %% Assert the expected bucket owner when an account ID is configured. S3
    %% then rejects the request with 403 if the bucket is owned by another
    %% account, blocking bucket redirection. Applied after the merge so a
    %% caller-supplied header cannot override the configured value.
    %%
    %% account_id() reads application env on every request, but that is cheap
    %% on this hot path: application:get_env/2,3 is a single ets:lookup_element
    %% on the application controller's protected ac_tab (read_concurrency), not
    %% a gen_server call. No need to cache it in state like the region.
    Headers1 =
        case rabbitmq_stream_s3_config:account_id() of
            undefined ->
                MergedHeaders;
            AccountId ->
                MergedHeaders#{<<"x-amz-expected-bucket-owner">> => AccountId}
        end,
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

%% Extracts the bytes a Range request asked for out of a full-object body, used
%% to recover when a store answers a Range request with 200 instead of 206. The
%% slice mirrors what a conformant 206 would have returned, clamping the end to
%% the object size as S3 does. A range starting at or beyond the object size is
%% unsatisfiable.
-spec slice_range(rabbitmq_stream_s3_api:range_spec(), binary()) ->
    {ok, binary()}
    | {error, {range_not_satisfiable, rabbitmq_stream_s3_api:range_spec(), non_neg_integer()}}.
slice_range({StartByte, undefined}, Data) when StartByte < byte_size(Data) ->
    {ok, binary:part(Data, StartByte, byte_size(Data) - StartByte)};
slice_range({StartByte, EndByte}, Data) when
    is_integer(EndByte) andalso StartByte =< EndByte andalso StartByte < byte_size(Data)
->
    Len = min(EndByte + 1, byte_size(Data)) - StartByte,
    {ok, binary:part(Data, StartByte, Len)};
slice_range(SuffixLen, Data) when
    is_integer(SuffixLen) andalso SuffixLen < 0 andalso byte_size(Data) > 0
->
    Len = min(-SuffixLen, byte_size(Data)),
    {ok, binary:part(Data, byte_size(Data) - Len, Len)};
slice_range(Range, Data) ->
    {error, {range_not_satisfiable, Range, byte_size(Data)}}.

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
    TDiff = rabbitmq_stream_s3_util:elapsed_ms(T0),
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

%% Server-side encryption headers for PUT requests. Always requests SSE-S3
%% (AES256) to satisfy bucket policies that deny uploads without an explicit
%% encryption header. When a KMS key is configured, uses SSE-KMS instead and
%% attaches an encryption context.
-spec sse_headers(key()) -> req_headers().
sse_headers(Key) ->
    case rabbitmq_stream_s3_config:kms_key_id() of
        undefined ->
            #{<<"x-amz-server-side-encryption">> => <<"AES256">>};
        KeyId ->
            #{
                <<"x-amz-server-side-encryption">> => <<"aws:kms">>,
                <<"x-amz-server-side-encryption-aws-kms-key-id">> => KeyId,
                <<"x-amz-server-side-encryption-context">> => encryption_context(Key)
            }
    end.

%% The SSE-KMS encryption context: the stream the object belongs to plus any
%% pairs the operator configured. KMS records the context in its CloudTrail
%% entry for every operation on the object, so the audit trail identifies the
%% stream without having to parse object keys.
%%
%% S3 requires the header value to be base64-encoded JSON. It stores the
%% context and re-supplies it to KMS on read, so the read path needs no
%% matching change.
%%
%% A key which does not belong to a stream yields a "stream_id" of "unknown"
%% rather than failing the upload: the context is auditing metadata, not an
%% input to correctness. Configured pairs are applied last, so an operator who
%% sets "stream_id" explicitly gets the value they asked for.
%%
%% Rebuilt on every PUT rather than memoised per stream. The result depends only
%% on the stream ID and the configured pairs, so it could be cached, but a PUT
%% moves a whole fragment or manifest object and the encode is a few hundred
%% bytes next to that, so the per-upload cost is not worth a cache to remove.
-spec encryption_context(key()) -> binary().
encryption_context(Key) ->
    StreamId =
        case rabbitmq_stream_s3:key_stream_id(Key) of
            undefined -> <<"unknown">>;
            Id -> Id
        end,
    Configured = rabbitmq_stream_s3_config:kms_encryption_context(),
    Context = maps:merge(#{<<"stream_id">> => StreamId}, Configured),
    %% `rabbit_json:encode/1` raises on a binary that is not valid UTF-8, but
    %% the stream ID cannot be one: it is the osiris stream name, which
    %% `rabbit_stream_queue:stream_name/1` produces through
    %% `osiris_util:to_base64uri/1`, so it holds only `[A-Za-z0-9_-]`. The
    %% AMQP queue name it derives from is not required to be valid UTF-8
    %% (`rabbit_channel:check_name/2` does not enforce it), but that encoding
    %% neutralises it before it reaches here. The configured values are
    %% rejected at config time if empty and are otherwise operator-supplied
    %% UTF-8.
    base64:encode(iolist_to_binary(rabbit_json:encode(Context))).

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

%% All counter mutations go through inc/2 so a missing counter is a no-op rather
%% than a badarg. `init/1` only installs the counter when this module is the
%% configured backend, so every call site would otherwise need to know whether
%% it can be reached with the counter absent.
inc(Idx, N) ->
    case persistent_term:get(?COUNTER_KEY, undefined) of
        undefined -> ok;
        Cnt -> counters:add(Cnt, Idx, N)
    end,
    ok.

-doc """
`active_requests` counts connections currently checked out of a pool: one
in-flight S3 request per checkout. The pool owns both edges, calling
`note_request_started/0` when it hands a connection to a caller and
`note_request_finished/0` when that checkout ends, whether by check-in, by the
caller dying while holding it, or by the connection dying under it.

Keeping both edges in the pool, at the checkout lifecycle points, is what makes
the gauge balanced by construction. The alternative - incrementing in the
requesting process and decrementing on each of its completion paths - left the
gauge to drift: a caller killed mid-request (e.g. by
`rabbitmq_stream_s3_governor:cancel/1`) or killed while still queued for a
connection ran none of those paths, so an increment had no matching decrement.

Both are a no-op if this module's counters aren't initialized: the pool can be
driven in isolation (e.g. api_aws_pool_statem_SUITE) without this gen_server,
and there is no gauge to move in that case.
""".
-spec note_request_started() -> ok.
note_request_started() ->
    inc(?C_ACTIVE_REQUESTS, 1).

-doc #{equiv => note_request_started / 0}.
-spec note_request_finished() -> ok.
note_request_finished() ->
    inc(?C_ACTIVE_REQUESTS, -1).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Without a KMS key, uploads ask for SSE-S3 and carry no encryption context.
sse_headers_without_kms_key_test() ->
    with_env([{kms_key_id, undefined}, {kms_encryption_context, undefined}], fun() ->
        Headers = sse_headers(<<"rabbitmq/stream/s1/data/x.fragment">>),
        ?assertEqual(<<"AES256">>, maps:get(<<"x-amz-server-side-encryption">>, Headers)),
        ?assertNot(maps:is_key(<<"x-amz-server-side-encryption-aws-kms-key-id">>, Headers)),
        ?assertNot(maps:is_key(<<"x-amz-server-side-encryption-context">>, Headers))
    end).

%% With a KMS key, uploads ask for SSE-KMS with that key and a context holding
%% the stream ID.
sse_headers_with_kms_key_test() ->
    with_env([{kms_key_id, <<"arn:key">>}, {kms_encryption_context, undefined}], fun() ->
        Headers = sse_headers(<<"rabbitmq/stream/s1/data/x.fragment">>),
        ?assertEqual(<<"aws:kms">>, maps:get(<<"x-amz-server-side-encryption">>, Headers)),
        ?assertEqual(
            <<"arn:key">>,
            maps:get(<<"x-amz-server-side-encryption-aws-kms-key-id">>, Headers)
        ),
        ?assertEqual(#{<<"stream_id">> => <<"s1">>}, decoded_context(Headers))
    end).

%% Configured pairs are added to the context, and one named "stream_id"
%% replaces the stream ID.
sse_headers_context_includes_configured_pairs_test() ->
    Configured = #{<<"cluster">> => <<"eu-prod-1">>, <<"tier">> => <<"gold">>},
    with_env([{kms_key_id, <<"arn:key">>}, {kms_encryption_context, Configured}], fun() ->
        ?assertEqual(
            Configured#{<<"stream_id">> => <<"s1">>},
            decoded_context(sse_headers(<<"rabbitmq/stream/s1/data/x.fragment">>))
        )
    end),
    Override = #{<<"stream_id">> => <<"fixed">>},
    with_env([{kms_key_id, <<"arn:key">>}, {kms_encryption_context, Override}], fun() ->
        ?assertEqual(
            Override, decoded_context(sse_headers(<<"rabbitmq/stream/s1/data/x.fragment">>))
        )
    end).

%% A key outside a stream's prefix still uploads, with a placeholder stream ID.
sse_headers_context_for_unrecognized_key_test() ->
    with_env([{kms_key_id, <<"arn:key">>}, {kms_encryption_context, undefined}], fun() ->
        ?assertEqual(
            #{<<"stream_id">> => <<"unknown">>},
            decoded_context(sse_headers(<<"some/other/key">>))
        )
    end).

decoded_context(Headers) ->
    rabbit_json:decode(
        base64:decode(maps:get(<<"x-amz-server-side-encryption-context">>, Headers))
    ).

%% Runs Fun with the given `rabbitmq_stream_s3` application environment entries
%% in place, restoring the previous values afterwards. `undefined` means that
%% the entry is unset.
with_env(Entries, Fun) ->
    App = rabbitmq_stream_s3,
    Previous = [{Key, application:get_env(App, Key, undefined)} || {Key, _} <- Entries],
    _ = [apply_env(App, Key, Value) || {Key, Value} <- Entries],
    try
        Fun()
    after
        _ = [apply_env(App, Key, Value) || {Key, Value} <- Previous]
    end.

apply_env(App, Key, undefined) -> application:unset_env(App, Key);
apply_env(App, Key, Value) -> application:set_env(App, Key, Value).

note_request_started_and_finished_balance_test() ->
    with_counter(fun(Read) ->
        ?assertEqual(ok, note_request_started()),
        ?assertEqual(1, Read(active_requests)),
        ?assertEqual(ok, note_request_finished()),
        ?assertEqual(0, Read(active_requests))
    end).

%% Both edges must tolerate a missing counter: init/1 installs it only when this
%% module is the configured backend, and the pool can be driven standalone.
note_request_edges_are_noop_without_counters_test() ->
    without_counter(fun() ->
        ?assertEqual(ok, note_request_started()),
        ?assertEqual(ok, note_request_finished())
    end).

%% Every counter mutation goes through inc/2, so one no-counter check covers all
%% of them rather than each call site needing its own.
inc_is_noop_without_counters_test() ->
    without_counter(fun() ->
        ?assertEqual(ok, inc(?C_TOTAL_REQUESTS, 1)),
        ?assertEqual(ok, inc(?C_REQUEST_TIMEOUTS, 1))
    end).

-doc """
Install a private counter at this module's `persistent_term` key, call
`Fun(Read)`, then restore whatever was there before.

`Read` takes a metric name from `?COUNTERS` (e.g. `active_requests`) and returns
its current value. Callers get the counter's size and indices from the real
`?COUNTERS` list rather than restating them, so adding a metric cannot leave a
test asserting on the wrong slot.

Exported because the pool moves `active_requests` while this module owns the
counter, so the pool's own tests need it too.
""".
-spec with_counter(fun((fun((atom()) -> integer())) -> Ret)) -> Ret.
with_counter(Fun) ->
    Previous = persistent_term:get(?COUNTER_KEY, undefined),
    Cnt = counters:new(length(?COUNTERS), []),
    persistent_term:put(?COUNTER_KEY, Cnt),
    Read = fun(Name) ->
        {Name, Idx, _Type, _Help} = lists:keyfind(Name, 1, ?COUNTERS),
        counters:get(Cnt, Idx)
    end,
    try
        Fun(Read)
    after
        case Previous of
            undefined -> persistent_term:erase(?COUNTER_KEY);
            _ -> persistent_term:put(?COUNTER_KEY, Previous)
        end
    end.

%% Run Fun with ?COUNTER_KEY absent, restoring whatever was there. Erasing
%% without restoring would leave the module's real counter missing for whatever
%% runs next in the same VM.
without_counter(Fun) ->
    Previous = persistent_term:get(?COUNTER_KEY, undefined),
    _ = persistent_term:erase(?COUNTER_KEY),
    try
        Fun()
    after
        case Previous of
            undefined -> ok;
            _ -> persistent_term:put(?COUNTER_KEY, Previous)
        end
    end.

transient_status_test() ->
    %% Retryable: server-side 5xx (except 501) and throttling.
    ?assert(transient_status(500)),
    ?assert(transient_status(503)),
    ?assert(transient_status(429)),
    %% Not retryable: configuration/compatibility problems.
    ?assertNot(transient_status(501)),
    ?assertNot(transient_status(400)),
    ?assertNot(transient_status(403)),
    ?assertNot(transient_status(404)),
    ok.

normalize_transport_error_test() ->
    %% The exact terms gun produces when the upload pool takes a connection
    %% down with a PUT stream checked out.
    ?assertEqual({error, stream_error}, normalize_transport_error({error, {stream_error, closed}})),
    ?assertEqual(
        {error, stream_error},
        normalize_transport_error({error, {stream_error, {closed, {error, timeout}}}})
    ),
    ?assertEqual(
        {error, connection_error},
        normalize_transport_error({error, {connection_error, closed}})
    ),
    ?assertEqual({error, connection_error}, normalize_transport_error({error, {down, normal}})),
    %% Everything else passes through untouched.
    ?assertEqual({error, timeout}, normalize_transport_error({error, timeout})),
    ?assertEqual({error, not_found}, normalize_transport_error({error, not_found})),
    ?assertEqual(
        {error, #{status => 403}}, normalize_transport_error({error, #{status => 403}})
    ),
    ?assertEqual({ok, #{status => 200}}, normalize_transport_error({ok, #{status => 200}})),
    ok.

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

slice_range_test() ->
    Data = <<"0123456789">>,
    %% Start-to-end: every spec form recovers exactly what a 206 would return.
    ?assertEqual({ok, <<"3456789">>}, slice_range({3, undefined}, Data)),
    ?assertEqual({ok, Data}, slice_range({0, undefined}, Data)),
    %% Absolute, inclusive range.
    ?assertEqual({ok, <<"345">>}, slice_range({3, 5}, Data)),
    ?assertEqual({ok, <<"0">>}, slice_range({0, 0}, Data)),
    %% An end past the object is clamped to the object size, as S3 does.
    ?assertEqual({ok, <<"789">>}, slice_range({7, 100}, Data)),
    %% Suffix range: the last N bytes, clamped to the whole object.
    ?assertEqual({ok, <<"789">>}, slice_range(-3, Data)),
    ?assertEqual({ok, Data}, slice_range(-100, Data)),
    %% A range starting at or beyond the object size is unsatisfiable.
    ?assertEqual(
        {error, {range_not_satisfiable, {10, undefined}, 10}}, slice_range({10, undefined}, Data)
    ),
    ?assertEqual({error, {range_not_satisfiable, {12, 15}, 10}}, slice_range({12, 15}, Data)),
    ?assertEqual({error, {range_not_satisfiable, -3, 0}}, slice_range(-3, <<>>)),
    ok.

%% A Range request answered with 200 (full object) must enter slice mode so the
%% body is buffered and sliced, not forwarded incrementally at the wrong offset.
async_range_200_enters_slice_mode_test() ->
    C = self(),
    R = make_ref(),
    State0 = #{conn => C, stream_ref => R, range => {3, 5}},
    {continue, S} = handle_async({gun_response, C, R, nofin, 200, []}, R, State0),
    ?assertEqual({3, 5}, maps:get(slice_full, S)),
    ?assertEqual([], maps:get(data, S)),
    ?assertEqual(0, maps:get(pending_bytes, S)).

%% A conformant 206 streams normally: no slice mode, data forwarded as it
%% arrives.
async_range_206_streams_normally_test() ->
    C = self(),
    R = make_ref(),
    State0 = #{conn => C, stream_ref => R, range => {3, 5}},
    {continue, S} = handle_async({gun_response, C, R, nofin, 206, []}, R, State0),
    ?assertNot(maps:is_key(slice_full, S)).

%% A 200 to a non-range request (e.g. an upload) is normal and must not enter
%% slice mode.
async_non_range_200_no_slice_test() ->
    C = self(),
    R = make_ref(),
    State0 = #{conn => C, stream_ref => R},
    {continue, S} = handle_async({gun_response, C, R, nofin, 200, []}, R, State0),
    ?assertNot(maps:is_key(slice_full, S)).

%% In slice mode, body frames are buffered (prepended) and never forwarded; the
%% slice happens at fin.
async_slice_mode_buffers_data_test() ->
    C = self(),
    R = make_ref(),
    State0 = #{
        conn => C, stream_ref => R, slice_full => {3, 5}, data => [<<"01">>], pending_bytes => 2
    },
    {continue, S} = handle_async({gun_data, C, R, nofin, <<"23">>}, R, State0),
    ?assertEqual([<<"23">>, <<"01">>], maps:get(data, S)),
    ?assertEqual(4, maps:get(pending_bytes, S)).

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

decode_delete_errors_test() ->
    %% A clean delete: every key reported under <Deleted>, no <Error>.
    AllOk =
        <<
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
            "<Deleted><Key>a</Key></Deleted><Deleted><Key>b</Key></Deleted>"
            "</DeleteResult>"
        >>,
    ?assertEqual([], decode_delete_errors(AllOk)),
    %% A partial failure: one key deleted, one reported under <Error>.
    Partial =
        <<
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
            "<Deleted><Key>a</Key></Deleted>"
            "<Error><Key>b</Key><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
            "</DeleteResult>"
        >>,
    ?assertEqual([{<<"b">>, <<"AccessDenied">>}], decode_delete_errors(Partial)),
    %% A well-formed but unexpected root is treated as success, matching the
    %% prior behaviour of accepting any 200. A malformed body falls to the same
    %% empty result via the catch, but is not asserted here because xmerl logs a
    %% fatal report on unparseable input.
    Unexpected =
        <<"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Other><Key>a</Key></Other>">>,
    ?assertEqual([], decode_delete_errors(Unexpected)),
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

static_credentials_opt_in_test() ->
    %% Pin the managed source to a fixed value rather than sampling the ambient
    %% environment: with the container-credentials URI unset,
    %% managed_credentials_source/0 resolves to imds. The assertions then check
    %% against a known expected source, not "whatever this host produces".
    ContainerUri = os:getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI"),
    os:unsetenv("AWS_CONTAINER_CREDENTIALS_FULL_URI"),
    ok = application:set_env(rabbitmq_stream_s3, aws_access_key, <<"AKIAIOSFODNN7EXAMPLE">>),
    ok = application:set_env(rabbitmq_stream_s3, aws_secret_key, <<"wJalrXUtnFEMI">>),
    try
        %% Configured but not opted in: ignored, so it falls back to imds.
        ?assertEqual(imds, resolve_credentials_source()),
        ok = application:set_env(rabbitmq_stream_s3, allow_static_credentials, true),
        ?assertEqual(
            {static, <<"AKIAIOSFODNN7EXAMPLE">>, <<"wJalrXUtnFEMI">>},
            resolve_credentials_source()
        )
    after
        application:unset_env(rabbitmq_stream_s3, aws_access_key),
        application:unset_env(rabbitmq_stream_s3, aws_secret_key),
        application:unset_env(rabbitmq_stream_s3, allow_static_credentials),
        case ContainerUri of
            false -> ok;
            _ -> os:putenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", ContainerUri)
        end
    end.

partial_static_credentials_test() ->
    %% Only one of access_key_id / secret_key set is a misconfiguration. It must
    %% fall back to managed credentials, not crash resolve_credentials_source/0
    %% with a case_clause (which would restart-loop the credential gen_server).
    ContainerUri = os:getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI"),
    os:unsetenv("AWS_CONTAINER_CREDENTIALS_FULL_URI"),
    try
        ok = application:set_env(rabbitmq_stream_s3, aws_access_key, <<"AKIAIOSFODNN7EXAMPLE">>),
        application:unset_env(rabbitmq_stream_s3, aws_secret_key),
        ?assertEqual(imds, resolve_credentials_source()),
        application:unset_env(rabbitmq_stream_s3, aws_access_key),
        ok = application:set_env(rabbitmq_stream_s3, aws_secret_key, <<"wJalrXUtnFEMI">>),
        ?assertEqual(imds, resolve_credentials_source())
    after
        application:unset_env(rabbitmq_stream_s3, aws_access_key),
        application:unset_env(rabbitmq_stream_s3, aws_secret_key),
        case ContainerUri of
            false -> ok;
            _ -> os:putenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", ContainerUri)
        end
    end.

expected_bucket_owner_test() ->
    Sign = fun(Headers) ->
        sign_headers(
            {{2013, 5, 24}, {0, 0, 0}},
            <<"examplebucket.s3.amazonaws.com">>,
            <<"us-east-1">>,
            Headers,
            <<"AKIAIOSFODNN7EXAMPLE">>,
            <<"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY">>,
            undefined,
            <<"GET">>,
            <<"/test.txt">>,
            <<>>,
            #{}
        )
    end,
    %% No account ID configured: the header is not sent at all.
    ?assertNot(maps:is_key(<<"x-amz-expected-bucket-owner">>, Sign(#{}))),
    ok = application:set_env(rabbitmq_stream_s3, account_id, <<"123456789012">>),
    try
        Signed = Sign(#{<<"x-amz-expected-bucket-owner">> => <<"999999999999">>}),
        %% The configured value wins over a caller-supplied header...
        ?assertEqual(<<"123456789012">>, maps:get(<<"x-amz-expected-bucket-owner">>, Signed)),
        %% ...and is covered by the signature.
        #{<<"authorization">> := Authorization} = Signed,
        ?assertNotEqual(
            nomatch, binary:match(Authorization, <<"x-amz-expected-bucket-owner">>)
        )
    after
        application:unset_env(rabbitmq_stream_s3, account_id)
    end.

match_async_active_request_test() ->
    Ref = make_ref(),
    Conn = self(),
    Reqs = #{Ref => #{conn => Conn, stream_ref => Ref}},
    ?assertEqual(
        {ok, Ref},
        match_async({gun_data, Conn, Ref, nofin, <<"data">>}, Reqs, #{})
    ),
    ?assertEqual(
        {ok, Ref},
        match_async({gun_response, Conn, Ref, nofin, 200, []}, Reqs, #{})
    ),
    ?assertEqual(
        {ok, Ref},
        match_async({gun_error, Conn, Ref, some_reason}, Reqs, #{})
    ),
    ?assertEqual(
        {ok, Ref},
        match_async({request_timeout, Ref}, Reqs, #{})
    ),
    ok.

match_async_cancelled_request_test() ->
    Ref = make_ref(),
    Conn = self(),
    CancelledReqs = #{Ref => ok},
    %% Terminal frames: fin data, fin response, stream-level gun_error,
    %% request_timeout.
    ?assertEqual(
        {cancelled, Ref, final},
        match_async({gun_data, Conn, Ref, fin, <<"stale">>}, #{}, CancelledReqs)
    ),
    ?assertEqual(
        {cancelled, Ref, final},
        match_async({gun_response, Conn, Ref, fin, 200, []}, #{}, CancelledReqs)
    ),
    ?assertEqual(
        {cancelled, Ref, final},
        match_async({gun_error, Conn, Ref, some_reason}, #{}, CancelledReqs)
    ),
    ?assertEqual(
        {cancelled, Ref, final},
        match_async({request_timeout, Ref}, #{}, CancelledReqs)
    ),
    %% Non-terminal frames: nofin data, nofin response.
    ?assertEqual(
        {cancelled, Ref, more},
        match_async({gun_data, Conn, Ref, nofin, <<"stale">>}, #{}, CancelledReqs)
    ),
    ?assertEqual(
        {cancelled, Ref, more},
        match_async({gun_response, Conn, Ref, nofin, 200, []}, #{}, CancelledReqs)
    ),
    ok.

match_async_unknown_request_test() ->
    Ref = make_ref(),
    Conn = self(),
    ?assertEqual(
        error,
        match_async({gun_data, Conn, Ref, nofin, <<"x">>}, #{}, #{})
    ),
    ?assertEqual(
        error,
        match_async(some_other_message, #{}, #{})
    ),
    ok.

match_async_connection_error_ignores_cancelled_test() ->
    %% A connection-level gun_error (3-tuple) should match an active request
    %% on that connection, not a cancelled one - even if the only request on
    %% the connection is cancelled, we return `error` (the connection dying
    %% has no active request to notify).
    Conn = self(),
    Ref = make_ref(),
    ?assertEqual(
        error,
        match_async({gun_error, Conn, some_reason}, #{}, #{Ref => ok})
    ),
    %% With an active request on the connection, match_async returns it.
    ActiveRef = make_ref(),
    Reqs = #{ActiveRef => #{conn => Conn, stream_ref => ActiveRef}},
    ?assertEqual(
        {ok, ActiveRef},
        match_async({gun_error, Conn, some_reason}, Reqs, #{Ref => ok})
    ),
    ok.

-endif.
