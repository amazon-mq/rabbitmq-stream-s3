%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_http).
-moduledoc """
The HTTP transport shared by the object store clients.

This module holds what is transport and not protocol. A second store needed all
of it unchanged:

- Send a request on a pooled `gun` connection.
- Buffer an async response.
- Recover a Range request that the store answers with 200.
- Drain the body of an error response.
- Time a request out.

A client keeps the protocol: how it addresses and signs a request, which status
means success, and how it parses a response body.

The counters are transport facts too. Request counts, refusals, timeouts and
in-flight connections describe this module, not a store's API. A backend
installs them from its `start_link/0`, because only the backend knows whether it
uses HTTP.

The read path maps 404 to `not_found`, 500 to `internal_error` and 503 to
`slow_down` here, not in a client. A ranged GET means the same thing in every
store this plugin uses, and
`rabbitmq_stream_s3_replica_reader_core:is_retriable/1` matches those atoms.
""".

-include_lib("kernel/include/logger.hrl").

-export([
    init_counters/0,
    note_request_started/0,
    note_request_finished/0,
    request/5,
    request_async/5,
    stream_request/4,
    await_response/3,
    match_async/3,
    handle_async/3,
    cancel_async/2,
    finish_async/1,
    finish_async_close/1,
    cancel_request_timer/1,
    normalize_transport_error/1,
    compose_query/1,
    range_specifier/1,
    slice_range/2,
    start_timeout_window/1,
    end_timeout_window/2
]).

-ifdef(TEST).
%% For tests that need to observe these counters, including the pool's, since
%% the pool moves active_requests but does not own them.
-export([with_counter/1]).
-endif.

-define(C_ACTIVE_REQUESTS, 1).
-define(C_TOTAL_REQUESTS, 2).
-define(C_RESPONSE_403, 3).
-define(C_RESPONSE_500, 4).
-define(C_RESPONSE_503, 5).
-define(C_REQUEST_TIMEOUTS, 6).
-define(COUNTERS, [
    {active_requests, ?C_ACTIVE_REQUESTS, gauge, "Current number of requests to the object store"},
    {total_requests, ?C_TOTAL_REQUESTS, counter, "Total number of requests to the object store"},
    {response_403, ?C_RESPONSE_403, counter, "Number of HTTP 403 responses"},
    {response_500, ?C_RESPONSE_500, counter, "Number of HTTP 500 responses"},
    {response_503, ?C_RESPONSE_503, counter, "Number of HTTP 503 responses"},
    {request_timeouts, ?C_REQUEST_TIMEOUTS, counter, "Number of requests that timed out"}
]).
-define(COUNTER_KEY, {?MODULE, counter}).

-define(GENERAL_POOL, rabbitmq_stream_s3_general_pool).
-define(UPLOAD_POOL, rabbitmq_stream_s3_upload_pool).
%% How long the read path waits for a pooled connection before returning
%% pool_busy. A same-region TLS handshake takes 5-34ms in practice. 100ms gives
%% about 3x headroom and keeps reads responsive.
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

-doc """
Uppercase HTTP method name, as a binary.

Called "HTTP Verb" in S3 docs. "GET", "PUT", "HEAD", "POST", "DELETE", etc..
""".
-type http_method() :: binary().
-type http_response() :: #{
    status := pos_integer(),
    %% TODO: why is gun:resp_headers() not exported?
    headers := [{binary(), binary()}],
    %% Always present, empty when the response carried no body, so that a client
    %% matching on `body` reads a zero-length object as the empty object it is
    %% rather than as an unexpected response.
    body := binary()
}.
%% Map keys must be lowercase.
-type req_headers() :: #{binary() => binary()}.
-type pool() :: ?GENERAL_POOL | ?UPLOAD_POOL.
-type async_state() :: #{
    pool := pool(),
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
    %% Present for a ranged async GET: the byte range the request asked for.
    %% Kept so a non-conformant 200 (full object) response can be sliced down to
    %% the requested range, mirroring the synchronous recovery.
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

-export_type([
    http_method/0,
    http_response/0,
    req_headers/0,
    async_state/0,
    async_req/0
]).

-doc """
Install this module's counters.

Called from an API backend's `start_link/0`: whether a backend reaches its store
over HTTP is the backend's own business, so it says when these exist rather than
having them installed for a backend that never issues a request.
""".
-spec init_counters() -> ok.
init_counters() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?COUNTER_KEY, Cnt),
    ok.

%% All counter mutations go through inc/2 so a missing counter is a no-op rather
%% than a badarg. The counters are only installed by a backend that speaks HTTP,
%% and the pool can be driven standalone in tests, so every call site would
%% otherwise need to know whether it can be reached with them absent.
inc(Idx, N) ->
    case persistent_term:get(?COUNTER_KEY, undefined) of
        undefined -> ok;
        Cnt -> counters:add(Cnt, Idx, N)
    end,
    ok.

-doc """
`active_requests` counts connections currently checked out of a pool: one
in-flight request per checkout. The pool owns both edges, calling
`note_request_started/0` when it hands a connection to a caller and
`note_request_finished/0` when that checkout ends, whether by check-in, by the
caller dying while holding it, or by the connection dying under it.

Keeping both edges in the pool, at the checkout lifecycle points, is what makes
the gauge balanced by construction. The alternative - incrementing in the
requesting process and decrementing on each of its completion paths - left the
gauge to drift: a caller killed mid-request (e.g. by
`rabbitmq_stream_s3_governor:cancel/1`) or killed while still queued for a
connection ran none of those paths, so an increment had no matching decrement.

Both are a no-op if the counters aren't installed: only a backend that speaks
HTTP installs them, and the pool can be driven in isolation (e.g.
api_aws_pool_statem_SUITE) with no backend at all.
""".
-spec note_request_started() -> ok.
note_request_started() ->
    inc(?C_ACTIVE_REQUESTS, 1).

-doc #{equiv => note_request_started / 0}.
-spec note_request_finished() -> ok.
note_request_finished() ->
    inc(?C_ACTIVE_REQUESTS, -1).

%%---------------------------------------------------------------------------
%% Synchronous requests
%%---------------------------------------------------------------------------

-doc """
Issue a request on a pooled connection and read the whole response.

`Headers` are already authorized: signing happens in the client, which knows
what its scheme covers.
""".
-spec request(http_method(), binary(), req_headers(), iodata(), map()) ->
    {ok, http_response()} | {error, any()}.
request(Method, Path, Headers, Body, Opts) when
    is_binary(Method) andalso
        is_binary(Path) andalso
        is_map(Headers) andalso
        is_map(Opts)
->
    %% active_requests is owned by the pool, tied to the checkout that
    %% request1/5 does below (see note_request_started/0).
    ok = inc(?C_TOTAL_REQUESTS, 1),
    normalize_transport_error(request0(Method, Path, Headers, Body, Opts)).

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
    Pool = pool_for(Method),
    Timeout = maps:get(timeout, Opts, 5_000),
    T1 = start_timeout_window(Timeout),
    rabbitmq_stream_s3_api_aws_pool:with(Pool, Timeout, fun(Conn) ->
        StreamRef = gun:request(Conn, Method, Path, Headers, Body),
        await_response(Conn, StreamRef, end_timeout_window(Timeout, T1))
    end).

%% Writes take longer than reads because they upload tens of megabytes, so they
%% get their own pool and cannot starve the read path.
pool_for(<<"PUT">>) -> ?UPLOAD_POOL;
pool_for(_) -> ?GENERAL_POOL.

-doc "Read a response the caller has already issued on `Conn`.".
-spec await_response(pid(), gun:stream_ref(), timeout()) ->
    {ok, http_response()} | {error, any()}.
await_response(Conn, StreamRef, Timeout) ->
    T1 = start_timeout_window(Timeout),
    case gun:await(Conn, StreamRef, Timeout) of
        {response, fin, Status, RespHeaders} ->
            Response = #{status => Status, headers => RespHeaders, body => <<>>},
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
                    ok = inc(?C_REQUEST_TIMEOUTS, 1),
                    Err;
                {error, _} = Err ->
                    Err
            end;
        {error, timeout} = Err ->
            ok = inc(?C_REQUEST_TIMEOUTS, 1),
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

postprocess_response(#{status := 403}) -> inc(?C_RESPONSE_403, 1);
postprocess_response(#{status := 500}) -> inc(?C_RESPONSE_500, 1);
postprocess_response(#{status := 503}) -> inc(?C_RESPONSE_503, 1);
postprocess_response(_Response) -> ok.

%%---------------------------------------------------------------------------
%% Asynchronous requests
%%---------------------------------------------------------------------------

-doc """
Issue a request whose response is delivered to the caller as gun messages.

The caller drives it with `match_async/3` and `handle_async/3`.
""".
-spec request_async(http_method(), binary(), req_headers(), iodata(), map()) ->
    {ok, async_req(), async_state()} | {error, any()}.
request_async(Method, Path, Headers, Body, Opts) ->
    case rabbitmq_stream_s3_api_aws_pool:checkout(?GENERAL_POOL, ?READ_CHECKOUT_TIMEOUT_MS) of
        {ok, Conn} ->
            %% active_requests is owned by the pool, tied to this checkout.
            ok = inc(?C_TOTAL_REQUESTS, 1),
            %% NOTE: no need to wrap this in try/catch and checkin the conn
            %% since gun:request/5 cannot exit/error/throw.
            StreamRef = gun:request(Conn, Method, Path, Headers, Body),
            %% Phase markers for timeout reporting (see `describe_stall/1`).
            State = #{
                pool => ?GENERAL_POOL,
                conn => Conn,
                stream_ref => StreamRef,
                req_started_at => rabbitmq_stream_s3_util:now(),
                headers_at => undefined,
                first_data_at => undefined,
                bytes_received => 0
            },
            {ok, StreamRef, maybe_set_timer(Opts, StreamRef, State)};
        {error, Saturation} = Err when
            Saturation =:= pool_busy; Saturation =:= pool_exhausted
        ->
            Err
    end.

-doc """
Begin a request whose body the caller streams with `gun:data/4`.

Returns the checked-out connection and stream so the client can frame the body
however its protocol wants, and read the response with `await_response/3`.
""".
-spec stream_request(http_method(), binary(), req_headers(), map()) ->
    {ok, async_state()} | {error, any()}.
stream_request(Method, Path, Headers, Opts) ->
    %% active_requests is owned by the pool and moved on the checkout itself, so
    %% a failed checkout cannot leak it and a caller killed mid-request is still
    %% balanced by the pool.
    case rabbitmq_stream_s3_api_aws_pool:checkout(?UPLOAD_POOL, 10_000) of
        {ok, Conn} ->
            ok = inc(?C_TOTAL_REQUESTS, 1),
            StreamRef = gun:headers(Conn, Method, Path, Headers),
            {ok, #{
                pool => ?UPLOAD_POOL,
                conn => Conn,
                stream_ref => StreamRef,
                data => [],
                pending_bytes => 0,
                timeout => maps:get(timeout, Opts, 60_000)
            }};
        {error, Saturation} = Err when
            Saturation =:= pool_busy; Saturation =:= pool_exhausted
        ->
            Err
    end.

maybe_set_timer(#{timeout := Timeout}, StreamRef, State) ->
    TimerRef = erlang:send_after(Timeout, self(), {request_timeout, StreamRef}),
    State#{timer_ref => TimerRef};
maybe_set_timer(_Opts, _StreamRef, State) ->
    State.

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
    | {data, binary(), async_state() | done}
    | {done, ok | {error, any()}}
    | {done_cancel, {error, any()}}
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
            200 -> ok;
            _ -> {error, status_reason(Status, Headers)}
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
                    %% slice the range out at fin, mirroring the synchronous
                    %% path. The extra buffering only applies to this
                    %% non-conformant case.
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
            State = cancel_request_timer(State0),
            {continue, State#{draining => status_reason(Status, Headers)}}
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
    %% connection-level failure (no response at all) from a slow transfer.
    ?LOG_WARNING(
        "Object store request timed out on ~tw/~tw: ~ts",
        [Conn, StreamRef, describe_stall(State)]
    ),
    ok = inc(?C_REQUEST_TIMEOUTS, 1),
    %% gun cannot cancel an HTTP/1.1 stream on the wire - it only stops
    %% forwarding events to the owner. The cancelled stream continues to
    %% occupy the connection, blocking subsequent requests until its full
    %% response arrives. Close the whole connection instead. The pool sees the
    %% 'DOWN' and opens a replacement.
    gun:close(Conn),
    finish_async_close(State),
    {done_cancel, {error, timeout}}.

%% The read path's error vocabulary. These atoms are what
%% rabbitmq_stream_s3_replica_reader_core:is_retriable/1 classifies, so a store
%% reporting throttling or an internal error under the same status codes gets
%% the same retry treatment without the caller knowing which store answered.
status_reason(404, _Headers) -> not_found;
status_reason(500, _Headers) -> internal_error;
status_reason(503, _Headers) -> slow_down;
status_reason(Status, Headers) -> #{status => Status, headers => Headers}.

%% Record the time and running byte count of the first body frame. Runs on
%% every body frame, at a cost of two map operations, and `describe_stall/1`
%% reports it on a timeout. Revisit this if profiling shows the per-frame cost.
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
    %% On HTTP/1.1, gun:cancel only marks the stream dead. The response body
    %% keeps draining on the wire and blocks the requests behind it. Close the
    %% connection so the pool replaces it.
    gun:close(Conn),
    ok = finish_async_close(State).

-doc """
Cancel the request timer without checking in the connection or decrementing the
active-request counter.

Used when the response body still needs to be drained before the request is
complete.
""".
-spec cancel_request_timer(async_state()) -> async_state().
cancel_request_timer(State) ->
    case State of
        #{timer_ref := TimerRef, stream_ref := StreamRef} ->
            ok = flush_timer(TimerRef, StreamRef),
            maps:remove(timer_ref, State);
        _ ->
            State
    end.

-spec finish_async(async_state()) -> ok.
finish_async(#{conn := Conn, pool := Pool} = State) ->
    ok = maybe_flush_timer(State),
    %% The pool decrements active_requests on the checkin below (see the pool's
    %% note_request_finished/0).
    ok = rabbitmq_stream_s3_api_aws_pool:checkin(Pool, Conn).

-doc """
Finish an async request when the connection is being closed, on a timeout or a
cancel.

Same bookkeeping as `finish_async/1` but omits the pool checkin: the caller has
already closed the connection, so the pool's 'DOWN' handler removes the conn,
accounts the checkout end (active_requests), and grows a replacement.
""".
-spec finish_async_close(async_state()) -> ok.
finish_async_close(State) ->
    maybe_flush_timer(State).

maybe_flush_timer(#{timer_ref := TimerRef, stream_ref := StreamRef}) ->
    flush_timer(TimerRef, StreamRef);
maybe_flush_timer(_State) ->
    ok.

flush_timer(TimerRef, StreamRef) ->
    case erlang:cancel_timer(TimerRef) of
        false ->
            receive
                {request_timeout, StreamRef} -> ok
            after 0 -> ok
            end;
        _ ->
            ok
    end.

%%---------------------------------------------------------------------------
%% Query strings
%%---------------------------------------------------------------------------

-doc """
Encode query parameters with strict percent-encoding.

`uri_string:compose_query/1` writes a space as `+`, which is the
form-urlencoded convention rather than the URI one. A bare `+` in a query is
then ambiguous - Azure reads it as a space, AWS reads it as a plus - and a
signature computed by decoding it one way is rejected by a service that decodes
it the other. Encoding a space as `%20` and a plus as `%2B` leaves nothing to
interpret.

Parameters are written in the order given; a scheme that signs a canonical query
sorts them first.
""".
-spec compose_query([{binary(), binary()}]) -> binary().
compose_query(Params) ->
    iolist_to_binary(
        lists:join($&, [
            [uri_string:quote(Name), $=, uri_string:quote(Value)]
         || {Name, Value} <- Params
        ])
    ).

%%---------------------------------------------------------------------------
%% Ranges
%%---------------------------------------------------------------------------

%% https://www.rfc-editor.org/rfc/rfc9110.html#rule.ranges-specifier
-spec range_specifier(rabbitmq_stream_s3_api:range_spec()) -> binary().
range_specifier({StartByte, undefined}) ->
    <<"bytes=", (integer_to_binary(StartByte))/binary, "-">>;
range_specifier({StartByte, EndByte}) ->
    <<"bytes=", (integer_to_binary(StartByte))/binary, "-", (integer_to_binary(EndByte))/binary>>;
range_specifier(SuffixLen) when is_integer(SuffixLen) andalso SuffixLen < 0 ->
    %% integer_to_binary/1 will format the '-' for us.
    <<"bytes=", (integer_to_binary(SuffixLen))/binary>>.

-doc """
Extract the bytes a Range request asked for out of a full-object body.

Used to recover when a store answers a Range request with 200 instead of 206.
The slice mirrors what a conformant 206 would have returned, clamping the end to
the object size as S3 does. A range starting at or beyond the object size is
unsatisfiable.
""".
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

%%---------------------------------------------------------------------------
%% Timeout windows
%%---------------------------------------------------------------------------

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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

note_request_started_and_finished_balance_test() ->
    with_counter(fun(Read) ->
        ?assertEqual(ok, note_request_started()),
        ?assertEqual(1, Read(active_requests)),
        ?assertEqual(ok, note_request_finished()),
        ?assertEqual(0, Read(active_requests))
    end).

%% Both edges must tolerate missing counters: they are installed only by a
%% backend that speaks HTTP, and the pool can be driven standalone.
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
counters, so the pool's own tests need it too.
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

compose_query_test() ->
    ?assertEqual(<<"a=1&b=2">>, compose_query([{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}])),
    %% The point of the function: a space is %20 and a plus is %2B, so neither
    %% can be mistaken for the other.
    ?assertEqual(<<"prefix=a%20b%2Bc%3Dd">>, compose_query([{<<"prefix">>, <<"a b+c=d">>}])),
    ?assertEqual(<<"prefix=">>, compose_query([{<<"prefix">>, <<>>}])),
    ?assertEqual(<<>>, compose_query([])).

status_reason_test() ->
    ?assertEqual(not_found, status_reason(404, [])),
    ?assertEqual(internal_error, status_reason(500, [])),
    ?assertEqual(slow_down, status_reason(503, [])),
    ?assertEqual(#{status => 418, headers => []}, status_reason(418, [])).

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
    %% The saturation kinds in particular: the remote reader's look-ahead reads
    %% them by name off a failed group fetch to pick which clock to retry on.
    ?assertEqual({error, pool_busy}, normalize_transport_error({error, pool_busy})),
    ?assertEqual({error, pool_exhausted}, normalize_transport_error({error, pool_exhausted})),
    ?assertEqual(
        {error, #{status => 403}}, normalize_transport_error({error, #{status => 403}})
    ),
    ?assertEqual({ok, #{status => 200}}, normalize_transport_error({ok, #{status => 200}})),
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

%% In slice mode this buffers every body frame and forwards none. The slice
%% happens at fin.
async_slice_mode_buffers_data_test() ->
    C = self(),
    R = make_ref(),
    State0 = #{
        conn => C, stream_ref => R, slice_full => {3, 5}, data => [<<"01">>], pending_bytes => 2
    },
    {continue, S} = handle_async({gun_data, C, R, nofin, <<"23">>}, R, State0),
    ?assertEqual([<<"23">>, <<"01">>], maps:get(data, S)),
    ?assertEqual(4, maps:get(pending_bytes, S)).

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
