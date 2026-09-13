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

%% For the pool. Not to be called by anyone else.
-export([endpoint/0]).

%% S3 requires at least 5 MiB per part, except the last, and caps an upload at
%% 10,000 parts. 8 MiB clears that floor and bounds what an uploader holds.
-define(MULTIPART_PART_BYTES, 8388608).

-behaviour(rabbitmq_stream_s3_api).

-type key() :: rabbitmq_stream_s3:key().
-type request_opts() ::
    rabbitmq_stream_s3_api:request_opts()
    | #{
        %% uri_string:compose_query/1's QueryList parameter:
        query => [{binary(), binary() | true}]
    }.
-type http_method() :: rabbitmq_stream_s3_http:http_method().
-type http_response() :: rabbitmq_stream_s3_http:http_response().
-type req_headers() :: rabbitmq_stream_s3_http:req_headers().
-type continuation_token() :: binary().
-type async_state() :: rabbitmq_stream_s3_http:async_state().
-type async_req() :: rabbitmq_stream_s3_http:async_req().

-doc """
Install the transport's counters and start the auth backend this one needs.

The counters are installed from here rather than centrally because whether a
backend reaches its store over HTTP at all is the backend's own business.

The S3 client holds no state of its own, but its requests have to be signed, so
what ends up supervised here is the auth backend's credential server. A scheme
needing no refreshable state returns `ignore` and nothing is supervised.
""".
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    ok = rabbitmq_stream_s3_http:init_counters(),
    rabbitmq_stream_s3_auth:start_link().

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
    Headers = #{<<"range">> => rabbitmq_stream_s3_http:range_specifier(Range)},
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
            rabbitmq_stream_s3_http:slice_range(Range, Data);
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
    Headers = #{<<"range">> => rabbitmq_stream_s3_http:range_specifier(Range)},
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
stream_put(Key, ContentLength, Opts) when is_binary(Key) andalso is_map(Opts) ->
    case rabbitmq_stream_s3_config:streaming_upload() of
        chunked -> chunked_stream_put(Key, ContentLength, Opts);
        multipart -> multipart_stream_put(Key, Opts)
    end.

chunked_stream_put(Key, ContentLength, Opts0) ->
    Method = <<"PUT">>,
    Path = key_to_path(Key),
    EncodedLength = aws_chunked_encoded_length(ContentLength),
    Headers0 = (sse_headers(Key))#{
        <<"content-length">> => integer_to_binary(EncodedLength),
        <<"content-encoding">> => <<"aws-chunked">>,
        <<"x-amz-decoded-content-length">> => integer_to_binary(ContentLength),
        <<"x-amz-trailer">> => <<"x-amz-checksum-crc32">>
    },
    Req = #{
        method => Method,
        path => Path,
        body => no_body,
        opts => Opts0#{stream_payload => true}
    },
    case authorize_request(Req, Headers0) of
        {ok, Headers} ->
            case rabbitmq_stream_s3_http:stream_request(Method, Path, Headers, Opts0) of
                {ok, State} -> {ok, State#{mode => chunked}};
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

-spec stream_data(async_state(), iodata()) -> async_state().
stream_data(#{data := PendingData0, pending_bytes := PendingBytes0} = State0, Data) ->
    PendingData = [PendingData0, Data],
    PendingBytes = PendingBytes0 + iolist_size(Data),
    State = State0#{data := PendingData, pending_bytes := PendingBytes},
    case State of
        #{mode := chunked} -> flush_chunks(State);
        #{mode := multipart} -> flush_parts(State)
    end.

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
stream_finish(#{mode := multipart} = State, _Crc32) ->
    %% A multipart object's ETag is not a digest of its bytes, so there is no
    %% aggregate checksum to send. Each part carries content-md5 instead.
    multipart_stream_finish(State);
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
    try rabbitmq_stream_s3_http:await_response(Conn, StreamRef, Timeout) of
        {ok, #{status := 200}} ->
            ok;
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other),
            {error, Other};
        {error, _} = Err ->
            rabbitmq_stream_s3_http:normalize_transport_error(Err)
    after
        rabbitmq_stream_s3_http:finish_async(State)
    end.

%% -------------------------------------------------------------------------
%% Internal: multipart upload
%% -------------------------------------------------------------------------

multipart_stream_put(Key, Opts) ->
    Path = key_to_path(Key),
    case request(<<"POST">>, <<Path/binary, "?uploads=">>, sse_headers(Key), <<>>, Opts) of
        {ok, #{status := 200, body := Body}} ->
            case decode_upload_id(Body) of
                <<>> ->
                    {error, no_upload_id};
                UploadId ->
                    {ok, #{
                        mode => multipart,
                        path => Path,
                        upload_id => UploadId,
                        part_number => 1,
                        %% Newest first. multipart_complete/1 reverses it.
                        parts => [],
                        data => [],
                        pending_bytes => 0,
                        opts => Opts
                    }}
            end;
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other, Key),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

%% stream_data/2 returns a state, not a result. A failed part latches here, and
%% stream_finish/2 reports it.
flush_parts(#{failed := _} = State) ->
    State#{data := [], pending_bytes := 0};
%% The remainder stays buffered. Only the last part of an upload can be under
%% the minimum size.
flush_parts(#{pending_bytes := PendingBytes, data := PendingData} = State0) when
    PendingBytes >= ?MULTIPART_PART_BYTES
->
    <<Part:?MULTIPART_PART_BYTES/binary, Rest/binary>> = iolist_to_binary(PendingData),
    case upload_part(State0, Part) of
        {ok, State} ->
            flush_parts(State#{data := [Rest], pending_bytes := byte_size(Rest)});
        {error, Reason} ->
            State0#{failed => Reason, data := [], pending_bytes := 0}
    end;
flush_parts(State) ->
    State.

upload_part(
    #{path := Path, upload_id := UploadId, part_number := PartNumber, parts := Parts, opts := Opts} =
        State,
    Part
) ->
    Query = <<
        "?partNumber=",
        (integer_to_binary(PartNumber))/binary,
        "&uploadId=",
        (uri_string:quote(UploadId))/binary
    >>,
    %% content-md5, not an x-amz checksum header. It is the one integrity
    %% mechanism every S3-compatible store implements.
    Headers = #{<<"content-md5">> => base64:encode(crypto:hash(md5, Part))},
    case request(<<"PUT">>, <<Path/binary, Query/binary>>, Headers, Part, Opts) of
        {ok, #{status := 200, headers := RespHeaders}} ->
            case proplists:get_value(<<"etag">>, RespHeaders) of
                undefined ->
                    {error, no_etag};
                ETag ->
                    {ok, State#{
                        part_number := PartNumber + 1, parts := [{PartNumber, ETag} | Parts]
                    }}
            end;
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

multipart_stream_finish(#{failed := Reason} = State) ->
    ok = stream_abort(State),
    {error, Reason};
multipart_stream_finish(#{data := PendingData} = State0) ->
    %% Unconditional. An upload needs at least one part, so an empty fragment
    %% still sends one.
    case upload_part(State0, iolist_to_binary(PendingData)) of
        {ok, State} ->
            multipart_complete(State);
        {error, Reason} ->
            ok = stream_abort(State0),
            {error, Reason}
    end.

multipart_complete(#{parts := Parts} = State) ->
    Body = complete_multipart_body(lists:reverse(Parts)),
    case multipart_request(<<"POST">>, State, Body, #{}) of
        %% A completion can fail with 200 and an <Error> body. The store sends
        %% the status before it finishes assembling the object.
        {ok, #{status := 200, body := RespBody}} ->
            case decode_complete_result(RespBody) of
                ok ->
                    ok;
                {error, Code} ->
                    ok = stream_abort(State),
                    {error, {complete_multipart_upload, Code}}
            end;
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other),
            ok = stream_abort(State),
            {error, Other};
        {error, _} = Err ->
            ok = stream_abort(State),
            rabbitmq_stream_s3_http:normalize_transport_error(Err)
    end.

multipart_request(Method, #{path := Path, upload_id := UploadId, opts := Opts}, Body, Headers) ->
    Query = <<"?uploadId=", (uri_string:quote(UploadId))/binary>>,
    request(Method, <<Path/binary, Query/binary>>, Headers, Body, Opts).

-spec decode_upload_id(binary()) -> binary().
decode_upload_id(Body) ->
    try xmerl_scan:string(binary_to_list(Body), [{allow_entities, false}]) of
        {#xmlElement{name = 'InitiateMultipartUploadResult', content = Content}, _} ->
            child_text('UploadId', Content);
        _ ->
            <<>>
    catch
        _:_ -> <<>>
    end.

%% Anything that is not a recognizable error document reads as `ok`. This
%% matches how the other paths treat a plain 200.
-spec decode_complete_result(binary()) -> ok | {error, binary()}.
decode_complete_result(Body) ->
    try xmerl_scan:string(binary_to_list(Body), [{allow_entities, false}]) of
        {#xmlElement{name = 'Error', content = Content}, _} ->
            {error, child_text('Code', Content)};
        _ ->
            ok
    catch
        _:_ -> ok
    end.

-spec complete_multipart_body([{pos_integer(), binary()}]) -> binary().
complete_multipart_body(Parts) ->
    Content = [
        #xmlElement{
            name = 'Part',
            content = [
                #xmlElement{
                    name = 'PartNumber',
                    content = [#xmlText{value = integer_to_list(PartNumber)}]
                },
                #xmlElement{name = 'ETag', content = [#xmlText{value = binary_to_list(ETag)}]}
            ]
        }
     || {PartNumber, ETag} <- Parts
    ],
    Doc = #xmlElement{name = 'CompleteMultipartUpload', content = Content},
    iolist_to_binary(xmerl:export_simple([Doc], xmerl_xml, [])).

send_chunk(Conn, StreamRef, Chunk) when is_binary(Chunk) ->
    Size = byte_size(Chunk),
    gun:data(Conn, StreamRef, nofin, [integer_to_binary(Size, 16), <<"\r\n">>, Chunk, <<"\r\n">>]).

-spec stream_abort(async_state()) -> ok.
stream_abort(#{mode := multipart} = State) ->
    %% Not merely tidying: an abandoned upload's parts stay billable until it is
    %% aborted. A failure here leaves only an orphan for GC.
    _ = multipart_request(<<"DELETE">>, State, <<>>, #{}),
    ok;
stream_abort(#{conn := Conn} = State) ->
    %% The streaming PUT body was only partially sent, so this HTTP/1.1
    %% connection is mid-request and cannot be reused. Close it without checking
    %% it back in; the pool's 'DOWN' handler removes it, accounts the checkout
    %% end (active_requests), and opens a replacement. Mirrors the timeout/cancel
    %% path; the half-sent PUT never reaches S3 as an object, so nothing is left
    %% behind except an orphan at most.
    gun:close(Conn),
    rabbitmq_stream_s3_http:finish_async_close(State).

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

%% -------------------------------------------------------------------------
%% Internal: requests
%% -------------------------------------------------------------------------

%% Authorize, then hand the request to the shared transport. Signing is this
%% module's concern because only it knows what its scheme covers; everything
%% below the headers is not.
-spec request(http_method(), key(), req_headers(), iodata(), request_opts()) ->
    {ok, http_response()} | {error, any()}.
request(Method, Path, Headers0, Body, Opts) when
    is_binary(Method) andalso
        is_binary(Path) andalso
        is_map(Headers0) andalso
        is_map(Opts)
->
    Req = #{method => Method, path => Path, body => Body, opts => Opts},
    case authorize_request(Req, Headers0) of
        {ok, Headers} ->
            rabbitmq_stream_s3_http:request(Method, Path, Headers, Body, Opts);
        {error, _} = Err ->
            Err
    end.

request_async(Method, Path, Headers0, Body, Opts) ->
    Req = #{method => Method, path => Path, body => Body, opts => Opts},
    case authorize_request(Req, Headers0) of
        {ok, Headers} ->
            rabbitmq_stream_s3_http:request_async(Method, Path, Headers, Body, Opts);
        {error, _} = Err ->
            Err
    end.

-spec match_async(
    Msg :: term(),
    Reqs :: #{async_req() := async_state()},
    CancelledReqs :: #{async_req() => _}
) ->
    {ok, async_req()} | {cancelled, async_req(), final | more} | error.
match_async(Msg, Reqs, CancelledReqs) ->
    rabbitmq_stream_s3_http:match_async(Msg, Reqs, CancelledReqs).

-spec handle_async(Msg :: term(), async_req(), async_state()) ->
    {continue, async_state()}
    | {data, binary(), async_state() | done}
    | {done, ok | {error, any()}}
    | {done_cancel, {error, any()}}
    | ignore.
handle_async(Msg, Req, State) ->
    rabbitmq_stream_s3_http:handle_async(Msg, Req, State).

-spec cancel_async(async_req(), async_state()) -> ok.
cancel_async(Req, State) ->
    rabbitmq_stream_s3_http:cancel_async(Req, State).

-doc """
The endpoint host, without the bucket.

The connection pool connects to this host, and TLS verifies it. Requests use
virtual-hosted addressing, so the bucket is a subdomain of it. See
`request_host/0`.

A store whose endpoint carries no region configures this host directly.
Otherwise it is derived from the region.
""".
-spec endpoint() -> {ok, binary()} | {error, any()}.
endpoint() ->
    case rabbitmq_stream_s3_config:endpoint() of
        undefined ->
            case rabbitmq_stream_s3_auth_aws:region() of
                {ok, Region} -> {ok, derived_endpoint(Region)};
                {error, _} = Err -> Err
            end;
        Endpoint ->
            {ok, Endpoint}
    end.

-spec derived_endpoint(Region :: binary()) -> binary().
derived_endpoint(Region) ->
    <<"s3.", Region/binary, $., (tld(Region))/binary>>.

%% The bucket as a subdomain of the endpoint. This goes in `host`, so a
%% signature covers it.
-spec request_host() -> {ok, binary()} | {error, any()}.
request_host() ->
    case endpoint() of
        {ok, Endpoint} ->
            {ok, <<(rabbitmq_stream_s3_config:bucket())/binary, $., Endpoint/binary>>};
        {error, _} = Err ->
            Err
    end.

%% This client decides where a request goes, not the auth backend. It sets
%% `host` here, and the backend signs the host it receives.
-spec authorize_request(rabbitmq_stream_s3_auth:request(), req_headers()) ->
    {ok, req_headers()} | {error, any()}.
authorize_request(Req, Headers) ->
    case request_host() of
        {ok, Host} ->
            rabbitmq_stream_s3_auth:authorize(Req, Headers#{<<"host">> => Host});
        {error, _} = Err ->
            Err
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

range_spec_test() ->
    ?assertEqual(<<"bytes=-5">>, rabbitmq_stream_s3_http:range_specifier(-5)),
    ?assertEqual(<<"bytes=10-20">>, rabbitmq_stream_s3_http:range_specifier({10, 20})),
    ?assertEqual(<<"bytes=100-">>, rabbitmq_stream_s3_http:range_specifier({100, undefined})),
    ok.

decode_upload_id_test() ->
    Body =
        <<
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<InitiateMultipartUploadResult><Bucket>b</Bucket><Key>k</Key>"
            "<UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRz</UploadId>"
            "</InitiateMultipartUploadResult>"
        >>,
    ?assertEqual(<<"VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRz">>, decode_upload_id(Body)),
    %% A response without an upload id must not be mistaken for one: the caller
    %% turns an empty id into an error rather than uploading parts nowhere.
    ?assertEqual(<<>>, decode_upload_id(<<"<Error><Code>AccessDenied</Code></Error>">>)),
    ?assertEqual(<<>>, decode_upload_id(<<"not xml">>)).

decode_complete_result_test() ->
    ?assertEqual(
        ok,
        decode_complete_result(
            <<"<CompleteMultipartUploadResult><ETag>\"x\"</ETag></CompleteMultipartUploadResult>">>
        )
    ),
    %% The case this exists for: 200 carrying a failure.
    ?assertEqual(
        {error, <<"InternalError">>},
        decode_complete_result(<<"<Error><Code>InternalError</Code></Error>">>)
    ),
    %% Anything unrecognisable counts as success, as a bare 200 does elsewhere.
    ?assertEqual(ok, decode_complete_result(<<"not xml">>)).

complete_multipart_body_test() ->
    ?assertEqual(
        <<
            "<?xml version=\"1.0\"?><CompleteMultipartUpload>"
            "<Part><PartNumber>1</PartNumber><ETag>\"a\"</ETag></Part>"
            "<Part><PartNumber>2</PartNumber><ETag>\"b\"</ETag></Part>"
            "</CompleteMultipartUpload>"
        >>,
        complete_multipart_body([{1, <<"\"a\"">>}, {2, <<"\"b\"">>}])
    ).

multipart_state_after_a_failed_part_test() ->
    %% Once a part fails there is nothing to report it to until stream_finish/2,
    %% so the state must stop accumulating rather than buffer a whole fragment.
    State = flush_parts(#{
        mode => multipart, failed => some_reason, data => [<<"buffered">>], pending_bytes => 8
    }),
    ?assertEqual(0, maps:get(pending_bytes, State)),
    ?assertEqual([], maps:get(data, State)),
    ?assertEqual(some_reason, maps:get(failed, State)).

derived_endpoint_test() ->
    ?assertEqual(<<"s3.us-east-1.amazonaws.com">>, derived_endpoint(<<"us-east-1">>)),
    %% Partitions outside the default TLD.
    ?assertEqual(<<"s3.cn-north-1.amazonaws.com.cn">>, derived_endpoint(<<"cn-north-1">>)).

request_host_puts_the_bucket_under_the_endpoint_test() ->
    Region = persistent_term:get(rabbitmq_stream_s3_api_aws_region, undefined),
    Bucket = application:get_env(rabbitmq_stream_s3, bucket),
    persistent_term:put(rabbitmq_stream_s3_api_aws_region, <<"us-east-1">>),
    ok = application:set_env(rabbitmq_stream_s3, bucket, <<"examplebucket">>),
    try
        %% The pool connects to the endpoint. Only the request carries the bucket.
        ?assertEqual({ok, <<"s3.us-east-1.amazonaws.com">>}, endpoint()),
        ?assertEqual({ok, <<"examplebucket.s3.us-east-1.amazonaws.com">>}, request_host())
    after
        case Bucket of
            undefined -> application:unset_env(rabbitmq_stream_s3, bucket);
            {ok, B} -> application:set_env(rabbitmq_stream_s3, bucket, B)
        end,
        case Region of
            undefined -> persistent_term:erase(rabbitmq_stream_s3_api_aws_region);
            _ -> persistent_term:put(rabbitmq_stream_s3_api_aws_region, Region)
        end
    end.

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

configured_endpoint_replaces_the_derived_one_test() ->
    Bucket = application:get_env(rabbitmq_stream_s3, bucket),
    ok = application:set_env(rabbitmq_stream_s3, bucket, <<"examplebucket">>),
    try
        ok = application:set_env(rabbitmq_stream_s3, endpoint, <<"storage.googleapis.com">>),
        %% The endpoint replaces the derived host, region included. A store
        %% reached this way has one host with no region in it.
        ?assertEqual({ok, <<"storage.googleapis.com">>}, endpoint()),
        %% Addressing stays virtual-hosted, so the bucket is still a subdomain.
        ?assertEqual({ok, <<"examplebucket.storage.googleapis.com">>}, request_host())
    after
        application:unset_env(rabbitmq_stream_s3, endpoint),
        case Bucket of
            undefined -> application:unset_env(rabbitmq_stream_s3, bucket);
            {ok, B} -> application:set_env(rabbitmq_stream_s3, bucket, B)
        end
    end.

-endif.
