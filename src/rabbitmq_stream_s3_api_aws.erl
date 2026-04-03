%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_api_aws).
-moduledoc """
A wrapper around the AWS S3 HTTP API.
""".

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("xmerl/include/xmerl.hrl").

%% API:
-export([
    init/0,
    reload_config/0,
    open/0,
    close/1,
    get/3,
    get_range/4,
    put/4,
    delete/3,
    delete_prefix/3,
    list/4
]).

%% For apply/3:
-export([get_credentials/0, get_credentials/2]).

-define(ALGORITHM, "AWS4-HMAC-SHA256").
-define(ISOFORMAT_BASIC, "~4.10.0b~2.10.0b~2.10.0bT~2.10.0b~2.10.0b~2.10.0bZ").
-define(TABLE, ?MODULE).
-define(METADATA_TOKEN_TTL_SECONDS, 60).
%% A margin to add to a TTL. Subtracting a few seconds reduces the chances that
%% we use a token just as it expires.
-define(TTL_SECONDS_BUFFER, 5).
-define(REGION_KEY, rabbitmq_stream_s3_api_aws_region).

-define(C_ACTIVE_REQUESTS, 1).
-define(C_TOTAL_REQUESTS, 2).
-define(C_GET, 3).
-define(C_GET_RANGE, 4).
-define(C_PUT, 5).
-define(C_DELETE_MANY, 6).
-define(C_DELETE_ONE, 7).
-define(C_LIST, 8).
-define(C_RESPONSE_500, 9).
-define(C_RESPONSE_503, 10).
-define(COUNTERS, [
    {active_requests, ?C_ACTIVE_REQUESTS, gauge, "Current number of requests to S3"},
    {total_requests, ?C_TOTAL_REQUESTS, counter, "Total number of requests to S3"},
    {get, ?C_GET, counter, "Number of full-object GET requests"},
    {get_range, ?C_GET_RANGE, counter, "Number of range GET requests"},
    {put, ?C_PUT, counter, "Number of PUT requests"},
    {delete_many, ?C_DELETE_MANY, counter, "Number of multi-object DELETE requests"},
    {delete_one, ?C_DELETE_ONE, counter, "Number of single-object DELETE requests"},
    {list, ?C_LIST, counter, "Number of LIST requests"},
    {response_500, ?C_RESPONSE_500, counter, "Number of HTTP 500 responses"},
    {response_503, ?C_RESPONSE_503, counter, "Number of HTTP 503 responses"}
]).
-define(COUNTER_KEY, {?MODULE, counter}).

-record(container_creds_req, {host, port, path, conn, stream_ref}).

-behaviour(rabbitmq_stream_s3_api).

%% NOTE: the `rabbitmq_stream_s3_api:connection()` type is a `pid()` here - the
%% gun connection PID.
-type connection() :: rabbitmq_stream_s3_api:connection().

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

-spec init() -> ok.
init() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?COUNTER_KEY, Cnt),
    _ = ets:new(?TABLE, [public, named_table]),
    ok = rabbitmq_stream_s3_request_metrics:init(),
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

-doc """
Opens a connection to S3 in the configured region.
""".
-spec open() -> {ok, connection()} | {error, any()}.
open() ->
    %% NOTE: unfortunately, `inet:hostname()` is a string not a binary.
    Host = binary_to_list(hostname()),
    %% NOTE: AWS S3 only supports HTTP/1.1 but other providers like Google Cloud
    %% support HTTP/2 (from what I heard). TODO: Evaluate HTTP/2 on other
    %% providers. Maybe just pin to HTTP/1.1.
    Opts = #{transport => tls, protocols => [http2, http]},
    case gun:open(Host, 443, Opts) of
        {ok, Conn} ->
            case gun:await_up(Conn) of
                {ok, _Protocol} ->
                    {ok, Conn};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

-doc """
Closes a connection created with `open_connection/0`.
""".
-spec close(connection()) -> ok.
close(Conn) when is_pid(Conn) ->
    gun:close(Conn).

-doc "Gets the body of an object at key `Key`".
-spec get(connection(), key(), request_opts()) -> {ok, binary()} | {error, any()}.
get(Conn, Key, Opts) when is_pid(Conn) andalso is_binary(Key) andalso is_map(Opts) ->
    counters:add(counter(), ?C_GET, 1),
    case request(Conn, <<"GET">>, key_to_path(Key), #{}, <<>>, Opts) of
        {ok, #{status := 200, body := Data}} ->
            {ok, Data};
        {ok, #{status := 404}} ->
            {error, not_found};
        {ok, Other} ->
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
-spec get_range(connection(), key(), rabbitmq_stream_s3_api:range_spec(), request_opts()) ->
    {ok, binary()} | {error, any()}.
get_range(Conn, Key, Range, Opts) when is_pid(Conn) andalso is_binary(Key) andalso is_map(Opts) ->
    counters:add(counter(), ?C_GET_RANGE, 1),
    Headers = #{<<"range">> => range_specifier(Range)},
    case request(Conn, <<"GET">>, key_to_path(Key), Headers, <<>>, Opts) of
        %% HTTP Range requests must return 206 if only a partial range is served,
        %% according to the RFC.
        {ok, #{status := Status, body := Data}} when Status =:= 200 orelse Status =:= 206 ->
            {ok, Data};
        {ok, #{status := 404}} ->
            {error, not_found};
        {ok, Other} ->
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-doc "Uploads the given `Data` as an object at key `Key`".
-spec put(connection(), key(), iodata(), request_opts()) -> ok | {error, any()}.
put(Conn, Key, Data, Opts) when is_pid(Conn) andalso is_binary(Key) andalso is_map(Opts) ->
    counters:add(counter(), ?C_PUT, 1),
    Headers =
        case Opts of
            #{crc32 := Checksum} ->
                #{<<"x-amz-checksum-crc32">> => base64:encode(<<Checksum:32/unsigned>>)};
            _ ->
                #{}
        end,
    case request(Conn, <<"PUT">>, key_to_path(Key), Headers, Data, Opts) of
        {ok, #{status := 200}} ->
            ok;
        {ok, Other} ->
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-doc "Deletes the given key or list of keys".
-spec delete(connection(), key() | [key()], request_opts()) ->
    ok | {error, any()}.
delete(Conn, Keys, Opts) when is_pid(Conn) andalso is_list(Keys) andalso is_map(Opts) ->
    %% <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html>
    ?assert(length(Keys) =< 1000),
    %% Though not documented, S3 will reject the request if the list of keys
    %% is empty.
    ?assertNotEqual([], Keys),
    counters:add(counter(), ?C_DELETE_MANY, 1),
    Data = delete_many_body(Keys),
    Headers = #{
        %% A checksum header seems to be required on this endpoint...
        <<"x-amz-checksum-crc32">> => base64:encode(<<(erlang:crc32(Data)):32/unsigned>>)
    },
    case request(Conn, <<"POST">>, <<"/?delete=">>, Headers, Data, Opts) of
        {ok, #{status := 200}} ->
            ok;
        {ok, Other} ->
            {error, Other};
        {error, _} = Err ->
            Err
    end;
delete(Conn, Key, Opts) when is_pid(Conn) andalso is_binary(Key) andalso is_map(Opts) ->
    counters:add(counter(), ?C_DELETE_ONE, 1),
    %% <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html>.
    case request(Conn, <<"DELETE">>, key_to_path(Key), #{}, <<>>, Opts) of
        {ok, #{status := 204}} ->
            ok;
        {ok, Other} ->
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
-spec delete_prefix(connection(), key(), request_opts()) ->
    {ok, objects_metadata()} | {error, any()}.
delete_prefix(Conn, Prefix, Opts) when
    is_pid(Conn) andalso is_binary(Prefix) andalso is_map(Opts)
->
    delete_prefix(Conn, Prefix, Opts, undefined, 0, 0, 0).

delete_prefix(Conn, Prefix, Opts, Token, Objects0, TotalSize0, Pages0) ->
    case list(Conn, Prefix, Token, Opts) of
        {ok, {[], 0, undefined}} ->
            Meta = #{
                objects => Objects0,
                total_size => TotalSize0,
                pages => Pages0
            },
            {ok, Meta};
        {ok, {PageKeys, PageSize, NextToken}} ->
            case delete(Conn, PageKeys, Opts) of
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
                                Conn,
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

list(Conn, Prefix, ContinuationToken, Opts) ->
    counters:add(counter(), ?C_LIST, 1),
    Params0 = [{<<"list-type">>, <<"2">>}, {<<"prefix">>, Prefix}],
    Params1 =
        case ContinuationToken of
            undefined ->
                Params0;
            _ ->
                [{<<"continuation-token">>, ContinuationToken} | Params0]
        end,
    Params = uri_string:compose_query(Params1),
    case request(Conn, <<"GET">>, <<"/?", Params/binary>>, #{}, <<>>, Opts) of
        {ok, #{status := 200, body := Body}} ->
            {ok, decode_list_bucket_result(Body)};
        {ok, Other} ->
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

-spec request(connection(), http_method(), key(), req_headers(), iodata(), request_opts()) ->
    {ok, http_response()}
    | {error, any()}.
request(Conn, Method, Path, Headers0, Body, Opts) when
    is_pid(Conn) andalso
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
            T0 = erlang:monotonic_time(millisecond),
            try
                Timeout = maps:get(timeout, Opts, 5_000),
                T1 = start_timeout_window(Timeout),
                StreamRef = gun:request(Conn, Method, Path, Headers, Body),
                case gun:await(Conn, StreamRef, Timeout) of
                    {response, fin, Status, RespHeaders} ->
                        Response = #{status => Status, headers => RespHeaders},
                        postprocess_response(Response),
                        {ok, Response};
                    {response, nofin, Status, RespHeaders} ->
                        Timeout1 = end_timeout_window(Timeout, T1),
                        case gun:await_body(Conn, StreamRef, Timeout1) of
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
                end
            after
                counters:sub(Cnt, ?C_ACTIVE_REQUESTS, 1),
                DurationMs = erlang:monotonic_time(millisecond) - T0,
                rabbitmq_stream_s3_request_metrics:observe(method_to_kind(Method), DurationMs)
            end;
        {error, _} = Err ->
            Err
    end.

method_to_kind(<<"GET">>) ->
    read;
method_to_kind(_) ->
    write.

postprocess_response(#{status := 503}) ->
    counters:add(counter(), ?C_RESPONSE_503, 1);
postprocess_response(#{status := 500}) ->
    counters:add(counter(), ?C_RESPONSE_500, 1);
postprocess_response(_) ->
    ok.

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
            _ ->
                hex(sha256hash(Body))
        end,
    DefaultHeaders0 = #{
        %% TODO: support chunked transfer.
        <<"content-length">> => integer_to_binary(iolist_size(Body)),
        <<"host">> => Host,
        <<"x-amz-date">> => RequestTimestamp,
        <<"x-amz-content-sha256">> => PayloadHash
    },
    DefaultHeaders =
        case SecurityToken of
            undefined ->
                DefaultHeaders0;
            _ ->
                DefaultHeaders0#{<<"x-amz-security-token">> => SecurityToken}
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

counter() ->
    persistent_term:get(?COUNTER_KEY).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

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
