%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_auth_aws).
-moduledoc """
AWS SigV4 authorization for remote tier requests.

Holds the credential chain: static keys, container credentials and EC2 instance
metadata. Signs each request with AWS4-HMAC-SHA256.

Credentials expire and need refreshing, so this is a gen_server.
`rabbitmq_stream_s3_api_aws` starts it. The supervisor does not list it.

It also resolves the region. SigV4 needs the region for the credential scope,
and the S3 client needs it to derive a regional host.
""".

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, reload_config/0, authorize/2, region/0]).

%% For apply/3:
-export([get_credentials/0]).

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

-record(container_creds_req, {host, port, path, conn, stream_ref}).

-behaviour(gen_server).

-behaviour(rabbitmq_stream_s3_auth).

-record(state, {
    metadata_token :: {Token :: binary(), Expiration :: non_neg_integer()} | undefined,
    refresh_timer :: reference() | undefined,
    source :: static | imds | {container, string()} | undefined
}).

-type http_method() :: binary().
-type http_response() :: #{
    status := pos_integer(),
    headers := [{binary(), binary()}],
    body => binary()
}.
%% Map keys must be lowercase.
-type req_headers() :: #{binary() => binary()}.

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    %% The credential state is what needs an owner: the table dies with this
    %% process and the refresh timer needs somewhere to fire.
    _ = ets:new(?TABLE, [protected, named_table, {read_concurrency, true}]),
    State = do_reload_config(#state{metadata_token = undefined, refresh_timer = undefined}),
    {ok, State}.

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

%% Region is required to build the request host and to sign requests, so a
%% failure here cannot be papered over: callers must surface it. We return a
%% tagged tuple (rather than the bare binary) so the failure propagates as a
%% clean {error, _} through endpoint/0 and sign_headers/8 instead of crashing
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

-spec authorize(rabbitmq_stream_s3_auth:request(), req_headers()) ->
    {ok, req_headers()} | {error, any()}.
authorize(#{method := Method, path := Path, body := Body, opts := Opts}, Headers) ->
    %% TODO: pass timeout through get_credentials/0?
    case get_credentials() of
        {ok, AccessKey, SecretKey, SecurityToken} ->
            sign_headers(Headers, AccessKey, SecretKey, SecurityToken, Method, Path, Body, Opts);
        {error, _} = Err ->
            Err
    end.

sign_headers(Headers, AccessKey, SecretKey, SecurityToken, Method, Path, Body, Opts) ->
    case region() of
        {ok, Region} ->
            {ok,
                sign_headers(
                    calendar:universal_time(),
                    %% The client addressed the request. The signature covers
                    %% the host it chose, not one derived again here.
                    maps:get(<<"host">>, Headers),
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

parse_iso8601_test() ->
    ?assertEqual(
        calendar:datetime_to_gregorian_seconds({{2026, 1, 21}, {1, 47, 0}}),
        parse_iso8601(<<"2026-01-21T01:47:00Z">>)
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

authorize_dispatches_through_auth_behaviour_test() ->
    Region = persistent_term:get(?REGION_KEY, undefined),
    Bucket = application:get_env(rabbitmq_stream_s3, bucket),
    %% init/1 owns the table in a running node; create it here so the cached
    %% credential lookup has something to read.
    OwnedTable = ets:info(?TABLE, name) =:= undefined,
    case OwnedTable of
        true -> _ = ets:new(?TABLE, [protected, named_table, {read_concurrency, true}]);
        false -> ok
    end,
    persistent_term:put(?REGION_KEY, <<"us-east-1">>),
    ok = application:set_env(rabbitmq_stream_s3, bucket, <<"examplebucket">>),
    true = ets:insert(
        ?TABLE,
        {credentials, <<"AKIAIOSFODNN7EXAMPLE">>, <<"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY">>,
            undefined, undefined}
    ),
    try
        Req = #{method => <<"GET">>, path => <<"/test.txt">>, body => <<>>, opts => #{}},
        %% Through the dispatcher rather than this module directly: the
        %% behaviour wiring is what is under test, not the signing.
        {ok, Headers} = rabbitmq_stream_s3_auth:authorize(
            Req, #{<<"host">> => <<"examplebucket.s3.us-east-1.amazonaws.com">>}
        ),
        ?assertEqual(
            <<"examplebucket.s3.us-east-1.amazonaws.com">>, maps:get(<<"host">>, Headers)
        ),
        ?assertMatch(
            {0, _},
            binary:match(
                maps:get(<<"authorization">>, Headers),
                <<"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/">>
            )
        )
    after
        case OwnedTable of
            true -> ets:delete(?TABLE);
            false -> ets:delete(?TABLE, credentials)
        end,
        case Bucket of
            undefined -> application:unset_env(rabbitmq_stream_s3, bucket);
            {ok, B} -> application:set_env(rabbitmq_stream_s3, bucket, B)
        end,
        case Region of
            undefined -> persistent_term:erase(?REGION_KEY);
            _ -> persistent_term:put(?REGION_KEY, Region)
        end
    end.

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

-endif.
