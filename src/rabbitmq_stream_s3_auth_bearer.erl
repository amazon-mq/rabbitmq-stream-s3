%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_auth_bearer).
-moduledoc """
OAuth2 bearer token authorization, for Google Cloud Storage and Azure Blob.

Both clouds return an access token from a link-local metadata server. Both then
take it as an `Authorization: Bearer` header. One backend therefore covers both.
The provider selects the token request shape and the expiry encoding.

Amazon S3 has no bearer mode, so SigV4 stays in its own backend.

SigV4 computes a header per request. This scheme does not: one token authorizes
every request until it expires. This process refreshes the token before it
expires and caches it in an ETS table it owns.

The token is the only header this backend adds. It does not address the request.
The client sets `host` before the request reaches here.
""".

-include_lib("kernel/include/logger.hrl").

-behaviour(gen_server).
-behaviour(rabbitmq_stream_s3_auth).

-export([start_link/0, authorize/2, get_token/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, format_status/1]).

-define(TABLE, ?MODULE).
%% Subtracting a margin from the advertised TTL reduces the chance of using a
%% token just as it expires.
-define(TTL_SECONDS_BUFFER, 30).
%% The metadata server both clouds expose on the link-local address. Google also
%% answers on metadata.google.internal, but the literal needs no DNS.
-define(METADATA_HOST, "169.254.169.254").
-define(METADATA_PORT, 80).
-define(GCP_TOKEN_PATH,
    <<"/computeMetadata/v1/instance/service-accounts/default/token">>
).
-define(AZURE_TOKEN_PATH, <<"/metadata/identity/oauth2/token">>).
-define(AZURE_API_VERSION, <<"2018-02-01">>).

-record(state, {refresh_timer :: reference() | undefined}).

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    _ = ets:new(?TABLE, [protected, named_table, {read_concurrency, true}]),
    {ok, #state{refresh_timer = undefined}}.

-spec authorize(rabbitmq_stream_s3_auth:request(), rabbitmq_stream_s3_auth:req_headers()) ->
    {ok, rabbitmq_stream_s3_auth:req_headers()} | {error, term()}.
authorize(_Request, Headers) ->
    case get_token() of
        {ok, Token} ->
            {ok, Headers#{<<"authorization">> => <<"Bearer ", Token/binary>>}};
        {error, _} = Err ->
            Err
    end.

-spec get_token() -> {ok, binary()} | {error, term()}.
get_token() ->
    case cached_token() of
        {ok, _} = Ok -> Ok;
        error -> safe_call(refresh_token, 15_000)
    end.

cached_token() ->
    case ets:lookup(?TABLE, token) of
        [{token, Token, Expiration}] ->
            case is_expired(Expiration) of
                true -> error;
                false -> {ok, Token}
            end;
        [] ->
            error
    end.

is_expired(Expiration) ->
    now_seconds() + ?TTL_SECONDS_BUFFER > Expiration.

now_seconds() ->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()).

%% Callers are pool workers and osiris readers, where the exit gen_server:call/3
%% raises on timeout or a dead server is a crash rather than a handleable error.
%% See rabbitmq_stream_s3_auth_aws:safe_call/2.
safe_call(Request, Timeout) ->
    try
        gen_server:call(?MODULE, Request, Timeout)
    catch
        exit:{Reason, {gen_server, call, _}} ->
            {error, {auth_server, Reason}}
    end.

handle_call(refresh_token, _From, State0) ->
    %% Re-check the cache: concurrent callers queue behind one refresh, and all
    %% but the first would otherwise fetch a token that has just arrived.
    case cached_token() of
        {ok, _} = Ok ->
            {reply, Ok, State0};
        error ->
            {Result, State} = do_refresh_token(State0),
            {reply, Result, State}
    end;
handle_call(_Msg, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(refresh_token, State0) ->
    {_, State} = do_refresh_token(State0),
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

format_status(#{state := #state{refresh_timer = TRef}} = Status) ->
    %% Never render the token itself: format_status/1 output reaches crash
    %% reports and observer.
    Status#{state => #{refresh_timer => TRef}}.

do_refresh_token(State0) ->
    Provider = rabbitmq_stream_s3_config:bearer_provider(),
    ?LOG_INFO(?MODULE_STRING ": refreshing ~ts access token", [Provider]),
    {Msec, Result} = timer:tc(fun() -> request_token(Provider) end, millisecond),
    case Result of
        {ok, Token, Expiration} ->
            _ = ets:insert(?TABLE, {token, Token, Expiration}),
            ?LOG_INFO(?MODULE_STRING ": refreshed ~ts access token in ~bms", [Provider, Msec]),
            {{ok, Token}, schedule_refresh(Expiration, State0)};
        {error, Reason} = Err ->
            ?LOG_WARNING(?MODULE_STRING ": could not refresh ~ts access token after ~bms: ~0p", [
                Provider, Msec, Reason
            ]),
            {Err, schedule_retry(State0)}
    end.

%% Refresh at the halfway point of the token's life so a transient metadata
%% failure has room to retry before anything expires.
schedule_refresh(Expiration, #state{refresh_timer = Old} = State) ->
    ok = cancel_timer(Old),
    Remaining = max(Expiration - now_seconds(), 0),
    After = max(Remaining div 2, 1) * 1_000,
    State#state{refresh_timer = erlang:send_after(After, self(), refresh_token)}.

schedule_retry(#state{refresh_timer = Old} = State) ->
    ok = cancel_timer(Old),
    State#state{refresh_timer = erlang:send_after(5_000, self(), refresh_token)}.

cancel_timer(undefined) ->
    ok;
cancel_timer(TRef) ->
    _ = erlang:cancel_timer(TRef),
    ok.

request_token(gcp) ->
    with_metadata_conn(fun(Conn) ->
        request_token(Conn, ?GCP_TOKEN_PATH, #{<<"metadata-flavor">> => <<"Google">>})
    end);
request_token(azure) ->
    Query0 = [
        {<<"api-version">>, ?AZURE_API_VERSION},
        {<<"resource">>, rabbitmq_stream_s3_config:bearer_resource()}
    ],
    Query =
        case rabbitmq_stream_s3_config:bearer_client_id() of
            undefined -> Query0;
            ClientId -> [{<<"client_id">>, ClientId} | Query0]
        end,
    Path = <<?AZURE_TOKEN_PATH/binary, $?, (uri_string:compose_query(Query))/binary>>,
    with_metadata_conn(fun(Conn) ->
        request_token(Conn, Path, #{<<"metadata">> => <<"true">>})
    end).

request_token(Conn, Path, Headers) ->
    StreamRef = gun:get(Conn, Path, Headers),
    case gun:await(Conn, StreamRef, 13_000) of
        {response, nofin, 200, _} ->
            case gun:await_body(Conn, StreamRef, 6_000) of
                {ok, Body} -> decode_token(Body);
                {error, _} = Err -> Err
            end;
        {response, _, Status, _} ->
            {error, {unexpected_status, Status}};
        {error, _} = Err ->
            Err
    end.

%% GCP returns expires_in as a number, Azure as a decimal string. Both mean
%% seconds from now.
decode_token(Body) ->
    try json:decode(Body) of
        #{<<"access_token">> := Token, <<"expires_in">> := ExpiresIn} ->
            {ok, Token, now_seconds() + to_integer(ExpiresIn)};
        _ ->
            {error, malformed_token_response}
    catch
        _:_ ->
            {error, malformed_token_response}
    end.

to_integer(N) when is_integer(N) -> N;
to_integer(B) when is_binary(B) -> binary_to_integer(B).

with_metadata_conn(Fun) when is_function(Fun, 1) ->
    case gun:open(?METADATA_HOST, ?METADATA_PORT, #{transport => tcp, protocols => [http]}) of
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

decode_token_test() ->
    Now = now_seconds(),
    %% GCP: expires_in is a number.
    {ok, <<"gcp-token">>, GcpExp} = decode_token(
        <<"{\"access_token\":\"gcp-token\",\"expires_in\":3599,\"token_type\":\"Bearer\"}">>
    ),
    ?assert(GcpExp >= Now + 3599),
    %% Azure: expires_in is a decimal string.
    {ok, <<"azure-token">>, AzureExp} = decode_token(
        <<"{\"access_token\":\"azure-token\",\"expires_in\":\"3599\",\"token_type\":\"Bearer\"}">>
    ),
    ?assert(AzureExp >= Now + 3599),
    ?assertEqual({error, malformed_token_response}, decode_token(<<"{\"error\":\"nope\"}">>)),
    ?assertEqual({error, malformed_token_response}, decode_token(<<"not json">>)).

authorize_adds_bearer_header_test() ->
    OwnedTable = ets:info(?TABLE, name) =:= undefined,
    case OwnedTable of
        true -> _ = ets:new(?TABLE, [protected, named_table, {read_concurrency, true}]);
        false -> ok
    end,
    true = ets:insert(?TABLE, {token, <<"tok">>, now_seconds() + 3600}),
    try
        Req = #{method => <<"GET">>, path => <<"/k">>, body => <<>>, opts => #{}},
        ?assertEqual(
            {ok, #{<<"authorization">> => <<"Bearer tok">>}},
            authorize(Req, #{})
        ),
        %% This backend adds a header. It never replaces one.
        ?assertEqual(
            {ok, #{
                <<"authorization">> => <<"Bearer tok">>,
                <<"range">> => <<"bytes=0-9">>
            }},
            authorize(Req, #{<<"range">> => <<"bytes=0-9">>})
        )
    after
        case OwnedTable of
            true -> ets:delete(?TABLE);
            false -> ets:delete(?TABLE, token)
        end
    end.

expired_token_is_not_served_test() ->
    OwnedTable = ets:info(?TABLE, name) =:= undefined,
    case OwnedTable of
        true -> _ = ets:new(?TABLE, [protected, named_table, {read_concurrency, true}]);
        false -> ok
    end,
    try
        %% Inside the buffer window counts as expired, so a token is never used
        %% in the seconds before it lapses.
        true = ets:insert(?TABLE, {token, <<"tok">>, now_seconds() + 1}),
        ?assertEqual(error, cached_token()),
        true = ets:insert(?TABLE, {token, <<"tok">>, now_seconds() + 3600}),
        ?assertEqual({ok, <<"tok">>}, cached_token())
    after
        case OwnedTable of
            true -> ets:delete(?TABLE);
            false -> ets:delete(?TABLE, token)
        end
    end.

-endif.
