%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_auth_azure).
-moduledoc """
Azure Shared Key authorization.

Signs each request with the storage account's key. Azure Blob accepts this
scheme where no managed identity can supply a token.
`rabbitmq_stream_s3_auth_bearer` is the better choice on an Azure VM. This
backend reaches a storage account from anywhere else.

This is SigV4's smaller relative: an HMAC-SHA256 over a canonical description of
the request. Two differences matter:

- The description is a fixed list of headers by position, not a negotiated
  signed-headers set. A header the string omits is not covered at all.
- The account name belongs to the canonicalized resource, not to a credential
  scope.

The key is a long-lived plaintext secret with full access to the account. It
therefore sits behind the same `stream_s3.allow_static_credentials` opt-in as
static AWS keys, and warns the same way. Nothing here refreshes, so this backend
starts no process.
""".

-include_lib("kernel/include/logger.hrl").

-behaviour(rabbitmq_stream_s3_auth).

-export([start_link/0, authorize/2]).

-type req_headers() :: rabbitmq_stream_s3_auth:req_headers().

-doc """
No process: a Shared Key signature is computed from configuration that does not
expire, so there is nothing to hold or refresh.
""".
-spec start_link() -> ignore.
start_link() ->
    ignore.

-spec authorize(rabbitmq_stream_s3_auth:request(), req_headers()) ->
    {ok, req_headers()} | {error, term()}.
authorize(#{method := Method, path := Path, body := Body}, Headers0) ->
    case credentials() of
        {ok, Account, Key} ->
            Headers = Headers0#{<<"x-ms-date">> => rfc1123_now()},
            StringToSign = string_to_sign(Method, Path, Body, Headers, Account),
            Signature = base64:encode(crypto:mac(hmac, sha256, Key, StringToSign)),
            {ok, Headers#{
                <<"authorization">> =>
                    <<"SharedKey ", Account/binary, $:, Signature/binary>>
            }};
        {error, _} = Err ->
            Err
    end.

%%---------------------------------------------------------------------------
%% Signing
%%---------------------------------------------------------------------------

-doc """
Build the string a Shared Key signature covers.

Exported under TEST because the whole scheme is this function: a signature is
only as correct as the string, and a wrong one is indistinguishable from a wrong
key at the far end.
""".
-spec string_to_sign(binary(), binary(), iodata() | no_body, req_headers(), binary()) -> binary().
string_to_sign(Method, Path, Body, Headers, Account) ->
    %% Fixed positions, in this order. An absent header contributes an empty
    %% line rather than being skipped, so the line count is constant.
    %% <https://learn.microsoft.com/rest/api/storageservices/authorize-with-shared-key>
    Fixed = [
        Method,
        header(<<"content-encoding">>, Headers),
        header(<<"content-language">>, Headers),
        content_length(Body, Headers),
        header(<<"content-md5">>, Headers),
        header(<<"content-type">>, Headers),
        %% Empty because x-ms-date is always set, and it wins over Date.
        <<>>,
        header(<<"if-modified-since">>, Headers),
        header(<<"if-match">>, Headers),
        header(<<"if-none-match">>, Headers),
        header(<<"if-unmodified-since">>, Headers),
        header(<<"range">>, Headers)
    ],
    iolist_to_binary([
        lists:join($\n, Fixed),
        $\n,
        canonicalized_headers(Headers),
        canonicalized_resource(Path, Account)
    ]).

%% A zero-length body signs as an empty line, not as "0". gun rewrites
%% content-length from the body it is given, so for a request carrying one the
%% body is the authority and the header is only consulted for a streamed upload,
%% where the client set it and no body has been seen yet.
content_length(no_body, Headers) ->
    case header(<<"content-length">>, Headers) of
        <<"0">> -> <<>>;
        Value -> Value
    end;
content_length(Body, _Headers) ->
    case iolist_size(Body) of
        0 -> <<>>;
        Size -> integer_to_binary(Size)
    end.

header(Name, Headers) ->
    maps:get(Name, Headers, <<>>).

%% Every x-ms- header, lowercased, sorted by name, one "name:value" line each.
%% Header names in this plugin are lowercase by convention. Lowercasing again
%% costs nothing and removes the dependency on that convention.
canonicalized_headers(Headers) ->
    Lines = [
        [string:lowercase(Name), $:, trim(Value), $\n]
     || {Name, Value} <- lists:keysort(1, maps:to_list(Headers)),
        is_ms_header(Name)
    ],
    iolist_to_binary(Lines).

is_ms_header(<<"x-ms-", _/binary>>) -> true;
is_ms_header(_) -> false.

%% Leading and trailing whitespace is stripped, and internal runs collapse to a
%% single space, because a header's value is signed as the service parses it.
trim(Value) ->
    Collapsed = re:replace(Value, <<"\\s+">>, <<" ">>, [global, {return, binary}]),
    string:trim(Collapsed).

-doc """
`/<account><path>` followed by one sorted, URL-decoded line per query parameter.

`Path` is the request target, so it carries the query string. The account is
prepended to it, which is the service's rule and not a property of how the
request was addressed.
""".
-spec canonicalized_resource(binary(), binary()) -> binary().
canonicalized_resource(Path, Account) ->
    {PathOnly, Query} =
        case binary:split(Path, <<"?">>) of
            [P] -> {P, <<>>};
            [P, Q] -> {P, Q}
        end,
    iolist_to_binary([$/, Account, PathOnly, canonicalized_query(Query)]).

canonicalized_query(<<>>) ->
    [];
canonicalized_query(Query) ->
    %% Values for a repeated parameter are comma-separated in sorted order.
    Params = [split_param(P) || P <- binary:split(Query, <<"&">>, [global]), P =/= <<>>],
    Grouped = lists:foldl(
        fun({Name, Value}, Acc) ->
            maps:update_with(Name, fun(Vs) -> [Value | Vs] end, [Value], Acc)
        end,
        #{},
        Params
    ),
    [
        [$\n, Name, $:, lists:join($,, lists:sort(Values))]
     || {Name, Values} <- lists:keysort(1, maps:to_list(Grouped))
    ].

split_param(Param) ->
    case binary:split(Param, <<"=">>) of
        [Name] -> {string:lowercase(unquote(Name)), <<>>};
        [Name, Value] -> {string:lowercase(unquote(Name)), unquote(Value)}
    end.

%% A bare `+` in a query is a space to Azure, and `uri_string:unquote/1` leaves
%% it alone, so it is turned into one before percent-escapes are resolved. A
%% literal plus reaches here as `%2B` and is unaffected.
unquote(Bin) ->
    Spaced = binary:replace(Bin, <<"+">>, <<" ">>, [global]),
    case uri_string:unquote(Spaced) of
        {error, _, _} -> Spaced;
        Unquoted -> Unquoted
    end.

%% e.g. "Fri, 26 Jun 2015 23:39:12 GMT". Always GMT: the service rejects
%% anything else.
rfc1123_now() ->
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:universal_time(),
    DayOfWeek = calendar:day_of_the_week(Year, Month, Day),
    iolist_to_binary(
        io_lib:format(
            "~s, ~2..0b ~s ~4..0b ~2..0b:~2..0b:~2..0b GMT",
            [weekday(DayOfWeek), Day, month(Month), Year, Hour, Min, Sec]
        )
    ).

weekday(1) -> "Mon";
weekday(2) -> "Tue";
weekday(3) -> "Wed";
weekday(4) -> "Thu";
weekday(5) -> "Fri";
weekday(6) -> "Sat";
weekday(7) -> "Sun".

month(1) -> "Jan";
month(2) -> "Feb";
month(3) -> "Mar";
month(4) -> "Apr";
month(5) -> "May";
month(6) -> "Jun";
month(7) -> "Jul";
month(8) -> "Aug";
month(9) -> "Sep";
month(10) -> "Oct";
month(11) -> "Nov";
month(12) -> "Dec".

%%---------------------------------------------------------------------------
%% Credentials
%%---------------------------------------------------------------------------

-spec credentials() -> {ok, binary(), binary()} | {error, term()}.
credentials() ->
    Account = rabbitmq_stream_s3_config:azure_account(),
    Key = rabbitmq_stream_s3_config:azure_account_key(),
    case {Account, Key} of
        {undefined, _} ->
            {error, no_azure_account};
        {_, undefined} ->
            {error, no_azure_account_key};
        _ ->
            case rabbitmq_stream_s3_config:allow_static_credentials() of
                true ->
                    decode_key(Account, Key);
                false ->
                    ?LOG_WARNING(
                        ?MODULE_STRING
                        ": a storage account key is configured but "
                        "stream_s3.allow_static_credentials is not set to true. Shared Key "
                        "authorization cannot be used without it; set it, or use "
                        "stream_s3.auth = bearer with a managed identity.",
                        []
                    ),
                    {error, static_credentials_not_allowed}
            end
    end.

%% Account keys are base64 and the HMAC is over the raw bytes, so a key that
%% does not decode is a configuration error worth naming rather than a
%% signature that is silently always wrong.
decode_key(Account, Key) ->
    try base64:decode(Key) of
        Decoded -> {ok, Account, Decoded}
    catch
        _:_ -> {error, malformed_azure_account_key}
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% The worked example from Microsoft's Shared Key documentation, which pins the
%% line order, the empty content-length line and the x-ms- header handling.
string_to_sign_test() ->
    Headers = #{
        <<"x-ms-date">> => <<"Fri, 26 Jun 2015 23:39:12 GMT">>,
        <<"x-ms-version">> => <<"2015-02-21">>,
        <<"host">> => <<"myaccount.blob.core.windows.net">>
    },
    ?assertEqual(
        <<
            "GET\n\n\n\n\n\n\n\n\n\n\n\n"
            "x-ms-date:Fri, 26 Jun 2015 23:39:12 GMT\n"
            "x-ms-version:2015-02-21\n"
            "/myaccount/mycontainer\ncomp:list\nrestype:container"
        >>,
        string_to_sign(
            <<"GET">>,
            <<"/mycontainer?restype=container&comp=list">>,
            <<>>,
            Headers,
            <<"myaccount">>
        )
    ).

%% The signature covers the body length. An empty body signs as an empty line,
%% not as "0". That difference decides between a working PUT and a 403.
content_length_test() ->
    ?assertEqual(<<>>, content_length(<<>>, #{})),
    ?assertEqual(<<"5">>, content_length(<<"hello">>, #{})),
    ?assertEqual(<<"5">>, content_length([<<"he">>, <<"llo">>], #{})),
    %% Streamed: no body yet, so the client's header is the only source.
    ?assertEqual(<<"64">>, content_length(no_body, #{<<"content-length">> => <<"64">>})),
    ?assertEqual(<<>>, content_length(no_body, #{<<"content-length">> => <<"0">>})),
    ?assertEqual(<<>>, content_length(no_body, #{})).

canonicalized_resource_test() ->
    ?assertEqual(
        <<"/acct/container/blob">>,
        canonicalized_resource(<<"/container/blob">>, <<"acct">>)
    ),
    %% Parameters sort by name, lowercased, one per line.
    ?assertEqual(
        <<"/acct/c\nblockid:YmxvY2sx\ncomp:block">>,
        canonicalized_resource(<<"/c?comp=block&blockid=YmxvY2sx">>, <<"acct">>)
    ),
    %% Values are URL-decoded before signing.
    ?assertEqual(
        <<"/acct/c\nprefix:a/b c">>,
        canonicalized_resource(<<"/c?prefix=a%2Fb%20c">>, <<"acct">>)
    ),
    %% A bare plus is a space, which is how the service reads it. An escaped one
    %% is a plus. A mistake here is a 403 on any key that contains a space.
    ?assertEqual(
        <<"/acct/c\nprefix:a b+c">>,
        canonicalized_resource(<<"/c?prefix=a+b%2Bc">>, <<"acct">>)
    ),
    %% Repeated parameters collapse to one sorted, comma-separated line.
    ?assertEqual(
        <<"/acct/c\ninclude:metadata,snapshots">>,
        canonicalized_resource(<<"/c?include=snapshots&include=metadata">>, <<"acct">>)
    ).

canonicalized_headers_test() ->
    Headers = #{
        <<"x-ms-version">> => <<"2021-08-06">>,
        <<"x-ms-blob-type">> => <<"BlockBlob">>,
        <<"content-type">> => <<"application/octet-stream">>,
        <<"host">> => <<"acct.blob.core.windows.net">>
    },
    %% Only x-ms- headers, sorted, and nothing else.
    ?assertEqual(
        <<"x-ms-blob-type:BlockBlob\nx-ms-version:2021-08-06\n">>,
        canonicalized_headers(Headers)
    ),
    %% Whitespace in a value collapses the way the service parses it.
    ?assertEqual(
        <<"x-ms-meta-a:b c\n">>,
        canonicalized_headers(#{<<"x-ms-meta-a">> => <<"  b   c  ">>})
    ).

rfc1123_now_is_gmt_test() ->
    Date = rfc1123_now(),
    ?assertMatch({_, _}, binary:match(Date, <<" GMT">>)),
    ?assertEqual(29, byte_size(Date)).

authorize_requires_opt_in_test() ->
    with_config(
        [{azure_account, <<"acct">>}, {azure_account_key, base64:encode(<<"key">>)}],
        fun() ->
            Req = #{method => <<"GET">>, path => <<"/c/b">>, body => <<>>, opts => #{}},
            ?assertEqual({error, static_credentials_not_allowed}, authorize(Req, #{}))
        end
    ).

authorize_adds_shared_key_header_test() ->
    with_config(
        [
            {azure_account, <<"acct">>},
            {azure_account_key, base64:encode(<<"key">>)},
            {allow_static_credentials, true}
        ],
        fun() ->
            Req = #{method => <<"GET">>, path => <<"/c/b">>, body => <<>>, opts => #{}},
            {ok, Headers} = authorize(Req, #{<<"host">> => <<"acct.blob.core.windows.net">>}),
            ?assertMatch(<<"SharedKey acct:", _/binary>>, maps:get(<<"authorization">>, Headers)),
            %% The date the signature covers is sent, or the service computes a
            %% different string.
            ?assert(maps:is_key(<<"x-ms-date">>, Headers)),
            %% Caller headers survive.
            ?assertEqual(<<"acct.blob.core.windows.net">>, maps:get(<<"host">>, Headers))
        end
    ).

malformed_key_is_named_test() ->
    with_config(
        [
            {azure_account, <<"acct">>},
            {azure_account_key, <<"not base64 at all!">>},
            {allow_static_credentials, true}
        ],
        fun() ->
            Req = #{method => <<"GET">>, path => <<"/c/b">>, body => <<>>, opts => #{}},
            ?assertEqual({error, malformed_azure_account_key}, authorize(Req, #{}))
        end
    ).

missing_credentials_are_named_test() ->
    Req = #{method => <<"GET">>, path => <<"/c/b">>, body => <<>>, opts => #{}},
    ?assertEqual({error, no_azure_account}, authorize(Req, #{})),
    with_config([{azure_account, <<"acct">>}], fun() ->
        ?assertEqual({error, no_azure_account_key}, authorize(Req, #{}))
    end).

with_config(Envs, Fun) ->
    [application:set_env(rabbitmq_stream_s3, K, V) || {K, V} <- Envs],
    try
        Fun()
    after
        [application:unset_env(rabbitmq_stream_s3, K) || {K, _} <- Envs]
    end.

-endif.
