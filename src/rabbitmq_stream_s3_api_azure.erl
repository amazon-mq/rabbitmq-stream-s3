%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_api_azure).
-moduledoc """
A client for the Azure Blob Storage REST API.

Azure Blob is not S3-compatible. This is a second implementation of
`rabbitmq_stream_s3_api`, not an addressing change on the first.

It shares `rabbitmq_stream_s3_http` with `rabbitmq_stream_s3_api_aws`. The
connection pool, the async read code and the error atoms the retry classifier
matches are transport, and are identical here.

The protocol differs:

- Every request declares an `x-ms-version`. A request without one gets a 400.
- A container is a path segment under the account's host, not a subdomain. A
  connection therefore opens to the account, not to the container.
- A write succeeds with `201 Created`. A delete succeeds with `202 Accepted`.
- A listing pages with `marker` and `NextMarker` over `EnumerationResults`, not
  with a continuation token over `ListBucketResult`.
- Azure has no multi-object delete. `delete/2` over a list sends one request per
  key. The behaviour permits this, and the reaper pays for it.
- Azure has no suffix range. `get_range/3` and `get_range_async/3` answer the
  `bytes=-N` that `range_spec()` allows with `{error, not_supported}`. See
  `unsupported_range/1`.

## Uploads

This uploads a fragment as blocks: one `Put Block` per block, then one
`Put Block List` that commits them. It does not send one framed PUT.

Azure accepts a single streaming PUT of a known content length, which is one
request per 64 MiB fragment instead of nine. Blocks win anyway, on integrity.
Azure has no trailing checksum, so only TLS covers a single PUT. A block carries
a `content-md5` over exactly the bytes it sends.

The fragment's CRC32 arrives too late for a header. The commit records it as
`x-ms-meta-crc32`, so the value the writer computed stays with the object.

Uncommitted blocks need no explicit abort. Azure discards them one week after
the last write to the blob, and no reader sees them until `Put Block List`
commits.
""".

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("xmerl/include/xmerl.hrl").

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

-ifdef(TEST).
%% For the integration suite, which pages a listing directly to get hold of a
%% marker: list/3 does not expose maxresults.
-export([decode_enumeration_results/1]).
-endif.

-behaviour(rabbitmq_stream_s3_api).

%% Azure caps a block at 4000 MiB and a blob at 50,000 blocks, so neither bounds
%% the choice here. 8 MiB matches the S3 client's part size and bounds what an
%% uploader holds while a block is hashed and sent.
-define(BLOCK_BYTES, 8388608).
-define(DEFAULT_HOST_SUFFIX, <<"blob.core.windows.net">>).

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
-type async_state() :: rabbitmq_stream_s3_http:async_state().
-type async_req() :: rabbitmq_stream_s3_http:async_req().
%% An upload in progress: blocks already committed to the service but not yet
%% listed, plus whatever has not filled a block. `failed` latches the first
%% error, because stream_data/2 returns a state and has no channel to report on.
-type upload_state() :: #{
    path := binary(),
    opts := request_opts(),
    block_number := pos_integer(),
    %% Newest first. put_block_list/2 reverses it.
    blocks := [binary()],
    data := iodata(),
    pending_bytes := non_neg_integer(),
    failed => term()
}.

-doc """
Install the transport's counters and start the auth backend this one needs.

The client holds no state of its own, so what ends up supervised is whatever the
auth backend needs: nothing for Shared Key, a token refresher for a managed
identity.
""".
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    ok = rabbitmq_stream_s3_http:init_counters(),
    rabbitmq_stream_s3_auth:start_link().

%%---------------------------------------------------------------------------
%% Reads
%%---------------------------------------------------------------------------

-doc "Gets the body of the blob at `Key`.".
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
Gets the given range of bytes of the blob at `Key`.

A suffix range is answered with `{error, not_supported}`; see
`unsupported_range/1`.
""".
-spec get_range(key(), rabbitmq_stream_s3_api:range_spec(), request_opts()) ->
    {ok, binary()} | {error, any()}.
get_range(Key, Range, Opts) when is_binary(Key) andalso is_map(Opts) ->
    case unsupported_range(Range) of
        {error, _} = Err -> Err;
        ok -> do_get_range(Key, Range, Opts)
    end.

do_get_range(Key, Range, Opts) ->
    Headers = #{<<"range">> => rabbitmq_stream_s3_http:range_specifier(Range)},
    case request(<<"GET">>, key_to_path(Key), Headers, <<>>, Opts) of
        {ok, #{status := 206, body := Data}} ->
            {ok, Data};
        {ok, #{status := 200, body := Data}} ->
            %% A Range request must be answered with 206 Partial Content. An
            %% intermediary or a non-conformant store can ignore the Range
            %% header and answer 200 with the full blob. Returning that whole
            %% body as the requested range would feed the caller bytes from the
            %% wrong offset, so slice the range out of the full blob here.
            ?LOG_DEBUG(
                "~ts received 200 for a Range request, slicing ~p from the full blob for key ~ts",
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

-doc """
Starts a ranged read whose response is delivered as messages.

A suffix range is answered with `{error, not_supported}`; see
`unsupported_range/1`.
""".
-spec get_range_async(key(), rabbitmq_stream_s3_api:range_spec(), request_opts()) ->
    {ok, async_req(), async_state()} | {error, any()}.
get_range_async(Key, Range, Opts) when is_binary(Key) andalso is_map(Opts) ->
    case unsupported_range(Range) of
        {error, _} = Err ->
            Err;
        ok ->
            Headers = #{<<"range">> => rabbitmq_stream_s3_http:range_specifier(Range)},
            case request_async(<<"GET">>, key_to_path(Key), Headers, <<>>, Opts) of
                {ok, StreamRef, State} ->
                    %% Keep the requested range so handle_async/3 can recover if
                    %% the store ignores the Range header and answers 200 (full
                    %% blob).
                    {ok, StreamRef, State#{range => Range}};
                {error, _} = Err ->
                    Err
            end
    end.

-doc """
Reject a range Azure cannot express.

Azure's Range grammar is `bytes=start-end` and `bytes=start-` only. There is no
suffix form, so the `bytes=-N` that `range_spec()` allows - and that S3 and the
filesystem backend both serve - has no equivalent here.

It could be emulated by resolving the blob's length with a Get Blob Properties
request and subtracting, but that spends a round trip hiding the fact that the
store does not do this, and no caller has ever asked for it: the manifest
records where a fragment's index starts, so every read the plugin makes is an
absolute range. A caller that does want to read from the end is better told so
than quietly charged for it.
""".
-spec unsupported_range(rabbitmq_stream_s3_api:range_spec()) -> ok | {error, not_supported}.
unsupported_range(SuffixLen) when is_integer(SuffixLen) andalso SuffixLen < 0 ->
    {error, not_supported};
unsupported_range(_Range) ->
    ok.

%%---------------------------------------------------------------------------
%% Writes
%%---------------------------------------------------------------------------

-doc "Uploads `Data` as the blob at `Key`.".
-spec put(key(), iodata(), request_opts()) -> ok | {error, any()}.
put(Key, Data, Opts) when is_binary(Key) andalso is_map(Opts) ->
    %% The whole body is in hand, so it is covered end to end: Azure rejects a
    %% PUT whose bytes do not hash to the content-md5 it was given.
    Headers0 = #{
        <<"x-ms-blob-type">> => <<"BlockBlob">>,
        <<"content-md5">> => content_md5(Data)
    },
    Headers =
        case Opts of
            #{crc32 := Checksum} -> Headers0#{<<"x-ms-meta-crc32">> => crc32_value(Checksum)};
            _ -> Headers0
        end,
    case request(<<"PUT">>, key_to_path(Key), Headers, Data, Opts) of
        {ok, #{status := 201}} ->
            ok;
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other, Key),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-doc """
Begin a block upload.

Unlike an S3 multipart upload there is nothing to initiate: a block is addressed
by the blob it will belong to, so the first request this makes is the first
block. `ContentLength` is therefore unused - it is the caller's promise about
what it will stream, and Azure needs it per block rather than up front.
""".
-spec stream_put(key(), pos_integer(), request_opts()) -> {ok, upload_state()} | {error, any()}.
stream_put(Key, _ContentLength, Opts) when is_binary(Key) andalso is_map(Opts) ->
    {ok, #{
        path => key_to_path(Key),
        opts => Opts,
        block_number => 1,
        blocks => [],
        data => [],
        pending_bytes => 0
    }}.

-spec stream_data(upload_state(), iodata()) -> upload_state().
stream_data(#{data := PendingData0, pending_bytes := PendingBytes0} = State0, Data) ->
    State = State0#{
        data := [PendingData0, Data],
        pending_bytes := PendingBytes0 + iolist_size(Data)
    },
    flush_blocks(State).

%% stream_data/2 returns a state, not a result, so a failed block is latched
%% here and reported by stream_finish/2.
flush_blocks(#{failed := _} = State) ->
    State#{data := [], pending_bytes := 0};
flush_blocks(#{pending_bytes := PendingBytes, data := PendingData} = State0) when
    PendingBytes >= ?BLOCK_BYTES
->
    <<Block:?BLOCK_BYTES/binary, Rest/binary>> = iolist_to_binary(PendingData),
    case put_block(State0, Block) of
        {ok, State} ->
            flush_blocks(State#{data := [Rest], pending_bytes := byte_size(Rest)});
        {error, Reason} ->
            State0#{failed => Reason, data := [], pending_bytes := 0}
    end;
flush_blocks(State) ->
    State.

-spec stream_finish(upload_state(), non_neg_integer()) -> ok | {error, any()}.
stream_finish(#{failed := Reason}, _Crc32) ->
    {error, Reason};
stream_finish(#{data := PendingData} = State0, Crc32) ->
    %% Unconditional: a blob needs at least one block, so an empty fragment
    %% still sends one. Azure has no minimum block size.
    case put_block(State0, iolist_to_binary(PendingData)) of
        {ok, State} -> put_block_list(State, Crc32);
        {error, Reason} -> {error, Reason}
    end.

-doc """
Abandon an upload whose blocks will never be committed.

Nothing to do: uncommitted blocks are invisible to readers and Azure discards
them a week after the last write to the blob, so there is no equivalent of S3's
`AbortMultipartUpload` to call and nothing billable left behind for GC.
""".
-spec stream_abort(upload_state()) -> ok.
stream_abort(_State) ->
    ok.

put_block(
    #{path := Path, block_number := BlockNumber, blocks := Blocks, opts := Opts} = State, Block
) ->
    BlockId = block_id(BlockNumber),
    Query = <<"?comp=block&blockid=", (uri_string:quote(BlockId))/binary>>,
    Headers = #{<<"content-md5">> => content_md5(Block)},
    case request(<<"PUT">>, <<Path/binary, Query/binary>>, Headers, Block, Opts) of
        {ok, #{status := 201}} ->
            {ok, State#{block_number := BlockNumber + 1, blocks := [BlockId | Blocks]}};
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

put_block_list(#{path := Path, blocks := Blocks, opts := Opts}, Crc32) ->
    Body = block_list_body(lists:reverse(Blocks)),
    Headers = #{
        <<"content-md5">> => content_md5(Body),
        %% The fragment's checksum has no place on the wire - Azure takes one
        %% only as a request header, and this is computed while streaming - so
        %% it is recorded on the blob instead.
        <<"x-ms-meta-crc32">> => crc32_value(Crc32)
    },
    case request(<<"PUT">>, <<Path/binary, "?comp=blocklist">>, Headers, Body, Opts) of
        {ok, #{status := 201}} ->
            ok;
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

%% Every block id of a blob must decode from base64 and be the same length, so
%% the number is fixed-width before it is encoded.
-spec block_id(pos_integer()) -> binary().
block_id(BlockNumber) ->
    base64:encode(<<BlockNumber:64/unsigned>>).

-spec block_list_body([binary()]) -> binary().
block_list_body(BlockIds) ->
    Content = [
        #xmlElement{name = 'Latest', content = [#xmlText{value = binary_to_list(Id)}]}
     || Id <- BlockIds
    ],
    Doc = #xmlElement{name = 'BlockList', content = Content},
    iolist_to_binary(xmerl:export_simple([Doc], xmerl_xml, [])).

content_md5(Data) ->
    base64:encode(crypto:hash(md5, Data)).

crc32_value(Crc32) ->
    integer_to_binary(Crc32).

%%---------------------------------------------------------------------------
%% Housekeeping
%%---------------------------------------------------------------------------

-doc """
Deletes the given key, or every key in the given list.

Azure has no multi-object delete, so a list costs one request per key. The
behaviour documents multi-delete as non-atomic and best-effort, which is what
this is: every key is attempted and the failures are reported together.
""".
-spec delete(key() | [key()], request_opts()) -> ok | {error, any()}.
delete([], _Opts) ->
    ok;
delete(Keys, Opts) when is_list(Keys) andalso is_map(Opts) ->
    Errors = lists:foldl(
        fun(Key, Acc) ->
            case delete(Key, Opts) of
                ok -> Acc;
                {error, Reason} -> [{Key, Reason} | Acc]
            end
        end,
        [],
        Keys
    ),
    case Errors of
        [] -> ok;
        _ -> {error, {delete_errors, lists:reverse(Errors)}}
    end;
delete(Key, Opts) when is_binary(Key) andalso is_map(Opts) ->
    case request(<<"DELETE">>, key_to_path(Key), #{}, <<>>, Opts) of
        {ok, #{status := 202}} ->
            ok;
        {ok, #{status := 404}} ->
            %% Already gone. S3 answers 204 whether or not the object existed,
            %% and every caller here deletes to reach that state rather than to
            %% learn what was there, so report the state, not the difference.
            ok;
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other, Key),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-doc """
Lists one page of blobs under `Prefix`.

Pass the returned continuation back to get the next page. Azure pages with an
opaque marker, which is `NextMarker` in the response and `marker` in the
request; an empty `NextMarker` means the listing is complete.
""".
-spec list(key(), rabbitmq_stream_s3_api:list_continuation(), request_opts()) ->
    {ok, [key()], rabbitmq_stream_s3_api:list_continuation()} | {error, any()}.
list(Prefix, Continuation, Opts) ->
    Params0 = [
        {<<"restype">>, <<"container">>},
        {<<"comp">>, <<"list">>},
        {<<"prefix">>, Prefix}
    ],
    Params1 =
        case Continuation of
            start -> Params0;
            Marker -> [{<<"marker">>, Marker} | Params0]
        end,
    Params = rabbitmq_stream_s3_http:compose_query(lists:keysort(1, Params1)),
    case request(<<"GET">>, <<(container_path())/binary, "?", Params/binary>>, #{}, <<>>, Opts) of
        {ok, #{status := 200, body := Body}} ->
            {Keys, NextMarker} = decode_enumeration_results(Body),
            Next =
                case NextMarker of
                    <<>> -> done;
                    _ -> NextMarker
                end,
            {ok, Keys, Next};
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-doc """
Probe the container with a Get Container Properties request.

200 when the container exists and the credentials may reach it, 404 when it does
not exist, 403 when access is denied. Anything else is returned verbatim so the
caller can tell a transient failure from a definitive misconfiguration.
""".
-spec check_bucket(request_opts()) ->
    ok | {error, no_such_bucket | access_denied | term()}.
check_bucket(Opts) when is_map(Opts) ->
    Path = <<(container_path())/binary, "?restype=container">>,
    case request(<<"HEAD">>, Path, #{}, <<>>, Opts) of
        {ok, #{status := 200}} ->
            ok;
        {ok, #{status := 404}} ->
            {error, no_such_bucket};
        {ok, #{status := 403}} ->
            {error, access_denied};
        {ok, #{status := _} = Other} ->
            log_unexpected_status(?FUNCTION_NAME, Other),
            {error, Other};
        {error, _} = Err ->
            Err
    end.

-doc """
Extract the blob names and the next marker from a List Blobs response.

An absent or empty `NextMarker` is the end of the listing.
""".
-spec decode_enumeration_results(binary()) -> {[key()], binary()}.
decode_enumeration_results(Data) ->
    {#xmlElement{name = 'EnumerationResults', content = Result}, []} = xmerl_scan:string(
        binary_to_list(Data), [{allow_entities, false}]
    ),
    lists:foldl(
        fun
            (#xmlElement{name = 'NextMarker', content = Content}, {Keys, _Marker}) ->
                {Keys, text(Content)};
            (#xmlElement{name = 'Blobs', content = Blobs}, {Keys0, Marker}) ->
                Keys = [
                    child_text('Name', BlobContent)
                 || #xmlElement{name = 'Blob', content = BlobContent} <- Blobs
                ],
                {Keys0 ++ Keys, Marker};
            (#xmlElement{}, Acc) ->
                Acc
        end,
        {[], <<>>},
        Result
    ).

-spec child_text(atom(), [term()]) -> binary().
child_text(Name, Content) ->
    case lists:keyfind(Name, #xmlElement.name, Content) of
        #xmlElement{content = ChildContent} -> text(ChildContent);
        false -> <<>>
    end.

text([#xmlText{value = Value}]) -> list_to_binary(Value);
text(_) -> <<>>.

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
                "~ts: Azure Blob returned transient HTTP status ~b~ts; will retry",
                [Function, Status, format_key_suffix(Key)]
            );
        false ->
            ?LOG_WARNING(
                "~ts: Azure Blob returned unexpected HTTP status ~b~ts "
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

%%---------------------------------------------------------------------------
%% Async reads
%%---------------------------------------------------------------------------

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

%%---------------------------------------------------------------------------
%% Requests
%%---------------------------------------------------------------------------

-spec request(http_method(), binary(), req_headers(), iodata(), request_opts()) ->
    {ok, http_response()} | {error, any()}.
request(Method, Path, Headers0, Body, Opts) ->
    Headers = Headers0#{<<"x-ms-version">> => rabbitmq_stream_s3_config:azure_api_version()},
    Req = #{method => Method, path => Path, body => Body, opts => Opts},
    case authorize_request(Req, Headers) of
        {ok, SignedHeaders} ->
            rabbitmq_stream_s3_http:request(Method, Path, SignedHeaders, Body, Opts);
        {error, _} = Err ->
            Err
    end.

request_async(Method, Path, Headers0, Body, Opts) ->
    Headers = Headers0#{<<"x-ms-version">> => rabbitmq_stream_s3_config:azure_api_version()},
    Req = #{method => Method, path => Path, body => Body, opts => Opts},
    case authorize_request(Req, Headers) of
        {ok, SignedHeaders} ->
            rabbitmq_stream_s3_http:request_async(Method, Path, SignedHeaders, Body, Opts);
        {error, _} = Err ->
            Err
    end.

%% Where a request goes is this client's decision, not the auth backend's, so
%% `host` is set here and the backend signs what it is handed.
-spec authorize_request(rabbitmq_stream_s3_auth:request(), req_headers()) ->
    {ok, req_headers()} | {error, any()}.
authorize_request(Req, Headers) ->
    case endpoint() of
        {ok, Host} ->
            rabbitmq_stream_s3_auth:authorize(Req, Headers#{<<"host">> => Host});
        {error, _} = Err ->
            Err
    end.

%%---------------------------------------------------------------------------
%% Addressing
%%---------------------------------------------------------------------------

-doc """
The host a request goes to. The connection pool also connects to it.

S3 puts the bucket in the host. Azure does not: a container is a path segment,
so this is the account's host and nothing is appended to it. The request host
and the endpoint are therefore the same value, and this module needs no
`request_host/0`.
""".
-spec endpoint() -> {ok, binary()} | {error, any()}.
endpoint() ->
    case rabbitmq_stream_s3_config:azure_account() of
        undefined -> {error, no_azure_account};
        Account -> {ok, <<Account/binary, $., (configured_endpoint())/binary>>}
    end.

configured_endpoint() ->
    case rabbitmq_stream_s3_config:endpoint() of
        undefined -> ?DEFAULT_HOST_SUFFIX;
        Endpoint -> Endpoint
    end.

%% `/<container>`. The account is in the host, not the path, so every request
%% path starts here.
-spec container_path() -> binary().
container_path() ->
    <<$/, (rabbitmq_stream_s3_config:bucket())/binary>>.

-spec key_to_path(key()) -> binary().
key_to_path(Key) ->
    <<(container_path())/binary, $/, (uri_string:quote(Key, "/"))/binary>>.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

with_config(Envs, Fun) ->
    [application:set_env(rabbitmq_stream_s3, K, V) || {K, V} <- Envs],
    try
        Fun()
    after
        [application:unset_env(rabbitmq_stream_s3, K) || {K, _} <- Envs]
    end.

endpoint_is_the_account_host_test() ->
    with_config([{azure_account, <<"acct">>}], fun() ->
        ?assertEqual({ok, <<"acct.blob.core.windows.net">>}, endpoint())
    end),
    %% A configured endpoint replaces the suffix, not the account.
    with_config(
        [{azure_account, <<"acct">>}, {endpoint, <<"blob.core.chinacloudapi.cn">>}], fun() ->
            ?assertEqual({ok, <<"acct.blob.core.chinacloudapi.cn">>}, endpoint())
        end
    ),
    %% Without an account there is no host to derive, and saying so beats
    %% connecting to "blob.core.windows.net" and reporting an opaque failure.
    ?assertEqual({error, no_azure_account}, endpoint()).

key_to_path_test() ->
    with_config([{bucket, <<"streams">>}, {azure_account, <<"acct">>}], fun() ->
        ?assertEqual(<<"/streams/a/b.segment">>, key_to_path(<<"a/b.segment">>)),
        %% Separators survive quoting. Everything else that needs it is quoted.
        ?assertEqual(<<"/streams/a%20b/c">>, key_to_path(<<"a b/c">>))
    end).

block_ids_are_equal_length_test() ->
    %% Azure rejects a block list whose ids are not all the same length, which a
    %% decimal counter would produce at the 10th block.
    Ids = [block_id(N) || N <- [1, 9, 10, 1000, 99999]],
    ?assertEqual(1, length(lists:usort([byte_size(Id) || Id <- Ids]))),
    ?assertEqual(5, length(lists:usort(Ids))).

block_list_body_test() ->
    Body = block_list_body([<<"AAAAAAAAAAE=">>, <<"AAAAAAAAAAI=">>]),
    ?assertMatch({_, _}, binary:match(Body, <<"<BlockList>">>)),
    ?assertMatch({_, _}, binary:match(Body, <<"<Latest>AAAAAAAAAAE=</Latest>">>)),
    ?assertMatch({_, _}, binary:match(Body, <<"<Latest>AAAAAAAAAAI=</Latest>">>)).

decode_enumeration_results_test() ->
    Body =
        <<
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
            "<EnumerationResults ContainerName=\"streams\">"
            "<Prefix>a/</Prefix><MaxResults>2</MaxResults>"
            "<Blobs>"
            "<Blob><Name>a/one</Name><Properties><Content-Length>5</Content-Length></Properties></Blob>"
            "<Blob><Name>a/two</Name><Properties><Content-Length>7</Content-Length></Properties></Blob>"
            "</Blobs>"
            "<NextMarker>2!68!MDAwMD</NextMarker>"
            "</EnumerationResults>"
        >>,
    ?assertEqual(
        {[<<"a/one">>, <<"a/two">>], <<"2!68!MDAwMD">>},
        decode_enumeration_results(Body)
    ).

%% The last page carries an empty NextMarker, which is what ends the listing.
decode_enumeration_results_last_page_test() ->
    Body =
        <<
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
            "<EnumerationResults ContainerName=\"streams\">"
            "<Blobs><Blob><Name>only</Name></Blob></Blobs>"
            "<NextMarker />"
            "</EnumerationResults>"
        >>,
    ?assertEqual({[<<"only">>], <<>>}, decode_enumeration_results(Body)),
    %% An empty container lists no keys and still ends.
    Empty =
        <<
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
            "<EnumerationResults ContainerName=\"streams\"><Blobs /><NextMarker />"
            "</EnumerationResults>"
        >>,
    ?assertEqual({[], <<>>}, decode_enumeration_results(Empty)).

transient_status_test() ->
    ?assert(transient_status(500)),
    ?assert(transient_status(503)),
    ?assert(transient_status(429)),
    ?assertNot(transient_status(501)),
    ?assertNot(transient_status(400)),
    ?assertNot(transient_status(403)),
    ?assertNot(transient_status(404)).

suffix_ranges_are_refused_test() ->
    ?assertEqual({error, not_supported}, unsupported_range(-1)),
    ?assertEqual({error, not_supported}, unsupported_range(-1024)),
    %% Everything the plugin actually asks for is an absolute range.
    ?assertEqual(ok, unsupported_range({0, undefined})),
    ?assertEqual(ok, unsupported_range({0, 0})),
    ?assertEqual(ok, unsupported_range({12, 40})).

content_md5_test() ->
    ?assertEqual(<<"XUFAKrxLKna5cZ2REBfFkg==">>, content_md5(<<"hello">>)),
    %% iodata hashes as the bytes it flattens to.
    ?assertEqual(content_md5(<<"hello">>), content_md5([<<"he">>, <<"llo">>])).

-endif.
