%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_api_azure_SUITE).
-moduledoc """
Integration tests for the Azure Blob backend.

Runs against Azurite, Microsoft's Blob emulator, or against a real storage
account. The suite skips itself when neither is reachable, so it is safe to have
in the default run.

## Against Azurite

Azurite speaks the real protocol over plain HTTP with Shared Key
authorization, which is what makes the whole backend testable without an Azure
subscription:

```sh
npm install azurite
npx azurite-blob --blobHost 127.0.0.1 --blobPort 10000 --location /tmp/azurite
```

The suite then finds it on the default port with the well-known development
account, and needs no configuration. Point it elsewhere with `AZURITE_HOST` and
`AZURITE_PORT`.

## Against a real storage account

Set `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY` and `AZURE_STORAGE_CONTAINER`.
The container must already exist: the plugin never creates one, and neither does
this suite outside the emulator. The account key needs no special role; it is
the account's master key.

Requests go to `<account>.blob.core.windows.net` over TLS, so this exercises the
addressing a deployment uses, which the emulator - where the account is a path
segment - does not.
""".

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(M, rabbitmq_stream_s3_api_azure).
%% The account and key Azurite serves by default, published by Microsoft for
%% exactly this purpose. Not a credential.
-define(AZURITE_ACCOUNT, <<"devstoreaccount1">>).
-define(AZURITE_KEY,
    <<"Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==">>
).

all() ->
    [{group, integration}].

groups() ->
    [
        {integration, [], [
            check_bucket,
            check_bucket_missing_container,
            put_and_get,
            get_missing_key,
            get_range,
            get_range_async,
            stream_put,
            stream_put_empty,
            list_and_page,
            delete,
            keys_needing_quoting
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(integration, Config) ->
    case target() of
        {skip, _} = Skip ->
            Skip;
        {ok, Env} ->
            {ok, _} = application:ensure_all_started([gun, seshat]),
            _ = seshat:new_group(rabbitmq_stream_s3),
            [application:set_env(rabbitmq_stream_s3, K, V) || {K, V} <- Env],
            ignore = ?M:start_link(),
            Pools = [
                start_pool(rabbitmq_stream_s3_general_pool),
                start_pool(rabbitmq_stream_s3_upload_pool)
            ],
            ok = ensure_container(),
            [{env, Env}, {pools, Pools} | Config]
    end.

end_per_group(integration, Config) ->
    _ = ?M:delete(all_keys(<<>>), #{}),
    [gen_server:stop(Pool) || Pool <- ?config(pools, Config)],
    [application:unset_env(rabbitmq_stream_s3, K) || {K, _} <- ?config(env, Config)],
    Config;
end_per_group(_, Config) ->
    Config.

%% A real account is preferred when configured, because it is the addressing a
%% deployment actually uses; the emulator is the fallback that makes the suite
%% run anywhere.
target() ->
    case
        {
            os:getenv("AZURE_STORAGE_ACCOUNT"),
            os:getenv("AZURE_STORAGE_KEY"),
            os:getenv("AZURE_STORAGE_CONTAINER")
        }
    of
        {Account, Key, Container} when
            Account =/= false, Key =/= false, Container =/= false
        ->
            {ok, [
                {rabbitmq_stream_s3_api, ?M},
                {rabbitmq_stream_s3_auth, rabbitmq_stream_s3_auth_azure},
                {azure_account, list_to_binary(Account)},
                {azure_account_key, list_to_binary(Key)},
                {allow_static_credentials, true},
                {bucket, list_to_binary(Container)}
            ]};
        _ ->
            azurite_target()
    end.

azurite_target() ->
    Host = list_to_binary(os:getenv("AZURITE_HOST", "127.0.0.1")),
    Port = list_to_integer(os:getenv("AZURITE_PORT", "10000")),
    case is_listening(Host, Port) of
        true ->
            {ok, [
                {rabbitmq_stream_s3_api, ?M},
                {rabbitmq_stream_s3_auth, rabbitmq_stream_s3_auth_azure},
                {azure_account, ?AZURITE_ACCOUNT},
                {azure_account_key, ?AZURITE_KEY},
                {azure_path_style, true},
                {allow_static_credentials, true},
                {endpoint, Host},
                {http_port, Port},
                {http_tls, false},
                {bucket, <<"rabbitmq-stream-s3-ct">>}
            ]};
        false ->
            {skip,
                "Neither a storage account nor Azurite is configured. Skipping this group "
                "is OK! See the moduledoc for how to run it."}
    end.

is_listening(Host, Port) ->
    case gen_tcp:connect(binary_to_list(Host), Port, [{active, false}], 1_000) of
        {ok, Sock} ->
            ok = gen_tcp:close(Sock),
            true;
        {error, _} ->
            false
    end.

%% Unlinked: init_per_group runs in a process Common Test discards, and a linked
%% pool would go down with it before the first test case.
start_pool(Name) ->
    {ok, Pid} = rabbitmq_stream_s3_api_aws_pool:start_link(Name, #{
        name => Name, min_size => 0, max_size => 5
    }),
    true = erlang:unlink(Pid),
    Pid.

%%---------------------------------------------------------------------------
%% Test cases
%%---------------------------------------------------------------------------

check_bucket(_Config) ->
    ?assertEqual(ok, ?M:check_bucket(#{})).

check_bucket_missing_container(_Config) ->
    Container = application:get_env(rabbitmq_stream_s3, bucket, undefined),
    ok = application:set_env(rabbitmq_stream_s3, bucket, <<"nosuchcontainer-8f2a1c">>),
    try
        ?assertEqual({error, no_such_bucket}, ?M:check_bucket(#{}))
    after
        ok = application:set_env(rabbitmq_stream_s3, bucket, Container)
    end.

put_and_get(_Config) ->
    Key = key("put_and_get"),
    ?assertEqual(ok, ?M:put(Key, <<"hello world">>, #{})),
    ?assertEqual({ok, <<"hello world">>}, ?M:get(Key, #{})),
    %% Overwriting is how a manifest is updated, so it must not need a delete.
    ?assertEqual(ok, ?M:put(Key, <<"replaced">>, #{})),
    ?assertEqual({ok, <<"replaced">>}, ?M:get(Key, #{})),
    %% An iolist body and a crc32 both reach the service.
    ?assertEqual(ok, ?M:put(Key, [<<"io">>, <<"data">>], #{crc32 => erlang:crc32(<<"iodata">>)})),
    ?assertEqual({ok, <<"iodata">>}, ?M:get(Key, #{})).

get_missing_key(_Config) ->
    ?assertEqual({error, not_found}, ?M:get(key("no_such_blob"), #{})).

get_range(_Config) ->
    Key = key("get_range"),
    ok = ?M:put(Key, <<"0123456789">>, #{}),
    ?assertEqual({ok, <<"234">>}, ?M:get_range(Key, {2, 4}, #{})),
    ?assertEqual({ok, <<"3456789">>}, ?M:get_range(Key, {3, undefined}, #{})),
    %% An end past the object clamps rather than failing.
    ?assertEqual({ok, <<"89">>}, ?M:get_range(Key, {8, 100}, #{})),
    %% Azure has no suffix range and does not emulate one. Refused before a
    %% request is made, so a missing blob is not distinguished here.
    ?assertEqual({error, not_supported}, ?M:get_range(Key, -2, #{})),
    ?assertEqual({error, not_supported}, ?M:get_range(key("no_such_blob"), -2, #{})),
    ?assertEqual({error, not_supported}, ?M:get_range_async(Key, -2, #{})).

get_range_async(_Config) ->
    Key = key("get_range_async"),
    Body = binary:copy(<<"abcdefgh">>, 512 * 1024),
    ok = ?M:put(Key, Body, #{}),
    ?assertEqual(binary:part(Body, 0, 5), await_async(Key, {0, 4})),
    %% Larger than one buffered batch, so the data arrives in several pieces.
    ?assertEqual(Body, await_async(Key, {0, byte_size(Body) - 1})),
    %% An open-ended range reads to the end, which is how the plugin reads a
    %% fragment's index - the case a suffix range would otherwise be used for.
    ?assertEqual(Body, await_async(Key, {0, undefined})).

stream_put(_Config) ->
    Key = key("stream_put"),
    %% Three blocks plus a remainder, so the block list is exercised rather than
    %% a single-block upload.
    Body = binary:copy(<<"abcdefgh">>, 3 * 1024 * 1024),
    {ok, State0} = ?M:stream_put(Key, byte_size(Body), #{}),
    State = lists:foldl(
        fun(Chunk, S) -> ?M:stream_data(S, Chunk) end,
        State0,
        chunks(Body, 5_000_000)
    ),
    ?assertEqual(ok, ?M:stream_finish(State, erlang:crc32(Body))),
    ?assertEqual({ok, Body}, ?M:get(Key, #{})),
    %% The committed blob is one object, not a list of blocks: a range spanning
    %% a block boundary reads through it.
    ?assertEqual(
        {ok, binary:part(Body, 8388606, 4)},
        ?M:get_range(Key, {8388606, 8388609}, #{})
    ).

stream_put_empty(_Config) ->
    Key = key("stream_put_empty"),
    {ok, State} = ?M:stream_put(Key, 0, #{}),
    ?assertEqual(ok, ?M:stream_finish(State, erlang:crc32(<<>>))),
    ?assertEqual({ok, <<>>}, ?M:get(Key, #{})).

list_and_page(_Config) ->
    Prefix = key("list/"),
    Keys = [<<Prefix/binary, (integer_to_binary(N))/binary>> || N <- lists:seq(1, 7)],
    [ok = ?M:put(K, <<"x">>, #{}) || K <- Keys],
    ?assertEqual({ok, lists:sort(Keys), done}, sorted_list(Prefix, start)),
    %% A prefix with nothing under it lists nothing and still terminates.
    ?assertEqual({ok, [], done}, ?M:list(key("nothing/"), start, #{})),
    %% A marker the service handed out is accepted back, and the pages together
    %% are the whole listing with nothing repeated or dropped. list/3 does not
    %% expose maxresults, so the small page is requested directly.
    {FirstPage, Marker} = small_page(Prefix),
    ?assertEqual(2, length(FirstPage)),
    ?assertNotEqual(<<>>, Marker),
    Rest = page(Prefix, Marker, []),
    ?assertEqual(lists:sort(Keys), lists:sort(FirstPage ++ Rest)).

delete(_Config) ->
    Key = key("delete"),
    ok = ?M:put(Key, <<"x">>, #{}),
    ?assertEqual(ok, ?M:delete(Key, #{})),
    ?assertEqual({error, not_found}, ?M:get(Key, #{})),
    %% Deleting is idempotent: callers delete to reach a state, and Azure
    %% answers 404 where S3 answers 204.
    ?assertEqual(ok, ?M:delete(Key, #{})),
    ?assertEqual(ok, ?M:delete([], #{})),
    Prefix = key("delete_many/"),
    Keys = [<<Prefix/binary, (integer_to_binary(N))/binary>> || N <- lists:seq(1, 5)],
    [ok = ?M:put(K, <<"x">>, #{}) || K <- Keys],
    ?assertEqual(ok, ?M:delete(Keys, #{})),
    ?assertEqual({ok, [], done}, ?M:list(Prefix, start, #{})).

keys_needing_quoting(_Config) ->
    Key = <<(key("quoting"))/binary, "/with spaces+and=signs">>,
    ok = ?M:put(Key, <<"v">>, #{}),
    ?assertEqual({ok, <<"v">>}, ?M:get(Key, #{})),
    %% The name comes back as it was written, not as it was quoted on the wire.
    ?assertEqual({ok, [Key], done}, ?M:list(Key, start, #{})),
    ?assertEqual(ok, ?M:delete(Key, #{})).

%%---------------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------------

%% Namespaced per run so a shared container does not carry state between runs.
key(Name) ->
    <<"ct/", (integer_to_binary(erlang:phash2(node())))/binary, "/",
        (list_to_binary(Name))/binary>>.

sorted_list(Prefix, Continuation) ->
    case ?M:list(Prefix, Continuation, #{}) of
        {ok, Keys, Next} -> {ok, lists:sort(Keys), Next};
        Other -> Other
    end.

page(Prefix, Continuation, Acc) ->
    {ok, Keys, Next} = ?M:list(Prefix, Continuation, #{}),
    case Next of
        done -> Acc ++ Keys;
        _ -> page(Prefix, Next, Acc ++ Keys)
    end.

all_keys(Prefix) ->
    page(Prefix, start, []).

chunks(<<>>, _N) ->
    [];
chunks(Bin, N) when byte_size(Bin) =< N ->
    [Bin];
chunks(Bin, N) ->
    <<Chunk:N/binary, Rest/binary>> = Bin,
    [Chunk | chunks(Rest, N)].

await_async(Key, Range) ->
    {ok, Req, State} = ?M:get_range_async(Key, Range, #{timeout => 30_000}),
    drain(Req, State, <<>>).

drain(Req, State, Acc) ->
    receive
        Msg ->
            case ?M:match_async(Msg, #{Req => State}, #{}) of
                {ok, Req} ->
                    case ?M:handle_async(Msg, Req, State) of
                        {continue, State1} -> drain(Req, State1, Acc);
                        {data, Data, done} -> <<Acc/binary, Data/binary>>;
                        {data, Data, State1} -> drain(Req, State1, <<Acc/binary, Data/binary>>);
                        {done, ok} -> Acc;
                        {done, Err} -> ct:fail({async_failed, Err});
                        ignore -> drain(Req, State, Acc)
                    end;
                _ ->
                    drain(Req, State, Acc)
            end
    after 30_000 ->
        ct:fail(async_timeout)
    end.

%% One small page, for a marker worth feeding back through list/3.
small_page(Prefix) ->
    Params = uri_string:compose_query(
        lists:keysort(1, [
            {<<"restype">>, <<"container">>},
            {<<"comp">>, <<"list">>},
            {<<"prefix">>, Prefix},
            {<<"maxresults">>, <<"2">>}
        ])
    ),
    {ok, #{status := 200, body := Body}} = raw(
        <<"GET">>, <<(container_path())/binary, "?", Params/binary>>
    ),
    ?M:decode_enumeration_results(Body).

%% The plugin never creates a container. Against the emulator the suite makes
%% its own; against a real account the operator is expected to have one, and a
%% failure here is a configuration problem worth seeing.
ensure_container() ->
    case ?M:check_bucket(#{}) of
        ok ->
            ok;
        {error, no_such_bucket} ->
            case raw(<<"PUT">>, <<(container_path())/binary, "?restype=container">>) of
                {ok, #{status := Status}} when Status =:= 201; Status =:= 409 -> ok;
                Other -> ct:fail({create_container_failed, Other})
            end;
        {error, _} = Err ->
            ct:fail({check_bucket_failed, Err})
    end.

container_path() ->
    Container = application:get_env(rabbitmq_stream_s3, bucket, <<>>),
    case application:get_env(rabbitmq_stream_s3, azure_path_style, false) of
        false ->
            <<$/, Container/binary>>;
        true ->
            Account = application:get_env(rabbitmq_stream_s3, azure_account, <<>>),
            <<$/, Account/binary, $/, Container/binary>>
    end.

%% A signed request for the container operations the backend does not implement.
raw(Method, Path) ->
    {ok, Host} = ?M:endpoint(),
    Headers = #{
        <<"x-ms-version">> => rabbitmq_stream_s3_config:azure_api_version(),
        <<"host">> => Host
    },
    Req = #{method => Method, path => Path, body => <<>>, opts => #{}},
    {ok, Signed} = rabbitmq_stream_s3_auth:authorize(Req, Headers),
    rabbitmq_stream_s3_http:request(Method, Path, Signed, <<>>, #{}).
