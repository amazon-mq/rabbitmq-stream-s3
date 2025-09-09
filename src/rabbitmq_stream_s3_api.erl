%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_api).
-export([
    put_object/4, put_object/5,
    get_object/3, get_object/4,
    get_object_with_range/4, get_object_with_range/5,
    get_object_size/3, get_object_size/4,
    get_object_attributes/3,
    get_object_attributes/4,
    get_object_attributes/5
]).

-export([object_path/2]).

-type range_spec() ::
    {StartByte :: non_neg_integer(), EndByte :: non_neg_integer() | undefined}
    | SuffixRange :: integer().

-export_type([range_spec/0]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").

-define(DEFAULT_OPTS, []).

-spec put_object(term(), binary(), binary(), binary()) ->
    ok | {error, term()}.
put_object(Handle, Bucket, Key, Object) ->
    put_object(Handle, Bucket, Key, Object, []).

-spec put_object(term(), binary(), binary(), binary(), list()) ->
    ok | {error, term()}.
put_object(Handle, Bucket, Key, Object, Opts) ->
    Path = object_path(Bucket, Key),
    Headers =
        case proplists:get_value(crc32, Opts, []) of
            [] ->
                [];
            Checksum when is_integer(Checksum) ->
                C = base64:encode_to_string(<<Checksum:32/unsigned>>),
                [{"x-amz-checksum-crc32", C}]
        end,
    case rabbitmq_aws:put(Handle, Path, Object, Headers, [?DEFAULT_OPTS | Opts]) of
        {ok, {_Headers, <<>>}} ->
            ok;
        Error ->
            {error, Error}
    end.

-spec get_object(term(), binary(), binary()) ->
    {ok, binary()} | {error, term()}.
get_object(Handle, Bucket, Key) ->
    get_object(Handle, Bucket, Key, []).

get_object(Handle, Bucket, Key, Opts) ->
    Path = object_path(Bucket, Key),
    case rabbitmq_aws:get(Handle, Path, [], [?DEFAULT_OPTS | Opts]) of
        {ok, {_Headers, Body}} ->
            {ok, Body};
        {error, "Not Found", _} ->
            {error, not_found};
        Error ->
            {error, Error}
    end.

-spec get_object_attributes(term(), binary(), binary()) ->
    {ok, proplists:proplist()} | {error, term()}.
get_object_attributes(Handle, Bucket, Key) ->
    get_object_attributes(Handle, Bucket, Key, []).
get_object_attributes(Handle, Bucket, Key, Opts) ->
    get_object_attributes(Handle, Bucket, Key, [], Opts).

-spec get_object_attributes(term(), binary(), binary(), [string()], [term()]) ->
    {ok, proplists:proplist()} | {error, term()}.
get_object_attributes(Handle, Bucket, Key, Attributes, Opts) ->
    Path = object_path(Bucket, Key),
    case rabbitmq_aws:request(Handle, head, Path, <<"">>, [], [?DEFAULT_OPTS | Opts]) of
        {ok, {Headers, _Body}} ->
            {ok, parse_head_response_headers(Headers, Attributes)};
        {error, "Not Found", _} ->
            {error, not_found};
        Error ->
            {error, Error}
    end.

-spec get_object_with_range(term(), binary(), binary(), range_spec()) ->
    {ok, binary()} | {error, term()}.
get_object_with_range(Handle, Bucket, Key, RangeSpec) ->
    get_object_with_range(Handle, Bucket, Key, RangeSpec, []).

get_object_with_range(Handle, Bucket, Key, RangeSpec0, Opts) ->
    Path = object_path(Bucket, Key),
    RangeValue = range_specifier(RangeSpec0),
    Headers = [{"Range", lists:flatten(["bytes=" | RangeValue])}],
    case rabbitmq_aws:get(Handle, Path, Headers, [?DEFAULT_OPTS | Opts]) of
        {ok, {_Headers, Body}} ->
            {ok, Body};
        {error, "Not Found", _} ->
            {error, not_found};
        Error ->
            {error, Error}
    end.

%% https://www.rfc-editor.org/rfc/rfc9110.html#rule.ranges-specifier
range_specifier({StartByte, undefined}) ->
    io_lib:format("~b-", [StartByte]);
range_specifier({StartByte, EndByte}) ->
    io_lib:format("~b-~b", [StartByte, EndByte]);
range_specifier(SuffixLen) when is_integer(SuffixLen) andalso SuffixLen < 0 ->
    %% ~b will format the '-' for us.
    io_lib:format("~b", [SuffixLen]).

-spec get_object_size(term(), binary(), binary()) ->
    integer() | {error, term()}.
get_object_size(Handle, Bucket, Key) ->
    get_object_size(Handle, Bucket, Key, []).

get_object_size(Handle, Bucket, Key, Opts) ->
    {ok, [{_, Size}]} = get_object_attributes(Handle, Bucket, Key, [<<"content-length">>], Opts),
    binary_to_integer(Size).

object_path(Bucket, Key) ->
    BucketStr = ensure_string(Bucket),
    KeyStr = ensure_string(Key),
    {BucketStr, KeyStr}.

ensure_string(Binary) when is_binary(Binary) ->
    binary_to_list(Binary);
ensure_string(List) when is_list(List) ->
    List.

parse_head_response_headers(Headers, Attributes) ->
    filter_attributes(Headers, Attributes).

filter_attributes(Headers, []) ->
    Headers;
filter_attributes(Headers, Attributes) ->
    lists:filter(
        fun({Key, _Value}) ->
            lists:member(Key, Attributes)
        end,
        Headers
    ).
