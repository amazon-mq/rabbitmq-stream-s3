%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3).

-include("include/rabbitmq_stream_s3.hrl").

-doc """
A unique, randomly generated ID.

This is represented as a 32-bit unsigned integer. All manifest entries
(fragments and groups) share the same 34-byte layout with a 32-bit UID field.
The UID is included in S3 object keys to prevent overwrites when competing
writers upload to the same offset range.

With 32 bits and N competing pairs, collision probability is ~N^2/2^32. In
practice, elections producing competing uploads at the same offset are rare,
so 32 bits is sufficient.
""".
-type uid() :: non_neg_integer().

-doc """
A key within a bucket.

This identifies an object. Typically keys look like Unix paths, for example
`<<"rabbitmq/stream/data/__sq_12346786783/00000000000000000000.fragment">>`.
""".
-type key() :: binary().

-doc """
An entry in the entries array of a manifest.

This binary representation is the same for the root manifest and all kinds of
groups. See the `?ENTRY` macro for more.
""".
-type entry() :: <<_:(?ENTRY_B * 8)>>.
-doc """
An array of `entry()`s.

These are always sorted by offset ascending, and these arrays can be searched
efficiently using the `rabbitmq_stream_s3_array` module.
""".
-type entries() :: <<_:_*(?ENTRY_B * 8)>>.

-type kind() ::
    ?MANIFEST_KIND_FRAGMENT
    | ?MANIFEST_KIND_GROUP
    | ?MANIFEST_KIND_KILO_GROUP
    | ?MANIFEST_KIND_MEGA_GROUP.

-type milliseconds() :: non_neg_integer().

%% Subset of osiris:retention_spec(), as a map.
-type retention_spec() :: #{
    max_bytes := non_neg_integer(),
    max_age := milliseconds()
}.

-type range() :: empty | {From :: osiris:offset(), To :: osiris:offset()}.

-export_type([
    uid/0,
    key/0,
    entry/0,
    entries/0,
    kind/0,
    milliseconds/0,
    retention_spec/0,
    range/0
]).

-export([
    uid/0,
    format_uid/1,
    offset_filename/2,
    manifest_key/2,
    group_key/2,
    group_name/1,
    next_group/1,
    fragment_key/2,
    fragment_key/3,
    stream_prefix/1,
    index_file_offset/1,
    fragment_key_offset/1,
    segment_file_offset/1
]).

%% For use by a boot step:
-export([setup/0]).

-rabbit_boot_step(
    {rabbitmq_stream_s3, [
        {description, "metadata for the rabbitmq_stream_s3 plugin"},
        {mfa, {?MODULE, setup, []}},
        {enables, core_initialized}
    ]}
).

%%----------------------------------------------------------------------------

setup() ->
    _ = application:ensure_all_started(seshat),
    _ = seshat:new_group(rabbitmq_stream_s3),
    ok.

-doc "Creates a new random UID.".
-spec uid() -> uid().
uid() ->
    <<Uid:32/unsigned>> = crypto:strong_rand_bytes(4),
    Uid.

-doc "Formats a UID as human-readable text".
-spec format_uid(uid()) -> <<_:64>>.
format_uid(Uid) when is_integer(Uid) andalso Uid >= 0 ->
    binary:encode_hex(<<Uid:32/unsigned>>, lowercase).

-doc """
Creates a basename of a file or key which corresponds to the offset with the
given suffix.

The offset is padded with leading zeroes to a width of 20.
""".
-spec offset_filename(osiris:offset(), Suffix :: binary()) -> filename().
offset_filename(Offset, Suffix) when is_integer(Offset) andalso is_binary(Suffix) ->
    <<(pad_zeroes(Offset))/binary, $., Suffix/binary>>.

-spec pad_zeroes(osiris:offset()) -> <<_:20 * 8>>.
pad_zeroes(Offset) ->
    %% Same as `io_lib:format("~20..0B", [Offset])` but much more efficient.
    %% NOTE: 2^64 is 20 digits.
    Num = integer_to_binary(Offset),
    Pad = 20 - byte_size(Num),
    case Pad > 0 of
        true ->
            <<(binary:copy(<<$0>>, Pad))/binary, Num/binary>>;
        false ->
            Num
    end.

-doc "Creates the key for the given stream and UID".
-spec manifest_key(stream_id(), uid()) -> key().
manifest_key(StreamId, Uid) when is_binary(StreamId) andalso is_integer(Uid) ->
    manifest_key(StreamId, <<"root">>, Uid, <<"manifest">>).

-spec manifest_key(stream_id(), binary(), uid(), binary()) -> key().
manifest_key(StreamId, Prefix, Uid, Suffix) when
    is_binary(StreamId) andalso is_binary(Prefix) andalso is_integer(Uid) andalso is_binary(Suffix)
->
    <<"rabbitmq/stream/", StreamId/binary, "/metadata/", Prefix/binary, $.,
        (format_uid(Uid))/binary, $., Suffix/binary>>.

-doc "Creates the key for the given group".
-spec group_key(stream_id(), #group_ref{}) -> key().
group_key(StreamId, #group_ref{uid = Uid, kind = Kind, offset = Offset}) ->
    manifest_key(StreamId, pad_zeroes(Offset), Uid, group_name(Kind)).

%% TODO: this should be private.
-spec group_name(kind()) -> binary().
group_name(?MANIFEST_KIND_GROUP) -> <<"group">>;
group_name(?MANIFEST_KIND_KILO_GROUP) -> <<"kgroup">>;
group_name(?MANIFEST_KIND_MEGA_GROUP) -> <<"mgroup">>.

-doc "Returns next largest group above the given group".
-spec next_group(kind()) -> kind().
next_group(?MANIFEST_KIND_FRAGMENT) -> ?MANIFEST_KIND_GROUP;
next_group(?MANIFEST_KIND_GROUP) -> ?MANIFEST_KIND_KILO_GROUP;
next_group(?MANIFEST_KIND_KILO_GROUP) -> ?MANIFEST_KIND_MEGA_GROUP.

-doc "Returns the key for the given fragment offset".
-spec fragment_key(stream_id(), osiris:offset()) -> key().
fragment_key(StreamId, Offset) when is_binary(StreamId) andalso is_integer(Offset) ->
    stream_data_key(StreamId, offset_filename(Offset, <<"fragment">>)).

-doc "Returns the key for the given fragment offset and UID".
-spec fragment_key(stream_id(), osiris:offset(), uid()) -> key().
fragment_key(StreamId, Offset, Uid) when
    is_binary(StreamId) andalso is_integer(Offset) andalso is_integer(Uid)
->
    Filename = <<(pad_zeroes(Offset))/binary, $., (format_uid(Uid))/binary, ".fragment">>,
    stream_data_key(StreamId, Filename).

-spec stream_data_key(stream_id(), filename()) -> key().
stream_data_key(StreamId, Filename) when is_binary(StreamId) andalso is_binary(Filename) ->
    <<"rabbitmq/stream/", StreamId/binary, "/data/", Filename/binary>>.

-spec stream_prefix(stream_id()) -> key().
stream_prefix(StreamId) when is_binary(StreamId) ->
    <<"rabbitmq/stream/", StreamId/binary>>.

-doc "Extracts the first offset from a segment filename".
-spec segment_file_offset(file:filename_all()) -> osiris:offset().
segment_file_offset(Filename) ->
    filename_offset(filename:basename(Filename, <<".segment">>)).

-doc "Extracts the first offset from an index filename".
-spec index_file_offset(file:filename_all()) -> osiris:offset().
index_file_offset(Filename) ->
    filename_offset(filename:basename(Filename, <<".index">>)).

-doc "Extracts the first offset from a fragment key".
-spec fragment_key_offset(key()) -> osiris:offset().
fragment_key_offset(Key) ->
    filename_offset(filename:rootname(filename:basename(Key, <<".fragment">>))).

-spec filename_offset(file:filename_all()) -> osiris:offset().
filename_offset(Basename) when is_binary(Basename) ->
    binary_to_integer(Basename);
filename_offset(Basename) when is_list(Basename) ->
    list_to_integer(Basename).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

format_uid_test() ->
    ?assertEqual(<<"00000000">>, format_uid(0)),
    ok.

index_file_offset_test() ->
    %% Relative? Absolute? No directory at all? Doesn't matter. The answer
    %% is the same.
    ?assertEqual(100, index_file_offset(<<"00000000000000000100.index">>)),
    ?assertEqual(100, index_file_offset(<<"path/to/00000000000000000100.index">>)),
    ?assertEqual(100, index_file_offset(<<"/path/to/00000000000000000100.index">>)),
    ok.

fragment_key_offset_test() ->
    StreamId = <<"__my-stream">>,
    ?assertEqual(0, fragment_key_offset(fragment_key(StreamId, 0, 16#deadbeef))),
    ?assertEqual(1234, fragment_key_offset(fragment_key(StreamId, 1234, 16#deadbeef))),
    ok.

-endif.
