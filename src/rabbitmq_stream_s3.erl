%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3).

-include("include/rabbitmq_stream_s3.hrl").

-doc """
A unique, randomly generated ID.

This binary contains random bytes. Use `format_uid/1` to format into human
readable text.
""".
-type uid() :: <<_:64>>.

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

-export_type([
    uid/0,
    key/0,
    entry/0,
    entries/0,
    kind/0,
    milliseconds/0,
    retention_spec/0
]).

-export([
    uid/0,
    null_uid/0,
    format_uid/1,
    offset_filename/2,
    manifest_key/2,
    group_key/4,
    group_extension/1,
    next_group/1,
    fragment_key/2,
    index_file_offset/1,
    segment_file_offset/1
]).

-doc "Creates a new random UID.".
-spec uid() -> uid().
uid() ->
    crypto:strong_rand_bytes(8).

-doc """
Creates a zeroed UID binary.

This is meant for covering scenarios where the UID is not used.
""".
-spec null_uid() -> uid().
null_uid() ->
    <<0:64>>.

-doc "Formats a UID as human-readable text".
-spec format_uid(uid()) -> <<_:128>>.
format_uid(<<_:64>> = Uid) ->
    binary:encode_hex(Uid, lowercase).

-doc """
Creates a basename of a file or key which corresponds to the offset with the
given suffix.

The offset is padded with leading zeroes to a width of 20.
""".
-spec offset_filename(osiris:offset(), Suffix :: binary()) -> filename().
offset_filename(Offset, Suffix) when is_integer(Offset) andalso is_binary(Suffix) ->
    <<(iolist_to_binary(io_lib:format("~20..0B", [Offset])))/binary, ".", Suffix/binary>>.

-doc "Creates the key for the given stream and UID".
-spec manifest_key(stream_id(), uid()) -> key().
manifest_key(StreamId, Uid) when is_binary(StreamId) andalso is_binary(Uid) ->
    manifest_key(StreamId, Uid, <<"manifest">>).

-spec manifest_key(stream_id(), uid(), filename()) -> key().
manifest_key(StreamId, Uid, Filename) when
    is_binary(StreamId) andalso is_binary(Uid) andalso is_binary(Filename)
->
    <<"rabbitmq/stream/", StreamId/binary, "/metadata/", (format_uid(Uid))/binary, $.,
        Filename/binary>>.

-doc "Creates the key for the given group".
-spec group_key(stream_id(), uid(), kind(), osiris:offset()) -> key().
group_key(StreamId, Uid, Kind, Offset) ->
    manifest_key(StreamId, Uid, offset_filename(Offset, group_extension(Kind))).

%% TODO: this should be private.
-spec group_extension(kind()) -> binary().
group_extension(?MANIFEST_KIND_GROUP) -> <<"group">>;
group_extension(?MANIFEST_KIND_KILO_GROUP) -> <<"kgroup">>;
group_extension(?MANIFEST_KIND_MEGA_GROUP) -> <<"mgroup">>.

-doc "Returns next largest group above the given group".
-spec next_group(kind()) -> kind().
next_group(?MANIFEST_KIND_FRAGMENT) -> ?MANIFEST_KIND_GROUP;
next_group(?MANIFEST_KIND_GROUP) -> ?MANIFEST_KIND_KILO_GROUP;
next_group(?MANIFEST_KIND_KILO_GROUP) -> ?MANIFEST_KIND_MEGA_GROUP.

-doc "Returns the key for the given fragment offset".
-spec fragment_key(stream_id(), osiris:offset()) -> key().
fragment_key(StreamId, Offset) when is_binary(StreamId) andalso is_integer(Offset) ->
    stream_data_key(StreamId, offset_filename(Offset, <<"fragment">>)).

-spec stream_data_key(stream_id(), filename()) -> key().
stream_data_key(StreamId, Filename) when is_binary(StreamId) andalso is_binary(Filename) ->
    <<"rabbitmq/stream/", StreamId/binary, "/data/", Filename/binary>>.

-doc "Extracts the first offset from a segment filename".
-spec segment_file_offset(file:filename_all()) -> osiris:offset().
segment_file_offset(Filename) ->
    filename_offset(filename:basename(Filename, <<".segment">>)).

-doc "Extracts the first offset from an index filename".
-spec index_file_offset(file:filename_all()) -> osiris:offset().
index_file_offset(Filename) ->
    filename_offset(filename:basename(Filename, <<".index">>)).

-spec filename_offset(file:filename_all()) -> osiris:offset().
filename_offset(Basename) when is_binary(Basename) ->
    binary_to_integer(Basename);
filename_offset(Basename) when is_list(Basename) ->
    list_to_integer(Basename).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

format_uid_test() ->
    ?assertEqual(<<"0000000000000000">>, format_uid(null_uid())),
    ok.

index_file_offset_test() ->
    %% Relative? Absolute? No directory at all? Doesn't matter. The answer
    %% is the same.
    ?assertEqual(100, index_file_offset(<<"00000000000000000100.index">>)),
    ?assertEqual(100, index_file_offset(<<"path/to/00000000000000000100.index">>)),
    ?assertEqual(100, index_file_offset(<<"/path/to/00000000000000000100.index">>)),
    ok.

-endif.
