%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

%% Prefer binaries for filenames. This is a subtype of file:filename_all().
%% Binaries represent ASCII in a much more compact fashion than lists.
-type filename() :: binary().
-type directory() :: binary().

%% Segment and fragment file header size. 4 bytes for the magic and 4 bytes for
%% the version.
-define(SEGMENT_HEADER_B, 8).
-define(SEGMENT_VERSION, 1).
-define(SEGMENT_HEADER, <<"OSIL", ?SEGMENT_VERSION:32/unsigned>>).
-define(IDX_HEADER_B, 8).
-define(IDX_VERSION, 1).
-define(IDX_HEADER, ?IDX_HEADER(<<>>)).
-define(IDX_HEADER(Rem), <<"OSII", ?IDX_VERSION:32/unsigned, Rem/binary>>).
-define(INDEX_RECORD_SIZE_B, 29).

-define(CHUNK_HEADER_B, 48).
-define(MAX_FILTER_SIZE, 255).
-define(CHNK_USER, 0).
-define(CHNK_TRK_DELTA, 1).
-define(CHNK_TRK_SNAPSHOT, 2).
-define(REC_MATCH_SIMPLE(Len, Rem),
    <<0:1, Len:31/unsigned, Rem/binary>>
).
-define(REC_MATCH_SUBBATCH(CompType, NumRec, UncompLen, Len, Rem), <<
    1:1,
    CompType:3/unsigned,
    _:4/unsigned,
    NumRecs:16/unsigned,
    UncompressedLen:32/unsigned,
    Len:32/unsigned,
    Rem/binary
>>).

%% The index which is concatenated in with segment files.
-define(REMOTE_IDX_VERSION, 1).
-define(REMOTE_IDX_MAGIC, "OSII").
-define(REMOTE_IDX_HEADER, <<?REMOTE_IDX_MAGIC, ?REMOTE_IDX_VERSION:32/unsigned>>).
-define(REMOTE_IDX_HEADER_SIZE, 8).

%% Counter indexes from osiris_log.erl:
-define(C_OSIRIS_LOG_OFFSET, 1).
-define(C_OSIRIS_LOG_FIRST_OFFSET, 2).
-define(C_OSIRIS_LOG_FIRST_TIMESTAMP, 3).
-define(C_OSIRIS_LOG_CHUNKS, 4).
-define(C_OSIRIS_LOG_SEGMENTS, 5).

%% A pointer to a fragment object. Together with the stream ID this has all
%% the necessary info to construct the fragment's S3 key and locate the index
%% boundary within the object.
-record(fragment_ref, {
    offset :: osiris:offset(),
    uid :: rabbitmq_stream_s3:uid(),
    size :: non_neg_integer()
}).

%% A pointer to a manifest root object.
-record(manifest_ref, {
    epoch :: osiris:epoch(),
    uid :: rabbitmq_stream_s3:uid()
}).

-define(FRAGMENT_VERSION, 1).

-record(remote_location, {
    position :: byte_offset(),
    chunk_id :: osiris:offset(),
    fragment_ref :: #fragment_ref{},
    iterator :: rabbitmq_stream_s3_fragment_iterator:iterator()
}).

-define(INDEX_RECORD(Offset, Timestamp, FragmentFilePos), <<
    Offset:64/unsigned,
    Timestamp:64/signed,
    %% Absolute position in the fragment file of the chunk (includes the
    %% fragment header).
    FragmentFilePos:32/unsigned
>>).
%% offset (8) + timestamp (8) + segment file pos (4) = 20.
-define(INDEX_RECORD_B, 20).

%% Manifest tree.
-define(MANIFEST_ROOT_VERSION, 1).
-define(MANIFEST_ROOT_MAGIC, "OSIR").
-define(MANIFEST_GROUP_VERSION, 1).
-define(MANIFEST_GROUP_MAGIC, "OSIG").
-define(MANIFEST_KILO_GROUP_VERSION, 1).
-define(MANIFEST_KILO_GROUP_MAGIC, "OSIK").
-define(MANIFEST_MEGA_GROUP_VERSION, 1).
-define(MANIFEST_MEGA_GROUP_MAGIC, "OSIM").

%% NOTE: "kind" also happens to be the height in the tree.
-define(MANIFEST_KIND_FRAGMENT, 0).
-define(MANIFEST_KIND_GROUP, 1).
-define(MANIFEST_KIND_KILO_GROUP, 2).
-define(MANIFEST_KIND_MEGA_GROUP, 3).

%% The root and all groups have the same header.
%% * magic (4)
%% * version (4)
%% * first offset (8)
%% * next offset (8)
%% * first timestamp (8)
%% * first last timestamp (8)
%% * size (9)
%% = 41 bytes
-define(MANIFEST_HEADER_SIZE, 49).

-define(MANIFEST(FirstOffset, NextOffset, FirstTs, FirstLastTs, TotalSize, Entries), <<
    ?MANIFEST_ROOT_MAGIC,
    ?MANIFEST_ROOT_VERSION:32/unsigned,
    FirstOffset:64/unsigned,
    NextOffset:64/unsigned,
    FirstTs:64/signed,
    FirstLastTs:64/signed,
    0:2/unsigned,
    TotalSize:70/unsigned,
    %% Entries array:
    Entries/binary
>>).

%% Helper macros to form #manifest.entries array entries. Entries may either
%% point to fragments directly, or point to groups of fragments that have been
%% "rebalanced" out of the manifest root. These entries have different contents
%% but share the same amount of space - to make the entries arrays compact even
%% when there are thousands of fragments.
%%
%% Fragment and group entries share the same 34-byte layout, differentiated by
%% the Kind field. Both store the first offset, first timestamp, last
%% timestamp, size, and UID. For groups, Size is zero and UID identifies the
%% group object. For fragments, Size is the segment data size and UID
%% identifies the fragment object.
-define(ENTRY_B, 34).
-define(ENTRY(Offset, FirstTs, LastTs, Kind, Size, Uid), <<
    Offset:64/unsigned,
    FirstTs:64/signed,
    LastTs:64/signed,
    Kind:8/unsigned,
    Size:40/unsigned,
    Uid:32/unsigned
>>).
-define(ENTRY(Offset, FirstTs, LastTs, Kind, Size, Uid, Rest), <<
    Offset:64/unsigned,
    FirstTs:64/signed,
    LastTs:64/signed,
    Kind:8/unsigned,
    %% 40 bits can describe segment data of up to 1 TiB.
    %% 2^40 == 1024^4
    Size:40/unsigned,
    %% 32 bits of entropy. See the uid() type.
    Uid:32/unsigned,
    Rest/binary
>>).

%% A nicer version of the above `?MANIFEST/5' macro.
%%
%% This is the root of the manifest. A newly created, empty manifest can be
%% identified by checking `#manifest.next_offset =:= 0'. Checking
%% `#manifest.total_size =:= 0' or `#manifest.entries =:= <<>>' is not
%% sufficient since the remote tier can become empty from retention.
%% (TODO: is that actually true?)
%%
%% This record also contains the optimistic concurrency information necessary
%% for the stream, i.e. `revision'.
-record(manifest, {
    %% The offset of the first chunk in the first fragment in the remote
    %% tier. Used to set the first_offset counter.
    first_offset = 0 :: osiris:offset(),
    %% The timestamp of the first chunk in the first fragment in the
    %% remote tier. Used to set the first_timestamp counter.
    first_timestamp = -1 :: osiris:timestamp(),
    %% The timestamp of the last chunk in the first fragment in the remote
    %% tier. Used by max-age retention.
    first_last_timestamp = -1 :: osiris:timestamp(),
    %% The next offset which must be uploaded to the remote tier to ensure
    %% that the log has been uploaded without any holes. This corresponds to
    %% `#fragment.next_offset' for the last fragment which has been uploaded
    %% to the remote tier.
    next_offset = 0 :: osiris:offset(),
    %% Total size of segment data in the remote tier. This does not count
    %% headers or index data. This is the summed `#fragment.size` of
    %% all fragments in the remote tier.
    total_size = 0 :: non_neg_integer(),
    %% The revision the manifest was last fetched or uploaded at.
    %% This is used for an optimistic concurrency control.
    revision = 0 :: rabbitmq_stream_s3_db:revision(),
    %% An array of entries. Use the `?ENTRY/6' macro to access entries.
    entries = <<>> :: rabbitmq_stream_s3:entries()
}).

%% 64 MiB (2^26 B)
-define(MAX_FRAGMENT_SIZE_B, 67_108_864).
%% %% 1 GiB (2^30 B)
%% -define(MAX_SEGMENT_SIZE_BYTES, 1_073_741_824).
%% 1/2 GiB (2^29 B), reduced to 1 MiB in tests to trigger segment rolls.
-ifdef(TEST).
-define(MAX_SEGMENT_SIZE_BYTES, 1_048_576).
-else.
-define(MAX_SEGMENT_SIZE_BYTES, 536_870_912).
-endif.

-type byte_offset() :: non_neg_integer().

%% The name of a stream. This is a unique identifier for an incarnation of a
%% stream, meaning that it will not be identical if you delete a stream queue
%% and recreate it. RabbitMQ sets these to be the vhost name, stream name and
%% creation timestamp, concatenated with "_".
-type stream_id() :: binary().

%% An edit to the manifest entries array. This type generically covers
%% insertions, deletions and replacements. Edits are passed from the writer
%% to replica servers to express changes to the manifest.
%% * New fragments applied: pos points to the end of the array, len is zero and
%%   the entries are appended.
%% * Rebalancing: the section at pos with length len is replaced with the new
%%   entries sub-array.
%% * Retention: entries is empty and the section at pos with length len is
%%   deleted. Pos is always zero.
-record(edit, {
    first_offset :: osiris:offset(),
    first_timestamp :: osiris:timestamp(),
    first_last_timestamp :: osiris:timestamp(),
    next_offset :: osiris:offset() | undefined,
    %% The difference in total segment size. Adds or removes from
    %% #manifest.total_size depending on the operation.
    size = 0 :: integer(),
    entries = <<>> :: rabbitmq_stream_s3:entries(),
    pos = 0 :: non_neg_integer(),
    len = 0 :: non_neg_integer()
}).

%% A pointer to a group object. Together with the stream ID this has all of
%% the necessary info to create the group's object key.
-record(group_ref, {
    offset :: osiris:offset(),
    kind :: rabbitmq_stream_s3:kind(),
    uid :: rabbitmq_stream_s3:uid()
}).
