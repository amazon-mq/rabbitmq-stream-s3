%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

%% A logical identifier for a stream. This is set to be the stream queue's
%% resource name by RabbitMQ. This term is used for the `{osiris_offset,
%% stream_reference(), osiris:offset()}' message sent by Osiris and to trigger
%% deletion based on the queue name from the metadata store.
-type stream_reference() :: rabbit_amqqueue:name().

%% Prefer binaries for filenames. This is a subtype of file:filename_all().
%% Binaries represent ASCII in a much more compact fashion than lists.
-type filename() :: binary().
-type directory() :: binary().

%% Segment and fragment file header size. 4 bytes for the magic and 4 bytes for
%% the version.
-define(SEGMENT_HEADER_B, 8).
-define(SEGMENT_VERSION, 1).
-define(SEGMENT_HEADER, <<"OSIL", ?SEGMENT_VERSION:32/unsigned>>).
-define(SEGMENT_HEADER_HASH, erlang:crc32(?SEGMENT_HEADER)).
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

%% first offset (8) + first timestamp (8) + next offset (8) + seq no (2) +
%% size (4) + segment start pos (4) + num chunks in segment (4) +
%% index byte offset (4) + index size (4) = 46.
-define(FRAGMENT_TRAILER_B, 46).
-define(FRAGMENT_TRAILER(
    FirstOffset,
    FirstTimestamp,
    NextOffset,
    SeqNo,
    Size,
    SegmentStartPos,
    NumChunksInSegment,
    IdxStartPos,
    IdxSize
),
    <<
        FirstOffset:64/unsigned,
        FirstTimestamp:64/unsigned,
        NextOffset:64/unsigned,
        SeqNo:16/unsigned,
        Size:32/unsigned,
        SegmentStartPos:32/unsigned,
        NumChunksInSegment:32/unsigned,
        IdxStartPos:32/unsigned,
        IdxSize:32/unsigned
    >>
).
%% Info stored in a fragment's trailer. Nicer version of the above.
-record(fragment_info, {
    offset :: osiris:offset(),
    timestamp :: osiris:timestamp(),
    next_offset :: osiris:offset(),
    %% Zero-based sequence number within the segment.
    seq_no :: non_neg_integer(),
    %% The position into the segment file where this fragment data started.
    segment_start_pos :: pos_integer(),
    %% Number of chunks in the segment before this fragment.
    num_chunks_in_segment :: non_neg_integer(),
    %% Size of the fragment data. Doesn't including headers, index or trailers.
    size :: pos_integer(),
    %% Position into the fragment file where the index starts.
    index_start_pos :: pos_integer(),
    index_size :: pos_integer()
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
%% * first timestamp (8)
%% * next offset (8)
%% * size (9)
%% = 41 bytes
-define(MANIFEST_HEADER_SIZE, 41).

-define(MANIFEST(FirstOffset, FirstTimestamp, NextOffset, TotalSize, Entries), <<
    ?MANIFEST_ROOT_MAGIC,
    ?MANIFEST_ROOT_VERSION:32/unsigned,
    FirstOffset:64/unsigned,
    FirstTimestamp:64/signed,
    NextOffset:64/unsigned,
    0:2/unsigned,
    TotalSize:70/unsigned,
    %% Entries array:
    Entries/binary
>>).
-define(ENTRY(Offset, Timestamp, Kind, Size, SeqNo, Uid, Rest), <<
    Offset:64/unsigned,
    Timestamp:64/signed,
    Kind:2/unsigned,
    Size:70/unsigned,
    SeqNo:16/unsigned,
    (Uid):8/binary,
    %% Other entries:
    Rest/binary
>>).
%% * offset(8)
%% * timestamp (8)
%% * kind/size (9)
%% * seq no (2)
%% * uid (8)
%% = 35
-define(ENTRY_B, 35).

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
    %% The oldest offset which has been uploaded and not yet truncated by
    %% retention.
    first_offset = 0 :: osiris:offset(),
    %% The oldest timestamp which has been uploaded and not yet truncated
    %% by retention.
    first_timestamp = -1 :: osiris:timestamp(),
    %% The next offset which must be uploaded to the remote tier to ensure
    %% that the log has been uploaded without any holes. This corresponds to
    %% `#fragment.next_offset' for the last fragment which has been uploaded
    %% to the remote tier.
    next_offset = 0 :: osiris:offset(),
    %% Total size of segment data in the remote tier. This does not count
    %% headers, index data or trailers. This is the summed `#fragment.size` of
    %% all fragments in the remote tier.
    total_size = 0 :: non_neg_integer(),
    %% The revision the manifest was last fetched or uploaded at.
    %% This is used for an optimistic concurrency control.
    revision = 0 :: rabbitmq_stream_s3_db:revision(),
    %% An array of entries. Use the `?ENTRY/6' macro to access entries.
    entries = <<>> :: rabbitmq_stream_s3_log_manifest_entry:entries()
}).

%% Number of outgoing edges from this branch. Works for the entries array of
%% the root or any group.
-define(ENTRIES_LEN(Entries), erlang:byte_size(Entries) div ?ENTRY_B).

-define(MANIFEST_BRANCHING_FACTOR, 1024).
-define(MANIFEST_MAX_HEIGHT, 4).
-define(FRAGMENT_UPLOADS_PER_MANIFEST_UPDATE, 10).

%% 64 MiB (2^26 B)
-define(MAX_FRAGMENT_SIZE_B, 67_108_864).
%% %% 1 GiB (2^30 B)
%% -define(MAX_SEGMENT_SIZE_BYTES, 1_073_741_824).
%% 1/G GiB (2^29 B)
-define(MAX_SEGMENT_SIZE_BYTES, 536_870_912).

-type byte_offset() :: non_neg_integer().
-type checksum() :: non_neg_integer().

%% rabbitmq_stream_s3_log_manifest_machine types:

%% The name of a stream. This is a unique identifier for an incarnation of a
%% stream, meaning that it will not be identical if you delete a stream queue
%% and recreate it. RabbitMQ sets these to be the vhost name, stream name and
%% creation timestamp, concatenated with "_".
-type stream_id() :: binary().

-record(fragment, {
    first_offset :: osiris:offset() | undefined,
    last_offset :: osiris:offset() | undefined,
    next_offset :: osiris:offset() | undefined,
    segment_offset :: osiris:offset(),
    segment_pos = ?SEGMENT_HEADER_B :: pos_integer(),
    first_timestamp :: osiris:timestamp() | undefined,
    %% Number of chunks in prior fragments and number in current fragment.
    num_chunks = {0, 0} :: {non_neg_integer(), non_neg_integer()},
    %% Zero-based increasing integer for sequence number within the segment.
    seq_no = 0 :: non_neg_integer(),
    %% NOTE: `#fragment.size` is the bytes of segment data, not headers or
    %% index data.
    size = 0 :: non_neg_integer(),
    checksum = ?SEGMENT_HEADER_HASH :: checksum() | undefined
}).

%% Events.

%% The writer has written enough data and the given fragment is ready to be
%% handed off to the manifest. This is also emitted when a writer starts up
%% as it scans through the current segment and finds existing fragments - the
%% manifest server applies these idempotently.
-record(fragment_available, {
    stream :: stream_id(),
    fragment :: #fragment{}
}).
%% The writer notified the manifest that the commit offset has moved forward.
-record(commit_offset_increased, {
    stream :: stream_id(),
    offset :: osiris:offset()
}).
-record(fragment_uploaded, {
    stream :: stream_id(),
    info :: #fragment_info{}
}).
-record(manifest_uploaded, {
    stream :: stream_id(),
    entry :: rabbitmq_stream_s3_db:entry()
}).
-record(manifest_rebalanced, {
    stream :: stream_id(),
    manifest :: #manifest{}
}).
-record(manifest_requested, {
    stream :: stream_id(),
    requester :: gen_server:from() | pid()
}).
-record(manifest_resolved, {
    stream :: stream_id(),
    manifest :: #manifest{}
}).
-record(writer_spawned, {
    stream :: stream_id(),
    dir :: directory(),
    epoch :: osiris:epoch(),
    reference :: stream_reference(),
    pid :: pid(),
    replica_nodes = [] :: [node()],
    retention = [] :: [osiris:retention_spec()],
    %% Fragments available in the active segment. This list must be sorted
    %% descending by first offset.
    available_fragments = [] :: [#fragment{}]
}).
-record(acceptor_spawned, {stream :: stream_id()}).
%% Sent from the writer to replicas to notify them of when new fragments are
%% applied to the writer's manifest, or when truncation moves the first offset
%% and timestamp forward.
%%
%% For retention `fragments` may be empty. Kinda like an append-entries RPC in
%% Raft.
-record(fragments_applied, {
    stream :: stream_id(),
    first_offset :: osiris:offset(),
    first_timestamp :: osiris:timestamp(),
    fragments :: [#fragment_info{}]
}).
-record(tick, {}).
-record(retention_updated, {
    stream :: stream_id(),
    retention :: [osiris:retention_spec()]
}).
-record(manifest_upload_rejected, {
    stream :: stream_id(),
    conflict :: rabbitmq_stream_s3_db:entry()
}).
-record(stream_deleted, {stream :: stream_id()}).

-type event() ::
    #acceptor_spawned{}
    | #commit_offset_increased{}
    | #fragment_available{}
    | #fragment_uploaded{}
    | #fragments_applied{}
    | #manifest_rebalanced{}
    | #manifest_requested{}
    | #manifest_resolved{}
    | #manifest_upload_rejected{}
    | #manifest_uploaded{}
    | #retention_updated{}
    | #stream_deleted{}
    | #tick{}
    | #writer_spawned{}.

%% Effects.

-record(register_offset_listener, {
    writer_pid :: pid(),
    offset :: osiris:offset() | -1
}).
-record(upload_fragment, {
    stream :: stream_id(),
    dir :: directory(),
    fragment :: #fragment{}
}).
-record(upload_manifest, {
    stream :: stream_id(),
    epoch :: osiris:epoch(),
    reference :: stream_reference(),
    manifest :: #manifest{}
}).
-record(rebalance_manifest, {
    stream :: stream_id(),
    kind :: rabbitmq_stream_s3_log_manifest_entry:kind(),
    size :: pos_integer(),
    new_group :: rabbitmq_stream_s3_log_manifest_entry:entries(),
    rebalanced :: rabbitmq_stream_s3_log_manifest_entry:entries(),
    manifest :: #manifest{}
}).
%% Download the manifest from the remote tier and also check the tail of the
%% last fragment to see if fragments have been uploaded but not yet applied.
-record(resolve_manifest, {stream :: stream_id()}).
-record(reply, {to :: gen_server:from(), response :: term()}).
%% Set the range of the remote tier stream.
-record(set_range, {
    stream :: stream_id(),
    first :: osiris:offset() | -1,
    %% The exclusive end of the stream - an offset which may not exist yet.
    next :: osiris:offset()
}).
-record(send, {
    to :: pid() | {atom(), node()},
    message :: term(),
    %% See `erlang:send/3`.
    options = [] :: [nosuspend | noconnect | priority]
}).
%% TODO: include the fragment kind to make this generic for group objects
%% as well?
-record(delete_fragments, {
    stream :: stream_id(),
    offsets :: [osiris:offset()]
}).
%% Read through the local stream data and find available fragments.
%% This is done for the active segment when a writer spawns. This effect is
%% used to perform the same recovery for older fragments when the manifest
%% server recognizes a hole between the tail of the resolved manifest and the
%% first available fragment recovered during writer spawn.
-record(find_fragments, {
    stream :: stream_id(),
    dir :: directory(),
    from :: osiris:offset(),
    %% **Exclusive** end: if a fragment starts at this offset it does not need
    %% to be sent to the manifest server.
    to :: osiris:offset()
}).
-record(delete_stream, {stream :: stream_id()}).

-type effect() ::
    #delete_fragments{}
    | #delete_stream{}
    | #find_fragments{}
    | #rebalance_manifest{}
    | #register_offset_listener{}
    | #reply{}
    | #resolve_manifest{}
    | #send{}
    | #set_range{}
    | #upload_fragment{}
    | #upload_manifest{}.
