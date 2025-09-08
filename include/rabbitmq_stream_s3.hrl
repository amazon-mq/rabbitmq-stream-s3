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
%% magic (4) + version (4) + offset (8) + timestamp (8) + size (9) = 33
-define(MANIFEST_HEADER_SIZE, 33).

-define(MANIFEST(FirstOffset, FirstTimestamp, TotalSize, Entries), <<
    ?MANIFEST_ROOT_MAGIC,
    ?MANIFEST_ROOT_VERSION:32/unsigned,
    FirstOffset:64/unsigned,
    FirstTimestamp:64/signed,
    0:2/unsigned,
    TotalSize:70/unsigned,
    %% Entries array:
    Entries/binary
>>).
-define(ENTRY(Offset, Timestamp, Kind, Size, SeqNo, Rest), <<
    Offset:64/unsigned,
    Timestamp:64/signed,
    Kind:2/unsigned,
    Size:70/unsigned,
    SeqNo:16/unsigned,
    %% Other entries:
    Rest/binary
>>).

%% offset (8) + timestamp (8) + kind/size (9) + seq no (2) = 27
-define(ENTRY_B, 27).
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
