%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_counters).

-define(LOG_READER_RANGE_REQUESTS, 1).
-define(LOG_READER_BYTES_READ, 2).

-define(LOG_READER_COUNTERS, [
    {range_requests, ?LOG_READER_RANGE_REQUESTS, counter,
        "Number of S3 range requests made by log readers"},
    {segment_bytes_read, ?LOG_READER_BYTES_READ, counter,
        "Number of bytes read from S3 by log readers"}
]).

-define(MANIFESTS_READ, 1).
-define(MANIFEST_BYTES_READ, 2).
-define(MANIFESTS_WRITTEN, 3).
-define(MANIFEST_BYTES_WRITTEN, 4).
-define(SEGMENTS_WRITTEN, 5).
-define(SEGMENT_BYTES_WRITTEN, 6).
-define(INDEXES_WRITTEN, 7).
-define(INDEX_BYTES_WRITTEN, 8).

-define(LOG_MANIFEST_COUNTERS, [
    {manifests_read, ?MANIFESTS_READ, counter,
        "Number of (non-empty) manifests read"},
    {manifest_bytes_read, ?MANIFEST_BYTES_READ, counter,
        "Number of manifest file bytes read from S3"},
    {manifests_written, ?MANIFESTS_WRITTEN, counter,
        "Number of manifests written to S3"},
    {manifest_bytes_written, ?MANIFEST_BYTES_WRITTEN, counter,
        "Number of bytes of manifest files written to S3"},
    %% TODO will rename to "fragment" (maybe keep the segment ones too though).
    {uploaded_segments, ?SEGMENTS_WRITTEN, counter,
        "Number of segment files uploaded to S3"},
    {uploaded_segment_bytes, ?SEGMENT_BYTES_WRITTEN, counter,
        "Number of bytes of segment files written to S3"},
    {uploaded_indexes, ?INDEXES_WRITTEN, counter,
        "Number of index files uploaded to S3"},
    {uploaded_index_bytes, ?INDEX_BYTES_WRITTEN, counter,
        "Number of bytes of index files written to S3"}
]).

-export([
    init/0,
    overview/0,
    read_bytes/1,
    manifest_read/1,
    manifest_written/1,
    segment_uploaded/1,
    index_uploaded/1
]).

-type id() :: log_readers | log_manifest.

-spec init() -> ok.
init() ->
    _ = seshat:new_group(?MODULE),
    C1 = seshat:new(?MODULE, log_readers, ?LOG_READER_COUNTERS),
    persistent_term:put({?MODULE, log_readers}, C1),
    C2 = seshat:new(?MODULE, log_manifest, ?LOG_MANIFEST_COUNTERS),
    persistent_term:put({?MODULE, log_manifest}, C2),
    ok.

-spec overview() -> #{id() => #{atom() => non_neg_integer()}}.
overview() ->
    seshat:counters(?MODULE).

read_bytes(BytesRead) ->
    C = fetch(log_readers),
    counters:add(C, ?LOG_READER_RANGE_REQUESTS, 1),
    counters:add(C, ?LOG_READER_BYTES_READ, BytesRead).

manifest_read(BytesRead) ->
    C = fetch(log_manifest),
    counters:add(C, ?MANIFESTS_READ, 1),
    counters:add(C, ?MANIFEST_BYTES_READ, BytesRead).

manifest_written(BytesWritten) ->
    C = fetch(log_manifest),
    counters:add(C, ?MANIFESTS_WRITTEN, 1),
    counters:add(C, ?MANIFEST_BYTES_WRITTEN, BytesWritten).

segment_uploaded(BytesWritten) ->
    C = fetch(log_manifest),
    counters:add(C, ?SEGMENTS_WRITTEN, 1),
    counters:add(C, ?SEGMENT_BYTES_WRITTEN, BytesWritten).

index_uploaded(BytesWritten) ->
    C = fetch(log_manifest),
    counters:add(C, ?INDEXES_WRITTEN, 1),
    counters:add(C, ?INDEX_BYTES_WRITTEN, BytesWritten).

fetch(Id) ->
    persistent_term:get({?MODULE, Id}).
