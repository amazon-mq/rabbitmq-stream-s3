%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(read_buffer_SUITE).
-moduledoc """
Tests for the remote read path's block-queue buffer.

The buffer's model is a flat binary at a base offset; every operation here is
checked against that model. The sharing/copying tests pin down the memory
properties the module exists for: small reads pin no block, large
single-block reads share their block, and whole blocks are shared as-is.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile([export_all, nowarn_export_all]).

-define(BUF, rabbitmq_stream_s3_read_buffer).

all() ->
    [
        new_is_empty,
        append_advances_end_pos,
        empty_append_is_noop,
        read_within_single_block,
        read_across_blocks,
        read_whole_window,
        read_zero_bytes,
        read_out_of_range_raises,
        drop_before_is_block_granular,
        drop_before_start_is_noop,
        drop_before_end_drops_everything,
        drop_never_moves_empty_buffer,
        small_read_is_copied,
        small_read_as_iodata_is_copied,
        large_read_shares_block,
        whole_block_read_shares_block_term,
        read_iodata_copies_nothing_at_edges,
        copy_share_boundary,
        small_spanning_read_is_copied,
        read_spans_front_and_rear,
        interleaved_append_read_drop
    ].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------

%% Bytes whose value is a function of their absolute offset, so a read's
%% content proves which offsets it came from.
pattern(Pos, Len) ->
    <<<<(P rem 251)>> || P <- lists:seq(Pos, Pos + Len - 1)>>.

%% Builds a buffer at Pos from a list of block lengths, filled with pattern
%% data so contents are position-verifiable.
build(Pos, BlockLens) ->
    {Buf, _} = lists:foldl(
        fun(Len, {B, P}) ->
            {?BUF:append(pattern(P, Len), B), P + Len}
        end,
        {?BUF:new(Pos), Pos},
        BlockLens
    ),
    Buf.

%% ------------------------------------------------------------------
%% Basic accounting
%% ------------------------------------------------------------------

new_is_empty(_Config) ->
    Buf = ?BUF:new(100),
    ?assertEqual(100, ?BUF:start_pos(Buf)),
    ?assertEqual(100, ?BUF:end_pos(Buf)),
    ?assertEqual(0, ?BUF:size(Buf)),
    ?assertEqual(0, ?BUF:block_count(Buf)).

append_advances_end_pos(_Config) ->
    Buf = build(8, [10, 20, 30]),
    ?assertEqual(8, ?BUF:start_pos(Buf)),
    ?assertEqual(68, ?BUF:end_pos(Buf)),
    ?assertEqual(60, ?BUF:size(Buf)),
    ?assertEqual(3, ?BUF:block_count(Buf)).

empty_append_is_noop(_Config) ->
    Buf0 = build(8, [10]),
    Buf = ?BUF:append(<<>>, Buf0),
    ?assertEqual(18, ?BUF:end_pos(Buf)),
    ?assertEqual(1, ?BUF:block_count(Buf)).

%% ------------------------------------------------------------------
%% Reads
%% ------------------------------------------------------------------

read_within_single_block(_Config) ->
    Buf = build(8, [100]),
    ?assertEqual(pattern(20, 50), ?BUF:read(20, 50, Buf)),
    %% Block edges.
    ?assertEqual(pattern(8, 1), ?BUF:read(8, 1, Buf)),
    ?assertEqual(pattern(107, 1), ?BUF:read(107, 1, Buf)).

read_across_blocks(_Config) ->
    Buf = build(8, [10, 10, 10, 10]),
    %% Spans all four blocks.
    ?assertEqual(pattern(9, 38), ?BUF:read(9, 38, Buf)),
    %% Spans exactly one block boundary.
    ?assertEqual(pattern(17, 2), ?BUF:read(17, 2, Buf)).

read_whole_window(_Config) ->
    Buf = build(0, [7, 13, 3]),
    ?assertEqual(pattern(0, 23), ?BUF:read(0, 23, Buf)).

read_zero_bytes(_Config) ->
    Buf = build(8, [10]),
    ?assertEqual(<<>>, ?BUF:read(12, 0, Buf)),
    ?assertEqual([], ?BUF:read_iodata(12, 0, Buf)),
    %% Zero-length reads are in range even on an empty buffer.
    ?assertEqual(<<>>, ?BUF:read(5, 0, ?BUF:new(5))).

read_out_of_range_raises(_Config) ->
    Buf = build(8, [10]),
    ?assertError({out_of_range, _, _}, ?BUF:read(7, 1, Buf)),
    ?assertError({out_of_range, _, _}, ?BUF:read(8, 11, Buf)),
    ?assertError({out_of_range, _, _}, ?BUF:read(18, 1, Buf)),
    ?assertError({out_of_range, _, _}, ?BUF:read(0, 1, ?BUF:new(0))).

%% ------------------------------------------------------------------
%% Dropping
%% ------------------------------------------------------------------

drop_before_is_block_granular(_Config) ->
    Buf0 = build(8, [10, 10, 10]),
    %% 25 lies inside the second block [18, 28): only the first block can go.
    Buf = ?BUF:drop_before(25, Buf0),
    ?assertEqual(18, ?BUF:start_pos(Buf)),
    ?assertEqual(2, ?BUF:block_count(Buf)),
    %% Every byte at or past 25 is retained; so are the straddling block's
    %% earlier bytes.
    ?assertEqual(pattern(18, 20), ?BUF:read(18, 20, Buf)).

drop_before_start_is_noop(_Config) ->
    Buf0 = build(8, [10, 10]),
    ?assertEqual(Buf0, ?BUF:drop_before(8, Buf0)),
    ?assertEqual(Buf0, ?BUF:drop_before(0, Buf0)).

drop_before_end_drops_everything(_Config) ->
    Buf0 = build(8, [10, 10]),
    Buf = ?BUF:drop_before(28, Buf0),
    ?assertEqual(28, ?BUF:start_pos(Buf)),
    ?assertEqual(28, ?BUF:end_pos(Buf)),
    ?assertEqual(0, ?BUF:block_count(Buf)),
    %% Appends continue at end_pos.
    Buf1 = ?BUF:append(pattern(28, 5), Buf),
    ?assertEqual(pattern(28, 5), ?BUF:read(28, 5, Buf1)).

drop_never_moves_empty_buffer(_Config) ->
    Buf = ?BUF:new(8),
    %% Dropping past the end of an empty buffer does not reposition it;
    %% repositioning is new/1's job.
    ?assertEqual(Buf, ?BUF:drop_before(1000, Buf)).

%% ------------------------------------------------------------------
%% Sharing and copying
%% ------------------------------------------------------------------

small_read_is_copied(_Config) ->
    %% A small read must not pin its block: the result references only its
    %% own bytes. 303 bytes is the log reader's chunk-header over-read.
    Buf = build(8, [100_000]),
    Data = ?BUF:read(50, 303, Buf),
    ?assertEqual(303, binary:referenced_byte_size(Data)).

large_read_shares_block(_Config) ->
    %% A large single-block read shares the block (no copy): the result
    %% references the whole block's bytes.
    Buf = build(8, [100_000]),
    Data = ?BUF:read(50, 4096, Buf),
    ?assertEqual(4096, byte_size(Data)),
    ?assertEqual(100_000, binary:referenced_byte_size(Data)).

whole_block_read_shares_block_term(_Config) ->
    %% A read covering exactly one whole block returns the block itself.
    Block = pattern(8, 100_000),
    Buf = ?BUF:append(Block, ?BUF:new(8)),
    [Same] = ?BUF:read_iodata(8, 100_000, Buf),
    ?assertEqual(Block, Same),
    ?assertEqual(100_000, binary:referenced_byte_size(Same)).

read_iodata_copies_nothing_at_edges(_Config) ->
    %% A spanning read shares sub-binaries at the edges and whole blocks in
    %% the middle; flattening is the caller's choice.
    Buf = build(0, [1000, 1000, 1000]),
    IoData = ?BUF:read_iodata(500, 2000, Buf),
    ?assertEqual(3, length(IoData)),
    [First, Middle, Last] = IoData,
    ?assertEqual(500, byte_size(First)),
    ?assertEqual(1000, byte_size(Middle)),
    ?assertEqual(500, byte_size(Last)),
    ?assertEqual(pattern(500, 2000), iolist_to_binary(IoData)).

small_read_as_iodata_is_copied(_Config) ->
    %% The cutoff has to hold for iodata reads too, which is how the read path
    %% takes its bytes. Flattening does not save a caller that skips it:
    %% `iolist_to_binary/1` of a one-element list returns that element
    %% unchanged, so a shared sub-binary would still pin its block after the
    %% caller had flattened it - which is what the chunk-header over-read does,
    %% once per chunk, for a ~1 MiB delivery block.
    Buf = build(8, [100_000]),
    [Data] = ?BUF:read_iodata(50, 303, Buf),
    ?assertEqual(303, binary:referenced_byte_size(Data)),
    ?assertEqual(303, binary:referenced_byte_size(iolist_to_binary([Data]))),
    %% Straddling two blocks is the same read, assembled rather than copied.
    Spanning = build(0, [1000, 1000]),
    [Across] = ?BUF:read_iodata(950, 100, Spanning),
    ?assertEqual(pattern(950, 100), Across),
    ?assertEqual(100, binary:referenced_byte_size(Across)).

copy_share_boundary(_Config) ->
    %% Pins down the copy-vs-share cutoff (?COPY_MAX_B = 512). A read at the
    %% threshold is copied and pins nothing; one byte over it is shared as a
    %% sub-binary of the whole block. A flip of `=<` to `<` in `copy_short/2`
    %% would regress the threshold read into pinning the block.
    Buf = build(8, [100_000]),
    AtLimit = ?BUF:read(50, 512, Buf),
    ?assertEqual(512, byte_size(AtLimit)),
    ?assertEqual(512, binary:referenced_byte_size(AtLimit)),
    OverLimit = ?BUF:read(50, 513, Buf),
    ?assertEqual(513, byte_size(OverLimit)),
    ?assertEqual(100_000, binary:referenced_byte_size(OverLimit)).

small_spanning_read_is_copied(_Config) ->
    %% A small read that straddles two blocks cannot be a sub-binary of any one
    %% block, so read/3 assembles it into a fresh binary that pins nothing.
    Buf = build(0, [1000, 1000]),
    Data = ?BUF:read(950, 100, Buf),
    ?assertEqual(pattern(950, 100), Data),
    ?assertEqual(100, binary:referenced_byte_size(Data)).

read_spans_front_and_rear(_Config) ->
    %% Exercises take/4's front/rear split: a drop migrates blocks into `front`,
    %% later appends land in `rear`, and a read then crosses the boundary,
    %% forcing take to drain `front` and reverse `rear`. The deterministic
    %% build/2 helper only appends (everything lands in `rear`), so without this
    %% the split state is left to the stochastic property test.
    Buf0 = build(8, [10, 10, 10]),
    %% Dropping migrates rear into front and advances start_pos into the second
    %% block, leaving front = [b2, b3], rear = [].
    Buf1 = ?BUF:drop_before(20, Buf0),
    ?assertEqual(18, ?BUF:start_pos(Buf1)),
    %% Fresh appends land in rear, so the buffer now straddles both lists.
    %% end_pos is 38 after the drop, so append at 38 first, then at 48.
    Buf2 = ?BUF:append(pattern(48, 10), ?BUF:append(pattern(38, 10), Buf1)),
    ?assertEqual(4, ?BUF:block_count(Buf2)),
    %% A read from a front block into a rear block must be byte-exact.
    ?assertEqual(pattern(30, 25), ?BUF:read(30, 25, Buf2)),
    ?assertEqual(pattern(18, 40), ?BUF:read(18, 40, Buf2)).

%% ------------------------------------------------------------------
%% Interleaving
%% ------------------------------------------------------------------

interleaved_append_read_drop(_Config) ->
    %% Simulates the remote reader's usage: deliveries append, forward reads
    %% drop consumed blocks, and every read's content stays position-exact.
    Deliveries = [1024, 700, 2048, 100, 4096],
    {Buf, _} = lists:foldl(
        fun(Len, {B0, P}) ->
            B1 = ?BUF:append(pattern(P, Len), B0),
            %% Read forward to roughly the middle of the retained window.
            ReadPos = ?BUF:start_pos(B1) + ?BUF:size(B1) div 2,
            ReadLen = min(64, ?BUF:end_pos(B1) - ReadPos),
            ?assertEqual(pattern(ReadPos, ReadLen), ?BUF:read(ReadPos, ReadLen, B1)),
            B2 = ?BUF:drop_before(ReadPos, B1),
            ?assert(?BUF:start_pos(B2) =< ReadPos),
            ?assertEqual(?BUF:end_pos(B1), ?BUF:end_pos(B2)),
            {B2, P + Len}
        end,
        {?BUF:new(8), 8},
        Deliveries
    ),
    ?assert(?BUF:size(Buf) > 0).
