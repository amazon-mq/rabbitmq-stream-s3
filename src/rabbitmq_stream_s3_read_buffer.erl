%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_read_buffer).
-moduledoc """
A contiguous window of stream bytes stored as a queue of immutable binary
blocks.

The remote read path buffers data delivered by S3 range requests and serves
the log reader's reads out of that buffer. Storing the window as a single
binary and growing it with `<<Buffer/binary, Data/binary>>` interacts badly
with the runtime once reads are served from it:

- Serving a read with `binary:part/3` and replying with the result seals the
  underlying writable binary (the runtime pins it when the sub-binary is
  copied into a message), so the next append copies the entire window rather
  than appending in place. In steady state nearly every delivery re-copies
  the whole prefetch window (up to `read_size_max`, 64 MiB).
- A reply sub-binary keeps the entire window generation alive in the
  consumer's heap: a ~300-byte chunk-header read can pin 64 MiB until the
  consumer process happens to collect garbage.

This module avoids both by never appending into a binary. Deliveries are
retained as-is, in order, as sealed blocks; consumed blocks are dropped
whole; reads assemble exactly the requested bytes from the blocks they
overlap. Appending is O(1), dropping frees block-by-block, and a reply pins
at most the blocks it overlaps.

Addressing is absolute: positions are byte offsets within the fragment
object, the same coordinates the remote reader core uses. The buffer covers
`[start_pos, end_pos)` and maintains the invariant that blocks are
contiguous: `end_pos - start_pos` always equals the sum of the block sizes.

Reads of at most 512 bytes are returned as fresh copies rather than
sub-binaries, so the frequent small chunk-header over-reads
(`?CHUNK_HEADER_B + ?MAX_FILTER_SIZE` = 303 bytes) pin nothing at all.
""".

-include("include/rabbitmq_stream_s3.hrl").

%% Reads at or below this size are copied out rather than returned as
%% sub-binaries into a block. Copying a few hundred bytes is cheaper than
%% letting a short read pin a block (typically ~1 MiB, see
%% ?BUFFER_PENDING_DATA_BYTES in the S3 API module) in the consumer's heap.
%% Sized to cover the chunk-header over-read (?CHUNK_HEADER_B +
%% ?MAX_FILTER_SIZE = 303 bytes), the only small read on this path, with
%% headroom; if that over-read ever grows past this, header reads pin a
%% block again.
-define(COPY_MAX_B, 512).
%% Enforced at compile time so a grown filter size cannot silently
%% reintroduce block pinning for header reads.
-if(?CHUNK_HEADER_B + ?MAX_FILTER_SIZE > ?COPY_MAX_B).
-error("chunk-header over-read exceeds COPY_MAX_B; header reads would pin a block").
-endif.

-record(buffer, {
    %% Offset of the first retained byte (the base of the head block), or
    %% the position the next append lands at when the buffer is empty.
    start_pos :: byte_offset(),
    %% Offset one past the last retained byte.
    end_pos :: byte_offset(),
    %% Blocks in offset order, split amortized-deque style: `front` is
    %% oldest-first, `rear` is newest-first. The logical sequence is
    %% `front ++ lists:reverse(rear)`.
    front = [] :: [binary()],
    rear = [] :: [binary()]
}).

-doc "A contiguous byte window over a queue of immutable blocks.".
-opaque buffer() :: #buffer{}.

-export_type([buffer/0]).

-export([
    new/1,
    append/2,
    drop_before/2,
    read/3,
    read_iodata/3,
    start_pos/1,
    end_pos/1,
    size/1,
    block_count/1
]).

-doc "Returns an empty buffer positioned at `Pos`.".
-spec new(byte_offset()) -> buffer().
new(Pos) when is_integer(Pos) andalso Pos >= 0 ->
    #buffer{start_pos = Pos, end_pos = Pos}.

-doc """
Appends a block of bytes at `end_pos`.

The block is retained as-is (no copying); it must never be mutated by the
caller. Empty blocks are not retained.
""".
-spec append(binary(), buffer()) -> buffer().
append(<<>>, Buffer) ->
    Buffer;
append(Block, #buffer{end_pos = EndPos, rear = Rear} = Buffer) when is_binary(Block) ->
    Buffer#buffer{end_pos = EndPos + byte_size(Block), rear = [Block | Rear]}.

-doc """
Drops whole blocks that lie entirely before `Pos`.

Every byte at an offset at or past `Pos` is retained. Bytes before `Pos` in
a block that straddles it are retained too: blocks are dropped whole, so
`start_pos` advances block-granularly, up to but never past `Pos`.
""".
-spec drop_before(byte_offset(), buffer()) -> buffer().
drop_before(Pos, #buffer{start_pos = StartPos} = Buffer) when Pos =< StartPos ->
    Buffer;
drop_before(Pos, #buffer{start_pos = StartPos, front = [Block | Front]} = Buffer) when
    StartPos + byte_size(Block) =< Pos
->
    drop_before(Pos, Buffer#buffer{start_pos = StartPos + byte_size(Block), front = Front});
drop_before(Pos, #buffer{front = [], rear = [_ | _] = Rear} = Buffer) ->
    drop_before(Pos, Buffer#buffer{front = lists:reverse(Rear), rear = []});
drop_before(_Pos, Buffer) ->
    Buffer.

-doc """
Reads `Len` bytes at `Pos` as a single binary.

The range must lie within `[start_pos, end_pos)`. Reads of at most 512
bytes are copied so they pin no block; larger reads that fall within one
block are shared as a sub-binary of that block; reads spanning blocks are
assembled into a fresh binary.
""".
-spec read(byte_offset(), non_neg_integer(), buffer()) -> binary().
read(Pos, Len, Buffer) ->
    case read_iodata(Pos, Len, Buffer) of
        [Bin] when Len =< ?COPY_MAX_B ->
            binary:copy(Bin);
        [Bin] ->
            Bin;
        IoData ->
            iolist_to_binary(IoData)
    end.

-doc """
Reads `Len` bytes at `Pos` as a list of binaries, copying nothing.

The range must lie within `[start_pos, end_pos)`. Each element is either a
whole block or a sub-binary of the block at the range's edge, so the result
shares (and pins) only the blocks the range overlaps.
""".
-spec read_iodata(byte_offset(), non_neg_integer(), buffer()) -> [binary()].
read_iodata(_Pos, 0, _Buffer) ->
    [];
read_iodata(Pos, Len, #buffer{start_pos = StartPos, end_pos = EndPos} = Buffer) when
    is_integer(Pos) andalso is_integer(Len) andalso Len > 0 andalso
        Pos >= StartPos andalso Pos + Len =< EndPos
->
    #buffer{front = Front, rear = Rear} = Buffer,
    take(Pos - StartPos, Len, Front, Rear);
read_iodata(Pos, Len, #buffer{start_pos = StartPos, end_pos = EndPos}) ->
    error({out_of_range, {Pos, Len}, {StartPos, EndPos}}).

%% Walks `front` and reverses `rear` only if the requested range extends
%% into it, so a read near start_pos never pays for the whole block list.
take(_Skip, 0, _Front, _Rear) ->
    [];
take(_Skip, _Len, [], []) ->
    %% No blocks remain but Len > 0 (the clause above consumed Len == 0). This
    %% is unreachable while read_iodata/3's range check guards every call, but
    %% crashing here turns a broken invariant into a loud error rather than an
    %% infinite loop: the clause below would otherwise spin on lists:reverse([]).
    error(buffer_underrun);
take(Skip, Len, [], Rear) ->
    take(Skip, Len, lists:reverse(Rear), []);
take(Skip, Len, [Block | Blocks], Rear) when Skip >= byte_size(Block) ->
    take(Skip - byte_size(Block), Len, Blocks, Rear);
take(0, Len, [Block | Blocks], Rear) when Len >= byte_size(Block) ->
    %% The whole block is in range: share the block term itself rather than
    %% wrapping it in a full-range sub-binary.
    [Block | take(0, Len - byte_size(Block), Blocks, Rear)];
take(Skip, Len, [Block | Blocks], Rear) when Skip + Len > byte_size(Block) ->
    Taken = byte_size(Block) - Skip,
    [binary:part(Block, Skip, Taken) | take(0, Len - Taken, Blocks, Rear)];
take(Skip, Len, [Block | _Blocks], _Rear) ->
    [binary:part(Block, Skip, Len)].

-doc "Returns the offset of the first retained byte.".
-spec start_pos(buffer()) -> byte_offset().
start_pos(#buffer{start_pos = StartPos}) ->
    StartPos.

-doc "Returns the offset one past the last retained byte.".
-spec end_pos(buffer()) -> byte_offset().
end_pos(#buffer{end_pos = EndPos}) ->
    EndPos.

-doc "Returns the number of retained bytes.".
-spec size(buffer()) -> non_neg_integer().
size(#buffer{start_pos = StartPos, end_pos = EndPos}) ->
    EndPos - StartPos.

-doc "Returns the number of retained blocks.".
-spec block_count(buffer()) -> non_neg_integer().
block_count(#buffer{front = Front, rear = Rear}) ->
    length(Front) + length(Rear).
