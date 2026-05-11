%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_fragment_assembly).
-moduledoc """
Pure logic for assembling chunk metadata into fragments.

Tracks in-progress fragment state and decides when to cut based on
size threshold. Does not perform I/O.
""".

-include("include/rabbitmq_stream_s3.hrl").

-export([
    new/1,
    add_chunk/2,
    is_cut/1,
    metadata/1,
    index_records/1,
    info/1
]).

-export_type([state/0, chunk/0, fragment_meta/0]).

-type chunk() :: #{
    chunk_id := osiris:offset(),
    timestamp := osiris:timestamp(),
    num_records := non_neg_integer(),
    data_size := non_neg_integer(),
    position := non_neg_integer(),
    next_position := non_neg_integer(),
    segment_offset := osiris:offset(),
    crc := non_neg_integer()
}.

-record(span, {
    segment_offset :: osiris:offset(),
    start_pos :: non_neg_integer(),
    end_pos :: non_neg_integer()
}).

-record(chunk_meta, {
    chunk_id :: osiris:offset(),
    timestamp :: osiris:timestamp(),
    position :: non_neg_integer(),
    segment_offset :: osiris:offset(),
    crc :: non_neg_integer()
}).

-record(state, {
    target_size :: non_neg_integer(),
    first_offset :: osiris:offset() | undefined,
    first_timestamp :: osiris:timestamp() | undefined,
    last_timestamp :: osiris:timestamp() | undefined,
    next_offset :: osiris:offset() | undefined,
    size = 0 :: non_neg_integer(),
    num_chunks = 0 :: non_neg_integer(),
    spans = [] :: [#span{}],
    %% Accumulated in reverse order for efficient prepend.
    chunks = [] :: [#chunk_meta{}],
    cut = false :: boolean()
}).

-type state() :: #state{}.

-type fragment_meta() :: #{
    first_offset := osiris:offset(),
    first_timestamp := osiris:timestamp(),
    last_timestamp := osiris:timestamp(),
    next_offset := osiris:offset(),
    size := non_neg_integer(),
    num_chunks := non_neg_integer(),
    spans := [
        {
            SegmentOffset :: osiris:offset(),
            StartPos :: non_neg_integer(),
            EndPos :: non_neg_integer()
        }
    ]
}.

-doc "Create a new fragment assembly state with the given size target.".
-spec new(non_neg_integer()) -> state().
new(TargetSize) ->
    #state{target_size = TargetSize}.

-doc """
Add a chunk to the in-progress fragment.

If adding this chunk causes the fragment to exceed the size target,
the fragment is cut. The chunk is still included in the cut fragment.
""".
-spec add_chunk(chunk(), state()) -> state().
add_chunk(Chunk, #state{cut = false} = State0) ->
    #{
        chunk_id := ChunkId,
        timestamp := Ts,
        num_records := NumRecords,
        position := Pos,
        next_position := NextPos,
        segment_offset := SegOffset,
        crc := Crc
    } = Chunk,
    State1 =
        case State0#state.first_offset of
            undefined ->
                State0#state{
                    first_offset = ChunkId,
                    first_timestamp = Ts
                };
            _ ->
                State0
        end,
    Size = State1#state.size + (NextPos - Pos),
    NumChunks = State1#state.num_chunks + 1,
    Spans = update_spans(SegOffset, Pos, NextPos, State1#state.spans),
    Chunks = [
        #chunk_meta{
            chunk_id = ChunkId,
            timestamp = Ts,
            position = Pos,
            segment_offset = SegOffset,
            crc = Crc
        }
        | State1#state.chunks
    ],
    State2 = State1#state{
        last_timestamp = Ts,
        next_offset = ChunkId + NumRecords,
        size = Size,
        num_chunks = NumChunks,
        spans = Spans,
        chunks = Chunks
    },
    case Size >= State2#state.target_size of
        true -> State2#state{cut = true};
        false -> State2
    end.

-doc "Returns true if the fragment has been cut.".
-spec is_cut(state()) -> boolean().
is_cut(#state{cut = Cut}) ->
    Cut.

-doc "Returns a summary map for debugging.".
-spec info(state()) -> map().
info(#state{
    target_size = Target,
    first_offset = FirstOffset,
    next_offset = NextOffset,
    size = Size,
    num_chunks = NumChunks,
    cut = Cut
}) ->
    #{
        target_size => Target,
        first_offset => FirstOffset,
        next_offset => NextOffset,
        size => Size,
        num_chunks => NumChunks,
        cut => Cut
    }.

-doc "Returns the metadata for a cut fragment.".
-spec metadata(state()) -> fragment_meta().
metadata(#state{
    first_offset = FirstOffset,
    first_timestamp = FirstTs,
    last_timestamp = LastTs,
    next_offset = NextOffset,
    size = Size,
    num_chunks = NumChunks,
    spans = Spans
}) ->
    #{
        first_offset => FirstOffset,
        first_timestamp => FirstTs,
        last_timestamp => LastTs,
        next_offset => NextOffset,
        size => Size,
        num_chunks => NumChunks,
        spans => [
            {O, S, E}
         || #span{segment_offset = O, start_pos = S, end_pos = E} <-
                lists:reverse(Spans)
        ]
    }.

-doc """
Produce the 20-byte remote index records for the fragment.

Each record is: ChunkId:64, Timestamp:64, FragmentPos:32.
FragmentPos is the byte offset within the fragment object (after the
8-byte header) where the chunk starts.
""".
-spec index_records(state()) -> binary().
index_records(#state{chunks = Chunks, spans = Spans}) ->
    SpanBase = compute_span_bases(Spans),
    lists:foldr(
        fun(
            #chunk_meta{
                chunk_id = ChunkId,
                timestamp = Ts,
                position = Pos,
                segment_offset = SegOff
            },
            Acc
        ) ->
            <<Acc/binary, ChunkId:64/unsigned, Ts:64/signed,
                (fragment_pos(SegOff, Pos, SpanBase)):32/unsigned>>
        end,
        <<>>,
        Chunks
    ).

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

%% Compute the fragment-relative base offset for each span.
%% The fragment object is: [8-byte header][span0 data][span1 data][...][index]
%% So span0 starts at byte 0 (relative to data start after header),
%% span1 starts at span0 length, etc.
compute_span_bases(Spans) ->
    {Bases, _} = lists:mapfoldr(
        fun(#span{segment_offset = O, start_pos = S, end_pos = E}, Acc) ->
            {{O, S, Acc}, Acc + (E - S)}
        end,
        0,
        Spans
    ),
    Bases.

%% Convert a segment file position to a fragment-relative position.
fragment_pos(SegOffset, SegPos, [{SegOffset, SpanStart, Base} | _]) ->
    Base + (SegPos - SpanStart);
fragment_pos(SegOffset, SegPos, [_ | Rest]) ->
    fragment_pos(SegOffset, SegPos, Rest).

update_spans(SegOffset, Pos, NextPos, []) ->
    [#span{segment_offset = SegOffset, start_pos = Pos, end_pos = NextPos}];
update_spans(SegOffset, _Pos, NextPos, [#span{segment_offset = SegOffset} = Span | Rest]) ->
    [Span#span{end_pos = NextPos} | Rest];
update_spans(SegOffset, Pos, NextPos, Spans) ->
    [#span{segment_offset = SegOffset, start_pos = Pos, end_pos = NextPos} | Spans].
