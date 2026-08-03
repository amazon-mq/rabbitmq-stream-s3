%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(read_buffer_bench).
-moduledoc """
Benchmarks the remote reader's buffer under its real access pattern: S3
deliveries append ~1 MiB batches while the consumer's reads interleave
(small chunk-header over-reads plus chunk-data reads), each read replied to
another process, with the consumed prefix discarded as reads advance.

Three scenarios:

- `flat_binary`: the pre-block-queue implementation, kept for comparison.
  Grows one binary per delivery and serves reads as sub-binaries. Replying
  seals the writable binary (the runtime pins it when the sub-binary is
  copied into the message), so each subsequent append copies the whole
  window. This is the regression baseline: if `block_queue` ever
  approaches it, something reintroduced window-sized copying.
- `flat_binary_spanning`: as `flat_binary` but with 2 MiB reads, one per
  delivery. Each read event replies a sub-binary of a *different* buffer
  generation (the seal forces a fresh generation per delivery), so a
  consumer retaining replies across read events pins one full window per
  event — overlapping content, distinct binaries.
- `block_queue`: `rabbitmq_stream_s3_read_buffer` doing the same work.
- `block_queue_spanning`: as `block_queue` but with 2 MiB reads so every
  read spans blocks and pays the assembly copy, bounding the worst case.

One iteration processes a full window (`?DELIVERIES` x `?DELIVERY_B`).

Two phases. The timing phase measures buffer mechanics only, so every
delivery appends the same shared template binary (allocation cost excluded
equally from all scenarios). The memory phase copies the template per
delivery, as real S3 deliveries arrive as fresh binaries, and reports two
direct pinning measurements:

- Peak VM binary memory over the run (captures dead buffer generations and
  in-flight copies that no per-process view shows)
- Bytes kept alive by a consumer holding the last `?RETAINED_REPLIES`
  replies, measured from the sink's `process_info(_, binary)` after a
  forced GC, deduplicated per binary instance. The forced GC makes the
  number deterministic: only live references pin anything. Comparing it
  against the byte size of the replies themselves gives the pinning
  amplification (collateral memory per byte actually held).

Finally, a supply x demand matrix sweeps block size (what the S3 API
layer's batching supplies) against read size (what the consumer demands),
with a uniform-read consumer under the same prefetch lag. Block size is
the block queue's pinning bound, so the matrix is what justifies the
choice of `?BUFFER_PENDING_DATA_BYTES`; the flat binary appears as a
baseline row.
""".

-export([run/0]).
%% Exported for ad-hoc experiments in a shell.
-export([
    flat_binary_window/2,
    flat_binary_spanning_window/2,
    block_queue_window/2,
    block_queue_spanning_window/2
]).

%% 16 MiB window per iteration: large enough that flat_binary's quadratic
%% copying dominates, small enough that a bench run takes seconds.
-define(DELIVERY_B, 1_048_576).
-define(DELIVERIES, 16).
%% The prefetch runs ahead of the consumer; that is what the prefetch window
%% is for. The consumer starts reading only once this many unconsumed bytes are
%% buffered and then consumes one delivery's worth per delivery, holding the
%% lag steady (half the window). Without the lag the consumer tracks the
%% delivery edge, the old compaction keeps even the flat binary tiny, and
%% both the copying and the pinning vanish from the measurement.
-define(PREFETCH_LAG_B, 8_388_608).
%% Per delivery: chunk-header over-reads (?CHUNK_HEADER_B + ?MAX_FILTER_SIZE)
%% and chunk-data reads, spread across one delivery's worth of bytes at the
%% lagged read position.
-define(READS_PER_DELIVERY, 4).
-define(HEADER_READ_B, 303).
-define(CHUNK_READ_B, 32_768).
-define(SPANNING_READ_B, 2_097_152).
%% Replies a consumer plausibly holds at once (a delivery's worth of header
%% and chunk reads in the socket send queue).
-define(RETAINED_REPLIES, 8).
%% How long each memory-phase scenario runs (many windows, so the peak
%% reflects steady state rather than the first window).
-define(MEMORY_PHASE_MS, 300).

%% Supply x demand matrix axes: block sizes the S3 API layer could batch
%% to, against read sizes from the header over-read up to a large chunk.
-define(MATRIX_BLOCK_SIZES, [262_144, 1_048_576, 4_194_304]).
-define(MATRIX_READ_SIZES, [303, 32_768, 262_144, 2_097_152]).
%% Bytes supplied per matrix window; deliveries per window is this divided
%% by the row's block size.
-define(MATRIX_WINDOW_B, 16_777_216).
-define(MATRIX_CELL_MS, 250).

-spec run() -> ok.
run() ->
    Template = rand:bytes(?DELIVERY_B),
    Sink = spawn_link(fun sink/0),
    %% Timing phase: shared template, buffer mechanics only.
    SameBlock = fun() -> Template end,
    ok = rabbitmq_stream_s3_bench:run([
        {flat_binary, fun() -> flat_binary_window(SameBlock, Sink) end},
        {flat_binary_spanning, fun() -> flat_binary_spanning_window(SameBlock, Sink) end},
        {block_queue, fun() -> block_queue_window(SameBlock, Sink) end},
        {block_queue_spanning, fun() -> block_queue_spanning_window(SameBlock, Sink) end}
    ]),
    unlink(Sink),
    exit(Sink, kill),
    memory_report(Template),
    matrix_report().

sink() ->
    receive
        _ -> sink()
    end.

%% ------------------------------------------------------------------
%% Memory phase
%% ------------------------------------------------------------------

memory_report(Template) ->
    io:format(
        "~nMemory (fresh 1 MiB blocks; sink retains last ~b replies, then GCs):~n"
        "~-22s ~14s ~14s ~14s ~14s~n",
        [?RETAINED_REPLIES, "Name", "peak binary", "reply bytes", "pinned", "amplification"]
    ),
    lists:foreach(
        fun({Name, WindowFun}) ->
            NewBlock = fun() -> binary:copy(Template) end,
            Sink = spawn_link(fun() -> retaining_sink([]) end),
            #{baseline := Baseline, peak := Peak} = rabbitmq_stream_s3_bench:sample_binary_memory(
                fun() ->
                    repeat_for(?MEMORY_PHASE_MS, fun() -> WindowFun(NewBlock, Sink) end)
                end
            ),
            Sink ! {report, self()},
            receive
                {pinned, ReplyBytes, Pinned} ->
                    io:format(
                        "~-22s ~14s ~14s ~14s ~13.1fx~n",
                        [
                            Name,
                            format_bytes(Peak - Baseline),
                            format_bytes(ReplyBytes),
                            format_bytes(Pinned),
                            Pinned / max(1, ReplyBytes)
                        ]
                    )
            end,
            %% Wait for the sink to be gone so its pinned references cannot
            %% leak into the next scenario's baseline.
            MRef = erlang:monitor(process, Sink),
            unlink(Sink),
            exit(Sink, kill),
            receive
                {'DOWN', MRef, process, Sink, _} -> ok
            end
        end,
        [
            {flat_binary, fun flat_binary_window/2},
            {flat_binary_spanning, fun flat_binary_spanning_window/2},
            {block_queue, fun block_queue_window/2},
            {block_queue_spanning, fun block_queue_spanning_window/2}
        ]
    ),
    ok.

%% ------------------------------------------------------------------
%% Supply x demand matrix
%% ------------------------------------------------------------------

matrix_report() ->
    Rows = [{flat, ?DELIVERY_B} | [{queue, B} || B <- ?MATRIX_BLOCK_SIZES]],
    print_grid(
        io_lib:format(
            "Matrix: average time per ~s window (block size x read size, ~s lag)",
            [format_bytes(?MATRIX_WINDOW_B), format_bytes(?PREFETCH_LAG_B)]
        ),
        Rows,
        fun(Impl, BlockSize, ReadSize) ->
            Template = rand:bytes(BlockSize),
            Sink = spawn_link(fun sink/0),
            Window = fun() ->
                matrix_window(Impl, BlockSize, ReadSize, fun() -> Template end, Sink)
            end,
            %% Warm briefly, then time.
            repeat_for(?MATRIX_CELL_MS div 5, Window),
            T0 = erlang:monotonic_time(microsecond),
            N = count_for(?MATRIX_CELL_MS, Window),
            Elapsed = erlang:monotonic_time(microsecond) - T0,
            unlink(Sink),
            exit(Sink, kill),
            format_cell_us(Elapsed / N)
        end
    ),
    print_grid(
        io_lib:format(
            "Matrix: pinning amplification (sink retains last ~b replies, then GCs)",
            [?RETAINED_REPLIES]
        ),
        Rows,
        fun(Impl, BlockSize, ReadSize) ->
            Template = rand:bytes(BlockSize),
            NewBlock = fun() -> binary:copy(Template) end,
            Sink = spawn_link(fun() -> retaining_sink([]) end),
            repeat_for(?MATRIX_CELL_MS, fun() ->
                matrix_window(Impl, BlockSize, ReadSize, NewBlock, Sink)
            end),
            Sink ! {report, self()},
            Cell =
                receive
                    {pinned, ReplyBytes, Pinned} ->
                        io_lib:format("~.1fx", [Pinned / max(1, ReplyBytes)])
                end,
            MRef = erlang:monitor(process, Sink),
            unlink(Sink),
            exit(Sink, kill),
            receive
                {'DOWN', MRef, process, Sink, _} -> ok
            end,
            Cell
        end
    ).

print_grid(Title, Rows, CellFun) ->
    io:format("~n~ts~n~-22s", [Title, ""]),
    lists:foreach(
        fun(ReadSize) -> io:format(" ~12s", [["read ", format_bytes(ReadSize)]]) end,
        ?MATRIX_READ_SIZES
    ),
    io:format("~n"),
    lists:foreach(
        fun({Impl, BlockSize}) ->
            Label =
                case Impl of
                    flat -> ["flat ", format_bytes(BlockSize), " deliv"];
                    queue -> ["queue ", format_bytes(BlockSize), " blocks"]
                end,
            io:format("~-22ts", [Label]),
            lists:foreach(
                fun(ReadSize) ->
                    io:format(" ~12ts", [CellFun(Impl, BlockSize, ReadSize)])
                end,
                ?MATRIX_READ_SIZES
            ),
            io:format("~n")
        end,
        Rows
    ).

%% One window: supply ?MATRIX_WINDOW_B bytes in BlockSize deliveries; after
%% each delivery the consumer issues uniform ReadSize reads from its floor
%% while more than the prefetch lag is buffered ahead of it.
matrix_window(queue, BlockSize, ReadSize, NewBlock, Sink) ->
    Deliveries = ?MATRIX_WINDOW_B div BlockSize,
    lists:foldl(
        fun(_, {Buffer0, Floor0}) ->
            Buffer1 = rabbitmq_stream_s3_read_buffer:append(NewBlock(), Buffer0),
            EndPos = rabbitmq_stream_s3_read_buffer:end_pos(Buffer1),
            Floor = matrix_consume_queue(Floor0, EndPos, ReadSize, Buffer1, Sink),
            {rabbitmq_stream_s3_read_buffer:drop_before(Floor, Buffer1), Floor}
        end,
        {rabbitmq_stream_s3_read_buffer:new(0), 0},
        lists:seq(1, Deliveries)
    ),
    ok;
matrix_window(flat, BlockSize, ReadSize, NewBlock, Sink) ->
    Deliveries = ?MATRIX_WINDOW_B div BlockSize,
    lists:foldl(
        fun(_, {Buffer0, StartPos, CurrentPos, EndPos0}) ->
            Delivery = NewBlock(),
            Buffer1 =
                case CurrentPos =:= StartPos of
                    true ->
                        <<Buffer0/binary, Delivery/binary>>;
                    false ->
                        <<
                            (binary:part(
                                Buffer0, CurrentPos - StartPos, EndPos0 - CurrentPos
                            ))/binary,
                            Delivery/binary
                        >>
                end,
            EndPos = EndPos0 + byte_size(Delivery),
            Floor = matrix_consume_flat(CurrentPos, CurrentPos, EndPos, ReadSize, Buffer1, Sink),
            {Buffer1, CurrentPos, Floor, EndPos}
        end,
        {<<>>, 0, 0, 0},
        lists:seq(1, Deliveries)
    ),
    ok.

matrix_consume_queue(Floor, EndPos, ReadSize, Buffer, Sink) ->
    case consumer_ready(Floor, EndPos) of
        true ->
            Sink ! {ok, rabbitmq_stream_s3_read_buffer:read(Floor, ReadSize, Buffer)},
            matrix_consume_queue(Floor + ReadSize, EndPos, ReadSize, Buffer, Sink);
        false ->
            Floor
    end.

matrix_consume_flat(Base, Floor, EndPos, ReadSize, Buffer, Sink) ->
    case consumer_ready(Floor, EndPos) of
        true ->
            Sink ! {ok, binary:part(Buffer, Floor - Base, ReadSize)},
            matrix_consume_flat(Base, Floor + ReadSize, EndPos, ReadSize, Buffer, Sink);
        false ->
            Floor
    end.

format_cell_us(Us) when Us >= 1000 -> io_lib:format("~.2f ms", [Us / 1000]);
format_cell_us(Us) -> io_lib:format("~b us", [round(Us)]).

count_for(Ms, Fun) ->
    Deadline = erlang:monotonic_time(millisecond) + Ms,
    count_until(Deadline, Fun, 0).

count_until(Deadline, Fun, N) ->
    _ = Fun(),
    case erlang:monotonic_time(millisecond) >= Deadline of
        true -> N + 1;
        false -> count_until(Deadline, Fun, N + 1)
    end.

%% Keeps the newest ?RETAINED_REPLIES replies alive, then reports how much
%% binary memory those live references pin once everything dead is gone.
retaining_sink(Ring) ->
    receive
        {ok, Data} ->
            retaining_sink(lists:sublist([Data | Ring], ?RETAINED_REPLIES));
        {report, From} ->
            erlang:garbage_collect(),
            ReplyBytes = lists:sum([byte_size(D) || D <- Ring]),
            {binary, Bins} = process_info(self(), binary),
            %% One entry per reference; several references into the same
            %% binary must count its bytes once.
            Deduped = maps:from_list([{Id, Size} || {Id, Size, _RefCount} <- Bins]),
            From ! {pinned, ReplyBytes, lists:sum(maps:values(Deduped))},
            retaining_sink(Ring)
    end.

repeat_for(Ms, Fun) ->
    Deadline = erlang:monotonic_time(millisecond) + Ms,
    repeat_until(Deadline, Fun).

repeat_until(Deadline, Fun) ->
    _ = Fun(),
    case erlang:monotonic_time(millisecond) >= Deadline of
        true -> ok;
        false -> repeat_until(Deadline, Fun)
    end.

format_bytes(B) when B >= 1 bsl 20 -> io_lib:format("~.1f MiB", [B / (1 bsl 20)]);
format_bytes(B) when B >= 1 bsl 10 -> io_lib:format("~.1f KiB", [B / (1 bsl 10)]);
format_bytes(B) -> io_lib:format("~b B", [B]).

%% ------------------------------------------------------------------
%% Scenario: flat binary (the previous remote reader buffer)
%% ------------------------------------------------------------------

flat_binary_window(NewBlock, Sink) ->
    lists:foldl(
        fun(_, {Buffer0, StartPos, CurrentPos, EndPos0}) ->
            Delivery = NewBlock(),
            %% add_data_current from before the block queue: append, or
            %% compact-and-append once reads have consumed a prefix.
            Buffer1 =
                case CurrentPos =:= StartPos of
                    true ->
                        <<Buffer0/binary, Delivery/binary>>;
                    false ->
                        <<
                            (binary:part(
                                Buffer0, CurrentPos - StartPos, EndPos0 - CurrentPos
                            ))/binary,
                            Delivery/binary
                        >>
                end,
            EndPos = EndPos0 + byte_size(Delivery),
            ReadFloor =
                case consumer_ready(CurrentPos, EndPos) of
                    true ->
                        lists:foreach(
                            fun(I) ->
                                Pos = read_pos(CurrentPos, I),
                                Sink !
                                    {ok,
                                        binary:part(
                                            Buffer1, Pos - CurrentPos, ?HEADER_READ_B
                                        )},
                                Sink !
                                    {ok,
                                        binary:part(
                                            Buffer1,
                                            Pos + ?HEADER_READ_B - CurrentPos,
                                            ?CHUNK_READ_B
                                        )}
                            end,
                            lists:seq(0, ?READS_PER_DELIVERY - 1)
                        ),
                        CurrentPos + ?DELIVERY_B;
                    false ->
                        CurrentPos
                end,
            {Buffer1, CurrentPos, ReadFloor, EndPos}
        end,
        {<<>>, 0, 0, 0},
        lists:seq(1, ?DELIVERIES)
    ),
    ok.

%% As flat_binary_window but serving one ?SPANNING_READ_B sub-binary read
%% per delivery, the flat counterpart of block_queue_spanning. Each reply
%% comes from that delivery's freshly copied buffer generation.
flat_binary_spanning_window(NewBlock, Sink) ->
    lists:foldl(
        fun(_, {Buffer0, StartPos, CurrentPos, EndPos0}) ->
            Delivery = NewBlock(),
            Buffer1 =
                case CurrentPos =:= StartPos of
                    true ->
                        <<Buffer0/binary, Delivery/binary>>;
                    false ->
                        <<
                            (binary:part(
                                Buffer0, CurrentPos - StartPos, EndPos0 - CurrentPos
                            ))/binary,
                            Delivery/binary
                        >>
                end,
            EndPos = EndPos0 + byte_size(Delivery),
            ReadFloor =
                case consumer_ready(CurrentPos, EndPos) of
                    true ->
                        Sink ! {ok, binary:part(Buffer1, 0, ?SPANNING_READ_B)},
                        CurrentPos + ?SPANNING_READ_B;
                    false ->
                        CurrentPos
                end,
            {Buffer1, CurrentPos, ReadFloor, EndPos}
        end,
        {<<>>, 0, 0, 0},
        lists:seq(1, ?DELIVERIES)
    ),
    ok.

%% ------------------------------------------------------------------
%% Scenario: block queue (rabbitmq_stream_s3_read_buffer)
%% ------------------------------------------------------------------

block_queue_window(NewBlock, Sink) ->
    lists:foldl(
        fun(_, {Buffer0, Floor0}) ->
            Buffer1 = rabbitmq_stream_s3_read_buffer:append(NewBlock(), Buffer0),
            EndPos = rabbitmq_stream_s3_read_buffer:end_pos(Buffer1),
            Floor =
                case consumer_ready(Floor0, EndPos) of
                    true ->
                        lists:foreach(
                            fun(I) ->
                                Pos = read_pos(Floor0, I),
                                Sink !
                                    {ok,
                                        rabbitmq_stream_s3_read_buffer:read(
                                            Pos, ?HEADER_READ_B, Buffer1
                                        )},
                                Sink !
                                    {ok,
                                        rabbitmq_stream_s3_read_buffer:read(
                                            Pos + ?HEADER_READ_B, ?CHUNK_READ_B, Buffer1
                                        )}
                            end,
                            lists:seq(0, ?READS_PER_DELIVERY - 1)
                        ),
                        Floor0 + ?DELIVERY_B;
                    false ->
                        Floor0
                end,
            {rabbitmq_stream_s3_read_buffer:drop_before(Floor, Buffer1), Floor}
        end,
        {rabbitmq_stream_s3_read_buffer:new(0), 0},
        lists:seq(1, ?DELIVERIES)
    ),
    ok.

block_queue_spanning_window(NewBlock, Sink) ->
    lists:foldl(
        fun(_, {Buffer0, Floor0}) ->
            Buffer1 = rabbitmq_stream_s3_read_buffer:append(NewBlock(), Buffer0),
            EndPos = rabbitmq_stream_s3_read_buffer:end_pos(Buffer1),
            Floor =
                case consumer_ready(Floor0, EndPos) of
                    true ->
                        Sink !
                            {ok,
                                rabbitmq_stream_s3_read_buffer:read(
                                    Floor0, ?SPANNING_READ_B, Buffer1
                                )},
                        Floor0 + ?SPANNING_READ_B;
                    false ->
                        Floor0
                end,
            {rabbitmq_stream_s3_read_buffer:drop_before(Floor, Buffer1), Floor}
        end,
        {rabbitmq_stream_s3_read_buffer:new(0), 0},
        lists:seq(1, ?DELIVERIES)
    ),
    ok.

%% The consumer reads only while more than the prefetch lag is buffered
%% ahead of it, consuming from its floor. Forward-moving, so every read is
%% at or past the previous one (the log reader's pattern).
consumer_ready(Floor, EndPos) ->
    EndPos - Floor > ?PREFETCH_LAG_B.

read_pos(ReadBase, I) ->
    Step = ?DELIVERY_B div ?READS_PER_DELIVERY,
    ReadBase + I * Step.
