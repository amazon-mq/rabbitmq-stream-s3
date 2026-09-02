%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_test_helpers).
-moduledoc """
Shared test helpers for rabbitmq_stream_s3 CT suites.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbitmq_stream_s3/include/rabbitmq_stream_s3.hrl").

-compile([export_all, nowarn_export_all]).

%% Default deadline for the await_offset/1,2 barriers. The reader only has to
%% persist a manifest past the target, which is fast in isolation, but under
%% full-suite CPU contention it occasionally exceeds a tight deadline and the
%% gen_server:call exits with {timeout, _}. This is generous enough to absorb
%% that contention without masking a genuine stall (a real freeze never
%% clears, however long the wait). Callers that need a different deadline use
%% await_offset/3.
-define(AWAIT_OFFSET_TIMEOUT_MS, 5000).

%% ------------------------------------------------------------------
%% Log seeding DSL
%% ------------------------------------------------------------------

-type chunk_spec() :: {chunk, #{size := non_neg_integer(), records => non_neg_integer()}}.
-type segment_spec() :: {segment, [chunk_spec()]}.
-type log_metadata() :: #{
    next_offset := non_neg_integer(),
    total_size := non_neg_integer(),
    segments := [
        #{offset := non_neg_integer(), size := non_neg_integer(), num_chunks := non_neg_integer()}
    ],
    chunks := [
        #{
            offset := non_neg_integer(),
            records := non_neg_integer(),
            size := non_neg_integer(),
            segment_offset := non_neg_integer()
        }
    ]
}.

-doc """
Seed a local osiris log with deterministic chunk layout.

Writes chunks directly via `osiris_log:write/5`, bypassing the writer
process. Each `{chunk, #{size => S}}` becomes exactly one chunk on disk
with `data_size = S`. Segment boundaries are explicit.

Usage:

    Meta = seed_log(Config, [
        {segment, [
            {chunk, #{size => 300}},
            {chunk, #{size => 300}}
        ]},
        {segment, [
            {chunk, #{size => 300}}
        ]}
    ])

Returns metadata for use in assertions and barrier calls.
""".
-spec seed_log(ct_suite:ct_config(), [segment_spec()]) -> log_metadata().
seed_log(Config, SegmentSpecs) ->
    WriterCfg = ?config(writer_cfg, Config),
    OsirisDir = ?config(osiris_dir, Config),
    Name = maps:get(name, WriterCfg),
    Dir = filename:join(OsirisDir, Name),
    ok = filelib:ensure_path(Dir),
    Shared = osiris_log_shared:new(),
    seed_segments(Dir, Name, Shared, SegmentSpecs, 0, [], []).

seed_segments(_Dir, _Name, _Shared, [], _Offset, SegAcc, ChunkAcc) ->
    Chunks = ChunkAcc,
    Segments = lists:reverse(SegAcc),
    TotalSize = lists:sum([maps:get(size, C) || C <- Chunks]),
    NextOffset =
        case Chunks of
            [] -> 0;
            _ -> maps:get(offset, lists:last(Chunks)) + maps:get(records, lists:last(Chunks))
        end,
    #{
        next_offset => NextOffset,
        total_size => TotalSize,
        segments => Segments,
        chunks => Chunks
    };
seed_segments(Dir, Name, Shared, [{segment, ChunkSpecs} | Rest], Offset, SegAcc, ChunkAcc) ->
    MaxSegSize =
        case SegAcc of
            [] -> 1_000_000_000;
            _ -> 9
        end,
    LogCfg = #{
        dir => Dir,
        name => Name,
        epoch => 1,
        shared => Shared,
        max_segment_size_bytes => MaxSegSize,
        log_hooks => undefined,
        readers_counter_fun => fun(_) -> ok end,
        options => #{}
    },
    Log0 = osiris_log:init(LogCfg),
    {Log1, NextOffset, SegChunks} = write_chunks(Log0, ChunkSpecs, Offset, []),
    osiris_log:close(Log1),
    SegSize = lists:sum([maps:get(size, C) || C <- SegChunks]),
    SegMeta = #{offset => Offset, size => SegSize, num_chunks => length(SegChunks)},
    seed_segments(Dir, Name, Shared, Rest, NextOffset, [SegMeta | SegAcc], ChunkAcc ++ SegChunks).

write_chunks(Log, [], Offset, Acc) ->
    {Log, Offset, lists:reverse(Acc)};
write_chunks(Log0, [{chunk, Props} | Rest], Offset, Acc) ->
    Size = maps:get(size, Props),
    Records = maps:get(records, Props, 1),
    Entries = make_entries(Offset, Records, Size),
    Now = Offset * 10,
    Log1 = osiris_log:write(Entries, Now, Log0),
    ChunkMeta = #{
        offset => Offset,
        records => Records,
        size => Size,
        segment_offset => segment_offset_from_log(Log0)
    },
    write_chunks(Log1, Rest, Offset + Records, [ChunkMeta | Acc]).

%% Generate N entries whose encoded data_size totals exactly Size bytes.
%% Each entry is encoded as <<0:1, Len:31, Data/binary>> (4 + Len bytes).
%% So total data_size = N * 4 + sum(payload lengths) = Size.
%% We distribute payload evenly, with remainder on the last record.
%% Entries are returned in reverse order as osiris_log:write/3 expects.
make_entries(StartOffset, N, Size) ->
    TotalPayload = Size - (N * 4),
    BasePayload = TotalPayload div N,
    Remainder = TotalPayload rem N,
    lists:reverse([
        make_record(StartOffset + I, BasePayload, I == N - 1, Remainder)
     || I <- lists:seq(0, N - 1)
    ]).

make_record(Index, BaseSize, true = _IsLast, Remainder) ->
    Pad = binary:copy(<<0>>, BaseSize + Remainder - 4),
    <<Index:32, Pad/binary>>;
make_record(Index, BaseSize, false, _Remainder) ->
    Pad = binary:copy(<<0>>, max(0, BaseSize - 4)),
    <<Index:32, Pad/binary>>.

segment_offset_from_log(Log) ->
    File = osiris_log:get_current_file(Log),
    Bin =
        if
            is_list(File) -> list_to_binary(File);
            true -> File
        end,
    rabbitmq_stream_s3:segment_file_offset(Bin).

%% ------------------------------------------------------------------
%% Manifest tree builder
%% ------------------------------------------------------------------

-type fragment_spec() ::
    {fragment, #{
        offset := non_neg_integer(),
        size => non_neg_integer(),
        first_ts => integer(),
        last_ts => integer(),
        uid => non_neg_integer()
    }}.

-type tree_spec() ::
    fragment_spec()
    | {group, [tree_spec()]}
    | {kilo_group, [tree_spec()]}
    | {mega_group, [tree_spec()]}.

-doc """
Build a manifest and get_group_fun from a declarative tree spec.

Usage:

    {Manifest, GetGroupFun} = rabbitmq_stream_s3_test_helpers:build_manifest([
        {group, [
            {fragment, #{offset => 0}},
            {fragment, #{offset => 100}}
        ]},
        {fragment, #{offset => 200, size => 7000}}
    ])

Every node is `{Kind, ...}` where Kind is `fragment`, `group`,
`kilo_group`, or `mega_group`.

Fragments are `{fragment, Map}`. Only `offset` is required.
Optional keys: `size` (default 64000), `first_ts` (default Offset * 10),
`last_ts` (default (Offset + 1) * 10), `uid` (default random).

Groups are `{group, [Children]}`, `{kilo_group, [Children]}`,
`{mega_group, [Children]}`.
""".
-spec build_manifest([tree_spec()]) ->
    {#manifest{}, rabbitmq_stream_s3_fragment_iterator:get_group_fun()}.
build_manifest(Specs) ->
    {RootEntries, TotalSize, Groups} = build_children(Specs, #{}),
    Manifest =
        case RootEntries of
            <<>> ->
                #manifest{};
            _ ->
                ?ENTRY(FirstOffset, FirstTs, FirstLastTs, _, _, _, _) = RootEntries,
                #manifest{
                    first_offset = FirstOffset,
                    next_offset = next_offset(Specs),
                    first_timestamp = FirstTs,
                    first_last_timestamp = FirstLastTs,
                    total_size = TotalSize,
                    entries = RootEntries
                }
        end,
    GetGroupFun = fun(#group_ref{offset = O, kind = K, uid = U}) ->
        case Groups of
            #{{O, K, U} := Entries} ->
                {ok, Entries};
            #{} ->
                {error, not_found}
        end
    end,
    {Manifest, GetGroupFun}.

%% ------------------------------------------------------------------
%% Writer helpers
%% ------------------------------------------------------------------

start_writer(Config, RemoteConfig) ->
    start_writer(Config, #{}, RemoteConfig).

start_writer(Config, WriterOverrides, RemoteConfig) ->
    StreamId = ?config(stream_id, Config),
    WriterCfg0 = ?config(writer_cfg, Config),
    RemoteConfig1 = maps:merge(#{persist_threshold => 1}, RemoteConfig),
    WriterCfg = maps:merge(WriterCfg0, WriterOverrides#{remote_config => RemoteConfig1}),
    {ok, Writer} = osiris_writer:start(WriterCfg),
    flush_writer(Writer),
    ?assertMatch(
        Pid when is_pid(Pid),
        rabbitmq_stream_s3_registry:whereis_name({StreamId, node()})
    ),
    Writer.

start_cluster(Config, ReplicaNodes, RemoteConfig) ->
    start_cluster(Config, ReplicaNodes, #{}, RemoteConfig).

start_cluster(Config, ReplicaNodes, WriterOverrides, RemoteConfig) ->
    StreamId = ?config(stream_id, Config),
    WriterCfg0 = ?config(writer_cfg, Config),
    RemoteConfig1 = maps:merge(#{persist_threshold => 1}, RemoteConfig),
    ClusterCfg = maps:merge(WriterCfg0, WriterOverrides#{
        replica_nodes => ReplicaNodes,
        remote_config => RemoteConfig1
    }),
    {ok, #{leader_pid := Writer, replica_pids := ReplicaPids}} =
        osiris:start_cluster(ClusterCfg),
    flush_writer(Writer),
    ?assertMatch(
        Pid when is_pid(Pid),
        rabbitmq_stream_s3_registry:whereis_name({StreamId, node()})
    ),
    {Writer, ReplicaPids}.

flush_writer(Writer) ->
    _ = osiris_writer:query_replication_state(Writer),
    ok.

-doc """
Start a replica reader for an existing writer.

Use when the writer was started without plugin hooks and you want to trigger
upload separately.
""".
start_replica_reader(Writer, Config, RemoteConfig) ->
    StreamId = ?config(stream_id, Config),
    #{shared := Shared, dir := Dir} = gen_batch_server:call(Writer, get_reader_context),
    Counter = osiris_counters:fetch({osiris_writer, StreamId}),
    Defaults = #{
        stream => StreamId,
        writer_pid => Writer,
        dir => iolist_to_binary(Dir),
        shared => Shared,
        counter => Counter,
        reference => StreamId,
        epoch => 1,
        persist_threshold => 1
    },
    {ok, Pid} = rabbitmq_stream_s3_replica_reader_sup:start_child(
        maps:merge(Defaults, RemoteConfig)
    ),
    Pid.

-doc """
Block until the remote replica reader has persisted past `Offset`.

`Offset` is an exclusive upper bound: `await_offset(StreamId, N)` returns
when the durable `next_offset >= N`. This means all records with offsets
0..N-1 are in S3 and referenced by a persisted manifest.

Use this as the barrier between writing data and asserting on the remote
tier. Do not assert on fragment count, segment count, or remote tier
contents without calling this first.

Accepts either a StreamId binary or a CT Config proplist as the first
argument. The Config form provides better diagnostics on timeout.
""".
-spec await_offset(binary() | list(), osiris:offset()) -> ok.
await_offset(StreamId, Offset) when is_binary(StreamId) ->
    await_offset(StreamId, Offset, ?AWAIT_OFFSET_TIMEOUT_MS);
await_offset(Config, Offset) when is_list(Config) ->
    StreamId = ?config(stream_id, Config),
    try
        await_offset(StreamId, Offset, ?AWAIT_OFFSET_TIMEOUT_MS)
    catch
        exit:{timeout, _} ->
            Name = {via, rabbitmq_stream_s3_registry, {StreamId, node()}},
            State = rabbitmq_stream_s3_replica_reader:format_state(
                sys:get_state(Name)
            ),
            ct:fail(
                "await_offset timed out waiting for offset ~b.~n"
                "  Replica reader: ~p",
                [Offset, State]
            )
    end.

-spec await_offset(stream_id(), osiris:offset(), timeout()) -> ok.
await_offset(StreamId, Offset, Timeout) ->
    ok = gen_server:call(
        {via, rabbitmq_stream_s3_registry, {StreamId, node()}},
        {await_offset, Offset},
        Timeout
    ).

%% ------------------------------------------------------------------
%% Write helpers
%% ------------------------------------------------------------------

%% Write N records (<<0:32, Padding>>, <<1:32, Padding>>, ...) in batches of
%% at most BatchSize, flushing after each batch to force one chunk per batch.
write_sequential(Writer, N, BatchSize) ->
    write_sequential(Writer, 0, N, BatchSize).

write_sequential(_Writer, I, N, _BatchSize) when I >= N ->
    ok;
write_sequential(Writer, I, N, BatchSize) ->
    End = min(I + BatchSize, N),
    Padding = binary:copy(<<0>>, 100),
    [osiris_writer:write(Writer, <<J:32, Padding/binary>>) || J <- lists:seq(I, End - 1)],
    flush_writer(Writer),
    write_sequential(Writer, End, N, BatchSize).

%% ------------------------------------------------------------------
%% Read helpers
%% ------------------------------------------------------------------

reader_config(Writer, Config) ->
    StreamId = ?config(stream_id, Config),
    #{shared := Shared, dir := StreamDir} =
        gen_batch_server:call(Writer, get_reader_context),
    #{
        name => StreamId,
        dir => StreamDir,
        epoch => 1,
        shared => Shared,
        options => #{transport => tcp},
        readers_counter_fun => fun(_) -> ok end
    }.

read_all(Reader0) ->
    read_all(Reader0, 0, []).

read_all(Reader0, StartOffset) ->
    read_all(Reader0, StartOffset, []).

read_all(Reader0, StartOffset, Acc) ->
    case rabbitmq_stream_s3_log_reader:chunk_iterator(Reader0, 1, undefined) of
        {ok, _Header, Iter, Reader1} ->
            Records = drain_iter(Iter, StartOffset, []),
            read_all(Reader1, StartOffset, Acc ++ Records);
        {end_of_stream, _Reader} ->
            Acc;
        {error, Reason} ->
            error({read_error, Reason})
    end.

drain_iter(Iter, StartOffset, Acc) ->
    case rabbitmq_stream_s3_log_reader:iterator_next(Iter) of
        {{Offset, Record}, Iter1} when Offset >= StartOffset ->
            drain_iter(Iter1, StartOffset, [Record | Acc]);
        {{Offset, _Record}, Iter1} ->
            ct:pal("drain_iter: skipping offset ~b (start_offset=~b)", [Offset, StartOffset]),
            drain_iter(Iter1, StartOffset, Acc);
        end_of_chunk ->
            lists:reverse(Acc)
    end.

%% ------------------------------------------------------------------
%% Barrier helpers
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% Inspection helpers
%% ------------------------------------------------------------------

list_segment_offsets(Config) ->
    list_segment_offsets(Config, node()).

list_segment_offsets(Config, Node) ->
    StreamId = ?config(stream_id, Config),
    erpc:call(Node, ?MODULE, list_segment_offsets_local, [StreamId]).

list_segment_offsets_local(StreamId) ->
    {ok, Dir} = application:get_env(osiris, data_dir),
    Pattern = filename:join([Dir, binary_to_list(StreamId), "*.segment"]),
    lists:sort([
        rabbitmq_stream_s3:segment_file_offset(list_to_binary(F))
     || F <- filelib:wildcard(Pattern)
    ]).

list_fragment_offsets(Config) ->
    RemoteDir = ?config(remote_dir, Config),
    StreamId = ?config(stream_id, Config),
    Pattern = filename:join([
        RemoteDir,
        "rabbitmq",
        "stream",
        binary_to_list(StreamId),
        "data",
        "*.fragment"
    ]),
    lists:sort([
        rabbitmq_stream_s3:fragment_key_offset(list_to_binary(F))
     || F <- filelib:wildcard(binary_to_list(iolist_to_binary(Pattern)))
    ]).

get_range(Config) ->
    get_range(Config, node()).

get_range(Config, Node) ->
    StreamId = ?config(stream_id, Config),
    erpc:call(Node, rabbitmq_stream_s3_manifest_replica, get_range, [StreamId]).

%% ------------------------------------------------------------------
%% Assertion helpers
%% ------------------------------------------------------------------

assert_sequential(Records, N) ->
    assert_sequential(Records, 0, N - 1).

assert_sequential(Records, First, Last) ->
    Integers = [I || <<I:32, _/binary>> <- Records],
    Expected = lists:seq(First, Last),
    case Integers of
        Expected ->
            ok;
        _ ->
            Dups = Integers -- lists:usort(Integers),
            Missing = Expected -- Integers,
            Extra = Integers -- Expected,
            ct:fail(
                "Sequential assertion failed.~n"
                "  Expected ~b records (~b..~b), got ~b~n"
                "  Duplicates: ~w~n"
                "  Missing: ~w~n"
                "  Extra: ~w~n"
                "  First 20: ~w~n"
                "  Last 20: ~w",
                [
                    Last - First + 1,
                    First,
                    Last,
                    length(Records),
                    Dups,
                    Missing,
                    Extra,
                    lists:sublist(Integers, 20),
                    lists:sublist(lists:reverse(Integers), 20)
                ]
            )
    end.

%% ------------------------------------------------------------------
%% Internal (manifest builder)
%% ------------------------------------------------------------------

build_children(Specs, Groups) ->
    lists:foldl(
        fun(Spec, {EntriesAcc, SizeAcc, GroupsAcc}) ->
            {Entry, Size, GroupsAcc1} = build_entry(Spec, GroupsAcc),
            {<<EntriesAcc/binary, Entry/binary>>, SizeAcc + Size, GroupsAcc1}
        end,
        {<<>>, 0, Groups},
        Specs
    ).

build_entry({fragment, Props}, Groups) ->
    Offset = maps:get(offset, Props),
    Size = maps:get(size, Props, 64000),
    FirstTs = maps:get(first_ts, Props, Offset * 10),
    LastTs = maps:get(last_ts, Props, (Offset + 1) * 10),
    Uid = maps:get(uid, Props, rabbitmq_stream_s3:uid()),
    Entry = ?ENTRY(Offset, FirstTs, LastTs, ?MANIFEST_KIND_FRAGMENT, Size, Uid),
    {Entry, Size, Groups};
build_entry({group, Children}, Groups) ->
    build_group_entry(?MANIFEST_KIND_GROUP, Children, Groups);
build_entry({kilo_group, Children}, Groups) ->
    build_group_entry(?MANIFEST_KIND_KILO_GROUP, Children, Groups);
build_entry({mega_group, Children}, Groups) ->
    build_group_entry(?MANIFEST_KIND_MEGA_GROUP, Children, Groups).

build_group_entry(Kind, Children, Groups0) ->
    {ChildEntries, TotalSize, Groups1} = build_children(Children, Groups0),
    ?ENTRY(Offset, FirstTs, _, _, _, _, _) = ChildEntries,
    ?ENTRY(_, _, LastTs, _, _, _) = rabbitmq_stream_s3_array:last(?ENTRY_B, ChildEntries),
    Uid = rabbitmq_stream_s3:uid(),
    Entry = ?ENTRY(Offset, FirstTs, LastTs, Kind, 0, Uid),
    Groups2 = Groups1#{{Offset, Kind, Uid} => ChildEntries},
    {Entry, TotalSize, Groups2}.

next_offset(Specs) ->
    next_offset_spec(lists:last(Specs)).

next_offset_spec({fragment, Props}) ->
    maps:get(offset, Props) + 1;
next_offset_spec({group, Children}) ->
    next_offset(Children);
next_offset_spec({kilo_group, Children}) ->
    next_offset(Children);
next_offset_spec({mega_group, Children}) ->
    next_offset(Children).

%% ------------------------------------------------------------------
%% Log capture
%% ------------------------------------------------------------------

-spec capture_log(fun(() -> term())) -> binary().
capture_log(Fun) ->
    capture_log(#{}, Fun).

-spec capture_log(#{level => logger:level()}, fun(() -> term())) -> binary().
capture_log(Opts, Fun) ->
    {_, Log} = with_log(Opts, Fun),
    Log.

-spec with_log(fun(() -> Result)) -> {Result, binary()} when Result :: term().
with_log(Fun) ->
    with_log(#{}, Fun).

-spec with_log(#{level => logger:level()}, fun(() -> Result)) -> {Result, binary()} when
    Result :: term().
with_log(Opts, Fun) ->
    Level = maps:get(level, Opts, all),
    Ref = make_ref(),
    Self = self(),
    HandlerId = list_to_atom("capture_log_" ++ integer_to_list(erlang:unique_integer([positive]))),
    {ok, #{level := OrigLevel}} = logger:get_handler_config(default),
    ok = logger:add_handler(HandlerId, ?MODULE, #{
        pid => Self,
        ref => Ref,
        level => Level
    }),
    ok = logger:set_handler_config(default, level, none),
    try
        Result = Fun(),
        {Result, collect_log(Ref)}
    after
        _ = logger:remove_handler(HandlerId),
        _ = logger:set_handler_config(default, level, OrigLevel)
    end.

%% logger handler callback
log(Event, #{pid := Pid, ref := Ref}) ->
    Formatted = logger_formatter:format(Event, #{template => [level, ": ", msg, "\n"]}),
    Pid ! {captured_log, Ref, iolist_to_binary(Formatted)},
    ok.

collect_log(Ref) ->
    collect_log(Ref, []).

collect_log(Ref, Acc) ->
    receive
        {captured_log, Ref, Bin} -> collect_log(Ref, [Bin | Acc])
    after 0 -> iolist_to_binary(lists:reverse(Acc))
    end.

%% ------------------------------------------------------------------
%% Manifest and fragment iterator construction
%%
%% Shared by `remote_reader_core_SUITE` and `remote_reader_s3_bench`:
%% both drive `rabbitmq_stream_s3_remote_reader_core` directly, which needs a
%% fragment iterator, and there is no reason for two copies of the manifest
%% encoding to drift apart.
%% ------------------------------------------------------------------

-doc "A fragment reference, the shape the read core navigates by.".
-spec frag_ref(osiris:offset(), non_neg_integer(), non_neg_integer()) -> #fragment_ref{}.
frag_ref(Offset, Size, Uid) ->
    #fragment_ref{offset = Offset, uid = Uid, size = Size}.

-doc """
A fragment iterator over `Specs` (the `build_manifest/1` tree spec), already
advanced past the first entry.

The advance matches what `find_position` in the log reader does before handing
the iterator to the core, so the iterator points at the *next* fragment - ready
for prefetch and forward navigation - exactly as it does in production.

Takes the tree spec rather than a flat fragment list so that a caller can put
fragments behind group nodes, which is what makes the look-ahead's synchronous
group GET reachable.
""".
-spec mock_iterator([tree_spec()]) -> rabbitmq_stream_s3_fragment_iterator:iterator().
mock_iterator(Specs) ->
    mock_iterator(Specs, fun(GetGroup) -> GetGroup end).

-doc """
As `mock_iterator/1`, but with the manifest's own group-fetch fun passed
through `WrapGetGroup` first.

Descending into a group node is a synchronous S3 GET in production, so a caller
that wants to count those, charge latency for them, or fail them passes a
wrapper rather than replacing the fun and having to rebuild the group index by
hand.
""".
-spec mock_iterator(
    [tree_spec()],
    fun(
        (rabbitmq_stream_s3_fragment_iterator:get_group_fun()) ->
            rabbitmq_stream_s3_fragment_iterator:get_group_fun()
    )
) ->
    rabbitmq_stream_s3_fragment_iterator:iterator().
mock_iterator(Specs, WrapGetGroup) ->
    {#manifest{first_offset = FirstOffset} = Manifest, GetGroupFun} = build_manifest(Specs),
    Iterator = rabbitmq_stream_s3_fragment_iterator:init(
        Manifest, FirstOffset, WrapGetGroup(GetGroupFun)
    ),
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, _, It} -> It;
        _ -> Iterator
    end.
