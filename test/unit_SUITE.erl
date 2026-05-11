%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(unit_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("rabbitmq_stream_s3/include/rabbitmq_stream_s3.hrl").

all() ->
    [
        range_spec_to_location_number_suffix,
        range_spec_to_location_number_prefix,
        range_spec_to_location_number_byte_range,
        find_fragment_timestamp,
        array_partition_point,
        array_binary_search_by,
        array_rfind,
        array_fold,
        find_index_position_offset,
        find_index_position_timestamp
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

%% Suffix range (negative integer): last N bytes of a file.
range_spec_to_location_number_suffix(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_range_spec_to_location_number_suffix/0, [], 500
    ).

prop_range_spec_to_location_number_suffix() ->
    ?FORALL(
        {FileSize, N},
        ?LET(FS, pos_integer(), {FS, integer(1, FS)}),
        begin
            {Loc, Num} = rabbitmq_stream_s3_api_fs:range_spec_to_location_number(FileSize, -N),
            (Loc =:= FileSize - N) andalso (Num =:= N)
        end
    ).

%% Prefix range (positive integer): first N bytes, capped at file size.
range_spec_to_location_number_prefix(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_range_spec_to_location_number_prefix/0, [], 500
    ).

prop_range_spec_to_location_number_prefix() ->
    ?FORALL(
        {FileSize, N},
        {pos_integer(), pos_integer()},
        begin
            {Loc, Num} = rabbitmq_stream_s3_api_fs:range_spec_to_location_number(FileSize, N),
            (Loc =:= 0) andalso (Num =:= min(N, FileSize))
        end
    ).

%% Byte range: explicit end byte or open-ended (undefined).
range_spec_to_location_number_byte_range(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_range_spec_to_location_number_byte_range/0, [], 500
    ).

prop_range_spec_to_location_number_byte_range() ->
    ?FORALL(
        {FileSize, Start, MaybeEnd},
        ?LET(
            FS,
            pos_integer(),
            ?LET(
                S,
                integer(0, FS - 1),
                {FS, S, oneof([integer(S, FS + 100), undefined])}
            )
        ),
        begin
            {Loc, Num} = rabbitmq_stream_s3_api_fs:range_spec_to_location_number(
                FileSize, {Start, MaybeEnd}
            ),
            Expected =
                case MaybeEnd of
                    undefined -> FileSize - Start;
                    End -> End - Start + 1
                end,
            (Loc =:= Start) andalso (Num =:= Expected)
        end
    ).

find_fragment_timestamp(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_find_fragment_timestamp/0, [], 500
    ).

prop_find_fragment_timestamp() ->
    ?FORALL(
        Fragments,
        gen_fragments(),
        begin
            Entries = fragments_to_entries(Fragments),
            GetGroup = fun(_) -> erlang:error(no_groups) end,
            FindFragment = fun(Ts) ->
                rabbitmq_stream_s3_log_reader:find_fragment(Entries, {timestamp, Ts}, GetGroup)
            end,
            %% For each fragment, a timestamp within its range returns that fragment.
            WithinRange = lists:all(
                fun({Offset, FTs, LTs}) ->
                    Ts = FTs + (LTs - FTs) div 2,
                    FindFragment(Ts) =:= Offset
                end,
                Fragments
            ),
            %% For each gap between fragments, a timestamp in the gap returns the later fragment.
            InGap = lists:all(
                fun({{_O1, _FTs1, LTs1}, {O2, FTs2, _LTs2}}) ->
                    case LTs1 < FTs2 of
                        false ->
                            true;
                        true ->
                            Ts = LTs1 + (FTs2 - LTs1) div 2,
                            FindFragment(Ts) =:= O2
                    end
                end,
                lists:zip(lists:droplast(Fragments), tl(Fragments))
            ),
            %% A timestamp after all fragments returns the last fragment.
            {LastOffset, _FTs, LastLTs} = lists:last(Fragments),
            AfterAll = FindFragment(LastLTs + 1000) =:= LastOffset,
            WithinRange andalso InGap andalso AfterAll
        end
    ).

%% Generates a non-empty sorted list of non-overlapping fragments with optional gaps.
gen_fragments() ->
    ?LET(
        {N, Gaps},
        {integer(1, 20), non_empty(list(boolean()))},
        begin
            GapsN = lists:sublist(Gaps ++ lists:duplicate(N, false), N),
            {Frags, _, _} = lists:foldl(
                fun(HasGap, {Acc, NextOffset, NextTs}) ->
                    FTs = NextTs,
                    LTs = FTs + 100,
                    Gap =
                        case HasGap of
                            true -> 50;
                            false -> 0
                        end,
                    {[{NextOffset, FTs, LTs} | Acc], NextOffset + 20, LTs + Gap + 1}
                end,
                {[], 0, 1_000_000},
                GapsN
            ),
            lists:reverse(Frags)
        end
    ).

fragments_to_entries(Fragments) ->
    iolist_to_binary([
        ?ENTRY(O, FTs, LTs, ?MANIFEST_KIND_FRAGMENT, 200, 0)
     || {O, FTs, LTs} <- Fragments
    ]).

%% -------------------------------------------------------------------------
%% rabbitmq_stream_s3_array properties
%%
%% All properties use 8-byte entries containing a single uint64 value.
%% -------------------------------------------------------------------------

-define(ENTRY_SIZE, 8).

array_partition_point(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_array_partition_point/0, [], 500
    ).

%% partition_point(P, Array) returns index I such that:
%%   - all entries at index < I satisfy P
%%   - all entries at index >= I do not satisfy P
%%
%% The predicate must be monotone: true for a prefix, false for the remainder.
%% We enforce this by sorting the values and using a threshold predicate.
prop_array_partition_point() ->
    ?FORALL(
        {SortedValues, Threshold},
        ?LET(Vs, list(non_neg_integer()), {lists:sort(Vs), non_neg_integer()}),
        begin
            Array = ints_to_array(SortedValues),
            N = length(SortedValues),
            Predicate = fun(<<V:64/unsigned>>) -> V < Threshold end,
            Idx = rabbitmq_stream_s3_array:partition_point(Predicate, ?ENTRY_SIZE, Array),
            %% All entries before Idx satisfy the predicate.
            Before = lists:all(
                fun(I) ->
                    Predicate(rabbitmq_stream_s3_array:at(I, ?ENTRY_SIZE, Array))
                end,
                lists:seq(0, Idx - 1)
            ),
            %% All entries from Idx onwards do not satisfy the predicate.
            After = lists:all(
                fun(I) ->
                    not Predicate(rabbitmq_stream_s3_array:at(I, ?ENTRY_SIZE, Array))
                end,
                lists:seq(Idx, N - 1)
            ),
            Before andalso After
        end
    ).

array_binary_search_by(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_array_binary_search_by/0, [], 500
    ).

%% binary_search_by on a sorted ascending array:
%%   - {ok, Idx}    => entry at Idx compares eq to target
%%   - {error, Idx} => all entries before Idx compare lt (value < target),
%%                     all entries at Idx and after compare gt (value > target)
prop_array_binary_search_by() ->
    ?FORALL(
        {SortedValues, Target},
        ?LET(
            Vs,
            list(non_neg_integer()),
            {lists:usort(Vs), non_neg_integer()}
        ),
        begin
            Array = ints_to_array(SortedValues),
            N = length(SortedValues),
            CmpFn = fun(<<V:64/unsigned>>) ->
                if
                    V =:= Target -> eq;
                    V > Target -> gt;
                    true -> lt
                end
            end,
            Result = rabbitmq_stream_s3_array:binary_search_by(CmpFn, ?ENTRY_SIZE, Array),
            case Result of
                {ok, Idx} ->
                    CmpFn(rabbitmq_stream_s3_array:at(Idx, ?ENTRY_SIZE, Array)) =:= eq;
                {error, Idx} ->
                    %% All entries before Idx compare lt (value < target).
                    Before = lists:all(
                        fun(I) ->
                            CmpFn(rabbitmq_stream_s3_array:at(I, ?ENTRY_SIZE, Array)) =:= lt
                        end,
                        lists:seq(0, Idx - 1)
                    ),
                    %% All entries from Idx onwards compare gt (value > target).
                    After = lists:all(
                        fun(I) ->
                            CmpFn(rabbitmq_stream_s3_array:at(I, ?ENTRY_SIZE, Array)) =:= gt
                        end,
                        lists:seq(Idx, N - 1)
                    ),
                    Before andalso After
            end
        end
    ).

array_rfind(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_array_rfind/0, [], 500
    ).

%% rfind returns the rightmost index satisfying the predicate, or undefined.
%%   - undefined => no entry satisfies the predicate
%%   - Idx       => entry at Idx satisfies predicate, no entry at Idx+1..N-1 does
prop_array_rfind() ->
    ?FORALL(
        {Values, Threshold},
        {list(non_neg_integer()), non_neg_integer()},
        begin
            Array = ints_to_array(Values),
            N = length(Values),
            Predicate = fun(<<V:64/unsigned>>) -> V < Threshold end,
            Result = rabbitmq_stream_s3_array:rfind(Predicate, ?ENTRY_SIZE, Array),
            case Result of
                undefined ->
                    %% No entry satisfies the predicate.
                    lists:all(
                        fun(I) ->
                            not Predicate(rabbitmq_stream_s3_array:at(I, ?ENTRY_SIZE, Array))
                        end,
                        lists:seq(0, N - 1)
                    );
                Idx ->
                    %% Entry at Idx satisfies the predicate.
                    AtIdx = Predicate(rabbitmq_stream_s3_array:at(Idx, ?ENTRY_SIZE, Array)),
                    %% No entry after Idx satisfies the predicate.
                    NoneAfter = lists:all(
                        fun(I) ->
                            not Predicate(rabbitmq_stream_s3_array:at(I, ?ENTRY_SIZE, Array))
                        end,
                        lists:seq(Idx + 1, N - 1)
                    ),
                    AtIdx andalso NoneAfter
            end
        end
    ).

array_fold(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_array_fold/0, [], 500
    ).

%% fold accumulates entries left-to-right, equivalent to reading each entry
%% sequentially with at/3.
prop_array_fold() ->
    ?FORALL(
        Values,
        list(non_neg_integer()),
        begin
            Array = ints_to_array(Values),
            N = length(Values),
            Folded = rabbitmq_stream_s3_array:fold(
                fun(Entry, Acc) -> Acc ++ [Entry] end,
                [],
                ?ENTRY_SIZE,
                Array
            ),
            Sequential = [
                rabbitmq_stream_s3_array:at(I, ?ENTRY_SIZE, Array)
             || I <- lists:seq(0, N - 1)
            ],
            Folded =:= Sequential
        end
    ).

ints_to_array(Ints) ->
    iolist_to_binary([<<V:64/unsigned>> || V <- Ints]).

%% -------------------------------------------------------------------------
%% find_index_position properties
%% -------------------------------------------------------------------------

find_index_position_offset(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_find_index_position_offset/0, [], 500
    ).

%% Offset spec: returns the last chunk whose offset =< O.
%% If O is before all chunks, returns the first chunk.
prop_find_index_position_offset() ->
    ?FORALL(
        {Chunks, QueryOffset},
        gen_index_data(),
        begin
            IndexData = chunks_to_index(Chunks),
            {ChunkId, _Ts, _Pos} = rabbitmq_stream_s3_log_reader:find_index_position(
                IndexData, {offset, QueryOffset}
            ),
            Offsets = [O || {O, _T} <- Chunks],
            %% Expected: last offset =< QueryOffset, or first offset if all are greater.
            Expected =
                case lists:takewhile(fun(O) -> O =< QueryOffset end, Offsets) of
                    [] -> hd(Offsets);
                    Matching -> lists:last(Matching)
                end,
            ChunkId =:= Expected
        end
    ).

find_index_position_timestamp(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_find_index_position_timestamp/0, [], 500
    ).

%% Timestamp spec: returns the first chunk whose timestamp >= Ts.
%% If Ts is after all chunks, returns the last chunk.
prop_find_index_position_timestamp() ->
    ?FORALL(
        {Chunks, QueryTs},
        gen_index_data(),
        begin
            IndexData = chunks_to_index(Chunks),
            {ChunkId, _Ts, _Pos} = rabbitmq_stream_s3_log_reader:find_index_position(
                IndexData, {timestamp, QueryTs}
            ),
            %% Expected: first chunk whose timestamp >= QueryTs, or last chunk.
            Expected =
                case lists:dropwhile(fun({_O, Ts}) -> Ts < QueryTs end, Chunks) of
                    [] -> element(1, lists:last(Chunks));
                    [{O, _T} | _] -> O
                end,
            ChunkId =:= Expected
        end
    ).

%% Generates a non-empty list of {Offset, Timestamp} pairs with strictly
%% increasing offsets and timestamps, plus a random query value in a range
%% that covers before the first entry, within the range, and beyond the last.
gen_index_data() ->
    ?LET(
        {N, Steps},
        {integer(1, 20), non_empty(list(integer(1, 100)))},
        begin
            StepsN = lists:sublist(Steps ++ lists:duplicate(N, 1), N),
            {Chunks, LastOffset, LastTs} = lists:foldl(
                fun(Step, {Acc, NextOffset, NextTs}) ->
                    {[{NextOffset, NextTs} | Acc], NextOffset + Step, NextTs + Step}
                end,
                {[], 1, 1_000_000},
                StepsN
            ),
            SortedChunks = lists:reverse(Chunks),
            %% Query range covers before first entry (0), within range, and beyond last.
            ?LET(Query, integer(0, max(LastOffset, LastTs) + 200), {SortedChunks, Query})
        end
    ).

chunks_to_index(Chunks) ->
    iolist_to_binary([
        ?INDEX_RECORD(O, Ts, Pos)
     || {Pos, {O, Ts}} <- lists:zip(lists:seq(0, length(Chunks) - 1), Chunks)
    ]).
