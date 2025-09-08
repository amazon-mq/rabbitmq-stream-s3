-module(rabbitmq_stream_s3_log_manifest_entry).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-type entry() :: binary().
-type entries() :: binary().
-type kind() ::
    ?MANIFEST_KIND_FRAGMENT
    | ?MANIFEST_KIND_GROUP
    | ?MANIFEST_KIND_KILO_GROUP
    | ?MANIFEST_KIND_MEGA_GROUP.
%% Zero-based index into the array of entries.
-type index() :: non_neg_integer().

-export_type([entry/0, entries/0, kind/0]).

-export([
    total_size/1,
    rebalance/1,
    next_group/1
]).

-doc """
Returns the combined size of all entries in the array.
""".
-spec total_size(entries()) -> non_neg_integer().
total_size(Entries) when is_binary(Entries) ->
    rabbitmq_stream_s3_binary_array:fold(
        fun(?ENTRY(_O, _T, _K, S, _N, _), Acc) -> S + Acc end,
        0,
        ?ENTRY_B,
        Entries
    ).

-spec rebalance(entries()) ->
    {kind(), TotalSize :: non_neg_integer(), NewGroup :: entries(), Rebalanced :: entries()}
    | undefined.
rebalance(Entries) ->
    rebalance(Entries, 0).

rebalance(Entries0, Idx) when
    byte_size(Entries0) - (Idx * ?ENTRY_B) >= (?MANIFEST_BRANCHING_FACTOR * ?ENTRY_B)
->
    ?ENTRY(Offset, Ts, Kind, _, _, _) = rabbitmq_stream_s3_binary_array:at(Idx, ?ENTRY_B, Entries0),
    ?LOG_DEBUG("Considering grouping of kind ~b at idx ~b (~b bytes remaining)", [
        Kind, Idx, byte_size(Entries0) - (Idx * ?ENTRY_B)
    ]),
    %% Scan ahead to the entry which would satisfy the branching factor.
    %% If this entry has the same kind then it is the last entry in the new
    %% group.
    GroupEndIdx = Idx + ?MANIFEST_BRANCHING_FACTOR - 1,
    case rabbitmq_stream_s3_binary_array:at(GroupEndIdx, ?ENTRY_B, Entries0) of
        ?ENTRY(_, _, Kind, _, _, _) ->
            %% If the kind is the same, this chunk of entries can be compacted
            %% into a group.
            ?LOG_DEBUG("Found full group of kind ~b at idx ~b", [Kind, Idx]),
            GroupKind = next_group(Kind),
            Pos = Idx * ?ENTRY_B,
            Len = ?MANIFEST_BRANCHING_FACTOR * ?ENTRY_B,
            GroupEntries = binary:part(Entries0, Pos, Len),
            GroupSize = total_size(GroupEntries),
            Entries = <<
                %% From the beginning to the start of the compacted entries:
                (binary:part(Entries0, 0, Pos))/binary,
                %% Replace the new group's entries with the newly created group:
                (?ENTRY(Offset, Ts, GroupKind, GroupSize, 0, <<>>))/binary,
                %% And finally include the trailing entries which come after
                %% the new group entries.
                (binary:part(Entries0, Pos + Len, byte_size(Entries0) - Pos - Len))/binary
            >>,
            {GroupKind, GroupSize, GroupEntries, Entries};
        _ ->
            case find_next_smallest_kind(Kind, Entries0) of
                Idx ->
                    undefined;
                NextIdx ->
                    ?LOG_DEBUG("Failed find at idx ~b, trying next kind at ~b", [
                        Idx, NextIdx
                    ]),
                    %% Otherwise continue scanning to find the next kind.
                    rebalance(Entries0, NextIdx)
            end
    end;
rebalance(_, _Pos) ->
    undefined.

%% Find the index of the next **smallest** kind.
%%
%% Runs in logarithmic time on the number of entries in the array.
-spec find_next_smallest_kind(kind(), entries()) -> index().
find_next_smallest_kind(Kind, Entries0) ->
    rabbitmq_stream_s3_binary_array:partition_point(
        fun(?ENTRY(_, _, K, _, _, _)) -> Kind =< K end, ?ENTRY_B, Entries0
    ).

next_group(?MANIFEST_KIND_FRAGMENT) -> ?MANIFEST_KIND_GROUP;
next_group(?MANIFEST_KIND_GROUP) -> ?MANIFEST_KIND_KILO_GROUP;
next_group(?MANIFEST_KIND_KILO_GROUP) -> ?MANIFEST_KIND_MEGA_GROUP.
