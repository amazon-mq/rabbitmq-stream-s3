%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_array).
-moduledoc """
Helper functions and routines for working with an array of fixed-size items
stored in a binary.
""".

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-doc "An array represented as a binary with fixed-size entries".
-type array() :: binary().
-doc "One element within the array".
-type entry() :: binary().
-doc "The number of bytes which make up a fixed-size entry in an array".
-type entry_size() :: pos_integer().
-doc "A zero-based index into an array".
-type index() :: non_neg_integer().

-export_type([array/0, entry_size/0, index/0]).

-export([
    len/2,
    try_at/3,
    at/3,
    last/2,
    slice/3,
    rfind/3,
    binary_search_by/3,
    partition_point/3,
    fold/4
]).

-doc "Returns the number of elements in the array".
-spec len(entry_size(), array()) -> non_neg_integer().
len(EntrySize, Array) when is_binary(Array) ->
    byte_size(Array) div EntrySize.

-doc """
Tries to get the entry at the given index.

If the index is out of bounds then this function returns `undefined` instead.
""".
-spec try_at(index(), entry_size(), array()) -> entry() | undefined.
try_at(Index, EntrySize, Array) when is_binary(Array) ->
    case Index * EntrySize < byte_size(Array) of
        true ->
            binary:part(Array, Index * EntrySize, EntrySize);
        false ->
            undefined
    end.

-doc """
Gets the element at the given index.

If the index is out of bounds then this function returns a `badarg` error.
""".
-spec at(index(), entry_size(), array()) -> entry().
at(Index, EntrySize, Array) when is_integer(Index) andalso Index >= 0 andalso is_binary(Array) ->
    binary:part(Array, Index * EntrySize, EntrySize).

-spec last(entry_size(), array()) -> entry() | undefined.
last(_EntrySize, <<>>) ->
    undefined;
last(EntrySize, Array) when is_binary(Array) ->
    binary:part(Array, byte_size(Array), -EntrySize).

-spec slice(index(), entry_size(), array()) -> array().
slice(Index, EntrySize, Array) when is_binary(Array) ->
    Pos = Index * EntrySize,
    binary:part(Array, Pos, byte_size(Array) - Pos).

-doc "Searches for an element in an array from the right".
-spec rfind(fun((entry()) -> boolean()), entry_size(), array()) -> index() | undefined.
rfind(Predicate, EntrySize, Array) ->
    Len = len(EntrySize, Array),
    rfind(Predicate, EntrySize, Len - 1, Array).

rfind(_Predicate, _EntrySize, -1, _Array) ->
    undefined;
rfind(Predicate, EntrySize, Idx, Array) ->
    case Predicate(at(Idx, EntrySize, Array)) of
        true ->
            Idx;
        false ->
            rfind(Predicate, EntrySize, Idx - 1, Array)
    end.

-doc """
Find the position within the entries array which compares equal according to
search function, or the insertion position if no equal element exists.
""".
-spec binary_search_by(fun((entry()) -> lt | gt | eq), entry_size(), array()) ->
    {ok, index()} | {error, index()}.
binary_search_by(CmpFn, EntrySize, Array) when is_function(CmpFn, 1) andalso is_binary(Array) ->
    case len(EntrySize, Array) of
        0 -> {error, 0};
        Size -> binary_search_by(0, Size, CmpFn, EntrySize, Array)
    end.

binary_search_by(Base, _Edges = 1, CmpFn, EntrySize, Array) ->
    case CmpFn(at(Base, EntrySize, Array)) of
        eq -> {ok, Base};
        gt -> {error, Base};
        lt -> {error, Base + 1}
    end;
binary_search_by(Base, Size, CmpFn, EntrySize, Array) ->
    ?assert(Size > 1),
    Half = Size div 2,
    Mid = Base + Half,
    case CmpFn(at(Mid, EntrySize, Array)) of
        eq -> {ok, Mid};
        gt -> binary_search_by(Base, Size - Half, CmpFn, EntrySize, Array);
        lt -> binary_search_by(Mid, Size - Half, CmpFn, EntrySize, Array)
    end.

-doc """
Finds the index in the sorted array where inserting an element would make the
array sorted by the partition predicate.

The partition point is where the array 'turns' from the predicate returning
`true` to `false`. Inserting an element at the returned index would keep the
array split so that the predicate would continue to return `true` and then
eventually `false`.

If the predicate returns only `true` then this function returns `0`. If the
predicate only returns `false` then this function returns the length of the
array.

The partition function may never return `true`, or may never return `false` but
if it returns both, it must never return `false` at an index lesser than an
index where it returns `true`. Otherwise the index returned by this function
is meaningless.

For a simplified example:

```erlang
> partition_point(fun is_odd/1, [1,3,5,7,2,4,6,8]).
4
```

The partition point is where inserting an odd element would keep the list split
by the predicate. The list is odd at the beginning and eventually becomes even.
Inserting an odd element at index 4 would keep the partition function
satisfied.
""".
-spec partition_point(fun((entry()) -> boolean()), entry_size(), array()) -> index().
partition_point(Predicate, EntrySize, Array) when
    is_function(Predicate, 1) andalso is_binary(Array)
->
    CmpFn = fun(Entry) ->
        case Predicate(Entry) of
            true -> lt;
            false -> gt
        end
    end,
    %% Must be `error` since the `CmpFn` never returns `eq`.
    {error, Idx} = binary_search_by(CmpFn, EntrySize, Array),
    Idx.

-spec fold(fun((entry(), Acc) -> Acc), Acc, entry_size(), array()) -> Acc when Acc :: term().
fold(FoldFn, Acc, EntrySize, Array) when is_function(FoldFn, 2) andalso is_binary(Array) ->
    fold(FoldFn, Acc, EntrySize, 0, Array).

fold(FoldFn, Acc, EntrySize, Idx, Array) ->
    case try_at(Idx, EntrySize, Array) of
        undefined ->
            Acc;
        Entry ->
            fold(FoldFn, FoldFn(Entry, Acc), EntrySize, Idx + 1, Array)
    end.
