%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_manifest).
-moduledoc """
Functions for working with the _manifest_ data structure.

The manifest is a tree structure represented by objects in the remote tier
which tracks metadata about a stream: total size, oldest data, and pointers to
fragments (via tree-branch-like "group" objects, for large enough streams).
""".

-include_lib("stdlib/include/assert.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-type t() :: #manifest{}.
-type edit() :: #edit{}.

-export_type([t/0, edit/0]).

-export([
    new_edit/1,
    rebalance_edit/2,
    apply_infos/2,
    apply_edit/2
]).

%% ---------------------------------------------------------------------------

-doc "Creates a new blank edit from the current manifest metadata".
-spec new_edit(t()) -> edit().
new_edit(#manifest{
    first_offset = FirstOffset,
    first_timestamp = FirstTs,
    first_last_timestamp = FirstLastTs,
    next_offset = NextOffset
}) ->
    #edit{
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        next_offset = NextOffset
    }.

-doc "Create an edit which describes a rebalance from a group being uploaded".
-spec rebalance_edit(#group_uploaded{}, t()) -> edit().
rebalance_edit(#group_uploaded{entry = Entry, pos = Pos, len = Len}, #manifest{} = Manifest) ->
    %% TODO: I'm pretty sure we need to clear next_offset here. Add a test
    %% case for the machine where rebalancing starts, a fragment is uploaded
    %% and applied, and then the rebalancing completes.
    (new_edit(Manifest))#edit{entries = Entry, pos = Pos, len = Len}.

-doc """
Create an edit that adds successfully uploaded fragments to their manifest
entries array.

`Infos` is expected to be sorted by offset ascending.
""".
-spec apply_infos([#fragment_info{}], t()) -> {ok, edit() | undefined} | {error, #fragment_info{}}.
apply_infos(Infos, #manifest{entries = Entries} = Manifest) ->
    Edit0 = (new_edit(Manifest))#edit{pos = byte_size(Entries)},
    apply_infos0(Infos, Edit0).

apply_infos0([], Edit) ->
    {ok, Edit};
apply_infos0(
    [
        #fragment_info{
            first_offset = Offset,
            next_offset = NextOffset,
            first_timestamp = FirstTs,
            last_timestamp = LastTs,
            size = Size
        }
        | Rest
    ],
    #edit{
        next_offset = Offset,
        size = Size0,
        entries = Entries0
    } = Edit0
) ->
    Edit1 =
        case Offset of
            0 ->
                %% For the very first fragment, also set the offset and timestamps.
                Edit0#edit{
                    first_offset = Offset,
                    first_timestamp = FirstTs,
                    first_last_timestamp = LastTs
                };
            _ ->
                Edit0
        end,
    Edit = Edit1#edit{
        next_offset = NextOffset,
        size = Size0 + Size,
        entries =
            <<Entries0/binary,
                (?ENTRY(Offset, FirstTs, LastTs, ?MANIFEST_KIND_FRAGMENT, Size, 0))/binary>>
    },
    apply_infos0(Rest, Edit);
apply_infos0([Info | _], #edit{}) ->
    {error, Info}.

-doc """
Apply an edit to a manifest.

The edit type describes a few kinds of modifications to a manifest:

* Appends from new fragments being uploaded.
* Updates to first_offset / first_timestamp / etc from evaluating retention
  when the retention affected a group object. (The group object is not
  modified, just the top-level metadata in the manifest.)
* Truncation from fragments being deleted by retention from the beginning of
  the manifest's array.
""".
-spec apply_edit(edit(), t()) -> t().
apply_edit(
    #edit{
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        next_offset = EditNextOffset,
        size = Size,
        entries = EditEntries,
        pos = Pos,
        len = Len
    },
    #manifest{
        next_offset = ManifestNextOffset,
        total_size = TotalSize0,
        entries = Entries0
    } = Manifest0
) ->
    Entries =
        if
            %% Pure insertion (append)
            Pos =:= byte_size(Entries0) andalso Len =:= 0 ->
                <<Entries0/binary, EditEntries/binary>>;
            %% No-op (empty)
            Len =:= 0 andalso EditEntries =:= <<>> ->
                Entries0;
            %% Pure deletion (truncate)
            EditEntries =:= <<>> ->
                %% We only truncate from the beginning. No hole punching.
                ?assertEqual(0, Pos),
                binary:part(Entries0, Len, byte_size(Entries0) - Len);
            %% Replacement (for rebalancing)
            true ->
                <<
                    (binary:part(Entries0, 0, Pos))/binary,
                    EditEntries/binary,
                    (binary:part(Entries0, Pos + Len, byte_size(Entries0) - Pos - Len))/binary
                >>
        end,
    NextOffset =
        case EditNextOffset of
            undefined ->
                ManifestNextOffset;
            _ when is_integer(EditNextOffset) ->
                EditNextOffset
        end,
    Manifest0#manifest{
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        next_offset = NextOffset,
        total_size = TotalSize0 + Size,
        entries = Entries
    }.
