%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_log_manifest_machine_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-import(rabbit_ct_helpers, [get_config/2]).

-define(MAC, rabbitmq_stream_s3_log_manifest_machine).
-define(META(), #{time => ?LINE}).

all() ->
    [
        spawn_writer,
        simultaneous_manifest_requests,
        out_of_order_fragment_uploads,
        recover_uploaded_fragments
    ].

%%----------------------------------------------------------------------------

spawn_writer(Config) ->
    Dir = get_config(Config, priv_dir),
    Mac0 = ?MAC:new(),
    Pid = self(),
    Event1 = #writer_spawned{
        pid = Pid,
        writer_ref = rabbit_misc:r(<<"/">>, queue, <<"sq">>),
        dir = Dir
    },
    {Mac1, Effects1} = ?MAC:apply(?META(), Event1, Mac0),
    ?assertEqual(
        [
            #register_offset_listener{writer_pid = Pid, offset = -1},
            #resolve_manifest{dir = Dir}
        ],
        Effects1
    ),
    Event2 = #manifest_resolved{dir = Dir, manifest = undefined},
    {_Mac2, Effects2} = ?MAC:apply(?META(), Event2, Mac1),
    ?assertEqual([], Effects2),
    ok.

simultaneous_manifest_requests(Config) ->
    %% While a writer is starting up an offset reader could start up too, for
    %% example when using `stream-perf-test` on single-node RabbitMQ. The
    %% download of the manifest should be shared between any processes which
    %% request it without duplicate downloads effects.

    Dir = get_config(Config, priv_dir),
    WriterPid = self(),
    Event1 = #writer_spawned{
        pid = WriterPid,
        writer_ref = rabbit_misc:r(<<"/">>, queue, <<"sq">>),
        dir = Dir
    },

    %% Simulate two offset readers starting while the manifest is being
    %% downloaded.
    Pid1 = spawn(fun() -> ok end),
    From1 = {Pid1, erlang:make_ref()},
    Event2 = #manifest_requested{requester = From1, dir = Dir},
    Pid2 = spawn(fun() -> ok end),
    From2 = {Pid2, erlang:make_ref()},
    Event3 = #manifest_requested{requester = From2, dir = Dir},

    {Mac1, Effects1} = handle_events(?META(), [Event1, Event2, Event3], ?MAC:new()),
    ?assertEqual(
        [
            #register_offset_listener{writer_pid = WriterPid, offset = -1},
            #resolve_manifest{dir = Dir}
        ],
        Effects1
    ),

    Event4 = #manifest_resolved{dir = Dir, manifest = undefined},
    {_Mac2, Effects2} = ?MAC:apply(?META(), Event4, Mac1),
    ?assertEqual(
        [
            #reply{to = From1, response = undefined},
            #reply{to = From2, response = undefined}
        ],
        Effects2
    ),
    ok.

out_of_order_fragment_uploads(Config) ->
    {Mac0, Ref} = setup_writer(Config),
    Fragments = [fragment(From, To) || {From, To} <- [{0, 19}, {20, 39}, {40, 59}]],
    FragmentsAvailable = [#fragment_available{writer_ref = Ref, fragment = F} || F <- Fragments],
    {Mac1, Effects1} = handle_events(?META(), FragmentsAvailable, Mac0),
    ?assertEqual([], Effects1),
    Event1 = #commit_offset_increased{writer_ref = Ref, offset = 60},
    {Mac2, Effects2} = ?MAC:apply(?META(), Event1, Mac1),
    ?assertMatch(
        [
            #upload_fragment{writer_ref = Ref, fragment = #fragment{first_offset = 0}},
            #upload_fragment{writer_ref = Ref, fragment = #fragment{first_offset = 20}},
            #upload_fragment{writer_ref = Ref, fragment = #fragment{first_offset = 40}},
            #register_offset_listener{}
        ],
        Effects2
    ),
    [Up1, Up2, Up3] = [
        #fragment_uploaded{writer_ref = Ref, info = fragment_to_info(F)}
     || F <- Fragments
    ],
    {Mac3, Effects3} = handle_events(?META(), [Up2, Up3], Mac2),
    %% The manifest won't be updated until a run of fragments have been uploaded:
    %% no holes are allowed in the manifest.
    ?assertEqual([], Effects3),
    %% Once the first fragment has finished uploading then the manifest is
    %% updated for all fragments.
    {_Mac4, Effects4} = ?MAC:apply(?META(), Up1, Mac3),
    ?assertMatch([#upload_manifest{manifest = #manifest{first_offset = 0}}], Effects4),
    ok.

recover_uploaded_fragments(Config) ->
    %% The writer uploads fragments but the updated manifest is not yet
    %% uploaded. When restarting, the writer resolve the full manifest and
    %% upload it.
    {Mac0, Ref} = setup_writer(Config),
    [F1, F2, _F3] = Fragments = [fragment(From, To) || {From, To} <- [{0, 19}, {20, 39}, {40, 59}]],
    FragmentsAvailable = [#fragment_available{writer_ref = Ref, fragment = F} || F <- Fragments],
    {Mac1, Effects1} = handle_events(?META(), FragmentsAvailable, Mac0),
    ?assertEqual([], Effects1),
    COI1 = #commit_offset_increased{writer_ref = Ref, offset = 40},
    {Mac2, Effects2} = ?MAC:apply(?META(), COI1, Mac1),
    ?assertMatch(
        [
            #upload_fragment{writer_ref = Ref, fragment = #fragment{first_offset = 0}},
            #upload_fragment{writer_ref = Ref, fragment = #fragment{first_offset = 20}},
            #register_offset_listener{}
        ],
        Effects2
    ),
    [Up1, _Up2, Up3] = [
        #fragment_uploaded{writer_ref = Ref, info = fragment_to_info(F)}
     || F <- Fragments
    ],
    %% Say that 1 and 2 are uploaded but before the reboot the manifest server
    %% only sees 1 complete its upload.
    {_Mac3, Effects3} = ?MAC:apply(?META(), Up1, Mac2),
    ?assertMatch(
        [#upload_manifest{manifest = #manifest{first_offset = 0, total_size = 1}}],
        Effects3
    ),

    %% --- Say that the writer reboots now ---
    %% During recovery it will notify the manifest server of the available
    %% fragments in the current segment. The manifest server will resolve
    %% what has been uploaded (fragments 1 and 2 now) and

    Pid = self(),
    Dir = get_config(Config, priv_dir),
    WriterSpawned = #writer_spawned{
        pid = Pid,
        writer_ref = Ref,
        dir = Dir
    },
    {Mac4, Effects4} = ?MAC:apply(?META(), WriterSpawned, ?MAC:new()),
    ?assertMatch([#register_offset_listener{}, #resolve_manifest{dir = Dir}], Effects4),
    %% The existing local fragments are sent to the manifest server as
    %% available.
    {Mac5, Effects5} = handle_events(?META(), FragmentsAvailable, Mac4),
    ?assertEqual([], Effects5),
    %% Before the manifest is resolved, the commit offset increases.
    COI2 = #commit_offset_increased{writer_ref = Ref, offset = 60},
    {Mac6, Effects6} = ?MAC:apply(?META(), COI2, Mac5),
    ?assertMatch([#register_offset_listener{}], Effects6),
    ManifestResolved = #manifest_resolved{dir = Dir, manifest = fragments_to_manifest([F1, F2])},
    {Mac7, Effects7} = ?MAC:apply(?META(), ManifestResolved, Mac6),
    %% The last fragment is now eligible for upload since the commit offset
    %% increased.
    ?assertMatch(
        [#upload_fragment{writer_ref = Ref, fragment = #fragment{first_offset = 40}}],
        Effects7
    ),
    {_Mac8, Effects8} = ?MAC:apply(?META(), Up3, Mac7),
    ?assertEqual([], Effects8),
    ok.

%%----------------------------------------------------------------------------

setup_writer(Config) ->
    Dir = get_config(Config, priv_dir),
    Pid = self(),
    WriterRef = rabbit_misc:r(<<"/">>, queue, <<"sq">>),
    Event1 = #writer_spawned{
        pid = Pid,
        writer_ref = WriterRef,
        dir = Dir
    },
    Event2 = #manifest_resolved{dir = Dir, manifest = undefined},
    {Mac, _} = handle_events(?META(), [Event1, Event2], ?MAC:new()),
    {Mac, WriterRef}.

handle_events(Meta, Events, Mac) ->
    handle_events(Meta, Events, Mac, []).

handle_events(_Meta, [], Mac, Acc) ->
    Effects = lists:flatten(lists:reverse(Acc)),
    {Mac, Effects};
handle_events(Meta, [Event | Rest], Mac0, Acc) ->
    {Mac, Effects} = ?MAC:apply(Meta, Event, Mac0),
    handle_events(Meta, Rest, Mac, [Effects | Acc]).

fragment(Offset, LastOffset) ->
    #fragment{
        segment_offset = Offset,
        first_offset = Offset,
        first_timestamp = erlang:system_time(millisecond),
        last_offset = LastOffset,
        next_offset = LastOffset + 1,
        size = 1
    }.

fragment_to_info(#fragment{
    first_offset = O,
    first_timestamp = T,
    next_offset = N,
    seq_no = Seq,
    size = Size
}) ->
    #fragment_info{
        offset = O,
        timestamp = T,
        next_offset = N,
        seq_no = Seq,
        size = Size
    }.

fragments_to_manifest([
    #fragment{first_offset = Offset, first_timestamp = Ts, size = Size, seq_no = SeqNo} | Rest
]) ->
    fragments_to_manifest(Rest, #manifest{
        first_offset = Offset,
        first_timestamp = Ts,
        total_size = Size,
        entries = ?ENTRY(Offset, Ts, ?MANIFEST_KIND_FRAGMENT, Size, SeqNo, <<>>)
    }).

fragments_to_manifest([], Manifest) ->
    Manifest;
fragments_to_manifest(
    [#fragment{first_offset = Offset, first_timestamp = Ts, size = Size, seq_no = SeqNo} | Rest],
    #manifest{total_size = TotalSize0, entries = Entries0} = Manifest0
) ->
    Manifest = Manifest0#manifest{
        total_size = TotalSize0 + Size,
        entries =
            <<Entries0/binary,
                (?ENTRY(Offset, Ts, ?MANIFEST_KIND_FRAGMENT, Size, SeqNo, <<>>))/binary>>
    },
    fragments_to_manifest(Rest, Manifest).
