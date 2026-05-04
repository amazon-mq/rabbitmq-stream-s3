%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(log_reader_SUITE).
-moduledoc """
Low-level tests for `rabbitmq_stream_s3_log_reader`.

These tests bypass the full broker stack. They use:
- `rabbitmq_stream_s3_api_fs` as the storage backend
- `rabbitmq_stream_s3_server_ets` as the manifest server backend
- `osiris_log` directly to create local segment data
- Seed helpers to upload fragments synchronously

This allows precise control over the layout of the local and remote tiers.
""".

-compile([nowarn_export_all, export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("include/rabbitmq_stream_s3.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
        {group, parallel}
    ].

groups() ->
    [
        {parallel, [parallel], [
            read_from_first_with_remote_tier
        ]}
    ].

init_per_suite(Config) ->
    _ = application:ensure_all_started(logger),
    %% Suppress non-error logs sent to stdout. CommonTest is already capturing
    %% logs independently.
    logger:set_handler_config(default, level, error),
    {ok, _} = application:ensure_all_started(seshat),
    _ = seshat:new_group(rabbitmq_stream_s3),
    osiris:configure_logger(logger),
    application:set_env(rabbitmq_stream_s3, rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_fs),
    application:set_env(
        rabbitmq_stream_s3,
        rabbitmq_stream_s3_server,
        rabbitmq_stream_s3_server_ets
    ),
    application:set_env(osiris, max_segment_size_chunks, 10),
    PrivDir = ?config(priv_dir, Config),
    DataDir = filename:join(PrivDir, "shared"),
    ok = filelib:ensure_path(DataDir),
    rabbitmq_stream_s3_api_fs:set_data_dir(DataDir),
    ok = rabbitmq_stream_s3_api:init(),
    ok = rabbitmq_stream_s3_server:init_counters(),
    ok = rabbitmq_stream_s3_log_reader:init_counters(),
    {ok, SupPid} = rabbitmq_stream_s3_remote_reader_sup:start_link(),
    unlink(SupPid),
    %% Use a long-living process to create the ETS tables.
    %% Transfer ETS table ownership to a long-lived process so parallel
    %% test cases can access them after init_per_suite exits.
    Holder = spawn(fun() ->
        ok = rabbitmq_stream_s3_server_ets:setup(),
        receive
            stop -> ok
        end
    end),
    [{fs_data_dir, DataDir}, {sup_pid, SupPid}, {ets_holder, Holder} | Config].

end_per_suite(Config) ->
    ?config(ets_holder, Config) ! stop,
    Pid = ?config(sup_pid, Config),
    MRef = erlang:monitor(process, Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', MRef, process, Pid, _} ->
            ok
    after 100 ->
        ok
    end.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    %% Unique stream ID per test so parallel tests don't share remote storage.
    StreamId = iolist_to_binary(["__", atom_to_list(TestCase), "_1"]),
    Shared = osiris_log_shared:new(),
    OsirisCfg = #{
        dir => filename:join([?config(fs_data_dir, Config), atom_to_list(TestCase), "osiris"]),
        name => binary_to_list(StreamId),
        epoch => 1,
        readers_counter_fun => fun(_) -> ok end,
        shared => Shared,
        options => #{},
        max_segment_size_bytes => 10_000
    },
    ok = filelib:ensure_path(maps:get(dir, OsirisCfg)),
    [{stream_id, StreamId}, {osiris_cfg, OsirisCfg}, {shared, Shared} | Config].

end_per_testcase(_TestCase, Config) ->
    Config.

%%%===================================================================
%%% Test cases
%%%===================================================================

read_from_first_with_remote_tier(Config) ->
    Messages = messages(20, 500),

    %% Write messages to the local tier and upload to the remote tier.
    ok = seed_local_tier(Messages, Config),
    Manifest = upload_to_remote_tier(#manifest{}, Config),
    publish_manifest(Manifest, Config),

    %% Local tier: 2 segments starting at offsets 0 and 10.
    %% Remote tier: 2 fragments starting at offsets 0 and 10.
    ?assertEqual([0, 10], local_segments(Config)),
    ?assertEqual([0, 10], remote_fragments(Config)),

    %% After local retention, only the active segment (offset 10) remains.
    Manifest = execute_local_retention(Manifest, Config),
    ?assertEqual([10], local_segments(Config)),

    ReaderCfg = reader_config(Config),
    {ok, Reader0} = rabbitmq_stream_s3_log_reader:init_offset_reader(first, ReaderCfg),
    ?assertEqual(remote, rabbitmq_stream_s3_log_reader:mode(Reader0)),

    Records = read_all(Reader0),
    ?assertEqual(Messages, Records).

%%%===================================================================
%%% Helpers
%%%===================================================================

-doc "Generate N messages each padded to the given byte size.".
messages(N, Size) ->
    [
        begin
            Prefix = <<"msg-", (integer_to_binary(I))/binary, "-">>,
            Pad = max(0, Size - byte_size(Prefix)),
            <<Prefix/binary, (binary:copy(<<"x">>, Pad))/binary>>
        end
     || I <- lists:seq(1, N)
    ].

-doc "Write messages to the local osiris log and return the updated manifest placeholder.".
seed_local_tier(Messages, Config) ->
    OsirisCfg = ?config(osiris_cfg, Config),
    Shared = ?config(shared, Config),
    Log0 = osiris_log:init(OsirisCfg),
    Log1 = lists:foldl(fun(Msg, L) -> osiris_log:write([Msg], L) end, Log0, Messages),
    LastOffset = osiris_log:next_offset(Log1) - 1,
    osiris_log_shared:set_committed_chunk_id(Shared, LastOffset),
    ok = osiris_log:close(Log1).

-doc "Upload all new local segments to the remote tier and return the updated manifest.".
upload_to_remote_tier(Manifest0, Config) ->
    Dir = maps:get(dir, ?config(osiris_cfg, Config)),
    upload_all_fragments(Dir, Manifest0, Config).

-doc "First offsets of segment files currently in the local osiris log directory.".
local_segments(Config) ->
    Dir = maps:get(dir, ?config(osiris_cfg, Config)),
    {ok, Files} = file:list_dir(Dir),
    lists:sort([
        binary_to_integer(list_to_binary(filename:basename(F, ".segment")))
     || F <- Files, filename:extension(F) =:= ".segment"
    ]).

-doc "First offsets of fragment files currently stored in the remote tier.".
remote_fragments(Config) ->
    rabbitmq_stream_s3_api_fs:list_fragments(?config(stream_id, Config)).

-doc "Upload all segments in the osiris log directory and return the updated manifest".
upload_all_fragments(Dir, Manifest0, Config) ->
    StreamId = ?config(stream_id, Config),
    {ok, Files} = file:list_dir(Dir),
    IdxFiles = lists:sort([filename:join(Dir, F) || F <- Files, filename:extension(F) =:= ".index"]),
    NewIdxFiles = [
        F
     || F <- IdxFiles,
        rabbitmq_stream_s3:index_file_offset(F) >= Manifest0#manifest.next_offset
    ],
    lists:foldl(
        fun(IdxFile, ManifestAcc) ->
            {Fragment, _} = rabbitmq_stream_s3_log_manifest:recover_fragments(IdxFile),
            Effect = #upload_fragment{stream = StreamId, dir = Dir, fragment = Fragment},
            #fragment_uploaded{info = Info} = rabbitmq_stream_s3_server:execute_task(Effect),
            {ok, Edit} = rabbitmq_stream_s3_manifest:apply_infos([Info], ManifestAcc),
            rabbitmq_stream_s3_manifest:apply_edit(Edit, ManifestAcc)
        end,
        Manifest0,
        NewIdxFiles
    ).

-doc """
Evaluate local-tier retention, deleting segments whose data is fully in the
remote tier and updating the shared ref. Equivalent to the server's
`#trigger_retention` effect.
""".
execute_local_retention(Manifest, Config) ->
    StreamId = ?config(stream_id, Config),
    OsirisCfg = ?config(osiris_cfg, Config),
    Shared = ?config(shared, Config),
    Dir = maps:get(dir, OsirisCfg),
    Spec = [{'fun', rabbitmq_stream_s3_server:local_retention_fun(StreamId)}],
    case osiris_log:evaluate_retention(Dir, Spec) of
        {{FstOff, _}, _FstTs, _NumSeg} when is_integer(FstOff) ->
            osiris_log_shared:set_first_chunk_id(Shared, FstOff);
        _ ->
            ok
    end,
    Manifest.

-doc """
Evaluate remote-tier retention against the manifest with the given spec,
deleting fragment objects and updating the ETS manifest and range.
Equivalent to the server's `#evaluate_retention` task.
""".
execute_remote_retention(Spec, Manifest, Config) ->
    StreamId = ?config(stream_id, Config),
    Now = erlang:system_time(millisecond),
    GetGroupFun = rabbitmq_stream_s3_server:get_group_fun(StreamId, retention),
    {Edit, Deletions} = rabbitmq_stream_s3_machine:execute_retention(
        Manifest, Now, Spec, GetGroupFun
    ),
    case Deletions of
        [] ->
            ok;
        _ ->
            Effect = #delete_objects{stream = StreamId, objects = Deletions},
            rabbitmq_stream_s3_server:execute_task(Effect)
    end,
    NewManifest = rabbitmq_stream_s3_manifest:apply_edit(Edit, Manifest),
    publish_manifest(NewManifest, Config),
    NewManifest.

publish_manifest(Manifest, Config) ->
    StreamId = ?config(stream_id, Config),
    rabbitmq_stream_s3_server_ets:set_manifest(StreamId, Manifest),
    publish_range(Manifest#manifest.first_offset, Manifest#manifest.next_offset - 1, Config).

-doc "Update only the range in the ETS server, without changing the manifest.".
publish_range(First, Last, Config) ->
    rabbitmq_stream_s3_server_ets:set_range(?config(stream_id, Config), First, Last).

-doc "Delete a fragment from remote storage, simulating remote-tier retention.".
delete_remote_fragment(FirstOffset, Config) ->
    Key = rabbitmq_stream_s3:fragment_key(?config(stream_id, Config), FirstOffset),
    case rabbitmq_stream_s3_api:delete(Key) of
        ok -> ok;
        {error, [{Key, {error, enoent}}]} -> ok
    end.

-doc """
Re-open the local osiris log to update the shared ref from the actual segment
files on disk. Use this after remote-tier changes to ensure the local first
offset reflects reality.
""".
refresh_local_tier(Config) ->
    OsirisCfg = ?config(osiris_cfg, Config),
    Log = osiris_log:init(OsirisCfg),
    ok = osiris_log:close(Log).

reader_config(Config) ->
    #{
        name => ?config(stream_id, Config),
        dir => maps:get(dir, ?config(osiris_cfg, Config)),
        epoch => 1,
        shared => ?config(shared, Config),
        options => #{transport => tcp},
        readers_counter_fun => fun(_) -> ok end
    }.

-doc "Drain all records from a reader via chunk_iterator + iterator_next".
read_all(Reader0) ->
    read_all(Reader0, []).

read_all(Reader0, Acc) ->
    case rabbitmq_stream_s3_log_reader:chunk_iterator(Reader0, 1, undefined) of
        {ok, _Header, Iter, Reader1} ->
            Records = drain_iter(Iter, []),
            read_all(Reader1, Acc ++ Records);
        {end_of_stream, _Reader} ->
            Acc;
        {error, Reason} ->
            error({read_error, Reason})
    end.

drain_iter(Iter, Acc) ->
    case rabbitmq_stream_s3_log_reader:iterator_next(Iter) of
        {{_Offset, Record}, Iter1} ->
            drain_iter(Iter1, [Record | Acc]);
        end_of_chunk ->
            lists:reverse(Acc)
    end.
