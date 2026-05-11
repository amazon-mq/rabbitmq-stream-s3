%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_replica_reader).
-moduledoc """
Per-stream gen_server owning the upload lifecycle.

Reads committed chunks from the local log, assembles fragments,
uploads them to S3, updates the manifest, and broadcasts edits to
replica nodes. Monitors the writer process and stops on writer DOWN.
""".

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("include/logging.hrl").
-include("include/rabbitmq_stream_s3.hrl").

-export([start_link/1, await_offset/2, format_state/1]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_continue/2,
    handle_info/2,
    terminate/2,
    format_status/1
]).

-record(cfg, {
    stream :: stream_id(),
    dir :: directory(),
    writer_pid :: pid(),
    shared :: atomics:atomics_ref(),
    counter :: counters:counters_ref(),
    fragment_target_size :: non_neg_integer()
}).

-record(state, {
    cfg :: #cfg{},
    %% Set after manifest resolve + data reader open.
    log :: osiris_log:state() | undefined,
    assembly :: rabbitmq_stream_s3_fragment_assembly:state() | undefined,
    manifest :: #manifest{},
    %% Nodes registered for manifest broadcast.
    replicas = #{} :: #{node() => reference()},
    %% Callers blocked until next_offset passes their requested offset.
    waiters = [] :: [{osiris:offset(), gen_server:from()}]
}).

-doc "Start a remote replica reader for the given stream.".
-spec start_link(map()) -> gen_server:start_ret().
start_link(#{stream := StreamId} = Args) ->
    gen_server:start_link(
        {via, rabbitmq_stream_s3_registry, {StreamId, node()}},
        ?MODULE,
        Args,
        []
    ).

-doc "Block until the replica reader has uploaded past `Offset`.".
-spec await_offset(stream_id(), osiris:offset()) -> ok | {error, stopped}.
await_offset(StreamId, Offset) ->
    gen_server:call(
        {via, rabbitmq_stream_s3_registry, {StreamId, node()}},
        {await_offset, Offset},
        1000
    ).

init(#{stream := StreamId, writer_pid := WriterPid, dir := Dir} = Args) ->
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
    monitor(process, WriterPid),
    TargetSize = maps:get(
        fragment_target_size,
        Args,
        application:get_env(rabbitmq_stream_s3, fragment_target_size, ?MAX_FRAGMENT_SIZE_B)
    ),
    Shared = maps:get(shared, Args, undefined),
    ?LOG_INFO("Remote replica reader starting for stream ~ts", [StreamId]),
    Cfg = #cfg{
        stream = StreamId,
        dir = Dir,
        writer_pid = WriterPid,
        shared = Shared,
        counter = maps:get(counter, Args),
        fragment_target_size = TargetSize
    },
    {ok, #state{cfg = Cfg, manifest = #manifest{}}, {continue, resolve_manifest}}.

handle_call({await_offset, Offset}, From, #state{manifest = Manifest} = State) ->
    case Manifest#manifest.next_offset >= Offset of
        true ->
            {reply, ok, State};
        false ->
            {noreply, State#state{waiters = [{Offset, From} | State#state.waiters]}}
    end;
handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast(
    {register_acceptor, Node},
    #state{replicas = Replicas, manifest = Manifest, cfg = #cfg{stream = StreamId}} = State
) ->
    case maps:is_key(Node, Replicas) of
        true ->
            {noreply, State};
        false ->
            MonRef = monitor(process, {rabbitmq_stream_s3_manifest_replica, Node}),
            %% Send current manifest to the new replica.
            rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest, Node),
            {noreply, State#state{replicas = Replicas#{Node => MonRef}}}
    end;
handle_cast({retention_updated, _Retention}, State) ->
    %% TODO: forward to remote-tier retention evaluation
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_continue(resolve_manifest, #state{cfg = #cfg{stream = StreamId}} = State0) ->
    Manifest = resolve_manifest(StreamId),
    State = start_reading(State0#state{manifest = Manifest}),
    {noreply, State}.

handle_info({osiris_offset, _Ref, _Offset}, #state{} = State0) ->
    State = drain(State0),
    {noreply, State};
handle_info(retry_resolve, #state{cfg = #cfg{stream = StreamId}} = State0) ->
    Manifest = resolve_manifest(StreamId),
    State = start_reading(State0#state{manifest = Manifest}),
    {noreply, State};
handle_info(
    {'DOWN', _Mon, process, Pid, _Reason},
    #state{cfg = #cfg{writer_pid = Pid, stream = StreamId}} = State
) ->
    ?LOG_INFO("Writer down, stopping remote replica reader for stream ~ts", [StreamId]),
    {stop, normal, State};
handle_info(
    {'DOWN', MonRef, process, {rabbitmq_stream_s3_manifest_replica, Node}, _Reason},
    #state{replicas = Replicas} = State
) ->
    case maps:get(Node, Replicas, undefined) of
        MonRef -> {noreply, State#state{replicas = maps:remove(Node, Replicas)}};
        _ -> {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{cfg = #cfg{stream = StreamId}, waiters = Waiters}) ->
    [gen_server:reply(From, {error, stopped}) || {_Offset, From} <- Waiters],
    rabbitmq_stream_s3_registry:unregister_name({StreamId, node()}),
    ok.

format_status(#{state := State} = Status) ->
    Status#{state := format_state(State)}.

format_state(#state{
    cfg = #cfg{stream = StreamId, fragment_target_size = Target},
    log = Log,
    assembly = Assembly,
    manifest = Manifest,
    waiters = Waiters
}) ->
    #{
        stream => StreamId,
        fragment_target_size => Target,
        manifest_next_offset => Manifest#manifest.next_offset,
        manifest_entries => byte_size(Manifest#manifest.entries) div ?ENTRY_B,
        log_next_offset =>
            case Log of
                undefined -> undefined;
                _ -> osiris_log:next_offset(Log)
            end,
        assembly =>
            case Assembly of
                undefined -> undefined;
                _ -> rabbitmq_stream_s3_fragment_assembly:info(Assembly)
            end,
        waiters => [Offset || {Offset, _From} <- Waiters]
    }.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

-spec apply_upload(
    rabbitmq_stream_s3:uid(), rabbitmq_stream_s3_fragment_assembly:fragment_meta(), #state{}
) -> #state{}.
apply_upload(Uid, Meta, #state{cfg = #cfg{stream = StreamId}, manifest = Manifest0} = State) ->
    #{
        first_offset := FirstOffset,
        first_timestamp := FirstTs,
        last_timestamp := LastTs,
        next_offset := NextOffset,
        size := Size
    } = Meta,
    Entry = ?ENTRY(FirstOffset, FirstTs, LastTs, ?MANIFEST_KIND_FRAGMENT, Size, Uid),
    Edit = #edit{
        first_offset = Manifest0#manifest.first_offset,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = NextOffset,
        size = Size,
        entries = Entry,
        pos = byte_size(Manifest0#manifest.entries),
        len = 0
    },
    %% For the very first fragment, set the manifest's first_* fields.
    Edit1 =
        case Manifest0#manifest.next_offset of
            0 ->
                Edit#edit{
                    first_offset = FirstOffset,
                    first_timestamp = FirstTs,
                    first_last_timestamp = LastTs
                };
            _ ->
                Edit
        end,
    Manifest = rabbitmq_stream_s3_manifest:apply_edit(Edit1, Manifest0),
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest),
    broadcast_edit(StreamId, Edit1, State),
    maybe_evaluate_retention(Manifest0, Manifest, State),
    notify_waiters(State#state{manifest = Manifest}).

%% Kick retention evaluation when the uploaded range advances.
%% This allows osiris to reclaim fully-uploaded segments without
%% waiting for the next segment roll.
-spec maybe_evaluate_retention(#manifest{}, #manifest{}, #state{}) -> ok.
maybe_evaluate_retention(OldManifest, NewManifest, #state{cfg = Cfg}) ->
    case NewManifest#manifest.next_offset > OldManifest#manifest.next_offset of
        true ->
            #cfg{stream = StreamId, dir = Dir, shared = Shared, counter = Cnt} = Cfg,
            Spec = [{'fun', rabbitmq_stream_s3_hooks:local_retention_fun(StreamId)}],
            EvalFun = fun
                ({{FstOff, _}, _FstTs, NumSegLeft}) when is_integer(FstOff) ->
                    osiris_log_shared:set_first_chunk_id(Shared, FstOff),
                    update_counter(Cnt, FstOff, NumSegLeft);
                (_) ->
                    ok
            end,
            osiris_retention:eval(StreamId, Dir, Spec, EvalFun);
        false ->
            ok
    end.

update_counter(Cnt, FstOff, NumSegLeft) ->
    counters:put(Cnt, ?C_OSIRIS_LOG_FIRST_OFFSET, FstOff),
    counters:put(Cnt, ?C_OSIRIS_LOG_SEGMENTS, NumSegLeft).

-spec notify_waiters(#state{}) -> #state{}.
notify_waiters(#state{manifest = Manifest, waiters = Waiters} = State) ->
    NextOffset = Manifest#manifest.next_offset,
    {Satisfied, Remaining} = lists:partition(
        fun({Offset, _From}) -> NextOffset >= Offset end,
        Waiters
    ),
    [gen_server:reply(From, ok) || {_Offset, From} <- Satisfied],
    State#state{waiters = Remaining}.

-spec broadcast_edit(stream_id(), #edit{}, #state{}) -> ok.
broadcast_edit(StreamId, Edit, #state{replicas = Replicas}) ->
    maps:foreach(
        fun(Node, _MonRef) ->
            rabbitmq_stream_s3_manifest_replica:apply_edit(StreamId, Edit, Node)
        end,
        Replicas
    ).

-spec resolve_manifest(stream_id()) -> #manifest{}.
resolve_manifest(StreamId) ->
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{} = M ->
            M;
        undefined ->
            case catch rabbitmq_stream_s3_db:get(StreamId) of
                {ok, #{uid := Uid}} ->
                    Key = rabbitmq_stream_s3:manifest_key(StreamId, Uid),
                    case rabbitmq_stream_s3_api:get(Key, #{}) of
                        {ok, Data} ->
                            parse_manifest_root(Data);
                        {error, _} ->
                            #manifest{}
                    end;
                _ ->
                    #manifest{}
            end
    end.

-spec parse_manifest_root(binary()) -> #manifest{}.
parse_manifest_root(?MANIFEST(FirstOffset, NextOffset, FirstTs, FirstLastTs, TotalSize, Entries)) ->
    #manifest{
        first_offset = FirstOffset,
        next_offset = NextOffset,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        total_size = TotalSize,
        entries = Entries
    }.

-spec start_reading(#state{}) -> #state{}.
start_reading(
    #state{
        cfg = #cfg{
            writer_pid = WriterPid,
            fragment_target_size = TargetSize,
            stream = StreamId,
            dir = _CfgDir
        },
        manifest = Manifest
    } = State
) ->
    StartOffset = Manifest#manifest.next_offset,
    %% Writer barrier: ensures handle_continue has completed and the
    %% log is fully initialized before we open a data reader.
    _ = osiris_writer:query_replication_state(WriterPid),
    osiris:register_offset_listener(WriterPid, StartOffset),
    case osiris_writer:init_data_reader(WriterPid, {StartOffset, empty}, #{}) of
        {ok, Log} ->
            Assembly = rabbitmq_stream_s3_fragment_assembly:new(TargetSize),
            drain(State#state{log = Log, assembly = Assembly});
        {error, Reason} ->
            ?LOG_WARNING(
                "Failed to open data reader for stream ~ts: ~p",
                [StreamId, Reason]
            ),
            erlang:send_after(1000, self(), retry_resolve),
            State
    end.

-spec drain(#state{}) -> #state{}.
drain(#state{log = undefined} = State) ->
    State;
drain(
    #state{
        cfg = #cfg{
            stream = StreamId,
            dir = Dir,
            writer_pid = WriterPid,
            fragment_target_size = TargetSize
        },
        log = Log0,
        assembly = Assembly0
    } = State
) ->
    case osiris_log:read_header(Log0) of
        {ok, Header, Log1} ->
            SegFile = osiris_log:get_current_file(Log1),
            SegOffset = rabbitmq_stream_s3:segment_file_offset(SegFile),
            Chunk = #{
                chunk_id => maps:get(chunk_id, Header),
                timestamp => maps:get(timestamp, Header),
                num_records => maps:get(num_records, Header),
                data_size => maps:get(data_size, Header),
                position => maps:get(position, Header),
                next_position => maps:get(next_position, Header),
                segment_offset => SegOffset,
                crc => maps:get(crc, Header)
            },
            Assembly1 = rabbitmq_stream_s3_fragment_assembly:add_chunk(Chunk, Assembly0),
            case rabbitmq_stream_s3_fragment_assembly:is_cut(Assembly1) of
                true ->
                    State1 =
                        case upload_fragment(Dir, StreamId, Assembly1) of
                            {ok, Uid, Meta} ->
                                apply_upload(Uid, Meta, State);
                            {error, Reason} ->
                                ?LOG_ERROR(
                                    "Fragment upload failed for stream ~ts: ~p",
                                    [StreamId, Reason]
                                ),
                                State
                        end,
                    Assembly2 = rabbitmq_stream_s3_fragment_assembly:new(TargetSize),
                    drain(State1#state{log = Log1, assembly = Assembly2});
                false ->
                    drain(State#state{log = Log1, assembly = Assembly1})
            end;
        {end_of_stream, Log1} ->
            NextOffset = osiris_log:next_offset(Log1),
            osiris:register_offset_listener(WriterPid, NextOffset),
            osiris:register_offset_listener(WriterPid, NextOffset),
            State#state{log = Log1, assembly = Assembly0};
        {error, Reason} ->
            ?LOG_ERROR("Read error for stream ~ts: ~p", [StreamId, Reason]),
            State
    end.

%% ------------------------------------------------------------------
%% Upload
%% ------------------------------------------------------------------

%% 4 MiB
-define(READ_BUFFER_SIZE, 4_194_304).

-spec upload_fragment(directory(), stream_id(), rabbitmq_stream_s3_fragment_assembly:state()) ->
    {ok, rabbitmq_stream_s3:uid(), rabbitmq_stream_s3_fragment_assembly:fragment_meta()}
    | {error, term()}.
upload_fragment(Dir, StreamId, Assembly) ->
    Uid = rabbitmq_stream_s3:uid(),
    Meta = rabbitmq_stream_s3_fragment_assembly:metadata(Assembly),
    Spans = maps:get(spans, Meta),
    Size = maps:get(size, Meta),
    NumChunks = maps:get(num_chunks, Meta),
    FirstOffset = maps:get(first_offset, Meta),

    IndexSize = NumChunks * ?INDEX_RECORD_B,
    ContentLength = ?SEGMENT_HEADER_B + Size + IndexSize,
    Key = rabbitmq_stream_s3:fragment_key(StreamId, FirstOffset, Uid),

    maybe
        {ok, Stream0} ?= rabbitmq_stream_s3_api:stream_put(Key, ContentLength, #{}),
        Header = <<"OSIF", ?FRAGMENT_VERSION:32/unsigned>>,
        Stream1 = rabbitmq_stream_s3_api:stream_data(Stream0, Header),
        Crc0 = erlang:crc32(Header),
        %% TODO: per-chunk CRC validation during streaming
        {ok, Stream2, Crc1} ?= stream_spans(Stream1, Crc0, Dir, Spans),
        IdxRecords = rabbitmq_stream_s3_fragment_assembly:index_records(Assembly),
        Stream3 = rabbitmq_stream_s3_api:stream_data(Stream2, IdxRecords),
        Crc = erlang:crc32(Crc1, IdxRecords),
        ok ?= rabbitmq_stream_s3_api:stream_finish(Stream3, Crc),
        {ok, Uid, Meta}
    end.

stream_spans(Stream, Crc, _Dir, []) ->
    {ok, Stream, Crc};
stream_spans(Stream0, Crc0, Dir, [{SegOffset, StartPos, EndPos} | Rest]) ->
    SegFile = filename:join(Dir, rabbitmq_stream_s3:offset_filename(SegOffset, <<"segment">>)),
    case file:open(SegFile, [read, raw, binary]) of
        {ok, Fd} ->
            Result =
                try
                    stream_span(Stream0, Crc0, Fd, StartPos, EndPos - StartPos)
                after
                    file:close(Fd)
                end,
            case Result of
                {ok, Stream1, Crc1} ->
                    stream_spans(Stream1, Crc1, Dir, Rest);
                {error, _} = Err ->
                    Err
            end;
        {error, Reason} ->
            {error, {open_failed, SegFile, Reason}}
    end.

stream_span(Stream, Crc, _Fd, _Pos, 0) ->
    {ok, Stream, Crc};
stream_span(Stream0, Crc0, Fd, Pos, Remaining) ->
    ReadSize = min(Remaining, ?READ_BUFFER_SIZE),
    case file:pread(Fd, Pos, ReadSize) of
        {ok, Data} ->
            Stream1 = rabbitmq_stream_s3_api:stream_data(Stream0, Data),
            Crc1 = erlang:crc32(Crc0, Data),
            stream_span(Stream1, Crc1, Fd, Pos + ReadSize, Remaining - ReadSize);
        eof ->
            {error, {unexpected_eof, Pos, ReadSize}};
        {error, _} = Err ->
            Err
    end.
