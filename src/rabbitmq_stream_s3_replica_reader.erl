%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_replica_reader).
-moduledoc """
Per-stream gen_server owning the upload lifecycle.

Reads committed chunks from the local log, assembles fragments,
submits them to the governor for transfer, and executes effects
returned by the functional core module.
""".

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("include/logging.hrl").
-include("include/rabbitmq_stream_s3.hrl").

-export([start_link/1, format_state/1]).
-export([identity_formatter/1]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_continue/2,
    handle_info/2,
    terminate/2,
    format_status/1
]).

-export_type([config/0]).

-type config() :: #{
    stream := stream_id(),
    writer_pid := pid(),
    dir := directory(),
    counter := counters:counters_ref(),
    reference := term(),
    epoch := non_neg_integer(),
    shared => atomics:atomics_ref(),
    fragment_target_size => non_neg_integer(),
    durable_commit_threshold => non_neg_integer()
}.

-record(cfg, {
    stream :: stream_id(),
    dir :: directory(),
    writer_pid :: pid(),
    shared :: atomics:atomics_ref(),
    counter :: counters:counters_ref(),
    fragment_target_size :: non_neg_integer(),
    reference :: term(),
    epoch :: non_neg_integer()
}).

-define(OFFSET_FORMATTER, {?MODULE, identity_formatter, []}).
%% Once Osiris supports `identity` as an atom argument to
%% `register_offset_listener/3` (i.e. a `wrap_osiris_event(identity, Evt) ->
%% Evt;` clause in osiris_writer), we can replace ?OFFSET_FORMATTER with the
%% atom `identity` and drop the identity_formatter export. Change is on the
%% Osiris-upstream wishlist.

-record(state, {
    cfg :: #cfg{},
    %% The remote replica reader configuration map.
    config :: map(),
    %% Set after manifest resolve + data reader open.
    log :: osiris_log:state() | undefined,
    assembly :: rabbitmq_stream_s3_fragment_assembly:state() | undefined,
    %% Functional core state.
    core :: rabbitmq_stream_s3_replica_reader_core:state(),
    %% Nodes registered for manifest broadcast.
    replicas = #{} :: #{node() => reference()},
    %% Commit timer reference.
    commit_timer :: reference() | undefined
}).

-doc "Start a remote replica reader for the given stream.".
-spec start_link(config()) -> gen_server:start_ret().
start_link(#{stream := StreamId} = Args) ->
    gen_server:start_link(
        {via, rabbitmq_stream_s3_registry, {StreamId, node()}},
        ?MODULE,
        Args,
        []
    ).

init(
    #{
        stream := StreamId,
        writer_pid := WriterPid,
        dir := Dir,
        reference := Reference,
        epoch := Epoch
    } = Args
) ->
    process_flag(trap_exit, true),
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
        fragment_target_size = TargetSize,
        reference = Reference,
        epoch = Epoch
    },
    {ok, #state{cfg = Cfg, config = Args, core = undefined}, {continue, resolve_manifest}}.

handle_call({await_offset, Offset}, From, #state{core = Core0} = State) ->
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:await_offset(Offset, From, Core0),
    {noreply, execute_effects(Effects, State#state{core = Core})};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast(
    {register_acceptor, Node},
    #state{replicas = Replicas, core = Core, cfg = #cfg{stream = StreamId}} = State
) ->
    case maps:is_key(Node, Replicas) of
        true ->
            {noreply, State};
        false ->
            MonRef = monitor(process, {rabbitmq_stream_s3_manifest_replica, Node}),
            Manifest = core_manifest(Core),
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
    {Core, _Effects} = rabbitmq_stream_s3_replica_reader_core:init(Manifest, State0#state.config),
    State = start_reading(State0#state{core = Core}),
    {noreply, State}.

handle_info({osiris_offset, _Ref, Offset}, #state{cfg = #cfg{stream = StreamId}} = State0) ->
    ?LOG_DEBUG("~ts osiris_offset notification offset=~b", [StreamId, Offset]),
    State = drain(State0),
    {noreply, State};
handle_info(retry_resolve, #state{cfg = #cfg{stream = StreamId}} = State0) ->
    Manifest = resolve_manifest(StreamId),
    {Core, _} = rabbitmq_stream_s3_replica_reader_core:init(Manifest, State0#state.config),
    State = start_reading(State0#state{core = Core}),
    {noreply, State};
handle_info({transfer_result, Ref, {ok, Uid}}, #state{core = Core0, cfg = Cfg} = State0) ->
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref, Uid, Core0),
    State1 = State0#state{core = Core},
    %% Update the manifest cache before executing effects so that
    %% reply_waiters callers see the updated range immediately.
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(
        Cfg#cfg.stream, core_manifest(Core)
    ),
    State2 = execute_effects(Effects, State1),
    {noreply, State2};
handle_info({transfer_result, Ref, {error, Reason}}, #state{core = Core0} = State0) ->
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(Ref, Reason, Core0),
    {noreply, execute_effects(Effects, State0#state{core = Core})};
handle_info(commit_timer, #state{core = Core0} = State0) ->
    Now = erlang:system_time(millisecond),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, Core0),
    {noreply, execute_effects(Effects, State0#state{core = Core, commit_timer = undefined})};
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

terminate(_Reason, #state{cfg = #cfg{stream = StreamId}}) ->
    rabbitmq_stream_s3_registry:unregister_name({StreamId, node()}),
    ok.

format_status(#{state := State} = Status) ->
    Status#{state := format_state(State)}.

format_state(#state{
    cfg = #cfg{stream = StreamId, fragment_target_size = Target},
    log = Log,
    assembly = Assembly,
    core = Core
}) ->
    #{
        stream => StreamId,
        fragment_target_size => Target,
        manifest_next_offset =>
            case Core of
                undefined -> undefined;
                _ -> (core_manifest(Core))#manifest.next_offset
            end,
        log_next_offset =>
            case Log of
                undefined -> undefined;
                _ -> osiris_log:next_offset(Log)
            end,
        assembly =>
            case Assembly of
                undefined -> undefined;
                _ -> rabbitmq_stream_s3_fragment_assembly:info(Assembly)
            end
    }.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

identity_formatter(Evt) -> Evt.

%% ------------------------------------------------------------------
%% Effect execution
%% ------------------------------------------------------------------

-spec execute_effects([rabbitmq_stream_s3_replica_reader_core:core_effect()], #state{}) -> #state{}.
execute_effects([], State) ->
    State;
execute_effects([Effect | Rest], State0) ->
    State = execute_effect(Effect, State0),
    execute_effects(Rest, State).

execute_effect({submit_transfer, Ref, _StreamId, Dir, Meta}, #state{cfg = Cfg} = State) ->
    StreamId = Cfg#cfg.stream,
    Size = maps:get(size, Meta),
    Self = self(),
    Fun = fun() ->
        case upload_fragment(Dir, StreamId, Meta) of
            {ok, Uid} -> {ok, Uid};
            {error, _} = Err -> Err
        end
    end,
    rabbitmq_stream_s3_governor:submit(Fun, Size, Self, Ref),
    State;
execute_effect({resubmit_transfer, Ref, _StreamId, Dir, Meta}, #state{cfg = Cfg} = State) ->
    StreamId = Cfg#cfg.stream,
    Size = maps:get(size, Meta),
    Self = self(),
    Fun = fun() ->
        case upload_fragment(Dir, StreamId, Meta) of
            {ok, Uid} -> {ok, Uid};
            {error, _} = Err -> Err
        end
    end,
    rabbitmq_stream_s3_governor:submit(Fun, Size, Self, Ref),
    State;
execute_effect(
    {start_commit, _Manifest, _Epoch, _Reference, _Revision, _Edits}, #state{core = Core0} = State
) ->
    %% For now, complete the commit immediately (no Khepri).
    %% TODO: wire Khepri conditional write here.
    Revision = (core_manifest(Core0))#manifest.revision + 1,
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:commit_complete(Revision, Core0),
    execute_effects(Effects, State#state{core = Core});
execute_effect({update_range, _FirstOffset, _NextOffset}, #state{cfg = Cfg, core = Core} = State) ->
    Manifest = core_manifest(Core),
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(Cfg#cfg.stream, Manifest),
    State;
execute_effect({broadcast, StreamId, Edits}, #state{replicas = Replicas} = State) ->
    maps:foreach(
        fun(Node, _MonRef) ->
            [rabbitmq_stream_s3_manifest_replica:apply_edit(StreamId, Edit, Node) || Edit <- Edits]
        end,
        Replicas
    ),
    State;
execute_effect({evaluate_retention, _StreamId, _Dir}, #state{core = Core} = State) ->
    maybe_evaluate_retention(core_manifest(Core), State),
    State;
execute_effect({reply_waiters, Replies}, State) ->
    [gen_server:reply(From, Reply) || {From, Reply} <- Replies],
    State;
execute_effect({start_commit_timer, Ms}, #state{commit_timer = OldRef} = State) ->
    cancel_timer(OldRef),
    Ref = erlang:send_after(Ms, self(), commit_timer),
    State#state{commit_timer = Ref};
execute_effect({cancel_commit_timer}, #state{commit_timer = Ref} = State) ->
    cancel_timer(Ref),
    State#state{commit_timer = undefined};
execute_effect({reinitialize}, #state{cfg = Cfg} = State) ->
    ?LOG_WARNING("Commit conflict, re-resolving manifest for ~ts", [Cfg#cfg.stream]),
    erlang:send_after(0, self(), retry_resolve),
    State.

cancel_timer(undefined) -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).

%% ------------------------------------------------------------------
%% Retention
%% ------------------------------------------------------------------

-spec maybe_evaluate_retention(#manifest{}, #state{}) -> ok.
maybe_evaluate_retention(Manifest, #state{cfg = Cfg}) ->
    case Manifest#manifest.next_offset > 0 of
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

%% ------------------------------------------------------------------
%% Manifest access
%% ------------------------------------------------------------------

-spec core_manifest(rabbitmq_stream_s3_replica_reader_core:state()) -> #manifest{}.
core_manifest(Core) ->
    %% #state{cfg, manifest, ...} - manifest is element 3.
    element(3, Core).

%% ------------------------------------------------------------------
%% Reading
%% ------------------------------------------------------------------

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
            writer_pid = WriterPid
        }
    } = State
) ->
    Manifest = core_manifest(Core),
    StartOffset = Manifest#manifest.next_offset,
    _ = osiris_writer:query_replication_state(WriterPid),
    osiris:register_offset_listener(WriterPid, StartOffset, ?OFFSET_FORMATTER),
    ?LOG_DEBUG("~ts start_reading start_offset=~b", [StreamId, StartOffset]),
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
            writer_pid = WriterPid,
            fragment_target_size = TargetSize
        },
        log = Log0,
        assembly = Assembly0,
        core = Core0
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
                    Meta = rabbitmq_stream_s3_fragment_assembly:metadata(Assembly1),
                    IdxRecords = rabbitmq_stream_s3_fragment_assembly:index_records(Assembly1),
                    {Core, _Ref, Effects} =
                        rabbitmq_stream_s3_replica_reader_core:fragment_cut(
                            Meta#{index_records => IdxRecords}, Core0
                        ),
                    Assembly2 = rabbitmq_stream_s3_fragment_assembly:new(TargetSize),
                    State1 = execute_effects(
                        Effects, State#state{log = Log1, assembly = Assembly2, core = Core}
                    ),
                    drain(State1);
                false ->
                    drain(State#state{log = Log1, assembly = Assembly1})
            end;
        {end_of_stream, Log1} ->
            NextOffset = osiris_log:next_offset(Log1),
            ?LOG_DEBUG("~ts drain end_of_stream next_offset=~b", [StreamId, NextOffset]),
            osiris:register_offset_listener(WriterPid, NextOffset, ?OFFSET_FORMATTER),
            State#state{log = Log1, assembly = Assembly0};
        {error, Reason} ->
            ?LOG_ERROR("Read error for stream ~ts: ~p", [StreamId, Reason]),
            State
    end.

%% ------------------------------------------------------------------
%% Upload (executed inside governor task)
%% ------------------------------------------------------------------

%% 4 MiB
-define(READ_BUFFER_SIZE, 4_194_304).

-spec upload_fragment(
    directory(), stream_id(), rabbitmq_stream_s3_fragment_assembly:fragment_meta()
) ->
    {ok, rabbitmq_stream_s3:uid()} | {error, term()}.
upload_fragment(Dir, StreamId, Meta) ->
    Uid = rabbitmq_stream_s3:uid(),
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
        {ok, Stream2, Crc1} ?= stream_spans(Stream1, Crc0, Dir, Spans),
        IdxRecords = maps:get(index_records, Meta),
        Stream3 = rabbitmq_stream_s3_api:stream_data(Stream2, IdxRecords),
        Crc = erlang:crc32(Crc1, IdxRecords),
        ok ?= rabbitmq_stream_s3_api:stream_finish(Stream3, Crc),
        {ok, Uid}
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
