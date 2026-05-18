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
    durable_commit_threshold => non_neg_integer(),
    retention => [osiris:retention_spec()]
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
    core :: rabbitmq_stream_s3_replica_reader_core:state() | undefined,
    %% Nodes registered for manifest broadcast.
    replicas = #{} :: #{node() => reference()},
    %% Monotonic sequence number for broadcast edits. Incremented per edit
    %% batch sent. Replicas use this to detect gaps and request re-sync.
    broadcast_seq = 0 :: non_neg_integer(),
    %% User-configured retention specs for remote tier evaluation.
    retention = [] :: [osiris:retention_spec()],
    %% Commit timer reference.
    commit_timer :: reference() | undefined,
    %% Monitor ref and PID for the in-flight commit task.
    commit_mon :: reference() | undefined,
    commit_pid :: pid() | undefined
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
    Retention = maps:get(retention, Args, []),
    {ok, #state{cfg = Cfg, config = Args, retention = Retention, core = undefined},
        {continue, resolve_manifest}}.

handle_call({await_offset, Offset}, From, #state{core = Core0} = State) ->
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:await_offset(Offset, From, Core0),
    {noreply, execute_effects(Effects, State#state{core = Core})};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast(
    {register_acceptor, Node},
    #state{
        replicas = Replicas,
        core = Core,
        broadcast_seq = Seq,
        cfg = #cfg{stream = StreamId, epoch = Epoch}
    } = State
) ->
    case maps:is_key(Node, Replicas) of
        true ->
            {noreply, State};
        false ->
            MonRef = monitor(process, {rabbitmq_stream_s3_manifest_replica, Node}),
            Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
            rabbitmq_stream_s3_manifest_replica:sync(StreamId, Seq, Epoch, Manifest, Node),
            {noreply, State#state{replicas = Replicas#{Node => MonRef}}}
    end;
handle_cast({retention_updated, Retention}, State) ->
    UserRetention = [S || S <- Retention, element(1, S) =/= 'fun'],
    {noreply, State#state{retention = UserRetention}};
handle_cast(
    {resync, Node},
    #state{
        core = Core,
        broadcast_seq = Seq,
        cfg = #cfg{stream = StreamId, epoch = Epoch}
    } = State
) ->
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
    rabbitmq_stream_s3_manifest_replica:sync(StreamId, Seq, Epoch, Manifest, Node),
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
handle_info({transfer_result, Ref, {ok, Uid}}, #state{core = Core0} = State0) ->
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref, Uid, Core0),
    State1 = State0#state{core = Core},
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
handle_info({commit_result, {ok, Revision}}, #state{core = Core0, commit_mon = Mon} = State0) ->
    demonitor(Mon, [flush]),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:commit_complete(Revision, Core0),
    {noreply,
        execute_effects(Effects, State0#state{
            core = Core, commit_mon = undefined, commit_pid = undefined
        })};
handle_info(
    {commit_result, {error, {conflict, _Entry}}}, #state{core = Core0, commit_mon = Mon} = State0
) ->
    demonitor(Mon, [flush]),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:commit_failed(conflict, Core0),
    {noreply,
        execute_effects(Effects, State0#state{
            core = Core, commit_mon = undefined, commit_pid = undefined
        })};
handle_info({commit_result, {error, Reason}}, #state{core = Core0, commit_mon = Mon} = State0) ->
    demonitor(Mon, [flush]),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:commit_failed(Reason, Core0),
    {noreply,
        execute_effects(Effects, State0#state{
            core = Core, commit_mon = undefined, commit_pid = undefined
        })};
handle_info(
    {'DOWN', Mon, process, _, Reason},
    #state{commit_mon = Mon, core = Core0, cfg = #cfg{stream = StreamId}} = State0
) ->
    ?LOG_WARNING("~ts commit task crashed: ~p", [StreamId, Reason]),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:commit_failed(Reason, Core0),
    {noreply,
        execute_effects(Effects, State0#state{
            core = Core, commit_mon = undefined, commit_pid = undefined
        })};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{cfg = #cfg{stream = StreamId}, commit_mon = Mon, commit_pid = CommitPid}) ->
    %% Kill any in-flight commit task to prevent orphaned Khepri writes.
    %% An orphaned write advances the revision, causing conflicts for the
    %% next incarnation of this replica reader.
    case CommitPid of
        undefined ->
            ok;
        _ ->
            demonitor(Mon, [flush]),
            exit(CommitPid, kill)
    end,
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
                _ -> (rabbitmq_stream_s3_replica_reader_core:manifest(Core))#manifest.next_offset
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

delete_manifest_objects(StreamId, Manifest) ->
    spawn(fun() ->
        GetGroupFun = fun(GroupRef) ->
            Key = rabbitmq_stream_s3:group_key(StreamId, GroupRef),
            case rabbitmq_stream_s3_api:get(Key) of
                {ok, Data} -> {ok, Data};
                {error, _} = Err -> Err
            end
        end,
        Refs = rabbitmq_stream_s3_fragment_iterator:all_refs(Manifest, GetGroupFun),
        Keys = lists:map(
            fun
                (#fragment_ref{offset = Offset, uid = Uid}) ->
                    rabbitmq_stream_s3:fragment_key(StreamId, Offset, Uid);
                (#group_ref{} = GroupRef) ->
                    rabbitmq_stream_s3:group_key(StreamId, GroupRef)
            end,
            Refs
        ),
        rabbitmq_stream_s3_reaper:delete_objects(StreamId, Keys)
    end),
    ok.

%% ------------------------------------------------------------------
%% Commit (executed in spawned task)
%% ------------------------------------------------------------------

-spec do_commit(
    stream_id(), #manifest{}, non_neg_integer(), term(), rabbitmq_stream_s3_db:revision()
) ->
    {ok, rabbitmq_stream_s3_db:revision()} | {error, term()}.
do_commit(StreamId, Manifest, Epoch, Reference, ExpectedRevision) ->
    Uid = rabbitmq_stream_s3:uid(),
    Data = serialize_manifest(Manifest),
    Key = rabbitmq_stream_s3:manifest_key(StreamId, Uid),
    case rabbitmq_stream_s3_api:put(Key, Data) of
        ok ->
            commit_khepri(StreamId, Epoch, Reference, ExpectedRevision, Uid);
        {error, _} = Err ->
            Err
    end.

-spec commit_khepri(
    stream_id(),
    non_neg_integer(),
    term(),
    rabbitmq_stream_s3_db:revision(),
    rabbitmq_stream_s3:uid()
) ->
    {ok, rabbitmq_stream_s3_db:revision()} | {error, term()}.
commit_khepri(_StreamId, _Epoch, undefined, ExpectedRevision, _Uid) ->
    %% No Khepri reference (test mode). Synthesize a revision.
    {ok, ExpectedRevision + 1};
commit_khepri(StreamId, Epoch, Reference, ExpectedRevision, Uid) ->
    case rabbitmq_stream_s3_db:put(StreamId, Reference, Epoch, ExpectedRevision, Uid) of
        {ok, _Old, NewRevision} ->
            {ok, NewRevision};
        {error, _} = Err ->
            Err
    end.

-spec serialize_manifest(#manifest{}) -> binary().
serialize_manifest(#manifest{
    first_offset = FirstOffset,
    next_offset = NextOffset,
    first_timestamp = FirstTs,
    first_last_timestamp = FirstLastTs,
    total_size = TotalSize,
    entries = Entries
}) ->
    <<?MANIFEST_ROOT_MAGIC, ?MANIFEST_ROOT_VERSION:32/unsigned, FirstOffset:64/unsigned,
        NextOffset:64/unsigned, FirstTs:64/signed, FirstLastTs:64/signed, 0:2/unsigned,
        TotalSize:70/unsigned, Entries/binary>>.

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
    {start_commit, Manifest, Epoch, Reference, ExpectedRevision, _Edits},
    #state{cfg = #cfg{stream = StreamId}} = State
) ->
    Self = self(),
    {CommitPid, MonRef} = spawn_monitor(fun() ->
        Result = do_commit(StreamId, Manifest, Epoch, Reference, ExpectedRevision),
        Self ! {commit_result, Result}
    end),
    State#state{commit_mon = MonRef, commit_pid = CommitPid};
execute_effect({update_range, _FirstOffset, _NextOffset}, #state{cfg = Cfg, core = Core} = State) ->
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(Cfg#cfg.stream, Manifest),
    State;
execute_effect(
    {broadcast, StreamId, Edits},
    #state{replicas = Replicas, broadcast_seq = Seq0, cfg = #cfg{epoch = Epoch}} = State
) ->
    Seq = Seq0 + 1,
    maps:foreach(
        fun(Node, _MonRef) ->
            rabbitmq_stream_s3_manifest_replica:apply_edits(StreamId, Edits, Seq, Epoch, Node)
        end,
        Replicas
    ),
    State#state{broadcast_seq = Seq};
execute_effect(
    {evaluate_retention, _StreamId, _Dir},
    #state{core = Core, retention = Retention, cfg = Cfg} = State
) ->
    Manifest = rabbitmq_stream_s3_replica_reader_core:committed_manifest(Core),
    maybe_evaluate_retention(Manifest, State),
    maybe_evaluate_remote_retention(Manifest, Retention, Cfg#cfg.stream, State);
execute_effect({reply_waiters, Replies}, State) ->
    [gen_server:reply(From, Reply) || {From, Reply} <- Replies],
    State;
execute_effect({start_commit_timer, Ms}, #state{commit_timer = OldRef} = State) ->
    _ = cancel_timer(OldRef),
    Ref = erlang:send_after(Ms, self(), commit_timer),
    State#state{commit_timer = Ref};
execute_effect({cancel_commit_timer}, #state{commit_timer = Ref} = State) ->
    _ = cancel_timer(Ref),
    State#state{commit_timer = undefined};
execute_effect({reinitialize}, #state{cfg = #cfg{stream = StreamId}} = State) ->
    ?LOG_INFO("~ts reinitializing after commit conflict", [StreamId]),
    Manifest = resolve_manifest(StreamId),
    {Core, _} = rabbitmq_stream_s3_replica_reader_core:init(Manifest, State#state.config),
    start_reading(State#state{core = Core, log = undefined, assembly = undefined}).

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

-spec maybe_evaluate_remote_retention(
    #manifest{}, [osiris:retention_spec()], stream_id(), #state{}
) ->
    #state{}.
maybe_evaluate_remote_retention(_Manifest, [], _StreamId, State) ->
    State;
maybe_evaluate_remote_retention(Manifest, Retention, StreamId, #state{core = Core0} = State) ->
    Now = erlang:system_time(millisecond),
    case rabbitmq_stream_s3_manifest:evaluate_remote_retention(Manifest, Retention, Now) of
        unchanged ->
            State;
        {Edit, FragmentRefs} ->
            %% Delete the fragment objects in the background.
            Keys = [
                rabbitmq_stream_s3:fragment_key(StreamId, R#fragment_ref.offset, R#fragment_ref.uid)
             || R <- FragmentRefs
            ],
            rabbitmq_stream_s3_reaper:delete_objects(StreamId, Keys),
            %% Apply the edit to the core's manifest and execute effects.
            {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:apply_retention_edit(
                Edit, Core0
            ),
            execute_effects(Effects, State#state{core = Core})
    end.

%% ------------------------------------------------------------------
%% Reading
%% ------------------------------------------------------------------

-spec resolve_manifest(stream_id()) -> #manifest{}.
resolve_manifest(StreamId) ->
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{} = M ->
            %% Cache hit. Ensure revision is current from Khepri.
            case catch rabbitmq_stream_s3_db:get(StreamId) of
                {ok, #{revision := Rev}} -> M#manifest{revision = Rev};
                _ -> M
            end;
        undefined ->
            case catch rabbitmq_stream_s3_db:get(StreamId) of
                {ok, #{uid := Uid, revision := Rev}} ->
                    Key = rabbitmq_stream_s3:manifest_key(StreamId, Uid),
                    case rabbitmq_stream_s3_api:get(Key, #{}) of
                        {ok, Data} ->
                            (parse_manifest_root(Data))#manifest{revision = Rev};
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
    case is_process_alive(WriterPid) of
        false ->
            State;
        true ->
            start_reading0(State)
    end.

start_reading0(
    #state{
        cfg = #cfg{
            writer_pid = WriterPid,
            fragment_target_size = TargetSize,
            stream = StreamId
        },
        core = Core
    } = State
) ->
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
    StartOffset = Manifest#manifest.next_offset,
    _ = osiris_writer:query_replication_state(WriterPid),
    osiris:register_offset_listener(WriterPid, StartOffset, ?OFFSET_FORMATTER),
    ?LOG_DEBUG("~ts start_reading start_offset=~b", [StreamId, StartOffset]),
    try osiris_writer:init_data_reader(WriterPid, {StartOffset, empty}, #{}) of
        {ok, Log} ->
            Assembly = rabbitmq_stream_s3_fragment_assembly:new(TargetSize),
            drain(State#state{log = Log, assembly = Assembly});
        {error, {offset_out_of_range, {LocalFirst, _}}} when LocalFirst > StartOffset ->
            %% Local retention deleted data the remote tier never received.
            %% Discard the remote manifest (the data would have been retained
            %% there too) and restart from the local log's first offset.
            ?LOG_INFO(
                "~ts local log ahead of manifest "
                "(local_first=~b, manifest_next=~b). "
                "Discarding remote manifest and restarting.",
                [StreamId, LocalFirst, StartOffset]
            ),
            delete_manifest_objects(StreamId, Manifest),
            FreshManifest = #manifest{
                next_offset = LocalFirst, revision = Manifest#manifest.revision
            },
            {Core1, _} = rabbitmq_stream_s3_replica_reader_core:init(
                FreshManifest, State#state.config
            ),
            ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, FreshManifest),
            start_reading(State#state{core = Core1});
        {error, Reason} ->
            ?LOG_WARNING(
                "Failed to open data reader for stream ~ts: ~p",
                [StreamId, Reason]
            ),
            erlang:send_after(1000, self(), retry_resolve),
            State
    catch
        missing_file ->
            ?LOG_WARNING(
                "Segment file missing for stream ~ts, retrying",
                [StreamId]
            ),
            %% Retention deleted the segment between listing and opening.
            %% Retry immediately (same pattern as osiris_log:init_offset_reader).
            start_reading(State#state{core = Core, log = undefined, assembly = undefined})
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
