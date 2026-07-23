%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_log_reader).
-moduledoc """
Functions for reading from either the remote or local tiers.

This is an implementation of the `osiris_log_reader` behaviour which resolves
offset specs to absolute positions and reads forwards through the stream data.
If the data can be found locally, this module delegates to `osiris_log`.
Otherwise this module spawns a gen_server which fetches data from the remote
tier.
""".

-include_lib("kernel/include/logger.hrl").

-include("include/rabbitmq_stream_s3.hrl").
-include("include/logging.hrl").

-define(DOMAIN, #{domain => ?RMQLOG_DOMAIN_STREAM_S3}).

-behaviour(osiris_log_reader).

%% OTP 27 dialyzer infers narrower success types for osiris_log:send_file/3
%% and osiris_log:chunk_iterator/3 than their specs declare, causing false
%% positives for the {offset_not_found, ...} branches and maybe_become_remote/2.
%% Remove these suppressions once OTP 28 is required.
-dialyzer({no_match, [send_file/4, chunk_iterator/4]}).
-dialyzer({no_unused, maybe_become_remote/2}).

-define(READ_TIMEOUT, 10000).
-define(SLOW_READ_THRESHOLD_MS, 10_000).

-define(C_REMOTE_INIT, 1).
-define(C_LOCAL_INIT, 2).
-define(C_REMOTE_CLOSE, 3).
-define(C_LOCAL_CLOSE, 4).
-define(C_RESOLVE_REMOTE_FIRST, 5).
-define(C_RESOLVE_REMOTE_NEXT, 6).
-define(C_RESOLVE_REMOTE_LAST, 7).
-define(C_RESOLVE_REMOTE_OFFSET, 8).
-define(C_RESOLVE_REMOTE_TIMESTAMP, 9).
-define(C_RESOLVE_LOCAL, 10).
-define(C_RESOLVE_DURATION_MS, 11).
-define(C_RESOLVE, 12).
-define(C_REMOTE_READER_RESTART, 13).
-define(C_RESOLVE_FAILED_CLOSED, 14).
-define(COUNTERS, [
    {remote_init, ?C_REMOTE_INIT, counter, "Readers initialized in remote mode"},
    {local_init, ?C_LOCAL_INIT, counter, "Readers initialized in local mode"},
    %% A become_local transition increments both remote_close and local_init.
    %% In the future we could support a become_remote transition (local_close +
    %% remote_init) for when local segments are deleted before the consumer
    %% finishes reading them.
    {remote_close, ?C_REMOTE_CLOSE, counter, "Remote readers closed"},
    {local_close, ?C_LOCAL_CLOSE, counter, "Local readers closed"},
    {resolve_remote_first, ?C_RESOLVE_REMOTE_FIRST, counter,
        "Offset specs resolved to remote tier (first)"},
    {resolve_remote_next, ?C_RESOLVE_REMOTE_NEXT, counter,
        "Offset specs resolved to remote tier (next)"},
    {resolve_remote_last, ?C_RESOLVE_REMOTE_LAST, counter,
        "Offset specs resolved to remote tier (last)"},
    {resolve_remote_offset, ?C_RESOLVE_REMOTE_OFFSET, counter,
        "Offset specs resolved to remote tier (absolute offset)"},
    {resolve_remote_timestamp, ?C_RESOLVE_REMOTE_TIMESTAMP, counter,
        "Offset specs resolved to remote tier (timestamp)"},
    {resolve_local, ?C_RESOLVE_LOCAL, counter, "Offset specs resolved to local tier"},
    {resolve_duration_ms, ?C_RESOLVE_DURATION_MS, counter,
        "Total milliseconds spent resolving offset specs"},
    {resolve, ?C_RESOLVE, counter, "Number of offset spec resolutions"},
    {remote_reader_restart, ?C_REMOTE_READER_RESTART, counter,
        "Remote tier readers restarted after an unexpected exit"},
    {resolve_failed_closed, ?C_RESOLVE_FAILED_CLOSED, counter,
        "Offset spec resolutions failed closed on a pending (unresolved) manifest"}
]).
-define(COUNTER_KEY, {?MODULE, counter}).

%% Bounded restart attempts when the remote reader exits unexpectedly within a
%% single send_file/chunk_iterator pass, to avoid spinning on a poison chunk
%% that crashes every fresh reader.
-define(REMOTE_REINIT_ATTEMPTS, 3).

-export([init_counters/0]).

-record(remote, {
    pid :: pid(),
    stream :: stream_id(),
    transport :: tcp | ssl,
    next_offset :: osiris:offset(),
    shared :: atomics:atomics_ref(),
    fragment :: osiris:offset(),
    position :: byte_offset(),
    filter :: osiris_bloom:mstate() | undefined,
    chunk_selector :: all | user_data
}).

-record(?MODULE, {
    config :: osiris_log_reader:config(),
    mode :: #remote{} | osiris_log:state(),
    verify_crc :: boolean()
}).

-record(remote_iterator, {
    next_offset :: osiris:offset(),
    data :: binary()
}).

%% osiris_log_reader
-export([
    resolve_offset_spec/2,
    init_offset_reader/2,
    next_offset/1,
    committed_chunk_id/1,
    committed_offset/1,
    close/1,
    send_file/3,
    chunk_iterator/3,
    iterator_next/1
]).

%% Debugging, testing.
-export([mode/1, remote_pid/1]).

-ifdef(TEST).
-export([
    find_fragment/3,
    find_index_position/2,
    resolve_remote_location/2,
    resolve_first_lookup/1,
    total_range/1
]).
-endif.

%%%===================================================================
%%% Counters
%%%===================================================================

-spec init_counters() -> ok.
init_counters() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?COUNTER_KEY, Cnt),
    ok.

counter() ->
    persistent_term:get(?COUNTER_KEY).

record_resolve_failed_closed(T0) ->
    Cnt = counter(),
    counters:add(Cnt, ?C_RESOLVE_DURATION_MS, rabbitmq_stream_s3_util:elapsed_ms(T0)),
    counters:add(Cnt, ?C_RESOLVE, 1),
    counters:add(Cnt, ?C_RESOLVE_FAILED_CLOSED, 1).

record_resolve(T0, Tier, OffsetSpec) ->
    Cnt = counter(),
    Duration = rabbitmq_stream_s3_util:elapsed_ms(T0),
    counters:add(Cnt, ?C_RESOLVE_DURATION_MS, Duration),
    counters:add(Cnt, ?C_RESOLVE, 1),
    case Tier of
        local ->
            counters:add(Cnt, ?C_RESOLVE_LOCAL, 1);
        remote ->
            C =
                case OffsetSpec of
                    first -> ?C_RESOLVE_REMOTE_FIRST;
                    next -> ?C_RESOLVE_REMOTE_NEXT;
                    last -> ?C_RESOLVE_REMOTE_LAST;
                    {timestamp, _} -> ?C_RESOLVE_REMOTE_TIMESTAMP;
                    {abs, _} -> ?C_RESOLVE_REMOTE_OFFSET;
                    N when is_integer(N) -> ?C_RESOLVE_REMOTE_OFFSET
                end,
            counters:add(Cnt, C, 1)
    end.

%%%===================================================================
%%% osiris_log_reader callbacks
%%%===================================================================

resolve_offset_spec(OffsetSpec, Config) ->
    T0 = erlang:monotonic_time(),
    case resolve_remote_location(OffsetSpec, Config) of
        {ok, #remote_location{chunk_id = ChunkId}} ->
            {ok, ChunkId};
        {local, LocalSpec} ->
            osiris_log:resolve_offset_spec(LocalSpec, Config);
        {error, {manifest_not_resolved, _}} ->
            %% Same fail-closed accounting as init_offset_reader/2: without
            %% this, a caller that resolves through this callback instead of
            %% init_offset_reader/2 has its fail-closed attaches invisible to
            %% the alertable counter. osiris_log_reader's transient_error/0
            %% contract is the external shape; the specific reason stays
            %% internal to this module.
            record_resolve_failed_closed(T0),
            {error, unavailable};
        {error, _} = Err ->
            Err
    end.

init_offset_reader(OffsetSpec, Config) ->
    T0 = erlang:monotonic_time(),
    case resolve_remote_location(OffsetSpec, Config) of
        {ok, Location} ->
            record_resolve(T0, remote, OffsetSpec),
            init_remote_reader(Location, Config);
        {local, LocalSpec} ->
            record_resolve(T0, local, OffsetSpec),
            init_local_reader(LocalSpec, Config);
        {error, {manifest_not_resolved, StreamId}} ->
            %% Fail closed: the manifest cache row is still pending, so the
            %% remote tier's extent is unknown for a spec that may live in it.
            %% The consumer's retry lands after resolution (normally
            %% milliseconds after member start). INFO, not WARNING: expected
            %% during the attach window; sustained repeats mean manifest
            %% resolution is stuck and the resolve_failed_closed counter (and
            %% the replica reader's manifest_resolution_failures) is the
            %% alertable signal. osiris_log_reader's transient_error/0 contract
            %% ({error, unavailable}) is the external shape a caller sees; the
            %% specific reason logged here stays internal to this module.
            ?LOG_INFO(
                "Failing reader setup closed for stream '~ts': offset spec ~w "
                "may be in the remote tier but the manifest is not yet "
                "resolved on this node",
                [StreamId, OffsetSpec],
                ?DOMAIN
            ),
            record_resolve_failed_closed(T0),
            {error, unavailable};
        {error, _} = Err ->
            Err
    end.

resolve_remote_location(Spec, _Config) when Spec =:= last orelse Spec =:= next ->
    {local, Spec};
resolve_remote_location(first, #{name := StreamId, shared := Shared}) ->
    LocalFirstOffset = osiris_log_shared:first_chunk_id(Shared),
    rabbitmq_stream_s3_manifest_replica:with_manifest(StreamId, #{
        resolved => fun
            (#manifest{first_offset = RemoteFirstOffset}) when
                LocalFirstOffset =:= -1; RemoteFirstOffset < LocalFirstOffset
            ->
                %% The remote tier starts before the local log (or the local
                %% log is empty, first_chunk_id = -1, fully trimmed), so
                %% 'first' is in the remote tier. Without the -1 case an empty
                %% local log would resolve to the local tail and skip the
                %% entire remote tier.
                ?LOG_DEBUG(
                    "Attaching remote reader at first offset ~b for spec 'first'",
                    [RemoteFirstOffset],
                    ?DOMAIN
                ),
                resolve_first(StreamId, RemoteFirstOffset);
            (#manifest{}) ->
                %% The remote tier does not start before the local log: the
                %% local first offset is the stream's beginning.
                {local, first}
        end,
        %% Attached but the manifest is not yet resolved or synced, so where
        %% the stream begins is unknown. Fail closed: a local fallback here
        %% would silently skip the remote range below the local floor. The
        %% consumer retries.
        pending => fun() -> {error, {manifest_not_resolved, StreamId}} end,
        %% No plugin state for this stream on this node (un-tiered): the local
        %% log is the whole stream.
        absent => fun() -> {local, first} end
    });
resolve_remote_location(Offset, #{name := StreamId, shared := Shared}) when
    is_integer(Offset)
->
    ?LOG_DEBUG(
        ?MODULE_STRING ":~ts/2 finding offset ~b for stream '~ts'",
        [?FUNCTION_NAME, Offset, StreamId],
        ?DOMAIN
    ),
    FirstChunkId = osiris_log_shared:first_chunk_id(Shared),
    %% first_chunk_id = -1 means the local log is empty (fully trimmed or not yet
    %% populated): there is no local floor, so no offset is served locally and
    %% every offset must be resolved against the remote tier.
    case FirstChunkId =/= -1 andalso Offset >= FirstChunkId of
        true ->
            ?LOG_DEBUG(
                "Offset ~b is in the local tier of stream '~ts' (start ~b), using a local reader",
                [Offset, StreamId, FirstChunkId],
                ?DOMAIN
            ),
            {local, Offset};
        false ->
            ?LOG_DEBUG(
                "Offset ~b of stream '~ts' is not local (start ~b), trying the remote tier",
                [
                    Offset, StreamId, FirstChunkId
                ],
                ?DOMAIN
            ),
            rabbitmq_stream_s3_manifest_replica:with_manifest(StreamId, #{
                resolved => fun
                    (#manifest{next_offset = RemoteNext}) when
                        FirstChunkId =:= -1, Offset >= RemoteNext
                    ->
                        %% The local log is empty and the offset is at or
                        %% beyond the remote tier's tail, i.e. beyond the
                        %% committed stream. Attach at the live tail and wait
                        %% for new writes, as a local reader at or past the
                        %% tail would.
                        {local, next};
                    (#manifest{first_offset = FirstOffset}) when Offset < FirstOffset ->
                        %% Emulate osiris_log's behavior: attach at the
                        %% beginning of the stream.
                        resolve_first(StreamId, FirstOffset);
                    (#manifest{entries = Entries} = Manifest) when
                        byte_size(Entries) >= ?ENTRY_B
                    ->
                        find_position({offset, Offset}, Manifest, StreamId);
                    (#manifest{}) ->
                        %% The remote manifest has no fragment entries (the
                        %% remote tier is behind the local floor, or retention
                        %% emptied it). The offset is below the local floor and
                        %% no remote fragment can serve it, so emulate
                        %% osiris_log and attach at the local first offset
                        %% rather than crashing in find_fragment on an empty
                        %% entry array.
                        {local, first}
                end,
                %% Attached but not yet resolved or synced: the remote tier's
                %% extent is unknown and the offset is below the local floor,
                %% so it cannot be served without it. Fail closed rather than
                %% silently skip; the consumer retries.
                pending => fun() -> {error, {manifest_not_resolved, StreamId}} end,
                %% No plugin state for this stream on this node (un-tiered):
                %% the local log is the whole stream. The requested offset is
                %% below the local floor, so emulate osiris_log and attach at
                %% the local first offset. Returning {local, next} here would
                %% attach at the tail and silently skip all local data the
                %% consumer asked for.
                absent => fun() -> {local, first} end
            })
    end;
resolve_remote_location({timestamp, Ts} = Spec, #{name := StreamId}) ->
    ?LOG_DEBUG(
        ?MODULE_STRING ":~ts/2 finding timestamp ~b for stream '~ts'",
        [?FUNCTION_NAME, Ts, StreamId],
        ?DOMAIN
    ),
    %% We can't cheaply query the first timestamp from `osiris_log_shared`.
    %% Instead try the remote tier first.
    rabbitmq_stream_s3_manifest_replica:with_manifest(StreamId, #{
        resolved => fun
            (#manifest{first_offset = FirstOffset, first_timestamp = FirstTs}) when
                Ts < FirstTs
            ->
                resolve_first(StreamId, FirstOffset);
            (#manifest{entries = Entries} = Manifest) ->
                case rabbitmq_stream_s3_array:last(?ENTRY_B, Entries) of
                    ?ENTRY(_O, _FTs, LTs, _, _, _) when LTs >= Ts ->
                        find_position({timestamp, Ts}, Manifest, StreamId);
                    _ ->
                        {local, Spec}
                end
        end,
        %% Attached but not yet resolved or synced: the timestamp may live in
        %% the remote tier. Fail closed rather than silently skip.
        pending => fun() -> {error, {manifest_not_resolved, StreamId}} end,
        absent => fun() -> {local, Spec} end
    });
resolve_remote_location({abs, Offset}, #{name := StreamId} = Config) ->
    CheckRange = fun() ->
        case total_range(Config) of
            {First, Last} when First =< Offset andalso Offset =< Last ->
                resolve_remote_location(Offset, Config);
            Range ->
                {error, {offset_out_of_range, Range}}
        end
    end,
    rabbitmq_stream_s3_manifest_replica:with_manifest(StreamId, #{
        resolved => fun(_) -> CheckRange() end,
        %% {abs, Offset} validates the offset against the stream's total range,
        %% which is unknown until the manifest resolves. Fail closed with a
        %% retryable error; an out_of_range here would be a lie the client
        %% treats as permanent.
        pending => fun() -> {error, {manifest_not_resolved, StreamId}} end,
        absent => fun() -> CheckRange() end
    }).

-doc "Finds the range of offsets in both local and remote tiers".
-spec total_range(osiris_log:config()) -> rabbitmq_stream_s3:range().
total_range(#{name := StreamId, shared := Shared}) ->
    case osiris_log_shared:first_chunk_id(Shared) of
        -1 ->
            %% Local log empty (fully trimmed or not yet populated): the range,
            %% if any, is the remote tier's. Returning `empty` unconditionally
            %% made {abs, Offset} reads of valid remote offsets fail as
            %% out_of_range whenever the local log was empty.
            case rabbitmq_stream_s3_manifest_replica:get_range(StreamId) of
                {RemoteFirst, RemoteNext} ->
                    {RemoteFirst, RemoteNext - 1};
                empty ->
                    empty
            end;
        LocalFirst ->
            LocalLast = osiris_log_shared:committed_offset(Shared),
            case rabbitmq_stream_s3_manifest_replica:get_range(StreamId) of
                {RemoteFirst, RemoteNext} ->
                    {min(LocalFirst, RemoteFirst), max(LocalLast, RemoteNext - 1)};
                empty ->
                    {LocalFirst, LocalLast}
            end
    end.

next_offset(#?MODULE{mode = #remote{next_offset = NextOffset}}) ->
    NextOffset;
next_offset(#?MODULE{mode = Local}) ->
    osiris_log:next_offset(Local).

committed_chunk_id(#?MODULE{mode = #remote{shared = Shared}}) ->
    osiris_log_shared:committed_chunk_id(Shared);
committed_chunk_id(#?MODULE{mode = Local}) ->
    osiris_log:committed_chunk_id(Local).

committed_offset(#?MODULE{mode = #remote{shared = Shared}}) ->
    osiris_log_shared:committed_offset(Shared);
committed_offset(#?MODULE{mode = Local}) ->
    osiris_log:committed_offset(Local).

%% The remote reader monitors the log reader process and stops when it exits,
%% so no explicit close is needed.
close(#?MODULE{mode = #remote{}}) ->
    counters:add(counter(), ?C_REMOTE_CLOSE, 1),
    ok;
close(#?MODULE{mode = Local}) ->
    counters:add(counter(), ?C_LOCAL_CLOSE, 1),
    ok = osiris_log:close(Local).

close_deferred(undefined) -> ok;
close_deferred(Local) -> osiris_log:close(Local).

send_file(Socket, State, Callback) ->
    send_file(Socket, State, Callback, undefined).

send_file(Socket, State, Callback, DeferredClose) ->
    send_file(Socket, State, Callback, DeferredClose, ?REMOTE_REINIT_ATTEMPTS).

send_file(Socket, State, Callback, DeferredClose, Attempt) ->
    case send_file0(Socket, State, Callback, DeferredClose) of
        {remote_reader_down, NextOffset} ->
            close_deferred(DeferredClose),
            restart_remote_send_file(Socket, State, Callback, NextOffset, Attempt);
        Other ->
            Other
    end.

restart_remote_send_file(_Socket, _State, _Callback, _NextOffset, Attempt) when
    Attempt =< 0
->
    {error, remote_reader_unavailable};
restart_remote_send_file(Socket, #?MODULE{config = Config}, Callback, NextOffset, Attempt) ->
    case reinit_remote(NextOffset, Config) of
        {ok, NewState} ->
            send_file(Socket, NewState, Callback, undefined, Attempt - 1);
        {error, _} = Err ->
            Err
    end.

send_file0(
    Socket,
    #?MODULE{config = Config, mode = #remote{} = Remote0, verify_crc = VerifyCrc} = State0,
    Callback,
    DeferredClose
) ->
    case read_header(Remote0) of
        {ok,
            #{
                chunk_id := ChId,
                num_records := NumRecords,
                position := Position,
                next_position := NextPosition,
                header_data := HeaderData,
                crc := Crc,
                data_size := DataSize
            } = Header,
            #remote{
                pid = Pid,
                transport = Transport,
                chunk_selector = ChunkSelector
            } = Remote1} ->
            {ToSkip, ToSend} = select_amount_to_send(ChunkSelector, Header),
            DataPos = Position + ?CHUNK_HEADER_B + ToSkip,
            case read(Pid, DataPos, ToSend, within_chunk) of
                {ok, Data} ->
                    ok = maybe_validate_crc(ChId, Crc, Data, DataSize, VerifyCrc),
                    PrefixData = Callback(Header, ToSend + byte_size(HeaderData)),
                    case send(Transport, Socket, [PrefixData, HeaderData, Data]) of
                        ok ->
                            close_deferred(DeferredClose),
                            Remote = Remote1#remote{
                                next_offset = ChId + NumRecords,
                                position = NextPosition
                            },
                            {ok, State0#?MODULE{mode = Remote}};
                        {error, _} = Err ->
                            Err
                    end;
                {next_fragment, Offset} ->
                    ?LOG_DEBUG(
                        "send_file: data read returned next_fragment at ~b",
                        [Offset],
                        ?DOMAIN
                    ),
                    State = State0#?MODULE{
                        mode = Remote1#remote{
                            next_offset = Offset,
                            position = ?SEGMENT_HEADER_B
                        }
                    },
                    send_file(Socket, State, Callback, DeferredClose);
                {become_local, _} ->
                    close_deferred(DeferredClose),
                    counters:add(counter(), ?C_REMOTE_CLOSE, 1),
                    rabbitmq_stream_s3_remote_reader:stop(Remote1#remote.pid),
                    case become_local_init(Remote1#remote.next_offset, Config) of
                        {ok, State} ->
                            send_file(Socket, State, Callback, undefined);
                        {error, _} = Err ->
                            Err
                    end;
                {error, {remote_reader_down, _}} ->
                    {remote_reader_down, Remote1#remote.next_offset};
                end_of_stream ->
                    ?LOG_DEBUG(
                        "send_file: data read returned end_of_stream"
                        " (next_offset=~b position=~b)",
                        [Remote1#remote.next_offset, Position],
                        ?DOMAIN
                    ),
                    close_deferred(DeferredClose),
                    {end_of_stream, State0#?MODULE{mode = Remote1}};
                {error, timeout} = Err ->
                    ?LOG_DEBUG(
                        "send_file: data read returned timeout"
                        " (next_offset=~b position=~b)",
                        [Remote1#remote.next_offset, Position],
                        ?DOMAIN
                    ),
                    Err
            end;
        {become_local, _} ->
            close_deferred(DeferredClose),
            counters:add(counter(), ?C_REMOTE_CLOSE, 1),
            rabbitmq_stream_s3_remote_reader:stop(Remote0#remote.pid),
            case become_local_init(Remote0#remote.next_offset, Config) of
                {ok, State} ->
                    send_file(Socket, State, Callback, undefined);
                {error, _} = Err ->
                    Err
            end;
        {end_of_stream, Remote} ->
            close_deferred(DeferredClose),
            {end_of_stream, State0#?MODULE{mode = Remote}};
        {error, {remote_reader_down, _}} ->
            {remote_reader_down, Remote0#remote.next_offset};
        {error, timeout} = Err ->
            Err
    end;
send_file0(Socket, #?MODULE{mode = Local0} = State0, Callback, DeferredClose) ->
    case osiris_log:send_file(Socket, Local0, Callback) of
        {ok, Local} ->
            close_deferred(DeferredClose),
            {ok, State0#?MODULE{mode = Local}};
        {offset_not_found, Local1} ->
            Offset = osiris_log:next_offset(Local1),
            case maybe_become_remote(Offset, State0#?MODULE{mode = Local1}) of
                {ok, State, LocalToClose} ->
                    send_file(Socket, State, Callback, LocalToClose);
                false ->
                    case osiris_log:open_next_segment(Local1) of
                        {ok, Local} ->
                            send_file(
                                Socket, State0#?MODULE{mode = Local}, Callback, DeferredClose
                            );
                        not_found ->
                            close_deferred(DeferredClose),
                            {end_of_stream, State0#?MODULE{mode = Local1}}
                    end
            end;
        {end_of_stream, Local} ->
            close_deferred(DeferredClose),
            {end_of_stream, State0#?MODULE{mode = Local}};
        {error, _} = Err ->
            Err
    end.

chunk_iterator(State, Credit, PrevIter) ->
    chunk_iterator(State, Credit, PrevIter, undefined).

chunk_iterator(State, Credit, PrevIter, DeferredClose) ->
    chunk_iterator(State, Credit, PrevIter, DeferredClose, ?REMOTE_REINIT_ATTEMPTS).

chunk_iterator(State, Credit, PrevIter, DeferredClose, Attempt) ->
    case chunk_iterator0(State, Credit, PrevIter, DeferredClose) of
        {remote_reader_down, NextOffset} ->
            close_deferred(DeferredClose),
            restart_remote_chunk_iterator(State, Credit, NextOffset, Attempt);
        Other ->
            Other
    end.

restart_remote_chunk_iterator(_State, _Credit, _NextOffset, Attempt) when Attempt =< 0 ->
    {error, remote_reader_unavailable};
restart_remote_chunk_iterator(#?MODULE{config = Config}, Credit, NextOffset, Attempt) ->
    case reinit_remote(NextOffset, Config) of
        {ok, NewState} ->
            chunk_iterator(NewState, Credit, undefined, undefined, Attempt - 1);
        {error, _} = Err ->
            Err
    end.

chunk_iterator0(
    #?MODULE{config = Config, mode = #remote{} = Remote0, verify_crc = VerifyCrc} = State0,
    Credit,
    _PrevIter,
    DeferredClose
) ->
    case read_header(Remote0) of
        {ok,
            #{
                chunk_id := ChId,
                num_records := NumRecords,
                position := Position,
                next_position := NextPosition,
                filter_size := FilterSize,
                data_size := DataSize,
                crc := Crc
            } = Header,
            #remote{pid = Pid} = Remote1} ->
            DataPos = Position + ?CHUNK_HEADER_B + FilterSize,
            case read(Pid, DataPos, DataSize, within_chunk) of
                {ok, Data} ->
                    ok = maybe_validate_crc(ChId, Crc, Data, DataSize, VerifyCrc),
                    close_deferred(DeferredClose),
                    Iter = #remote_iterator{
                        next_offset = ChId,
                        data = Data
                    },
                    Remote = Remote1#remote{
                        next_offset = ChId + NumRecords,
                        position = NextPosition
                    },
                    State = State0#?MODULE{mode = Remote},
                    {ok, Header, Iter, State};
                {next_fragment, Offset} ->
                    State = State0#?MODULE{
                        mode = Remote1#remote{
                            next_offset = Offset,
                            position = ?SEGMENT_HEADER_B
                        }
                    },
                    chunk_iterator(State, Credit, undefined, DeferredClose);
                {become_local, _} ->
                    close_deferred(DeferredClose),
                    counters:add(counter(), ?C_REMOTE_CLOSE, 1),
                    rabbitmq_stream_s3_remote_reader:stop(Remote1#remote.pid),
                    case become_local_init(Remote1#remote.next_offset, Config) of
                        {ok, State} ->
                            chunk_iterator(State, Credit, undefined, undefined);
                        {error, _} = Err ->
                            Err
                    end;
                {error, {remote_reader_down, _}} ->
                    {remote_reader_down, Remote1#remote.next_offset};
                end_of_stream ->
                    close_deferred(DeferredClose),
                    {end_of_stream, State0#?MODULE{mode = Remote1}};
                {error, timeout} = Err ->
                    Err
            end;
        {become_local, _} ->
            close_deferred(DeferredClose),
            counters:add(counter(), ?C_REMOTE_CLOSE, 1),
            rabbitmq_stream_s3_remote_reader:stop(Remote0#remote.pid),
            case become_local_init(Remote0#remote.next_offset, Config) of
                {ok, State} ->
                    chunk_iterator(State, Credit, undefined, undefined);
                {error, _} = Err ->
                    Err
            end;
        {end_of_stream, Remote} ->
            close_deferred(DeferredClose),
            {end_of_stream, State0#?MODULE{mode = Remote}};
        {error, {remote_reader_down, _}} ->
            {remote_reader_down, Remote0#remote.next_offset};
        {error, timeout} = Err ->
            Err
    end;
chunk_iterator0(#?MODULE{mode = Local0} = State0, Credit, PrevIter, DeferredClose) ->
    case osiris_log:chunk_iterator(Local0, Credit, PrevIter) of
        {ok, Header, Iter, Local} ->
            close_deferred(DeferredClose),
            {ok, Header, Iter, State0#?MODULE{mode = Local}};
        {offset_not_found, Local1} ->
            Offset = osiris_log:next_offset(Local1),
            case maybe_become_remote(Offset, State0#?MODULE{mode = Local1}) of
                {ok, State, LocalToClose} ->
                    chunk_iterator(State, Credit, undefined, LocalToClose);
                false ->
                    case osiris_log:open_next_segment(Local1) of
                        {ok, Local} ->
                            chunk_iterator(
                                State0#?MODULE{mode = Local}, Credit, undefined, DeferredClose
                            );
                        not_found ->
                            close_deferred(DeferredClose),
                            {end_of_stream, State0#?MODULE{mode = Local1}}
                    end
            end;
        {end_of_stream, Local} ->
            close_deferred(DeferredClose),
            {end_of_stream, State0#?MODULE{mode = Local}};
        {error, _} = Err ->
            Err
    end.

iterator_next(#remote_iterator{next_offset = NextOffset0, data = Data0} = Iter0) ->
    case Data0 of
        ?REC_MATCH_SIMPLE(Len, Rem0) ->
            <<Record:Len/binary, Rem/binary>> = Rem0,
            Iter = Iter0#remote_iterator{next_offset = NextOffset0 + 1, data = Rem},
            {{NextOffset0, Record}, Iter};
        ?REC_MATCH_SUBBATCH(CompType, NumRecs, UncompressedLen, Len, Rem0) ->
            <<BatchData:Len/binary, Rem/binary>> = Rem0,
            Record = {batch, NumRecs, CompType, UncompressedLen, BatchData},
            Iter = Iter0#remote_iterator{next_offset = NextOffset0 + NumRecs, data = Rem},
            {{NextOffset0, Record}, Iter};
        <<>> ->
            end_of_chunk
    end;
iterator_next(Local) ->
    osiris_log:iterator_next(Local).

%%---------------------------------------------------------------------------
%% Helpers

-spec mode(#?MODULE{}) -> local | remote.
mode(#?MODULE{mode = #remote{}}) -> remote;
mode(#?MODULE{}) -> local.

-spec remote_pid(#?MODULE{}) -> pid() | undefined.
remote_pid(#?MODULE{mode = #remote{pid = Pid}}) -> Pid;
remote_pid(#?MODULE{}) -> undefined.

send(tcp, Socket, Data) ->
    gen_tcp:send(Socket, Data);
send(ssl, Socket, Data) ->
    ssl:send(Socket, Data).

%% This helper is mostly the same as osiris_log:read_header0/1. There are some
%% simplifications:
%% * Always over-read the chunk header so that we also read the chunk filter in
%%   a single read operation.
%% * Check the chunk selector within this helper. osiris_log does this in the
%%   callers, but we can always skip when the chunk selector doesn't match.
%% TODO: revise this comment as more moves into the server.
-spec read_header(#remote{}) ->
    {ok, osiris_log:header_map(), #remote{}}
    | {become_local, osiris:offset()}
    | {end_of_stream, #remote{}}
    | {error, timeout}
    | {error, {remote_reader_down, term()}}.
read_header(#remote{shared = Shared, next_offset = NextOffset} = Remote) ->
    LastChunkId = osiris_log_shared:last_chunk_id(Shared),
    CommittedChunkId = osiris_log_shared:committed_chunk_id(Shared),
    CanReadNext =
        LastChunkId >= NextOffset andalso
            CommittedChunkId >= NextOffset,
    case CanReadNext of
        true ->
            read_header1(Remote);
        false ->
            ?LOG_DEBUG(
                "Remote read_header returning end_of_stream:"
                " next_offset=~b last_chunk_id=~b committed_chunk_id=~b",
                [NextOffset, LastChunkId, CommittedChunkId],
                ?DOMAIN
            ),
            {end_of_stream, Remote}
    end.

read_header1(
    #remote{
        pid = Pid,
        position = Position
    } = Remote0
) ->
    %% Over-read the chunk header so that the filter (if it exists) is always
    %% included in the binary. Reading from the remote tier takes time, so
    %% over-reading is faster.
    case read(Pid, Position, ?CHUNK_HEADER_B + ?MAX_FILTER_SIZE, chunk_boundary) of
        {ok, Header} ->
            read_header2(Remote0, Header);
        {next_fragment, Offset} ->
            Remote = Remote0#remote{next_offset = Offset, position = ?SEGMENT_HEADER_B},
            read_header(Remote);
        {become_local, Offset} ->
            {become_local, Offset};
        end_of_stream ->
            {end_of_stream, Remote0};
        {error, {remote_reader_down, _}} = Err ->
            Err;
        {error, timeout} = Err ->
            Err
    end.

read_header2(
    #remote{
        next_offset = NextChId0,
        position = Position0,
        chunk_selector = ChunkSelector,
        filter = Filter0
    } = Remote0,
    HeaderBin
) ->
    <<HeaderBin1:?CHUNK_HEADER_B/binary, _/binary>> = HeaderBin,
    {ok, Header} = osiris_log:parse_header(HeaderBin1, Position0),
    #{
        type := ChunkType,
        chunk_id := NextChId0,
        num_records := NumRecords,
        next_position := NextPosition,
        filter_size := FilterSize
    } = Header,
    case is_chunk_selected(ChunkSelector, ChunkType) of
        true ->
            ChunkFilter = binary:part(HeaderBin, ?CHUNK_HEADER_B, FilterSize),
            case is_bloom_match(ChunkFilter, Filter0) of
                true ->
                    {ok, Header, Remote0};
                false ->
                    %% skip and recurse
                    Remote = Remote0#remote{
                        next_offset = NextChId0 + NumRecords,
                        position = NextPosition
                    },
                    read_header(Remote)
            end;
        false ->
            %% skip and recurse
            Remote = Remote0#remote{
                next_offset = NextChId0 + NumRecords,
                position = NextPosition
            },
            read_header(Remote)
    end.

is_bloom_match(ChunkFilter, Filter0) ->
    case osiris_bloom:is_match(ChunkFilter, Filter0) of
        true ->
            true;
        false ->
            false;
        {retry_with, Filter} ->
            is_bloom_match(ChunkFilter, Filter)
    end.

is_chunk_selected(all, _ChunkType) ->
    true;
is_chunk_selected(user_data, ?CHNK_USER) ->
    true;
is_chunk_selected(_ChunkSelector, _ChunkType) ->
    false.

select_amount_to_send(user_data, #{
    type := ?CHNK_USER,
    filter_size := FilterSize,
    data_size := DataSize
}) ->
    {FilterSize, DataSize};
select_amount_to_send(_ChunkSelector, #{
    filter_size := FilterSize,
    data_size := DataSize,
    trailer_size := TrailerSize
}) ->
    {FilterSize, DataSize + TrailerSize}.

verify_crc(Config) ->
    maps:get(verify_crc_on_read, Config, rabbitmq_stream_s3_config:verify_crc_on_read()).

%% Validates the CRC32 of chunk record data read from the remote tier.
%% In send_file, Data may include the trailer (DataSize + TrailerSize bytes);
%% the CRC only covers the first DataSize bytes.
maybe_validate_crc(_ChunkId, _Crc, _Data, _DataSize, false) ->
    ok;
maybe_validate_crc(ChunkId, Crc, Data, DataSize, true) ->
    RecordData = binary:part(Data, 0, DataSize),
    case erlang:crc32(RecordData) of
        Crc ->
            ok;
        Actual ->
            ?LOG_ERROR(
                "CRC validation failure reading chunk ~b from remote tier"
                " (expected=~b, actual=~b, size=~b)",
                [ChunkId, Crc, Actual, DataSize]
            ),
            exit({crc_validation_failure, {chunk_id, ChunkId}})
    end.

init_local_reader(OffsetSpec, Config) ->
    case osiris_log:init_offset_reader(OffsetSpec, Config) of
        {ok, Local} ->
            counters:add(counter(), ?C_LOCAL_INIT, 1),
            {ok, #?MODULE{
                config = Config,
                mode = Local,
                verify_crc = verify_crc(Config)
            }};
        {error, _} = Err ->
            Err
    end.

%% Transition from the remote tier to the local tier after a become_local.
%% Logs the local tier's offset bounds against the requested offset so a
%% failed or premature transition can be diagnosed from the logs.
become_local_init(NextOffset, #{name := StreamId, shared := Shared} = Config) ->
    Result = init_local_reader(NextOffset, Config),
    Tag =
        case Result of
            {ok, _} -> ok;
            {error, Reason} -> {error, Reason}
        end,
    ?LOG_DEBUG(
        "become_local transition for stream '~ts': next_offset=~b"
        " local_first=~b local_committed=~b local_last=~b result=~p",
        [
            StreamId,
            NextOffset,
            osiris_log_shared:first_chunk_id(Shared),
            osiris_log_shared:committed_chunk_id(Shared),
            osiris_log_shared:last_chunk_id(Shared),
            Tag
        ],
        ?DOMAIN
    ),
    Result.

init_remote_reader(
    #remote_location{
        position = Position,
        chunk_id = Offset,
        fragment_ref = FragRef
    } = Location,
    #{name := StreamId, options := Options, shared := Shared} = Config
) ->
    Filter =
        case Options of
            #{filter_spec := FilterSpec} ->
                osiris_bloom:init_matcher(FilterSpec);
            _ ->
                undefined
        end,
    Conf = #{
        reader => self(),
        stream => StreamId,
        location => Location
    },
    %% The remote reader is a data pipe for large refc binaries with a small
    %% heap of its own, the same profile as the pool's gun processes. Full
    %% sweeps are nearly free, and making every GC a full sweep releases
    %% dropped buffer blocks immediately instead of leaving tenured dead
    %% references pinned until a rare major GC (an idle reader could otherwise
    %% hold a full prefetch window indefinitely). The off-heap message queue
    %% keeps those full sweeps from scanning a backed-up mailbox of gun data
    %% frames. See docs/investigations/remote-reader-memory.md.
    SpawnOpts = [{spawn_opt, [{fullsweep_after, 0}, {message_queue_data, off_heap}]}],
    case gen_server:start(rabbitmq_stream_s3_remote_reader, Conf, SpawnOpts) of
        {ok, Pid} ->
            counters:add(counter(), ?C_REMOTE_INIT, 1),
            Reader = #?MODULE{
                config = Config,
                verify_crc = verify_crc(Config),
                mode = #remote{
                    pid = Pid,
                    stream = StreamId,
                    transport = maps:get(transport, Options, tcp),
                    next_offset = Offset,
                    shared = Shared,
                    fragment = FragRef#fragment_ref.offset,
                    position = Position,
                    filter = Filter,
                    chunk_selector = maps:get(chunk_selector, Options, user_data)
                }
            },
            {ok, Reader};
        {error, _} = Err ->
            Err
    end.

maybe_become_remote(Offset, #?MODULE{config = Config, mode = Local}) ->
    case resolve_remote_location(Offset, Config) of
        {ok, Location} ->
            case init_remote_reader(Location, Config) of
                {ok, RemoteState} ->
                    counters:add(counter(), ?C_LOCAL_CLOSE, 1),
                    {ok, RemoteState, Local};
                {error, _} ->
                    false
            end;
        _ ->
            false
    end.

%% The remote reader exited unexpectedly (a crash or an already-dead pid). Re-
%% resolve the current offset and start a fresh reader so the subscription
%% self-heals instead of wedging on the dead pid. The offset may now resolve to
%% the local tier, in which case a local reader is returned.
reinit_remote(NextOffset, Config) ->
    counters:add(counter(), ?C_REMOTE_READER_RESTART, 1),
    ?LOG_WARNING(
        "Remote tier reader exited unexpectedly; restarting at offset ~b",
        [NextOffset],
        ?DOMAIN
    ),
    init_offset_reader(NextOffset, Config).

-spec find_position(
    {offset, osiris:offset()} | {timestamp, osiris:timestamp()},
    #manifest{},
    stream_id()
) -> {ok, #remote_location{}} | {error, any()}.
find_position(Spec, #manifest{} = Manifest, StreamId) ->
    GetGroupFun = rabbitmq_stream_s3_manifest:get_group_fun(StreamId),
    case find_fragment(Manifest#manifest.entries, Spec, GetGroupFun) of
        {ok, #fragment_ref{offset = FragmentOffset, uid = Uid, size = Size} = FragRef} ->
            %% Download the index from the fragment to find the exact chunk position.
            IdxStartPos = ?SEGMENT_HEADER_B + Size,
            case index_data(StreamId, FragmentOffset, Uid, IdxStartPos) of
                {ok, IndexData} when byte_size(IndexData) >= ?INDEX_RECORD_B ->
                    warn_on_partial_index(StreamId, FragmentOffset, IndexData),
                    {ChunkId, _, FragPos} = find_index_position(IndexData, Spec),
                    %% Position within the fragment object (after the 8-byte header).
                    Position = ?SEGMENT_HEADER_B + FragPos,
                    %% Create an iterator positioned at this fragment for forward navigation.
                    Iterator = rabbitmq_stream_s3_fragment_iterator:init(
                        Manifest, FragmentOffset, GetGroupFun
                    ),
                    %% Advance past the current entry so the iterator is ready for `next`.
                    Iterator1 = advance_past_current(Iterator),
                    {ok, #remote_location{
                        chunk_id = ChunkId,
                        position = Position,
                        fragment_ref = FragRef,
                        iterator = Iterator1
                    }};
                {ok, IndexData} ->
                    %% The index region read back with fewer than one full record,
                    %% for example a truncated or partially written fragment
                    %% object. Resolving a position from it would crash the reader
                    %% on an empty array, so fail loudly and let the caller surface
                    %% the error.
                    ?LOG_WARNING(
                        "Empty or truncated index for stream '~ts' fragment ~b (~b bytes);"
                        " cannot resolve a remote position",
                        [StreamId, FragmentOffset, byte_size(IndexData)],
                        ?DOMAIN
                    ),
                    {error, {empty_index, FragmentOffset}};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% A well-formed index is an exact multiple of the record size. A trailing
%% partial record is a symptom of a truncated or partially written index
%% object. Resolution against the complete prefix still yields a valid
%% position and forward iteration recovers the unindexed tail chunk, so this
%% is tolerated rather than rejected, but it must not pass silently.
-spec warn_on_partial_index(stream_id(), osiris:offset(), binary()) -> ok.
warn_on_partial_index(StreamId, FragmentOffset, IndexData) ->
    case partial_index_bytes(IndexData) of
        0 ->
            ok;
        Trailing ->
            ?LOG_WARNING(
                "Index for stream '~ts' fragment ~b is ~b bytes, not a multiple"
                " of the ~b-byte record size; resolving against ~b complete"
                " records, ignoring ~b trailing bytes",
                [
                    StreamId,
                    FragmentOffset,
                    byte_size(IndexData),
                    ?INDEX_RECORD_B,
                    byte_size(IndexData) div ?INDEX_RECORD_B,
                    Trailing
                ],
                ?DOMAIN
            )
    end.

-spec partial_index_bytes(binary()) -> non_neg_integer().
partial_index_bytes(IndexData) ->
    byte_size(IndexData) rem ?INDEX_RECORD_B.

-doc """
Finds the manifest entry which contains the requested offset or timestamp.

This scans the entries array in logarithmic time. If the offset/timestamp
being searched for is within a group, the group will be fetched with `GetGroup`
and then searched recursively.
""".
-spec find_fragment(
    rabbitmq_stream_s3:entries(),
    {offset, osiris:offset()} | {timestamp, osiris:timestamp()},
    fun((#group_ref{}) -> {ok, rabbitmq_stream_s3:entries()} | {error, any()})
) -> {ok, #fragment_ref{}} | {error, {group_fetch_failed, term()}}.
find_fragment(Entries, Spec, GetGroup) ->
    PartitionPredicate =
        case Spec of
            {offset, Offset} ->
                fun(?ENTRY(O, _FTs, _LTs, _K, _Sz, _Uid, _)) -> Offset >= O end;
            {timestamp, Ts} ->
                %% Strict, like the chunk-level predicate in
                %% `find_index_position/2'. A fragment whose last_ts equals Ts
                %% contains the chunk at that timestamp, so it must not be
                %% skipped. Compare Osiris, which treats a segment as too old
                %% only when Ts is strictly greater than its end timestamp.
                fun(?ENTRY(_O, _FTs, LTs, _K, _Sz, _Uid, _)) -> Ts > LTs end
        end,
    Idx0 = rabbitmq_stream_s3_array:partition_point(
        PartitionPredicate,
        ?ENTRY_B,
        Entries
    ),
    NumEntries = byte_size(Entries) div ?ENTRY_B,
    Idx =
        case Spec of
            {offset, _} -> saturating_decr(Idx0);
            {timestamp, _} -> min(Idx0, NumEntries - 1)
        end,
    case rabbitmq_stream_s3_array:at(Idx, ?ENTRY_B, Entries) of
        ?ENTRY(EntryOffset, _FTs, _LTs, ?MANIFEST_KIND_FRAGMENT, Size, Uid, _) ->
            {ok, #fragment_ref{offset = EntryOffset, uid = Uid, size = Size}};
        ?ENTRY(GroupOffset, _FTs, _LTs, Kind, _Sz, Uid, _) ->
            %% Download the group and search recursively within that. The group
            %% object can be missing (a retention deferred-deletion window) or
            %% briefly unfetchable (a transient S3 error surviving retries);
            %% surface that as an error so the caller fails the read setup
            %% cleanly rather than crashing the consumer on a badmatch.
            ?LOG_DEBUG(
                "Entry is not a fragment. Searching within group ~b kind ~b",
                [GroupOffset, Kind],
                ?DOMAIN
            ),
            GroupRef = #group_ref{uid = Uid, kind = Kind, offset = GroupOffset},
            case GetGroup(GroupRef) of
                {ok, GroupEntries} ->
                    find_fragment(GroupEntries, Spec, GetGroup);
                {error, Reason} ->
                    {error, {group_fetch_failed, Reason}}
            end
    end.

find_index_position(IndexData, Spec) ->
    %% Osiris prefers different chunk boundaries for offset and timestamp
    %% lookups. If the requested offset is between two chunk IDs, Osiris
    %% resolves the offset as the earlier/lesser chunk ID. If the requested
    %% timestamp is between two chunk IDs, Osiris resolves the timestamp as
    %% the later/greater chunk ID.
    PartitionPredicate =
        case Spec of
            {offset, Offset} ->
                fun(?INDEX_RECORD(O, _T, _P)) -> Offset >= O end;
            {timestamp, Ts} ->
                fun(?INDEX_RECORD(_O, T, _P)) -> Ts > T end
        end,
    Idx0 =
        rabbitmq_stream_s3_array:partition_point(
            PartitionPredicate,
            ?INDEX_RECORD_B,
            IndexData
        ),
    ?INDEX_RECORD(ChunkId, ChunkTs, Pos) =
        case Spec of
            {offset, _} ->
                Idx = saturating_decr(Idx0),
                rabbitmq_stream_s3_array:at(Idx, ?INDEX_RECORD_B, IndexData);
            {timestamp, _} ->
                case rabbitmq_stream_s3_array:try_at(Idx0, ?INDEX_RECORD_B, IndexData) of
                    undefined ->
                        rabbitmq_stream_s3_array:last(?INDEX_RECORD_B, IndexData);
                    Record ->
                        Record
                end
        end,
    {ChunkId, ChunkTs, Pos}.

saturating_decr(0) -> 0;
saturating_decr(N) -> N - 1.

index_data(StreamId, FragmentOffset, Uid, IdxStartPos) ->
    Key = rabbitmq_stream_s3:fragment_key(StreamId, FragmentOffset, Uid),
    ?LOG_DEBUG("Looking up key ~ts (~ts)", [Key, ?FUNCTION_NAME], ?DOMAIN),
    rabbitmq_stream_s3_api:get_range(
        Key,
        {IdxStartPos, undefined},
        #{timeout => ?READ_TIMEOUT}
    ).

%% Advance the iterator past the current entry (so next/1 returns the
%% entry *after* the one we resolved to).
advance_past_current(Iterator) ->
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, _, Iterator1} -> Iterator1;
        end_of_manifest -> Iterator;
        {error, _} -> Iterator
    end.

resolve_first(StreamId, FirstOffset) ->
    rabbitmq_stream_s3_manifest_replica:with_manifest(StreamId, #{
        resolved => fun
            (#manifest{entries = Entries} = Manifest) when byte_size(Entries) >= ?ENTRY_B ->
                GetGroupFun = rabbitmq_stream_s3_manifest:get_group_fun(StreamId),
                Iterator = rabbitmq_stream_s3_fragment_iterator:init(
                    Manifest, FirstOffset, GetGroupFun
                ),
                Lookup = rabbitmq_stream_s3_fragment_iterator:next(Iterator),
                case resolve_first_lookup(Lookup) of
                    {remote, Location} ->
                        {ok, Location};
                    {local, first} ->
                        {local, first};
                    {retry, Reason} ->
                        %% A transient group fetch failed while descending to
                        %% the first fragment. Failing the read setup (rather
                        %% than falling back to the local tier) makes the
                        %% consumer retry instead of silently skipping the
                        %% remote range below the local floor.
                        {error, Reason}
                end;
            (#manifest{}) ->
                %% No fragment entries: the remote tier is empty here (behind
                %% the local floor, or retention emptied it), so the local
                %% first offset is the oldest data that exists.
                {local, first}
        end,
        %% Unreachable from the resolution clauses (they only call in after
        %% observing a resolved manifest, and a row is never downgraded), but
        %% stated: a pending row must never be collapsed into the local
        %% fallback.
        pending => fun() -> {error, {manifest_not_resolved, StreamId}} end,
        %% The row was released concurrently (the member is going down); the
        %% local fallback is moot, the reader is about to die with it.
        absent => fun() -> {local, first} end
    }).

%% Interpret the first-fragment lookup returned by the fragment iterator. Total
%% over the three outcomes of `fragment_iterator:next/1`, with no catch-all so a
%% transient fetch error cannot be silently absorbed into the "empty -> local"
%% branch: an `ok` serves the read remotely; `end_of_manifest` means the remote
%% tier is genuinely empty here, so the local tier serves it; a transient
%% `group_fetch_failed` must fail the read setup so the consumer retries rather
%% than skipping the remote range below the local floor. The same point fix has
%% been applied one site at a time (`find_fragment`, `remote_reader_core`); this
%% was the site left behind. Exercised directly by `read_resolution_SUITE`.
-spec resolve_first_lookup(
    {ok, #fragment_ref{}, rabbitmq_stream_s3_fragment_iterator:iterator()}
    | end_of_manifest
    | {error, {group_fetch_failed, term()}}
) ->
    {remote, #remote_location{}}
    | {local, first}
    | {retry, {group_fetch_failed, term()}}.
resolve_first_lookup({ok, #fragment_ref{offset = Offset} = FragRef, Iterator}) ->
    {remote, #remote_location{
        chunk_id = Offset,
        position = ?SEGMENT_HEADER_B,
        fragment_ref = FragRef,
        iterator = Iterator
    }};
resolve_first_lookup(end_of_manifest) ->
    {local, first};
resolve_first_lookup({error, {group_fetch_failed, _} = Reason}) ->
    {retry, Reason}.

-define(READ_RETRY_ATTEMPTS, 3).
-define(READ_RETRY_DELAY_MS, 500).

-spec read(pid(), byte_offset(), pos_integer(), rabbitmq_stream_s3_remote_reader:hint()) ->
    {ok, binary()}
    | {next_fragment, osiris:offset()}
    | {become_local, osiris:offset()}
    | end_of_stream
    | {error, timeout}
    | {error, {remote_reader_down, term()}}.
read(RemoteReader, Offset, Bytes, Hint) ->
    read(RemoteReader, Offset, Bytes, Hint, 1).

read(RemoteReader, Offset, Bytes, Hint, Attempt) ->
    {Ms, Result} = timer:tc(
        rabbitmq_stream_s3_remote_reader,
        read,
        [RemoteReader, Offset, Bytes, Hint],
        millisecond
    ),
    case Ms > ?SLOW_READ_THRESHOLD_MS of
        true ->
            ?LOG_WARNING(
                "Slow remote tier read: ~bms (~b bytes at offset ~b)", [Ms, Bytes, Offset], ?DOMAIN
            ),
            ok;
        false ->
            ok
    end,
    case Result of
        {error, timeout} when Attempt < ?READ_RETRY_ATTEMPTS ->
            ?LOG_DEBUG(
                "Remote tier read timeout after ~bms, retrying"
                " (attempt=~b/~b offset=~b bytes=~b hint=~p reader=~p)",
                [Ms, Attempt, ?READ_RETRY_ATTEMPTS, Offset, Bytes, Hint, RemoteReader],
                ?DOMAIN
            ),
            timer:sleep(?READ_RETRY_DELAY_MS),
            read(RemoteReader, Offset, Bytes, Hint, Attempt + 1);
        {error, timeout} ->
            ?LOG_DEBUG(
                "Remote tier read timeout after ~bms, giving up"
                " (attempt=~b/~b offset=~b bytes=~b hint=~p reader=~p)",
                [Ms, Attempt, ?READ_RETRY_ATTEMPTS, Offset, Bytes, Hint, RemoteReader],
                ?DOMAIN
            ),
            Result;
        _ ->
            Result
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% A stream with no cache row at all is un-tiered on this node (the plugin
%% never attached to it), so the local log is the whole stream: emulate
%% osiris_log and attach at the local first offset, not the tail. Attaching at
%% the tail ({local, next}) silently skips every record the consumer asked for.
resolve_below_floor_unattached_falls_back_to_first_test_() ->
    {setup,
        fun() ->
            {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
            unlink(Pid),
            Pid
        end,
        fun(Pid) -> gen_server:stop(Pid) end, fun(_) ->
            Shared = osiris_log_shared:new(),
            ok = osiris_log_shared:set_first_chunk_id(Shared, 100),
            Config = #{name => <<"untiered-stream">>, shared => Shared},
            [
                %% Below the local floor, no plugin state: fall back to local
                %% first.
                ?_assertEqual({local, first}, resolve_remote_location(5, Config)),
                ?_assertEqual({local, first}, resolve_remote_location(first, Config)),
                %% At/above the local floor: local reader at the offset (no
                %% cache needed).
                ?_assertEqual({local, 150}, resolve_remote_location(150, Config))
            ]
        end}.

%% A pending cache row means the plugin is attached but the manifest is not yet
%% resolved: the remote tier's extent is unknown, so any spec that may live in
%% it must fail closed (a retryable error) rather than fall back to the local
%% tier and silently skip the remote range below the local floor. Specs the
%% local tier answers by itself are unaffected.
resolve_pending_fails_closed_test_() ->
    {setup,
        fun() ->
            {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
            unlink(Pid),
            Pid
        end,
        fun(Pid) -> gen_server:stop(Pid) end, fun(_) ->
            StreamId = <<"pending-stream">>,
            ok = rabbitmq_stream_s3_manifest_replica:mark_pending(StreamId),
            Shared = osiris_log_shared:new(),
            ok = osiris_log_shared:set_first_chunk_id(Shared, 100),
            Config = #{name => StreamId, shared => Shared},
            Err = {error, {manifest_not_resolved, StreamId}},
            [
                ?_assertEqual(Err, resolve_remote_location(first, Config)),
                ?_assertEqual(Err, resolve_remote_location(5, Config)),
                ?_assertEqual(Err, resolve_remote_location({timestamp, 123}, Config)),
                ?_assertEqual(Err, resolve_remote_location({abs, 5}, Config)),
                %% At/above the local floor the local tier alone answers, so
                %% pending does not block the read.
                ?_assertEqual({local, 150}, resolve_remote_location(150, Config)),
                ?_assertEqual({local, last}, resolve_remote_location(last, Config)),
                ?_assertEqual({local, next}, resolve_remote_location(next, Config))
            ]
        end}.

%% A subscription by offset below the local floor, when the cached remote
%% manifest has no fragment entries (the remote tier is behind the local floor,
%% or retention emptied it), must fall back to the local first offset rather
%% than crashing in find_fragment on an empty entry array.
resolve_below_floor_empty_manifest_falls_back_to_first_test_() ->
    {setup,
        fun() ->
            {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
            unlink(Pid),
            Pid
        end,
        fun(Pid) -> gen_server:stop(Pid) end, fun(_) ->
            StreamId = <<"empty-manifest-stream">>,
            Shared = osiris_log_shared:new(),
            ok = osiris_log_shared:set_first_chunk_id(Shared, 100),
            %% An empty manifest: no entries, first_offset = next_offset = 10.
            ok = rabbitmq_stream_s3_manifest_replica:put_manifest(
                StreamId, #manifest{first_offset = 10, next_offset = 10}
            ),
            Config = #{name => StreamId, shared => Shared},
            [
                %% At/above the empty manifest's next_offset but below the local
                %% floor: fall back to local first instead of crashing.
                ?_assertEqual({local, first}, resolve_remote_location(50, Config)),
                %% Below the manifest's first_offset: attach at the beginning.
                ?_assertEqual({local, first}, resolve_remote_location(5, Config))
            ]
        end}.

%% An empty local log (first_chunk_id = -1) must not be treated as a local floor
%% of -1: every offset is then "local" and the populated remote tier is silently
%% skipped. With the local log empty the resolver must route to the remote tier.
resolve_empty_local_log_uses_remote_tier_test_() ->
    {setup,
        fun() ->
            {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
            unlink(Pid),
            Pid
        end,
        fun(Pid) -> gen_server:stop(Pid) end, fun(_) ->
            StreamId = <<"empty-local-stream">>,
            %% Fresh shared atomics: first_chunk_id = -1 (empty local log).
            Shared = osiris_log_shared:new(),
            %% Remote tier covers [10, 30) with one fragment entry.
            Entries = ?ENTRY(10, 1000, 2000, ?MANIFEST_KIND_FRAGMENT, 200, 42),
            Manifest = #manifest{first_offset = 10, next_offset = 30, entries = Entries},
            ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest),
            Config = #{name => StreamId, shared => Shared},
            [
                %% 'first' attaches to the remote tier, not the empty local log.
                ?_assertMatch({ok, #remote_location{}}, resolve_remote_location(first, Config)),
                %% An offset below the remote first attaches at the remote start.
                ?_assertMatch({ok, #remote_location{}}, resolve_remote_location(5, Config)),
                %% An offset at/beyond the remote tail waits at the live tail.
                ?_assertEqual({local, next}, resolve_remote_location(30, Config)),
                ?_assertEqual({local, next}, resolve_remote_location(100, Config)),
                %% total_range reports the remote range (not empty), so {abs}
                %% reads of remote offsets are no longer rejected as out_of_range.
                ?_assertEqual({10, 29}, total_range(Config)),
                ?_assertEqual(
                    {error, {offset_out_of_range, {10, 29}}},
                    resolve_remote_location({abs, 100}, Config)
                )
            ]
        end}.

find_fragment_test() ->
    Ts = erlang:system_time(millisecond),
    Size = 200,
    FragmentEntries = <<
        ?ENTRY(
            (N * 20),
            (Ts - 2000 + N * 20),
            (Ts - 2000 + (N + 1) * 20),
            ?MANIFEST_KIND_FRAGMENT,
            Size,
            N
        )
     || N <- lists:seq(0, 100)
    >>,
    %% Factor out those fragments into a group.
    NextFragmentEntries = <<
        ?ENTRY(
            (N * 20),
            (Ts - 2000 + N * 20),
            (Ts - 2000 + (N + 1) * 20),
            ?MANIFEST_KIND_FRAGMENT,
            Size,
            N
        )
     || N <- lists:seq(101, 150)
    >>,
    GroupUid = rabbitmq_stream_s3:uid(),
    Entries = ?ENTRY(
        0,
        (Ts - 2000),
        Ts,
        ?MANIFEST_KIND_GROUP,
        0,
        GroupUid,
        NextFragmentEntries
    ),
    GetGroup = fun(#group_ref{uid = Uid, kind = Kind, offset = Offset}) ->
        ?assertEqual(GroupUid, Uid),
        ?assertEqual(?MANIFEST_KIND_GROUP, Kind),
        ?assertEqual(0, Offset),
        {ok, FragmentEntries}
    end,
    FindFragment2 = fun(Spec) ->
        {ok, #fragment_ref{offset = FoundOffset}} = find_fragment(Entries, Spec, GetGroup),
        FoundOffset
    end,
    %% The new fragments can be found normally.
    ?assertEqual(2040, FindFragment2({offset, 2050})),
    ?assertEqual(3000, FindFragment2({offset, 10_000})),
    %% The group's fragments can be found recursively.
    ?assertEqual(0, FindFragment2({offset, 0})),
    ?assertEqual(40, FindFragment2({offset, 50})),
    ?assertEqual(40, FindFragment2({timestamp, Ts - 2000 + 50})),
    %% Offset and timestamp assertions on flat fragment lists are omitted here;
    %% they are covered by the property-based tests in unit_SUITE.
    ok.

%% A group object can be missing (a retention deferred-deletion window) or
%% briefly unfetchable (a transient S3 error surviving retries). find_fragment
%% must surface a clean error rather than crash the consumer on a badmatch.
find_fragment_group_fetch_error_is_surfaced_test() ->
    GroupUid = rabbitmq_stream_s3:uid(),
    Entries = ?ENTRY(0, 1000, 2000, ?MANIFEST_KIND_GROUP, 0, GroupUid),
    GetGroup = fun(#group_ref{}) -> {error, not_found} end,
    ?assertEqual(
        {error, {group_fetch_failed, not_found}},
        find_fragment(Entries, {offset, 5}, GetGroup)
    ),
    ?assertEqual(
        {error, {group_fetch_failed, not_found}},
        find_fragment(Entries, {timestamp, 1500}, GetGroup)
    ).

find_fragment_timestamp_boundary_test() ->
    %% A flat fragment list (no group), so this isolates fragment selection.
    %% Each fragment owns 100 offsets and a 1000-unit timestamp span, with a
    %% gap between fragments so the boundary case is unambiguous.
    Size = 200,
    Fragments = [
        %% {Offset, FirstTs, LastTs}
        {0, 1000, 2000},
        {100, 2100, 3000},
        {200, 3100, 4000}
    ],
    Entries = <<
        ?ENTRY(O, FTs, LTs, ?MANIFEST_KIND_FRAGMENT, Size, N)
     || {N, {O, FTs, LTs}} <- lists:enumerate(0, Fragments)
    >>,
    NoGroup = fun(_) -> error(unexpected_group_fetch) end,
    FindFragment2 = fun(Spec) ->
        {ok, #fragment_ref{offset = FoundOffset}} = find_fragment(Entries, Spec, NoGroup),
        FoundOffset
    end,
    %% A timestamp strictly inside a fragment resolves to that fragment.
    ?assertEqual(0, FindFragment2({timestamp, 1500})),
    ?assertEqual(100, FindFragment2({timestamp, 2500})),
    %% A timestamp in the gap above a fragment snaps to the later fragment,
    %% matching the right-boundary preference Osiris uses for timestamps.
    ?assertEqual(100, FindFragment2({timestamp, 2050})),
    %% Timestamps before the first and after the last fragment clamp to the ends.
    ?assertEqual(0, FindFragment2({timestamp, 500})),
    ?assertEqual(200, FindFragment2({timestamp, 5000})),
    %% The boundary case. A timestamp equal to a fragment's last_ts must resolve
    %% to that fragment, since the chunk at that timestamp is the fragment's last
    %% chunk and lives there. Selecting the next fragment skips it. This mirrors
    %% Osiris, whose segment search treats a segment as too old only when the
    %% timestamp is strictly greater than the segment's end timestamp.
    ?assertEqual(0, FindFragment2({timestamp, 2000})),
    ?assertEqual(100, FindFragment2({timestamp, 3000})),
    ok.

find_index_position_test() ->
    Ts = erlang:system_time(millisecond),
    IndexData = <<?INDEX_RECORD((N * 20), (Ts - 60 * 20 + N * 20), N) || N <- lists:seq(0, 60)>>,
    FindPosition = fun(Spec) ->
        {ChunkId, _Ts, _Pos} = find_index_position(IndexData, Spec),
        ChunkId
    end,
    %% See `find_index_position/2`. Osiris prefers different chunk IDs for
    %% offsets compared to timestamps.
    ?assertEqual(0, FindPosition({offset, 0})),
    ?assertEqual(40, FindPosition({offset, 40})),
    ?assertEqual(40, FindPosition({offset, 50})),

    ?assertEqual(0, FindPosition({timestamp, Ts - 60 * 20})),
    ?assertEqual(40, FindPosition({timestamp, Ts - 60 * 20 + 40})),
    ?assertEqual(60 * 20, FindPosition({timestamp, Ts + 1000})),
    %% For timestamps, though, we prefer the later chunk ID. Chunk 60, not 40:
    ?assertEqual(60, FindPosition({timestamp, Ts - 60 * 20 + 50})),
    ok.

find_index_position_empty_index_test() ->
    %% An index that reads back with fewer than one full record (a truncated or
    %% partially written fragment object) has nothing to resolve, and
    %% find_index_position/2 crashes on it in both the offset and timestamp
    %% paths. find_position/3 therefore guards on
    %% byte_size(IndexData) >= ?INDEX_RECORD_B and returns {error, _} rather
    %% than letting this reach the reader.
    ?assertError(_, find_index_position(<<>>, {offset, 0})),
    ?assertError(_, find_index_position(<<>>, {timestamp, 0})),
    ok.

partial_index_bytes_test() ->
    %% A well-formed index is an exact multiple of the record size.
    ?assertEqual(0, partial_index_bytes(<<>>)),
    ?assertEqual(0, partial_index_bytes(<<0:(?INDEX_RECORD_B * 8)>>)),
    ?assertEqual(0, partial_index_bytes(<<0:(?INDEX_RECORD_B * 3 * 8)>>)),
    %% A trailing partial record reports the leftover byte count, and
    %% warn_on_partial_index/3 tolerates it rather than failing.
    ?assertEqual(5, partial_index_bytes(<<0:((?INDEX_RECORD_B + 5) * 8)>>)),
    ?assertEqual(
        ok, warn_on_partial_index(<<"s1">>, 0, <<0:((?INDEX_RECORD_B + 5) * 8)>>)
    ),
    ?assertEqual(
        ok, warn_on_partial_index(<<"s1">>, 0, <<0:(?INDEX_RECORD_B * 2 * 8)>>)
    ),
    ok.

-endif.
