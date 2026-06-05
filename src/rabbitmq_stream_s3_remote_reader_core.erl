%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_remote_reader_core).
-moduledoc """
Functional core for the remote read path.

This module contains the pure decision logic for reading stream data from the
remote tier. It manages buffer state, AIMD-based prefetch sizing, fragment
transitions, and retry/timeout decisions. It produces effects that the
imperative shell (the remote reader gen_server) executes.

The core never performs I/O. It receives events describing what happened and
returns a new state plus a list of effects describing what should happen next.

## Events (inputs)

- `{read, Offset, Bytes, Hint}` - caller wants data at this position
- `{data, RequestId, Fragment, Data, done | continue}` - S3 delivered bytes
- `{request_error, RequestId, Fragment, Reason}` - S3 request failed
- `retry` - retry timer fired
- `deadline_expired` - pending read exceeded its deadline
- `{iterator_refreshed, Iterator}` - manifest cache provided new iterator

## Effects (outputs)

- `{reply, Result}` - respond to the pending read
- `{start_request, Key, Range, Fragment}` - initiate an S3 GET
- `{set_timer, Duration}` - schedule a retry timer
- `{refresh_iterator, Offset}` - rebuild iterator past the given offset
- `{observe, Kind, ReadSize}` - report a notable read-path event for metrics
- `stop` - shut down the remote reader

## Design

The core is structured around `try_read/1` which examines the buffer and
fragment state to determine if a pending read can be served. When it cannot,
the core returns effects to fetch more data. When data arrives, the shell
feeds it back and the core re-evaluates.

The AIMD algorithm adjusts `read_size` (prefetch window):
- Buffer hit: after N consecutive hits, additive increase by 1 MiB
- Buffer miss: multiplicative decrease (halve)

Fragment transitions happen when the read position exceeds the current
fragment's data region. The core checks for pre-fetched next-fragment data
and transitions immediately if available, or signals that more data is needed.
""".

-include_lib("stdlib/include/assert.hrl").
-include("include/rabbitmq_stream_s3.hrl").

%% ------------------------------------------------------------------
%% Types
%% ------------------------------------------------------------------

-record(cfg, {
    read_size_min :: pos_integer(),
    read_size_max :: pos_integer(),
    initial_read_size :: pos_integer(),
    hits_to_grow :: pos_integer(),
    grow_step :: pos_integer(),
    min_retry_delay_ms :: pos_integer(),
    max_retry_delay_ms :: pos_integer()
}).

-record(pending, {
    offset :: byte_offset(),
    bytes :: pos_integer(),
    hint :: chunk_boundary | within_chunk
}).

-record(state, {
    stream :: stream_id(),
    cfg :: #cfg{},
    %% AIMD state
    read_size :: pos_integer(),
    hits_since_last_miss = 0 :: non_neg_integer(),
    %% Retry state
    retry_delay :: pos_integer(),

    %% Current fragment
    fragment_ref :: #fragment_ref{},
    key :: rabbitmq_stream_s3:key(),
    buffer = <<>> :: binary(),
    start_pos :: byte_offset(),
    current_pos :: byte_offset(),
    end_pos :: byte_offset(),

    %% Next fragment (pre-fetched)
    next :: {#fragment_ref{}, binary()} | undefined | not_found,

    %% Fragment iterator
    iterator :: rabbitmq_stream_s3_fragment_iterator:iterator(),

    %% Tracking in-flight requests (by fragment offset)
    requests_in_flight = #{} :: #{fragment_offset() => request_id()},

    %% Pending read (at most one)
    pending :: #pending{} | undefined,

    %% Current fragment returned 404
    current_not_found = false :: boolean()
}).

-type state() :: #state{}.
-type request_id() :: reference().
-type fragment_offset() :: osiris:offset().

-type event() ::
    {read, byte_offset(), pos_integer(), chunk_boundary | within_chunk}
    | {data, request_id(), fragment_offset(), binary(), done | continue}
    | {request_error, request_id(), fragment_offset(), term()}
    | retry
    | deadline_expired
    | {iterator_refreshed, rabbitmq_stream_s3_fragment_iterator:iterator() | end_of_manifest}.

-type observe_kind() :: hit | miss | fragment_transition.

-type effect() ::
    {reply, read_result()}
    | {start_request, rabbitmq_stream_s3:key(), {byte_offset(), byte_offset()}, fragment_offset()}
    | {set_timer, pos_integer()}
    | {refresh_iterator, osiris:offset()}
    | {observe, observe_kind(), pos_integer()}
    | stop.

-type read_result() ::
    {ok, binary()}
    | {error, timeout}
    | {next_fragment, osiris:offset()}
    | {become_local, osiris:offset()}
    | end_of_stream.

-export_type([state/0, event/0, effect/0, read_result/0, request_id/0]).

%% ------------------------------------------------------------------
%% API
%% ------------------------------------------------------------------

-export([
    init/5,
    step/2,
    pending/1,
    current_fragment_offset/1
]).

%% @doc Initialize the read core.
%% `Position` is the byte offset within the fragment to start reading from
%% (typically `?SEGMENT_HEADER_B` for the beginning, or further in if the
%% consumer attached mid-fragment).
%% `Opts` is a map of configuration overrides (see `#cfg{}`). Pass `#{}`
%% for defaults.
-spec init(
    stream_id(),
    #fragment_ref{},
    byte_offset(),
    rabbitmq_stream_s3_fragment_iterator:iterator(),
    map()
) ->
    {state(), [effect()]}.
init(StreamId, FragRef, Position, Iterator, Opts) ->
    #fragment_ref{offset = Offset, uid = Uid} = FragRef,
    Key = rabbitmq_stream_s3:fragment_key(StreamId, Offset, Uid),
    Cfg = build_cfg(Opts),
    %% The iterator arrives already advanced past the current entry
    %% (done by find_position in the log reader). It points at the next
    %% fragment, ready for prefetch and forward navigation.
    State = #state{
        stream = StreamId,
        cfg = Cfg,
        read_size = Cfg#cfg.initial_read_size,
        retry_delay = Cfg#cfg.min_retry_delay_ms,
        fragment_ref = FragRef,
        key = Key,
        buffer = <<>>,
        start_pos = Position,
        current_pos = Position,
        end_pos = Position,
        iterator = Iterator,
        next = undefined
    },
    %% Immediately request data for the current fragment.
    {State1, Effects} = start_current_request(State),
    {State1, Effects}.

%% @doc Feed an event into the core, get back new state and effects.
-spec step(state(), event()) -> {state(), [effect()]}.
step(State, {read, Offset, Bytes, Hint}) ->
    State1 = State#state{pending = #pending{offset = Offset, bytes = Bytes, hint = Hint}},
    try_serve(State1);
step(State0, {data, _RequestId, Fragment, Data, DoneOrContinue}) ->
    State1 = State0#state{retry_delay = (State0#state.cfg)#cfg.min_retry_delay_ms},
    State2 = remove_request_if_done(Fragment, DoneOrContinue, State1),
    State3 = add_data(Fragment, Data, State2),
    {State4, Effects1} = maybe_start_requests(State3),
    {State5, Effects2} = try_serve(State4),
    {State5, Effects1 ++ Effects2};
step(State0, {request_error, _RequestId, Fragment, not_found}) ->
    case Fragment =:= current_fragment_offset(State0) of
        true ->
            %% Current fragment 404. Refresh the iterator past this offset.
            State = State0#state{current_not_found = true, requests_in_flight = #{}},
            case State#state.pending of
                undefined -> {State, []};
                _ -> {State, [{refresh_iterator, Fragment}]}
            end;
        false ->
            %% Next fragment 404. Mark it and try to serve (may trigger refresh
            %% when the consumer reads past the current fragment).
            State = State0#state{
                next = not_found,
                requests_in_flight = maps:remove(Fragment, State0#state.requests_in_flight)
            },
            try_serve(State)
    end;
step(
    #state{cfg = #cfg{max_retry_delay_ms = MaxDelay}} = State0,
    {request_error, _RequestId, _Fragment, Reason}
) when
    Reason =:= slow_down; Reason =:= internal_error
->
    RetryDelay = State0#state.retry_delay,
    NextDelay = min(RetryDelay * 2, MaxDelay),
    State = State0#state{retry_delay = NextDelay, requests_in_flight = #{}},
    {State, [{set_timer, RetryDelay}]};
step(
    #state{cfg = #cfg{max_retry_delay_ms = MaxDelay}} = State0,
    {request_error, _RequestId, _Fragment, Reason}
) when
    Reason =:= timeout;
    Reason =:= stream_error;
    Reason =:= connection_error;
    Reason =:= pool_busy
->
    %% Transient error. Retry with current delay.
    RetryDelay = State0#state.retry_delay,
    NextDelay = min(RetryDelay * 2, MaxDelay),
    State = State0#state{retry_delay = NextDelay, requests_in_flight = #{}},
    {State, [{set_timer, RetryDelay}]};
step(State0, {request_error, _RequestId, _Fragment, _Fatal}) ->
    {State0, [stop]};
step(State0, retry) ->
    {State1, Effects} = maybe_start_requests(State0),
    {State2, Effects2} = try_serve(State1),
    {State2, Effects ++ Effects2};
step(#state{cfg = #cfg{min_retry_delay_ms = MinDelay}} = State0, deadline_expired) ->
    %% The shell's pending-read deadline fired. Reply with an error and reset
    %% buffer state so the next read at any position passes Offset >= StartPos.
    %% See: https://github.com/amazon-mq/rabbitmq-stream-s3/issues/157
    %% See: https://github.com/amazon-mq/rabbitmq-stream-s3/issues/161
    State = State0#state{
        buffer = <<>>,
        start_pos = 0,
        current_pos = 0,
        end_pos = 0,
        pending = undefined,
        requests_in_flight = #{},
        retry_delay = MinDelay
    },
    {State, [{reply, {error, timeout}}]};
step(State0, {iterator_refreshed, end_of_manifest}) ->
    %% No new entries. Become local.
    State = goto_next_fragment(State0),
    {State, [{reply, {become_local, current_fragment_offset(State0)}}]};
step(State0, {iterator_refreshed, Iterator}) ->
    %% Iterator has been refreshed past the 404'd fragment. Reinitialize
    %% at the next available fragment.
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, FragRef, Iterator1} ->
            #fragment_ref{offset = Offset, uid = Uid} = FragRef,
            StreamId = State0#state.stream,
            Cfg = State0#state.cfg,
            Key = rabbitmq_stream_s3:fragment_key(StreamId, Offset, Uid),
            State = #state{
                stream = StreamId,
                cfg = Cfg,
                read_size = Cfg#cfg.initial_read_size,
                retry_delay = Cfg#cfg.min_retry_delay_ms,
                fragment_ref = FragRef,
                key = Key,
                buffer = <<>>,
                start_pos = ?SEGMENT_HEADER_B,
                current_pos = ?SEGMENT_HEADER_B,
                end_pos = ?SEGMENT_HEADER_B,
                iterator = Iterator1,
                next = undefined
            },
            {State1, Effects} = start_current_request(State),
            {State1, [{reply, {next_fragment, Offset}} | Effects]};
        _ ->
            %% Iterator exhausted after refresh. Become local.
            {State0, [{reply, {become_local, current_fragment_offset(State0)}}]}
    end.

%% @doc Returns the pending read, if any.
-spec pending(state()) -> undefined | {byte_offset(), pos_integer()}.
pending(#state{pending = undefined}) -> undefined;
pending(#state{pending = #pending{offset = O, bytes = B}}) -> {O, B}.

%% @doc Returns the offset of the fragment currently being read.
-spec current_fragment_offset(state()) -> osiris:offset().
current_fragment_offset(#state{fragment_ref = #fragment_ref{offset = Off}}) -> Off.

%% ------------------------------------------------------------------
%% Internal: try to serve the pending read
%% ------------------------------------------------------------------

try_serve(#state{pending = undefined} = State) ->
    {State, []};
try_serve(#state{pending = #pending{offset = Offset, bytes = Bytes}} = State) ->
    case try_read(State, Offset, Bytes) of
        {ok, Data, State1} ->
            State2 = adjust_read_size(hit, State1#state{pending = undefined}),
            {State3, Effects} = maybe_start_requests(State2),
            {State3, [
                {reply, {ok, Data}},
                {observe, hit, State3#state.read_size}
                | Effects
            ]};
        {next_fragment, NextOffset, State1} ->
            State2 = State1#state{pending = undefined},
            {State3, Effects} = maybe_start_requests(State2),
            {State3, [
                {reply, {next_fragment, NextOffset}},
                {observe, fragment_transition, State3#state.read_size}
                | Effects
            ]};
        {become_local, State1} ->
            State2 = State1#state{pending = undefined},
            {State2, [{reply, {become_local, current_fragment_offset(State1)}}]};
        {await, State1} ->
            State2 = adjust_read_size(miss, State1),
            {State3, Effects} = maybe_start_requests(State2),
            {State3, [{observe, miss, State3#state.read_size} | Effects]};
        {not_found_check_range, State1} ->
            %% Fragment was 404. Refresh iterator past it. If the iterator
            %% is exhausted (last fragment in manifest was deleted), fall
            %% back to refreshing past the current fragment's offset.
            case next_fragment_offset(State1) of
                {ok, NotFoundOffset} ->
                    {State1, [{refresh_iterator, NotFoundOffset}]};
                end_of_manifest ->
                    {State1, [{refresh_iterator, current_fragment_offset(State1)}]}
            end;
        {refresh_iterator, State1} ->
            %% Iterator exhausted. Refresh past current fragment.
            {State1, [{refresh_iterator, current_fragment_offset(State1)}]}
    end.

%% ------------------------------------------------------------------
%% Internal: buffer read logic
%% ------------------------------------------------------------------

try_read(#state{fragment_ref = #fragment_ref{size = FragSize}} = State, Offset, _Bytes) when
    Offset >= ?SEGMENT_HEADER_B + FragSize
->
    %% Past end of current fragment. Transition.
    try_fragment_transition(State);
try_read(#state{end_pos = EndPos} = State, Offset, Bytes) when
    Offset + Bytes > EndPos
->
    %% Not enough data buffered.
    IdxStartPos = ?SEGMENT_HEADER_B + (State#state.fragment_ref)#fragment_ref.size,
    case EndPos >= IdxStartPos andalso Offset + 64 =< EndPos of
        true ->
            %% Header over-read: cap at index boundary.
            try_read(State, Offset, EndPos - Offset);
        false ->
            case State of
                #state{current_not_found = true} ->
                    {not_found_check_range, State};
                _ ->
                    {await, State}
            end
    end;
try_read(
    #state{
        fragment_ref = #fragment_ref{size = FragSize},
        start_pos = StartPos,
        current_pos = _CurrentPos,
        buffer = Buffer
    } = State,
    Offset,
    Bytes0
) ->
    IdxStartPos = ?SEGMENT_HEADER_B + FragSize,
    Bytes = min(Bytes0, IdxStartPos - Offset),
    ?assert(Offset >= StartPos),
    Data = binary:part(Buffer, Offset - StartPos, Bytes),
    {ok, Data, State#state{current_pos = Offset}}.

try_fragment_transition(
    #state{next = {#fragment_ref{offset = NextOffset}, _}} = State0
) ->
    State = goto_next_fragment(State0),
    {next_fragment, NextOffset, State};
try_fragment_transition(
    #state{next = not_found} = State
) ->
    %% Next fragment 404. Need manifest range to decide.
    {not_found_check_range, State};
try_fragment_transition(
    #state{next = undefined, iterator = Iterator} = State
) ->
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, _FragRef, _} ->
            {await, State};
        end_of_manifest ->
            {refresh_iterator, State};
        {error, _} ->
            {become_local, State#state{pending = undefined}}
    end.

%% ------------------------------------------------------------------
%% Internal: fragment navigation
%% ------------------------------------------------------------------

%% Returns the offset of the next fragment in the iterator, or
%% `end_of_manifest` if the iterator is exhausted. Called from the
%% `not_found_check_range` branch of `try_serve`. There are two paths
%% into that branch:
%%
%%  1. `try_fragment_transition` matching `next = not_found`: the consumer
%%     read past the current fragment and the prefetched-next 404'd. The
%%     iterator is positioned at the 404'd entry, so this returns the
%%     404'd offset.
%%
%%  2. `try_read` matching `current_not_found = true`: the *current*
%%     fragment 404'd while no read was pending; a later read wants bytes
%%     past the partial buffer. The iterator was already advanced past
%%     the current fragment, so it points at the entry AFTER the 404'd
%%     one. If the current fragment was the last in the manifest, the
%%     iterator is exhausted and this returns `end_of_manifest`.
next_fragment_offset(#state{iterator = Iterator}) ->
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, #fragment_ref{offset = Offset}, _} -> {ok, Offset};
        _ -> end_of_manifest
    end.

goto_next_fragment(#state{stream = StreamId, next = Next0, iterator = Iterator0} = State) ->
    Iterator = advance_iterator(Iterator0),
    case Next0 of
        {#fragment_ref{offset = NextOffset, uid = NextUid} = NextFragRef, Buffer} ->
            Key = rabbitmq_stream_s3:fragment_key(StreamId, NextOffset, NextUid),
            State#state{
                start_pos = ?SEGMENT_HEADER_B,
                current_pos = ?SEGMENT_HEADER_B,
                end_pos = ?SEGMENT_HEADER_B + byte_size(Buffer),
                buffer = Buffer,
                fragment_ref = NextFragRef,
                key = Key,
                iterator = Iterator,
                next = undefined,
                current_not_found = false
            };
        _ ->
            State#state{
                start_pos = ?SEGMENT_HEADER_B,
                current_pos = ?SEGMENT_HEADER_B,
                end_pos = ?SEGMENT_HEADER_B,
                buffer = <<>>,
                iterator = Iterator,
                next = undefined,
                current_not_found = false
            }
    end.

advance_iterator(Iterator) ->
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, _, It} -> It;
        _ -> Iterator
    end.

%% ------------------------------------------------------------------
%% Internal: data buffering
%% ------------------------------------------------------------------

add_data(Fragment, Data, #state{fragment_ref = #fragment_ref{offset = Fragment}} = State) ->
    add_data_current(Data, State);
add_data(Fragment, Data, State) ->
    add_data_next(Fragment, Data, State).

add_data_current(
    Data,
    #state{
        start_pos = StartPos0,
        current_pos = CurrentPos,
        end_pos = EndPos0,
        buffer = Buffer0
    } = State
) ->
    Buffer =
        case CurrentPos =:= StartPos0 of
            true ->
                <<Buffer0/binary, Data/binary>>;
            false ->
                <<
                    (binary:part(Buffer0, CurrentPos - StartPos0, EndPos0 - CurrentPos))/binary,
                    Data/binary
                >>
        end,
    State#state{
        start_pos = CurrentPos,
        end_pos = EndPos0 + byte_size(Data),
        buffer = Buffer
    }.

add_data_next(NextOffset, Data, #state{next = Next0, iterator = Iterator} = State) ->
    Next =
        case Next0 of
            undefined ->
                case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
                    {ok, #fragment_ref{offset = NextOffset} = FragRef, _} ->
                        {FragRef, Data};
                    _ ->
                        {#fragment_ref{offset = NextOffset, uid = 0, size = 0}, Data}
                end;
            {#fragment_ref{offset = NextOffset} = FragRef, Buffer0} ->
                {FragRef, <<Buffer0/binary, Data/binary>>}
        end,
    State#state{next = Next}.

%% ------------------------------------------------------------------
%% Internal: request management
%% ------------------------------------------------------------------

remove_request_if_done(Fragment, done, #state{requests_in_flight = Reqs} = State) ->
    State#state{requests_in_flight = maps:remove(Fragment, Reqs)};
remove_request_if_done(_Fragment, continue, State) ->
    State.

maybe_start_requests(State0) ->
    {State1, Effects1} = maybe_start_current_request(State0),
    {State2, Effects2} = maybe_start_next_request(State1),
    {State2, Effects1 ++ Effects2}.

start_current_request(State) ->
    maybe_start_current_request(State).

maybe_start_current_request(
    #state{
        read_size = ReadSize,
        fragment_ref = #fragment_ref{offset = Fragment, size = FragSize},
        end_pos = EndPos,
        key = Key,
        requests_in_flight = Reqs
    } = State
) ->
    IdxStartPos = ?SEGMENT_HEADER_B + FragSize,
    case EndPos < IdxStartPos andalso not maps:is_key(Fragment, Reqs) of
        true ->
            Range = {EndPos, min(EndPos + ReadSize, IdxStartPos - 1)},
            ReqId = make_ref(),
            State1 = State#state{requests_in_flight = Reqs#{Fragment => ReqId}},
            {State1, [{start_request, Key, Range, Fragment}]};
        false ->
            {State, []}
    end.

maybe_start_next_request(
    #state{
        stream = StreamId,
        read_size = ReadSize,
        fragment_ref = #fragment_ref{size = FragSize},
        end_pos = EndPos,
        next = undefined,
        iterator = Iterator,
        requests_in_flight = Reqs
    } = State
) when EndPos >= ?SEGMENT_HEADER_B + FragSize ->
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, #fragment_ref{offset = NextOffset, uid = NextUid}, _} ->
            case maps:is_key(NextOffset, Reqs) of
                true ->
                    {State, []};
                false ->
                    Key = rabbitmq_stream_s3:fragment_key(StreamId, NextOffset, NextUid),
                    Range = {?SEGMENT_HEADER_B, ?SEGMENT_HEADER_B + ReadSize},
                    ReqId = make_ref(),
                    State1 = State#state{requests_in_flight = Reqs#{NextOffset => ReqId}},
                    {State1, [{start_request, Key, Range, NextOffset}]}
            end;
        _ ->
            {State, []}
    end;
maybe_start_next_request(State) ->
    {State, []}.

%% ------------------------------------------------------------------
%% Internal: configuration
%% ------------------------------------------------------------------

build_cfg(Opts) ->
    #cfg{
        read_size_min = maps:get(read_size_min, Opts, 1_048_576),
        read_size_max = maps:get(read_size_max, Opts, 67_108_864),
        initial_read_size = maps:get(initial_read_size, Opts, 4_194_304),
        hits_to_grow = maps:get(hits_to_grow, Opts, 8),
        grow_step = maps:get(grow_step, Opts, 1_048_576),
        min_retry_delay_ms = maps:get(min_retry_delay_ms, Opts, 1_000),
        max_retry_delay_ms = maps:get(max_retry_delay_ms, Opts, 30_000)
    }.

%% ------------------------------------------------------------------
%% Internal: AIMD
%% ------------------------------------------------------------------

adjust_read_size(miss, #state{read_size = Current, cfg = #cfg{read_size_min = Min}} = State) ->
    State#state{
        read_size = max(Min, Current div 2),
        hits_since_last_miss = 0
    };
adjust_read_size(
    hit, #state{hits_since_last_miss = Hits, cfg = #cfg{hits_to_grow = HitsToGrow}} = State
) when
    Hits + 1 < HitsToGrow
->
    State#state{hits_since_last_miss = Hits + 1};
adjust_read_size(
    hit, #state{read_size = Current, cfg = #cfg{read_size_max = Max, grow_step = Step}} = State
) ->
    State#state{
        read_size = min(Max, Current + Step),
        hits_since_last_miss = 0
    }.
