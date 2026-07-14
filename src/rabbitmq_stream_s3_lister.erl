%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_lister).
-moduledoc """
Drives remote tier deletion for whole streams under backpressure.

When a stream is deleted, its remote objects must be listed and removed. Listing
is cheap (one round-trip per page of up to 1000 keys) while deletion is the
slower, throttling-prone side, so listing can easily outrun deletion. A naive
lister that lists as fast as S3 answers and hands every page to the reaper grows
the reaper's mailbox without bound.

This singleton gen_server serializes stream deletions and paces listing against
deletion: it lists one page, hands it to `rabbitmq_stream_s3_reaper` via a
synchronous call that returns only once the page has been deleted, and only then
lists the next page. Stream-deletion jobs are processed first-in-first-out; a
large stream delays smaller ones queued behind it, which is acceptable for
best-effort background cleanup backstopped by orphan GC.
""".

-behaviour(gen_server).

-include("include/rabbitmq_stream_s3.hrl").
-include("include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/0, delete_stream/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([init_counters/0]).

-define(SERVER, ?MODULE).
%% Drive paging via a self-message so the gen_server returns to its loop between
%% pages and stays responsive to newly enqueued deletions.
-define(PAGE, page).

-define(C_STREAMS_DELETED, 1).
-define(COUNTERS, [
    {streams_deleted, ?C_STREAMS_DELETED, counter, "Streams whose deletion ran to completion"}
]).
-define(COUNTER_KEY, {?MODULE, counter}).

-record(state, {
    %% Streams awaiting deletion, processed in order.
    queue = queue:new() :: queue:queue(stream_id()),
    %% The stream currently being listed and deleted, if any.
    current :: undefined | {stream_id(), rabbitmq_stream_s3:key(), term()}
}).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-doc "Delete all remote tier objects for a stream (async, FIFO).".
-spec delete_stream(stream_id()) -> ok.
delete_stream(StreamId) ->
    gen_server:cast(?SERVER, {delete_stream, StreamId}).

%% ------------------------------------------------------------------
%% gen_server
%% ------------------------------------------------------------------

init([]) ->
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({delete_stream, StreamId}, State) ->
    {noreply, enqueue(StreamId, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(?PAGE, #state{current = {StreamId, Prefix, Continuation}} = State) ->
    {noreply, page(StreamId, Prefix, Continuation, State)};
handle_info(?PAGE, #state{current = undefined} = State) ->
    %% A stray page tick with no active job; nothing to do.
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

%% Enqueue a stream for deletion, skipping it when the same stream is already
%% in flight or already queued. The Khepri deletion trigger can fire more than
%% once for a stream (redelivery, leader change), and a duplicate would list an
%% already-empty prefix and double-count `streams_deleted`.
enqueue(StreamId, #state{current = {StreamId, _, _}} = State) ->
    State;
enqueue(StreamId, #state{queue = Q} = State) ->
    case queue:member(StreamId, Q) of
        true ->
            State;
        false ->
            maybe_start(State#state{queue = queue:in(StreamId, Q)})
    end.

%% Start the next queued stream if idle. When a job is already in flight the new
%% stream stays queued and is picked up when the current one finishes.
maybe_start(#state{current = undefined, queue = Q0} = State) ->
    case queue:out(Q0) of
        {{value, StreamId}, Q1} ->
            Prefix = rabbitmq_stream_s3:stream_prefix(StreamId),
            self() ! ?PAGE,
            State#state{current = {StreamId, Prefix, start}, queue = Q1};
        {empty, _} ->
            State
    end;
maybe_start(#state{} = State) ->
    State.

%% List one page and hand it to the reaper synchronously (the call returns only
%% once the page has been deleted), then schedule the next page. This is the
%% backpressure: the next list happens only after the previous page drained.
page(StreamId, Prefix, Continuation, State) ->
    case rabbitmq_stream_s3_api:list(Prefix, Continuation) of
        {ok, Keys, done} ->
            case delete_page(StreamId, Keys) of
                ok -> finish(StreamId, State);
                {error, _} -> next(State)
            end;
        {ok, Keys, NextContinuation} ->
            case delete_page(StreamId, Keys) of
                ok ->
                    self() ! ?PAGE,
                    State#state{current = {StreamId, Prefix, NextContinuation}};
                {error, _} ->
                    next(State)
            end;
        {error, Reason} ->
            ?LOG_WARNING(
                "Failed to list objects for prefix ~ts: ~p; leaving remaining "
                "objects for orphan GC",
                [Prefix, Reason]
            ),
            %% Best effort: abandon this stream (orphan GC catches the rest) and
            %% move on so one unlistable stream cannot wedge the queue.
            next(State)
    end.

%% Hand a page to the reaper for synchronous deletion. The reaper is an
%% independently supervised worker; if it crashes or is restarting, the
%% synchronous call exits. Treat that like any other transient failure (log and
%% leave the objects for orphan GC) rather than letting it cascade into a lister
%% crash that would discard the entire queue of pending stream deletions.
delete_page(StreamId, Keys) ->
    try rabbitmq_stream_s3_reaper:delete_objects_sync(StreamId, Keys) of
        ok -> ok
    catch
        Class:Reason ->
            ?LOG_WARNING(
                "Reaper unavailable while deleting a page for stream ~ts: "
                "~ts:~p; abandoning it for orphan GC",
                [StreamId, Class, Reason]
            ),
            {error, Reason}
    end.

finish(StreamId, State) ->
    inc(?C_STREAMS_DELETED, 1),
    ?LOG_DEBUG("Completed remote tier deletion for stream ~ts", [StreamId]),
    next(State).

%% Clear the current job and start the next queued one, if any.
next(State) ->
    maybe_start(State#state{current = undefined}).

%% ------------------------------------------------------------------
%% Counters
%% ------------------------------------------------------------------

-spec init_counters() -> ok.
init_counters() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?COUNTER_KEY, Cnt),
    ok.

inc(Idx, N) ->
    case persistent_term:get(?COUNTER_KEY, undefined) of
        undefined -> ok;
        Cnt -> counters:add(Cnt, Idx, N)
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% A stream in flight (current set) so enqueue/2 only appends to the queue and
%% maybe_start/1 leaves it untouched, making the queue contents observable.
busy_state() ->
    #state{current = {<<"in_flight">>, <<"prefix/">>, start}}.

enqueue_dedup_test_() ->
    [
        {"a fresh stream is queued", fun() ->
            S = enqueue(<<"s1">>, busy_state()),
            ?assertEqual([<<"s1">>], queue:to_list(S#state.queue))
        end},
        {"distinct streams are all queued in order", fun() ->
            S = enqueue(<<"s2">>, enqueue(<<"s1">>, busy_state())),
            ?assertEqual([<<"s1">>, <<"s2">>], queue:to_list(S#state.queue))
        end},
        {"a stream already queued is not queued again", fun() ->
            S0 = enqueue(<<"s1">>, busy_state()),
            S1 = enqueue(<<"s1">>, S0),
            ?assertEqual([<<"s1">>], queue:to_list(S1#state.queue))
        end},
        {"the in-flight stream is not re-queued", fun() ->
            S = enqueue(<<"in_flight">>, busy_state()),
            ?assertEqual([], queue:to_list(S#state.queue))
        end}
    ].

-endif.
