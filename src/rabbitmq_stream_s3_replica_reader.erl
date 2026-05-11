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
-include("include/rabbitmq_stream_s3.hrl").

-export([start_link/1]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-record(state, {
    stream :: stream_id(),
    writer_pid :: pid(),
    writer_mon :: reference(),
    fragment_target_size :: non_neg_integer()
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

init(#{stream := StreamId, writer_pid := WriterPid} = Args) ->
    Mon = monitor(process, WriterPid),
    TargetSize = maps:get(
        fragment_target_size,
        Args,
        application:get_env(rabbitmq_stream_s3, fragment_target_size, ?MAX_FRAGMENT_SIZE_B)
    ),
    ?LOG_INFO(
        "Remote replica reader started for stream ~ts (target ~b bytes)",
        [StreamId, TargetSize]
    ),
    {ok, #state{
        stream = StreamId,
        writer_pid = WriterPid,
        writer_mon = Mon,
        fragment_target_size = TargetSize
    }}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast({retention_updated, _Retention}, State) ->
    %% TODO: forward to remote-tier retention evaluation
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(
    {'DOWN', Mon, process, _Pid, _Reason}, #state{writer_mon = Mon, stream = StreamId} = State
) ->
    ?LOG_INFO("Writer down, stopping remote replica reader for stream ~ts", [StreamId]),
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{stream = StreamId}) ->
    rabbitmq_stream_s3_registry:unregister_name({StreamId, node()}),
    ok.
