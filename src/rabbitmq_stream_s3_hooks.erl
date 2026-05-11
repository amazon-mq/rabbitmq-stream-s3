%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_hooks).
-moduledoc """
Implementation of `osiris_log_hooks` for the tiered storage plugin.

This module is set as the `log_hooks` application env for osiris on
plugin start. It receives callbacks at writer/acceptor init and when
retention is updated.
""".

-behaviour(osiris_log_hooks).

-export([
    on_init/3,
    on_retention_updated/2,
    local_retention_fun/1
]).

-doc """
Called early in `osiris_log:init/2` before the config is consumed.
The plugin will use this to spawn the remote replica reader (for writers)
or register for manifest broadcast (for acceptors), and to append the
local retention function.
""".
-spec on_init(writer | acceptor, pid(), osiris_log:config()) -> osiris_log:config().
on_init(_Type, _Pid, Config) ->
    %% TODO
    Config.

-doc """
Called when retention is updated on a running stream.
The plugin will use this to re-append the local retention function and notify
the remote replica reader of the new user spec.
""".
-spec on_retention_updated([osiris:retention_spec()], map()) -> [osiris:retention_spec()].
on_retention_updated(Retention, _Config) ->
    %% TODO
    Retention.
