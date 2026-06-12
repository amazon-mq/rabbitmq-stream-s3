%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_stream_sup).
-moduledoc """
Per-stream supervisor wrapping a single replica reader.

This is the middle layer of a two-level supervision structure:

```
rabbitmq_stream_s3_replica_reader_sup   (simple_one_for_one factory)
└── rabbitmq_stream_s3_stream_sup        (one per stream, this module)
      └── rabbitmq_stream_s3_replica_reader   (the worker)
```

The middle layer exists to isolate failures between streams. Each stream
gets its own restart-intensity budget, so a replica reader that crash-loops
on bad state exhausts only *this* supervisor's budget and parks *this*
stream. Sibling streams keep uploading. Without this layer, every replica
reader on the node would share a single budget under the factory, and one
poison stream could trip it and take down all readers on the node.

The worker is `transient` and `significant`. The replica reader stops with
reason `normal` when its osiris writer goes down (leadership transfer or
stream deletion). `transient` means a normal stop is not restarted, and
`auto_shutdown => any_significant` means this supervisor then terminates
itself, so a departed stream does not leave an idle supervisor behind. A
genuine crash (abnormal exit) is restarted within the per-stream budget;
only once that budget is exhausted does the stream park, awaiting
reconciliation.
""".

-behaviour(supervisor).

-export([start_link/1, init/1]).

-spec start_link(rabbitmq_stream_s3_replica_reader:config()) ->
    supervisor:startlink_ret().
start_link(Args) ->
    supervisor:start_link(?MODULE, Args).

init(Args) ->
    SupFlags = #{
        strategy => one_for_one,
        %% Per-stream restart budget. A reader that deterministically
        %% crashes on startup gets a few attempts before this supervisor
        %% gives up and the stream parks. The budget is private to this
        %% stream, so exhausting it never affects other streams.
        intensity => 5,
        period => 10,
        %% Terminate this supervisor when the (significant) replica reader
        %% exits normally and is not restarted, i.e. when the writer is
        %% gone. Without this the supervisor would idle forever with no
        %% children, leaking one process per stream teardown.
        auto_shutdown => any_significant
    },
    ReplicaReader = #{
        id => rabbitmq_stream_s3_replica_reader,
        start => {rabbitmq_stream_s3_replica_reader, start_link, [Args]},
        %% transient: do not restart on the normal exit the reader performs
        %% when its writer goes down; do restart genuine crashes.
        restart => transient,
        %% significant: a normal (non-restarted) exit triggers the
        %% auto_shutdown above. Only legal because restart is not permanent.
        significant => true,
        shutdown => 5000,
        type => worker,
        modules => [rabbitmq_stream_s3_replica_reader]
    },
    {ok, {SupFlags, [ReplicaReader]}}.
