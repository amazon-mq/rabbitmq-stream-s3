%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(remote_reader_s3_bench).
-moduledoc """
The remote reader's prefetch behaviour, measured against a real object store.

The real `rabbitmq_stream_s3_remote_reader` gen_server reads through the real
`rabbitmq_stream_s3_api_aws` client and the real `rabbitmq_stream_s3_api_aws_pool`
over real `gun` connections against MinIO. Connection reuse, pool growth, HTTP
framing, backpressure and byte accounting are executed, not modelled. The only
thing modelled is network latency, applied as a `tc netem` qdisc inside MinIO's
network namespace (see the shaping section for why nothing else is).

## Infrastructure

Follows the pattern `jepsen/docker/` established, minus TLS: the pool is started
with an injected `open_fun` - a seam `rabbitmq_stream_s3_api_aws_pool` already
offers and `api_aws_pool_statem_SUITE` already uses - so connections are dialled
by address over plain TCP, and no certificate, `/etc/hosts` entry or privileged
port is needed. The client still signs for and sends
`Host: jepsen.s3.jepsen.local` exactly as it would against AWS.

    make s3-bench-up
    make bench-remote_reader_s3_bench
    ./scripts/s3-bench-sweep.sh depth 8 16 32
    make s3-bench-down

`run/0` skips with a message when the store is not up, so `make bench` stays
green without containers.

## Using it

**One configuration per OS process.** A scenario sweeping in-process inherits
the previous one's warm pool and grown prefetch window, and reads faster for
reasons that have nothing to do with what it is measuring.
`scripts/s3-bench-sweep.sh` starts a fresh VM per point; parameters come from
`S3B_*` environment variables.

**Budget for ten seconds or more.** The prefetch window starts at one request
and grows on misses, and the pool starts at `min_size`, so short runs measure
the ramp: the same configuration gave 140.7 MiB/s over 0.9 s and 219.3 MiB/s
over 9.3 s.

**Read results against the substrate.** `S3B_SUBSTRATE=1` measures what the
store delivers at a given concurrency with the reader taken out. A reader figure
tracking that line is measuring MinIO, not the prefetch policy.
""".

-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbitmq_stream_s3/include/rabbitmq_stream_s3.hrl").

-export([
    run/0,
    available/0,
    setup/0,
    teardown/0,
    seed/1,
    measure/1,
    substrate_ceiling/2
]).

%% The store, as `jepsen/docker/` configures it. Region and endpoint TLD are
%% chosen so `api_aws`'s `s3.<region>.<tld>` construction yields a host MinIO's
%% `MINIO_DOMAIN` will route.
-define(REGION, <<"jepsen">>).
-define(REGION_TLD, <<"local">>).
-define(BUCKET, <<"jepsen">>).
-define(ACCESS_KEY, <<"minioadmin">>).
-define(SECRET_KEY, <<"minioadmin">>).

%% MinIO's S3 listener, reached over the podman bridge rather than a published
%% port.
%%
%% Rootless podman forwards published ports through a userspace proxy that
%% absorbs traffic shaping: a `tc netem` qdisc on the container's interface had
%% no measurable effect on a bulk transfer through 127.0.0.1 (88.0 vs 87.8 MiB/s
%% at 22 ms), so runs that believed they were shaped were not. Reaching the
%% container's own address from inside the rootless network namespace crosses
%% the bridge, where the qdisc does apply - the same request measured 7 ms
%% unshaped and 408 ms under a 200 ms delay.
%%
%% `scripts/s3-bench-sweep.sh` runs the VM under `podman unshare --rootless-netns`
%% and passes the address in.
%%
%% The default is the port `s3-bench-env.sh` publishes on the host, so a plain
%% `gmake bench-remote_reader_s3_bench` reaches the store it just started. What
%% it measures is unshaped, for the reason above: a run outside the network
%% namespace cannot cross the bridge, so there is no qdisc on its path whatever
%% address it dials. Latency shaping is the sweep's, and the sweep passes its
%% own address in.
-define(S3_HOST, os:getenv("S3B_HOST", "127.0.0.1")).
-define(S3_PORT, env_int("S3B_PORT", 19000)).

-define(STREAM, <<"s3-bench-stream">>).

%% One read. The real log reader reads a chunk at a time, and a chunk is one
%% writer batch - far smaller than a megabyte at the message sizes the stress
%% runs used. Read size is not cosmetic: it is what the fetch ceiling is
%% measured against, and it sets how many gen_server round trips a byte costs.
%%
%% Read through `read_iodata/4` rather than `read/4`, because the latter
%% flattens the buffer's blocks into one binary - a copy of every byte
%% delivered, which the real log reader does not make on the data path since the
%% bytes go straight to a socket.
-define(READ_BYTES, env_int("S3B_READ_KB", 1024) * 1024).

%% ------------------------------------------------------------------
%% Entry point
%% ------------------------------------------------------------------

-doc """
Run exactly one configuration and print one result line.

One configuration per OS process, deliberately. A run leaves a warm connection
pool behind, so a scenario that follows another spends no handshakes where the
first one did and reads faster for a reason that has nothing to do with what it
is measuring. A benchmark whose earlier rows change its later ones is worse than
no benchmark.

So sweeping is `scripts/s3-bench-sweep.sh`'s job: it restarts the wire and
starts a fresh VM for every point. Parameters come from the environment so a
shell loop can set them:

    S3B_DEPTH, S3B_WINDOW_MIB, S3B_REQUEST_MIB, S3B_FRAGMENT_MIB,
    S3B_BUDGET_MIB, S3B_RATE_KBPS, S3B_LATENCY_MS, S3B_JITTER_MS,
    S3B_SUBSTRATE (set to measure the substrate ceiling instead of the reader)
""".
-spec run() -> ok.
run() ->
    case available() of
        false ->
            io:format(
                "~n=== remote reader S3 harness ===~n"
                "SKIPPED: no S3 endpoint at ~s:~b.~n"
                "Start one with `make s3-bench-up` (needs podman or docker).~n",
                [?S3_HOST, ?S3_PORT]
            );
        true ->
            run_one()
    end,
    ok.

run_one() ->
    ok = setup(),
    try
        Depth = env_int("S3B_DEPTH", 8),
        WindowMiB = env_int("S3B_WINDOW_MIB", 32),
        RequestMiB = env_int("S3B_REQUEST_MIB", 4),
        FragmentMiB = env_int("S3B_FRAGMENT_MIB", 64),
        BudgetMiB = env_int("S3B_BUDGET_MIB", 128),
        %% Reported only: shaping is applied by the sweep script, outside the
        %% network namespace this VM runs in, and after seeding.
        Latency = env_int("S3B_LATENCY_MS", 0),
        Budget = BudgetMiB * 1_048_576,
        FragmentBytes = FragmentMiB * 1_048_576,

        %% Seed with the wire unshaped: the netem delay models the read path,
        %% and uploading through it only makes setup slow.
        Fragments = seed(#{
            count => Budget div FragmentBytes + 2,
            bytes => FragmentBytes,
            stream => stream_for(FragmentMiB)
        }),
        %% Latency defaults to zero: a wrong time-to-first-byte model is worse
        %% than none, because it distorts exactly the axis this harness exists
        %% to sweep.

        case os:getenv("S3B_SEED_ONLY") of
            false -> ok;
            _ -> throw(seeded)
        end,
        ok = warn_if_window_limited(Latency),
        case os:getenv("S3B_SUBSTRATE") of
            false ->
                R = measure(#{
                    fragments => Fragments,
                    budget => Budget,
                    opts => #{
                        max_depth => Depth,
                        window_max => WindowMiB * 1_048_576,
                        request_size => RequestMiB * 1_048_576,
                        %% At 1 the reader holds exactly one prefetched
                        %% fragment, so sweeping this measures multi-fragment
                        %% look-ahead against single-fragment reach rather than
                        %% against a differently-configured reader.
                        max_lookahead => env_int("S3B_LOOKAHEAD", Depth),
                        %% Off, `S3B_DEPTH` is the concurrency the reader runs
                        %% at, which is how every fixed-concurrency sweep here
                        %% was measured. On, it is only the ceiling: the reader
                        %% searches below it, and the sweeps are the ground
                        %% truth for what it ought to find.
                        auto_tune => env_int("S3B_AUTO_TUNE", 0) =:= 1,
                        inflight_initial => env_int("S3B_INFLIGHT_INITIAL", Depth)
                    }
                }),
                %% One machine-readable line; the sweep script tabulates.
                io:format(
                    "S3BENCH depth=~b window=~bM request=~bM fragment=~bM "
                    "latency=~bms mib_s=~.1f inflight_avg=~.1f inflight_max=~.1f "
                    "target_final=~b target_max=~b "
                    "msgq_avg=~.1f msgq_max=~b reds_per_s=~.2fM elapsed=~.2f "
                    "miss=~b req=~b timeouts=~b queued=~b~n",
                    [
                        Depth,
                        WindowMiB,
                        RequestMiB,
                        FragmentMiB,
                        Latency,
                        maps:get(mib_s, R),
                        maps:get(inflight_mean, R),
                        float(maps:get(inflight_max, R)),
                        maps:get(target_final, R),
                        maps:get(target_max, R),
                        maps:get(msgq_mean, R),
                        maps:get(msgq_max, R),
                        maps:get(reds_per_s, R) / 1_000_000,
                        maps:get(elapsed_s, R),
                        maps:get(misses, maps:get(counters, R)),
                        maps:get(requests, maps:get(counters, R)),
                        maps:get(timeouts, maps:get(counters, R)),
                        maps:get(checkout_queued, maps:get(counters, R))
                    ]
                );
            _ ->
                MiBs = substrate_ceiling(Depth, 3000),
                io:format("S3BENCH substrate conns=~b latency=~bms mib_s=~.1f~n", [
                    Depth, Latency, MiBs
                ])
        end
    catch
        throw:seeded -> ok
    after
        teardown()
    end.

env_int(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Value -> list_to_integer(Value)
    end.

-spec available() -> boolean().
available() ->
    case gen_tcp:connect(?S3_HOST, ?S3_PORT, [{active, false}], 500) of
        {ok, Sock} ->
            gen_tcp:close(Sock),
            true;
        {error, _} ->
            false
    end.

%% ------------------------------------------------------------------
%% Environment
%% ------------------------------------------------------------------

-spec setup() -> ok.
setup() ->
    application:load(rabbitmq_stream_s3),
    Env = [
        {bucket, ?BUCKET},
        {aws_region, ?REGION},
        {aws_region_endpoints, #{?REGION => ?REGION_TLD}},
        {aws_access_key, ?ACCESS_KEY},
        {aws_secret_key, ?SECRET_KEY},
        %% MinIO's fixed credentials are static ones, and static credentials are
        %% ignored unless this says otherwise - the reader falls back to instance
        %% metadata, waits out three lookups against a service that is not there,
        %% and seeding fails on a timeout with nothing pointing at the cause.
        {allow_static_credentials, true},
        {rabbitmq_stream_s3_api, rabbitmq_stream_s3_api_aws}
    ],
    [application:set_env(rabbitmq_stream_s3, K, V) || {K, V} <- Env],
    {ok, _} = application:ensure_all_started(gun),
    {ok, _} = application:ensure_all_started(inets),
    {ok, _} = application:ensure_all_started(seshat),
    %% The counter group the API client and reader register into, which
    %% `rabbitmq_stream_s3_sup:init/1` normally creates.
    _ = seshat:new_group(rabbitmq_stream_s3),
    ok = start_api(),
    ok = start_pool(),
    %% One entry point initialises every counter and histogram the read path
    %% touches; calling the reader's alone leaves the API layer's own counter
    %% unset and the first range GET crashes on it.
    ok = rabbitmq_stream_s3_api:init(),
    ok.

start_api() ->
    case rabbitmq_stream_s3_api_aws:start_link() of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end.

%% The pool under the name `api_aws` looks it up by, with connections dialled
%% straight at the proxied endpoint. `open_fun` is the seam the pool already
%% exposes for exactly this (see `api_aws_pool_statem_SUITE`); everything else
%% about the pool - growth, checkout, idle expiry, the `pool_busy` path - is its
%% own real logic.
start_pool() ->
    %% Both pools: reads take connections from the general pool, and seeding
    %% writes through the same real client take them from the upload pool.
    %% `general_pool_min_size` is one of the defaults under consideration: a
    %% warm pool spends no handshake on a burst of concurrency.
    ok = start_pool(
        rabbitmq_stream_s3_general_pool,
        env_int("S3B_POOL_MIN", rabbitmq_stream_s3_config:general_pool_min_size()),
        rabbitmq_stream_s3_config:general_pool_max_size()
    ),
    ok = start_pool(
        rabbitmq_stream_s3_upload_pool,
        rabbitmq_stream_s3_config:upload_pool_min_size(),
        rabbitmq_stream_s3_config:upload_pool_max_size()
    ).

start_pool(Name, MinSize, MaxSize) ->
    Config = #{
        name => Name,
        min_size => MinSize,
        max_size => MaxSize,
        open_fun => fun open_conn/0
    },
    case rabbitmq_stream_s3_api_aws_pool:start_link(Name, Config) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end.

open_conn() ->
    gun:open(?S3_HOST, ?S3_PORT, #{
        transport => tcp,
        tcp_opts => [{recbuf, env_int("S3B_RECBUF_KB", 512) * 1024}],
        http_opts => #{keepalive => infinity},
        retry => 0
    }).

-spec teardown() -> ok.
teardown() ->
    catch gen_server:stop(rabbitmq_stream_s3_general_pool),
    catch gen_server:stop(rabbitmq_stream_s3_upload_pool),
    catch gen_server:stop(rabbitmq_stream_s3_api_aws),
    ok.

%% ------------------------------------------------------------------
%% Seeding
%% ------------------------------------------------------------------

-doc """
Upload `count` fragment objects of `bytes` each, through the real client, and
return the fragment refs describing them.

The bytes are a repeating pattern so a read can be checked against its position
without holding a copy of the stream.
""".
-spec seed(map()) -> {rabbitmq_stream_s3:stream_id(), [#fragment_ref{}]}.
seed(#{count := Count, bytes := Bytes} = Args) ->
    Stream = maps:get(stream, Args, ?STREAM),
    %% Skip if this stream is already seeded at this size. Streams are named per
    %% fragment size and MinIO outlives a run, so a sweep over any other axis
    %% re-uploads gigabytes for nothing - which made repeat runs impractical,
    %% and repeats are how a benchmark learns its own noise floor.
    Probe = rabbitmq_stream_s3:fragment_key(
        Stream, #fragment_ref{offset = (Count - 1) * 1000, uid = Count, size = Bytes}
    ),
    case rabbitmq_stream_s3_api:get_range(Probe, {?SEGMENT_HEADER_B + Bytes - 1, 1}) of
        {ok, _} ->
            io:format("seed: ~b x ~b MiB already present~n", [Count, Bytes div 1_048_576]),
            {Stream, [
                #fragment_ref{offset = N * 1000, uid = N + 1, size = Bytes}
             || N <- lists:seq(0, Count - 1)
            ]};
        _ ->
            seed_upload(Stream, Count, Bytes)
    end.

seed_upload(Stream, Count, Bytes) ->
    io:format("seeding ~b fragments x ~b MiB ... ", [Count, Bytes div 1_048_576]),
    Refs = [
        begin
            Ref = #fragment_ref{offset = N * 1000, uid = N + 1, size = Bytes},
            Key = rabbitmq_stream_s3:fragment_key(Stream, Ref),
            Body = fragment_body(Bytes),
            ok = rabbitmq_stream_s3_api_aws:put(Key, Body, #{}),
            Ref
        end
     || N <- lists:seq(0, Count - 1)
    ],
    io:format("done~n"),
    {Stream, Refs}.

%% A fragment object is an 8-byte segment header followed by the data region.
%% Only the sizes matter to the read path, which addresses bytes and never
%% parses them, so the body is a cheap repeating pattern.
fragment_body(DataBytes) ->
    Header = <<0:(?SEGMENT_HEADER_B * 8)>>,
    Block = list_to_binary([N rem 251 || N <- lists:seq(0, 65_535)]),
    [Header | pattern_blocks(DataBytes, byte_size(Block), Block)].

pattern_blocks(Remaining, BlockSize, Block) when Remaining >= BlockSize ->
    [Block | pattern_blocks(Remaining - BlockSize, BlockSize, Block)];
pattern_blocks(0, _, _) ->
    [];
pattern_blocks(Remaining, _, Block) ->
    [binary:part(Block, 0, Remaining)].

%% ------------------------------------------------------------------
%% Shaping
%% ------------------------------------------------------------------

%% Latency is modelled with `tc netem`, and nothing else shapes the wire.
%% Nothing sits between the client and MinIO.
%%
%% Delaying packets in a kernel queue is the only faithful way to do this: the
%% transfer pipelines through the queue, so the delay shows up once as time to
%% first byte, which is what network latency costs a range GET. Anything that
%% delays at the application layer instead charges the delay per chunk, and
%% those charges serialise over a read - a cost that grows with transfer size,
%% which real latency does not.
%%
%% The qdisc itself is `scripts/s3-bench-sweep.sh`'s to apply, not this module's,
%% and it has to be: it goes on after seeding, since a 64 MiB upload across a
%% delayed ACK path times out, and the sidecar that applies it cannot be started
%% from a VM already running inside the rootless network namespace. `limit` is
%% sized well above the bandwidth-delay product there, because netem's default
%% of 1000 packets drops under load and the retransmits look like latency that
%% is not there.
%%
%% KNOWN GAP: there is no per-connection bandwidth cap. A connection runs at
%% whatever MinIO and the loopback give - about 88 MiB/s alone, degrading to
%% about 47 with sixteen - against real S3's ~38 MiB/s. Concurrency is therefore
%% roughly 2.3x cheaper here, so depth sweeps saturate earlier than in
%% production. Fixing it means per-flow shaping (`tc htb` with port-hash
%% filters); netem's aggregate `rate` would model an instance-wide cap, which is
%% the wrong shape.

%% A connection sustains `window / rtt`, so a receive buffer the kernel has
%% quietly capped is indistinguishable in the results from a slow reader: every
%% connection tops out at the same figure whatever the concurrency, and a depth
%% sweep reads as a store that does not scale. `setsockopt` does not fail when
%% it cannot honour the size asked for - it silently gives what
%% `net.core.rmem_max` allows - so the only way to know is to read the cap and
%% do the arithmetic.
%%
%% Printed as `WARNING`, which is what `scripts/s3-bench-sweep.sh` filters for
%% alongside the result line.
-spec warn_if_window_limited(non_neg_integer()) -> ok.
warn_if_window_limited(0) ->
    %% Nothing to predict without a modelled delay: the loopback round trip is
    %% microseconds, so any buffer sustains far more than MinIO can serve.
    ok;
warn_if_window_limited(LatencyMs) ->
    Requested = env_int("S3B_RECBUF_KB", 512) * 1024,
    case rmem_max() of
        undefined ->
            ok;
        Cap when Cap >= Requested ->
            ok;
        Cap ->
            io:format(
                "WARNING net.core.rmem_max=~b caps the ~b bytes S3B_RECBUF_KB asks "
                "for, so every connection is window-limited to about ~.1f MiB/s at "
                "~bms - raise it before trusting any number below~n",
                [Cap, Requested, Cap * 1000 / LatencyMs / 1_048_576, LatencyMs]
            )
    end.

rmem_max() ->
    case file:read_file("/proc/sys/net/core/rmem_max") of
        {ok, Bin} ->
            try
                binary_to_integer(string:trim(Bin))
            catch
                _:_ -> undefined
            end;
        {error, _} ->
            undefined
    end.

%% ------------------------------------------------------------------
%% Measurement
%% ------------------------------------------------------------------

-doc """
Read every seeded fragment end to end through the real reader, and report what
it achieved.

Concurrency is sampled from the reader's own state rather than inferred: the
question the prefetch policy answers is how many requests to run at once, and a
number derived from throughput would assume the very relationship under test.
""".
-spec measure(map()) -> map().
measure(#{fragments := {Stream, [First | _] = Fragments}, opts := Opts} = Args) ->
    Iterator = build_iterator(
        [
            {fragment, #{offset => O, size => S, uid => U}}
         || #fragment_ref{offset = O, size = S, uid = U} <- Fragments
        ]
    ),
    {ok, Reader} = rabbitmq_stream_s3_remote_reader:start(#{
        reader => self(),
        stream => Stream,
        location => #remote_location{
            position = ?SEGMENT_HEADER_B,
            fragment_ref = First,
            iterator = Iterator
        },
        opts => Opts
    }),
    Sampler = spawn_link(fun() -> sample_loop(Reader, []) end),
    put(drain_limit, env_int("S3B_DRAIN_MIBS", 0)),
    put(drain_started, erlang:monotonic_time(microsecond)),
    T0 = erlang:monotonic_time(millisecond),
    Bytes = drain(Reader, maps:get(budget, Args), 0),
    T1 = erlang:monotonic_time(millisecond),
    Sampler ! {stop, self()},
    InFlight =
        receive
            {samples, S} -> S
        after 2000 -> []
        end,
    %% `stop/1` is a cast, so wait for the process to actually go: the next
    %% scenario drops every connection, and a reader still draining into one
    %% would log errors that belong to no measurement.
    MRef = erlang:monitor(process, Reader),
    catch rabbitmq_stream_s3_remote_reader:stop(Reader),
    receive
        {'DOWN', MRef, process, Reader, _} -> ok
    after 5000 -> erlang:demonitor(MRef, [flush])
    end,
    Elapsed = max(1, T1 - T0) / 1000,
    Depths = [D || {D, _, _, _} <- InFlight],
    Queues = [Q || {_, Q, _, _} <- InFlight],
    Reds = [R || {_, _, R, _} <- InFlight],
    Targets = [T || {_, _, _, T} <- InFlight],
    #{
        bytes => Bytes,
        elapsed_s => Elapsed,
        mib_s => Bytes / 1_048_576 / Elapsed,
        inflight_mean => mean(Depths),
        inflight_max => lists:max([0 | Depths]),
        %% Where the search ended up, and how high it went on the way. Against
        %% `inflight_mean` these separate a search that settled on the wrong
        %% target from one that settled on the right target too slowly.
        target_final => trunc(lists:last([0 | Targets])),
        target_max => trunc(lists:max([0 | Targets])),
        %% Reader-process saturation. A queue that grows means the single
        %% gen_server is the funnel, whatever the pool and the network manage.
        msgq_mean => mean(Queues),
        msgq_max => lists:max([0 | Queues]),
        counters => counters(),
        reds_per_s =>
            case Reds of
                [] -> 0.0;
                _ -> (lists:max(Reds) - lists:min(Reds)) / Elapsed
            end
    }.

%% Read a fixed byte budget, following fragment transitions the way the log
%% reader does.
%%
%% A budget rather than "read to the end" for two reasons. Every scenario then
%% does exactly the same work, so throughput comparisons are not confounded by
%% how much was read; and the reader never exhausts its iterator, which in a
%% real broker hands the consumer to the local tier through a manifest-cache
%% lookup this harness has no business standing up. Seed one more fragment than
%% the budget consumes.
drain(_Reader, Budget, Acc) when Acc >= Budget ->
    Acc;
drain(Reader, Budget, Acc) ->
    drain_from(Reader, ?SEGMENT_HEADER_B, Budget, Acc).

%% Hold the consumer to a maximum rate.
%%
%% The stress runs this harness is validated against were bound by the client's
%% own connection: a single TCP flow capped near 589 MiB/s, which is the
%% denominator of every "% of cap" figure in them. An in-process consumer has no
%% such limit and would let the reader run away, so the scenarios that were
%% client-bound cannot be reproduced without it. `S3B_DRAIN_MIBS` of 0 leaves
%% the consumer unthrottled, which measures what the tier could deliver if
%% nothing downstream held it back - a different question.
throttle(_Bytes, 0, _T0) ->
    ok;
throttle(Bytes, DrainMiBs, T0) ->
    Elapsed = erlang:monotonic_time(microsecond) - T0,
    Target = Bytes * 1_000_000 div (DrainMiBs * 1_048_576),
    case Target - Elapsed of
        Sleep when Sleep > 1000 -> timer:sleep(Sleep div 1000);
        _ -> ok
    end.

drain_from(_Reader, _Pos, Budget, Acc) when Acc >= Budget ->
    Acc;
drain_from(Reader, Pos, Budget, Acc) ->
    case rabbitmq_stream_s3_remote_reader:read(Reader, Pos, ?READ_BYTES, within_chunk) of
        {ok, Data} ->
            Size = iolist_size(Data),
            throttle(Acc + Size, drain_limit(), drain_started()),
            drain_from(Reader, Pos + Size, Budget, Acc + Size);
        {next_fragment, _} ->
            drain_from(Reader, ?SEGMENT_HEADER_B, Budget, Acc);
        {become_local, _} ->
            Acc;
        end_of_stream ->
            Acc;
        {error, Reason} ->
            error({read_failed, Reason, Acc})
    end.

%% The reader's own `requests_in_flight` gauge, sampled through seshat.
%%
%% Deliberately not `sys:get_state/1`. That is what the prefetch investigation
%% had to resort to, it depends on the exact shape of a private record, and it
%% would break silently the next time a field is added to the reader's state -
%% reporting a concurrency of zero rather than failing. The gauge is a supported
%% surface and it is what production would look at.
%% In-flight requests, plus what the reader process itself is doing.
%%
%% All delivered bytes funnel through one gen_server, so if concurrency stops
%% paying the first question is whether that process is saturated. A growing
%% message queue says it cannot keep up with the frames arriving; reductions per
%% second says how hard it is working to do so.
sample_loop(Reader, Acc) ->
    receive
        {stop, From} -> From ! {samples, lists:reverse(Acc)}
    after 20 ->
        Sample =
            case process_info(Reader, [message_queue_len, reductions]) of
                [{message_queue_len, Q}, {reductions, R}] ->
                    {inflight_gauge(), Q, R, target_gauge()};
                _ ->
                    {inflight_gauge(), 0, 0, target_gauge()}
            end,
        sample_loop(Reader, [Sample | Acc])
    end.

inflight_gauge() ->
    gauge(<<"requests_in_flight">>).

%% What the search is aiming for, against `inflight_gauge/0` for what it
%% achieves. A run's average concurrency cannot tell a search that converged on
%% the wrong target from one that converged on the right target slowly, and the
%% two want different fixes.
target_gauge() ->
    gauge(<<"remote_reader_inflight_target">>).

gauge(Name) ->
    try
        #{Name := #{values := Values}} = seshat:format(rabbitmq_stream_s3),
        lists:sum(maps:values(Values))
    catch
        _:_ -> 0
    end.

%% Counters worth having on every result line. A run that comes out slow needs
%% to say *why* in the same breath, or diagnosing it means reproducing it - and
%% the interesting runs are the ones that do not reproduce.
counters() ->
    M =
        try
            seshat:format(rabbitmq_stream_s3)
        catch
            _:_ -> #{}
        end,
    Get = fun(Key) ->
        case maps:get(Key, M, undefined) of
            #{values := V} -> round(lists:sum(maps:values(V)));
            _ -> 0
        end
    end,
    #{
        misses => Get(<<"buffer_miss">>),
        hits => Get(<<"buffer_hit">>),
        requests => Get(<<"remote_reader_total_requests">>),
        timeouts => Get(<<"request_timeouts">>),
        checkouts => Get(<<"checkouts">>),
        checkout_queued => Get(<<"checkout_queued">>)
    }.

%% The manifest the reader navigates: a flat run of leaf entries, or the tree a
%% rebalance actually produces once the root outgrows `rebalance_threshold`.
%%
%% The descent's cost is modelled as one round trip rather than fetched for
%% real: the group object would have to be uploaded and served, and what matters
%% to the read path is that the reader blocks for a round trip.
build_iterator(Specs) ->
    case os:getenv("S3B_MANIFEST", "flat") of
        "grouped" ->
            Latency = env_int("S3B_LATENCY_MS", 0),
            rabbitmq_stream_s3_test_helpers:mock_iterator(
                group_specs(Specs),
                fun(GetGroup) ->
                    fun(Ref) ->
                        timer:sleep(Latency),
                        GetGroup(Ref)
                    end
                end
            );
        _ ->
            rabbitmq_stream_s3_test_helpers:mock_iterator(Specs)
    end.

%% The tree `rabbitmq_stream_s3_replica_reader_core:needs_rebalance/2` builds: a
%% leading run of `rebalance_threshold` same-kind entries is factored into one
%% group, repeatedly, and the remainder stays in the root. So a group holds 1024
%% fragments, and a sequential reader pays one descent per *group*, not one per
%% fragment transition - `rabbitmq_stream_s3_fragment_iterator:descend/2` pushes
%% the parent onto the iterator's stack and swaps the child's entries in, so the
%% run of leaves behind a group node is then walked in memory.
%%
%% Grouping every fragment individually, as this did until it was checked
%% against the writer, charged a blocking round trip per transition that the
%% real read path never pays, and understated every profile that reads small
%% fragments in proportion to how fast it was otherwise going.
%% A run shorter than the threshold is not a flat manifest - it is the slice of
%% one real group that a budget-limited run reads. Modelling it as its own group
%% keeps the descent the read actually pays at its iterator's first `next/1`,
%% and none after it.
group_specs(Specs) ->
    Threshold = rabbitmq_stream_s3_config:rebalance_threshold(),
    group_runs(Specs, max(1, min(Threshold, length(Specs)))).

group_runs(Specs, Threshold) when length(Specs) >= Threshold ->
    {Group, Rest} = lists:split(Threshold, Specs),
    [{group, Group} | group_runs(Rest, Threshold)];
group_runs(Specs, _Threshold) ->
    Specs.

drain_limit() -> get(drain_limit).
drain_started() -> get(drain_started).

mean([]) -> 0.0;
mean(L) -> lists:sum(L) / length(L).

%% ------------------------------------------------------------------
%% Substrate ceiling
%% ------------------------------------------------------------------

-doc """
What the harness itself can deliver at a given concurrency, with the reader
taken out of the picture.

`N` processes each issue back-to-back range GETs through the real client and
pool for `DurationMs`; the result is aggregate MiB/s. This is the number every
reader measurement has to be read against. MinIO shares a host with the test and
serves over the loopback, so past some concurrency the *substrate* is what
declines - and a reader measurement that mistook that for a prefetch-policy
result would be exactly the kind of self-deception this harness exists to avoid.

A reader figure close to the substrate ceiling at the same concurrency is
measuring the harness. One well below it is measuring the reader.
""".
-spec substrate_ceiling(pos_integer(), pos_integer()) -> float().
substrate_ceiling(N, DurationMs) ->
    Key = rabbitmq_stream_s3:fragment_key(
        stream_for(env_int("S3B_FRAGMENT_MIB", 64)), 0, 1
    ),
    Parent = self(),
    Deadline = erlang:monotonic_time(millisecond) + DurationMs,
    Pids = [
        spawn_link(fun() -> Parent ! {self(), fetch_until(Key, Deadline, 0)} end)
     || _ <- lists:seq(1, N)
    ],
    Bytes = lists:sum([
        receive
            {Pid, B} -> B
        after DurationMs + 30_000 -> 0
        end
     || Pid <- Pids
    ]),
    Bytes / 1_048_576 / (DurationMs / 1000).

fetch_until(Key, Deadline, Acc) ->
    fetch_until(Key, Deadline, Acc, 0).

fetch_until(Key, Deadline, Acc, Nth) ->
    case erlang:monotonic_time(millisecond) >= Deadline of
        true ->
            Acc;
        false ->
            %% Walk distinct ranges forward, as the reader does. Re-reading one
            %% range has MinIO answer it from cache, which reports a ceiling the
            %% store cannot sustain and that every reader result would then be
            %% judged against. Cycling a small window is no better: it defeats
            %% readahead and re-reads cached bytes at the same time, pulling the
            %% ceiling in both directions at once.
            Size = 4 * 1_048_576,
            FragmentBytes = env_int("S3B_FRAGMENT_MIB", 64) * 1_048_576,
            Offset = ?SEGMENT_HEADER_B + (Nth * Size) rem max(Size, FragmentBytes - Size),
            case rabbitmq_stream_s3_api:get_range(Key, {Offset, Size}) of
                {ok, Data} -> fetch_until(Key, Deadline, Acc + byte_size(Data), Nth + 1);
                {error, _} -> fetch_until(Key, Deadline, Acc, Nth + 1)
            end
    end.

%% The stream a given fragment size is seeded into. One per size, so a run never
%% reads over keys a different size wrote.
stream_for(MiB) ->
    <<"s3-bench-f", (integer_to_binary(MiB))/binary>>.

%% ------------------------------------------------------------------
%% Reporting
%% ------------------------------------------------------------------
