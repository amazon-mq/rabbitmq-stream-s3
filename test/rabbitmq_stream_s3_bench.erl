%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_bench).
-moduledoc """
A minimal benchmark harness for `*_bench` modules (run with `gmake bench`).

Scenarios are `{Name, fun(() -> _)}` pairs. Each scenario runs in a fresh
process: a warmup period first (JIT, allocator, and code-path warm), then
timed iterations until the measurement period elapses. The report shows
iterations per second, average/median/p99 iteration time, and a
Benchee-style comparison against the fastest scenario.

Benchmarks are for humans comparing before/after numbers on a quiet
machine; they are deliberately not CI-gated and assert nothing. Wall time
is the primary signal. GC counts are VM-wide deltas around the measurement
loop (the VM is otherwise idle during a bench run, and receiver-side GC
caused by a scenario is part of its cost, so this is the honest number).

Note that `tprof`-style call_memory profiling does not see refc binary
payloads, which several plugin hot paths are dominated by; when a scenario
exists to compare binary-handling strategies, trust the wall clock.
""".

-export([run/1, run/2, sample_binary_memory/1]).

-define(DEFAULT_WARMUP_MS, 500).
-define(DEFAULT_TIME_MS, 2_000).

-type scenario() :: {Name :: atom() | string(), fun(() -> term())}.
-type opts() :: #{warmup_ms => non_neg_integer(), time_ms => pos_integer()}.

-spec run([scenario()]) -> ok.
run(Scenarios) ->
    run(Scenarios, #{}).

-spec run([scenario()], opts()) -> ok.
run(Scenarios, Opts) when is_list(Scenarios) andalso Scenarios =/= [] ->
    Results = [{name_to_list(Name), measure(Name, Fun, Opts)} || {Name, Fun} <- Scenarios],
    report(Results).

%% ------------------------------------------------------------------
%% Measurement
%% ------------------------------------------------------------------

measure(Name, Fun, Opts) ->
    WarmupMs = maps:get(warmup_ms, Opts, ?DEFAULT_WARMUP_MS),
    TimeMs = maps:get(time_ms, Opts, ?DEFAULT_TIME_MS),
    Parent = self(),
    {Pid, MRef} = spawn_monitor(fun() ->
        warmup(Fun, now_us() + WarmupMs * 1000),
        {GCs0, Reclaimed0, _} = erlang:statistics(garbage_collection),
        Durations = iterate(Fun, now_us() + TimeMs * 1000, []),
        {GCs1, Reclaimed1, _} = erlang:statistics(garbage_collection),
        Parent ! {bench_result, self(), {Durations, GCs1 - GCs0, Reclaimed1 - Reclaimed0}}
    end),
    receive
        {bench_result, Pid, Result} ->
            erlang:demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, process, Pid, Reason} ->
            error({benchmark_failed, Name, Reason})
    end.

warmup(Fun, Deadline) ->
    case now_us() >= Deadline of
        true ->
            ok;
        false ->
            _ = Fun(),
            warmup(Fun, Deadline)
    end.

%% Runs at least one iteration, then keeps iterating until the deadline.
iterate(Fun, Deadline, Durations0) ->
    T0 = now_us(),
    _ = Fun(),
    T1 = now_us(),
    Durations = [T1 - T0 | Durations0],
    case T1 >= Deadline of
        true -> Durations;
        false -> iterate(Fun, Deadline, Durations)
    end.

now_us() ->
    erlang:monotonic_time(microsecond).

-doc """
Runs `Fun` while a helper process samples `erlang:memory(binary)` roughly
every millisecond, and returns the VM-wide binary memory baseline (after a
GC of the calling process) and the peak observed during the run.

Peak-over-baseline captures what per-process views cannot: transient dead
buffer generations, in-flight copies, and the live working set together.
Like the GC numbers in `run/2`, it is a VM-wide measurement and assumes an
otherwise idle bench VM.
""".
-spec sample_binary_memory(fun(() -> term())) ->
    #{baseline := non_neg_integer(), peak := non_neg_integer()}.
sample_binary_memory(Fun) ->
    %% Quiesce the whole VM before taking the baseline. Refc binaries are
    %% freed when the last *reference* is collected, so garbage from an
    %% earlier scenario can linger in any process's heap: it would inflate
    %% this baseline, then deflate the apparent peak by being freed while
    %% Fun runs.
    lists:foreach(fun erlang:garbage_collect/1, processes()),
    Baseline = erlang:memory(binary),
    Parent = self(),
    Sampler = spawn_link(fun() -> memory_sampler(Parent, Baseline) end),
    _ = Fun(),
    Sampler ! stop,
    receive
        {binary_memory_peak, Peak} ->
            #{baseline => Baseline, peak => Peak}
    end.

memory_sampler(Parent, Peak0) ->
    Peak = max(Peak0, erlang:memory(binary)),
    receive
        stop ->
            Parent ! {binary_memory_peak, Peak}
    after 1 ->
        memory_sampler(Parent, Peak)
    end.

%% ------------------------------------------------------------------
%% Reporting
%% ------------------------------------------------------------------

report(Results) ->
    Stats = [
        {Name, stats(Durations, GCs, Reclaimed)}
     || {Name, {Durations, GCs, Reclaimed}} <- Results
    ],
    NameW = lists:max([length(Name) || {Name, _} <- Stats] ++ [8]),
    io:format(
        "~n~-*s ~12s ~12s ~12s ~12s ~10s~n",
        [NameW, "Name", "ips", "average", "median", "p99", "GC/iter"]
    ),
    lists:foreach(
        fun({Name, #{ips := Ips, avg := Avg, median := Median, p99 := P99, gcs_per_iter := GCs}}) ->
            io:format(
                "~-*s ~12s ~12s ~12s ~12s ~10.1f~n",
                [
                    NameW,
                    Name,
                    format_ips(Ips),
                    format_us(Avg),
                    format_us(Median),
                    format_us(P99),
                    GCs
                ]
            )
        end,
        Stats
    ),
    case Stats of
        [_] ->
            ok;
        _ ->
            [{FastIps, FastName} | Rest] = lists:reverse(
                lists:sort([{maps:get(ips, S), Name} || {Name, S} <- Stats])
            ),
            io:format("~nComparison:~n  ~-*s ~12s~n", [NameW, FastName, format_ips(FastIps)]),
            lists:foreach(
                fun({Ips, Name}) ->
                    io:format(
                        "  ~-*s ~12s -- ~.2fx slower~n",
                        [NameW, Name, format_ips(Ips), FastIps / Ips]
                    )
                end,
                Rest
            )
    end,
    ok.

stats(Durations, GCs, Reclaimed) ->
    Sorted = lists:sort(Durations),
    N = length(Sorted),
    TotalUs = lists:sum(Sorted),
    #{
        n => N,
        ips => N / (TotalUs / 1_000_000),
        avg => TotalUs / N,
        median => lists:nth(max(1, N div 2), Sorted),
        p99 => lists:nth(max(1, round(N * 0.99)), Sorted),
        gcs_per_iter => GCs / N,
        reclaimed_words => Reclaimed
    }.

format_ips(Ips) when Ips >= 1_000_000 -> io_lib:format("~.2fM", [Ips / 1_000_000]);
format_ips(Ips) when Ips >= 1_000 -> io_lib:format("~.2fK", [Ips / 1_000]);
format_ips(Ips) -> io_lib:format("~.2f", [Ips * 1.0]).

format_us(Us) when Us >= 1_000_000 -> io_lib:format("~.2f s", [Us / 1_000_000]);
format_us(Us) when Us >= 1_000 -> io_lib:format("~.2f ms", [Us / 1_000]);
format_us(Us) -> io_lib:format("~.2f us", [Us * 1.0]).

name_to_list(Name) when is_atom(Name) -> atom_to_list(Name);
name_to_list(Name) when is_list(Name) -> Name.
