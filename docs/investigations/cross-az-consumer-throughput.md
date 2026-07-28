# Cross-AZ consumer throughput

This investigation explains large run-to-run swings in consumer throughput observed on a fixed broker and node. The cause is not the remote tier, S3 retries, or the AIMD prefetch buffer. It is which availability zones the connection happens to cross on its way from client to broker.

## Setup

A single consumer read 1 million pre-published messages (message size 10KB, 10 GB total) from a stream that already had more than 1 million messages available, so the read never reached local tier data. No producers were active. Consumption was repeated over many runs with a pause between each so metrics could return to baseline.

Connections to the test broker go through a Network Load Balancer. The NLB has one zonal node per enabled AZ, and cross-zone load balancing is enabled, so a client connecting to any NLB node can be forwarded to a broker instance in any AZ. There are two independent places a connection can cross an AZ boundary: client to NLB, and NLB to broker instance.

## Method

To get a controlled sample, both hops were pinned deterministically instead of relying on random routing:

- Client to NLB: `/etc/hosts` was overridden to force the client to a specific zonal NLB entry point.
- NLB to broker: the NLB exposed per-instance listener ports provisioned, one per broker instance. Connecting to a specific port removes the NLB's target-selection randomness for that hop.

The full grid is 4 client-side entry AZs times 3 broker AZs, 12 cells, with 10 runs per cell. All 12 cells completed, 112 of 120 runs usable after discarding a few runs truncated by resolution effects at the CloudWatch measurement boundary.

Throughput per run was computed from the CloudWatch OTLP captures of Prometheus metrics: the `rabbitmq_global_messages_delivered_total` counter at 15 second resolution, using the counter's first and last non-flat points to define each run's active window.

## Result

Throughput falls off sharply and monotonically with the number of AZ boundaries crossed.

| hops | samples | mean msg/s | stdev | CV |
|---|---|---|---|---|
| 0 (same AZ throughout) | 8 | 7429.4 | 116.0 | 0.016 |
| 1 (one crossing) | 39 | 6176.0 | 437.3 | 0.071 |
| 2 (both crossings) | 65 | 4374.0 | 604.6 | 0.138 |

Pairwise Welch's t-tests on these groups are all overwhelming: 0 vs 1 hop is +20.3% (p near 0, Cohen's d 3.06), 1 vs 2 hops is +41.2% (p near 0, d 3.26), 0 vs 2 hops is +69.9% (p near 0, d 5.27). The full 12-cell breakdown by entry AZ and server AZ shows the same pattern cell by cell.

<details><summary>Full 12-cell results table</summary>

The client is in AZ `d`. `entry AZ` is the AZ of the NLB node the client connects to. `server AZ` is the AZ of the broker instance the NLB forwards to.

| entry AZ | server AZ | hops | samples | mean msg/s | stdev | CV |
|---|---|---|---|---|---|---|
| `a` | `b` | 2 | 5 | 4324.2 | 208.1 | 0.048 |
| `a` | `c` | 2 | 10 | 3794.9 | 163.8 | 0.043 |
| `a` | `d` | 2 | 10 | 4219.8 | 380.4 | 0.090 |
| `b` | `b` | 1 | 10 | 5745.7 | 183.9 | 0.032 |
| `b` | `c` | 2 | 10 | 4953.6 | 171.1 | 0.035 |
| `b` | `d` | 2 | 10 | 3827.5 | 63.4 | 0.017 |
| `c` | `b` | 2 | 10 | 5383.1 | 131.8 | 0.024 |
| `c` | `c` | 1 | 10 | 5965.2 | 174.4 | 0.029 |
| `c` | `d` | 2 | 10 | 4090.2 | 195.6 | 0.048 |
| `d` | `b` | 1 | 9 | 6767.0 | 362.5 | 0.054 |
| `d` | `c` | 1 | 10 | 6284.9 | 85.8 | 0.014 |
| `d` | `d` | 0 | 8 | 7429.4 | 116.0 | 0.016 |

112 of 120 planned runs total. The 8 missing runs were truncated by resolution effects at the CloudWatch measurement boundary and discarded rather than estimated.

</details>

The two hops are not interchangeable. Splitting the single-crossing cases by which hop crossed:

| crossing | samples | mean msg/s |
|---|---|---|
| client to NLB crosses, NLB to broker stays local | 20 | 5855.5 |
| client to NLB stays local, NLB to broker crosses | 19 | 6513.3 |

The client-to-NLB crossing costs noticeably more than the NLB-to-broker crossing (p near 0, d 2.22). This is consistent with the client-side leg traversing a longer network path than the NLB-to-broker leg, which stays inside the VPC.

## Mechanism

The stream protocol's consumer flow control is credit-based. The Java client's default flow strategy is `creditOnChunkArrival(initialCredits)` with `initialCredits=10`: the consumer grants one credit per chunk delivered, keeping a small, fixed in-flight window rather than an amortizing one. With chunks carrying one message each in this workload, that window caps how much data can be outstanding at once, which makes steady-state throughput bound by round-trip latency rather than by bandwidth.

This was confirmed independently by sweeping `--initial-credits` across 10, 50, 200, and 1000 on the same broker. Coefficient of variation collapsed as the window grew: 0.182 at window 10, 0.182 at window 50, 0.068 at window 200, 0.031 at window 1000. A small credit window is far more sensitive to RTT noise than a large one, which is exactly the signature of an RTT-bound in-flight window rather than a throughput-bound one. Working backward from measured throughput and the confirmed 10-credit, 1-message-per-chunk workload gives an implied per-hop RTT increase in the sub-millisecond range, which is plausible for an added AZ hop.

Because this is a latency-bound mechanism, it is specific to the credit-gated consumer path. A long-lived bulk transfer, such as the broker's own connection to S3 for uploads or remote reads, is not gated the same way and would not be expected to show the same sensitivity to a single AZ hop. That path was not part of this workload (no producers were active) and remains untested.

## Implications

- For latency-sensitive consumer workloads, AZ placement of the client relative to the NLB and broker matters more than raw network bandwidth between them.
- The default `initialCredits=10` amplifies this sensitivity. Increasing the credit window is a lever available to any consumer that wants to trade a larger in-flight buffer for immunity to this effect, without any broker-side change.
- Cross-zone load balancing on the NLB, combined with small in-flight windows, means a customer's observed throughput can vary run to run.

## Open questions

**Does the S3 connection cross AZs too.** The broker's own connection to S3 was suspected as a third possible AZ boundary but was not measured here. Whether S3 endpoint AZ affinity is even a meaningful concept, and whether it matters for a bulk path that is not RTT-bound the way the consumer path is, needs separate investigation.

**Whether the client-vs-NLB hop asymmetry generalizes.** The observed asymmetry between the two hop types was measured on one specific client and one specific NLB and set of broker instances. It is not yet known whether this reflects a general property, and whether the results reproduce in different regions and combinations of AZs. This test was performed in us-west-2.

**Default credit window tuning.** Given how sharply throughput responds to `initialCredits` at low values, it may be worth documenting/recommending that clients use an initial credit value larger than 10.
