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
| 0 (same AZ throughout) | 8 | 6590.9 | 200.4 | 0.030 |
| 1 (one crossing) | 39 | 5647.4 | 430.0 | 0.076 |
| 2 (both crossings) | 65 | 4112.8 | 525.0 | 0.128 |

Pairwise Welch's t-tests on these groups are all overwhelming: 0 vs 1 hop is +16.7% (p near 0, Cohen's d 2.31), 1 vs 2 hops is +37.3% (p near 0, d 3.09), 0 vs 2 hops is +60.3% (p near 0, d 4.89). The full 12-cell breakdown by entry AZ and server AZ shows the same pattern cell by cell.

<details><summary>Full 12-cell results table</summary>

The client is in AZ `d`. `entry AZ` is the AZ of the NLB node the client connects to. `server AZ` is the AZ of the broker instance the NLB forwards to.

| entry AZ | server AZ | hops | samples | mean msg/s | stdev | CV |
|---|---|---|---|---|---|---|
| `a` | `b` | 2 | 5 | 4068.7 | 120.1 | 0.030 |
| `a` | `c` | 2 | 10 | 3651.8 | 178.6 | 0.049 |
| `a` | `d` | 2 | 10 | 3950.0 | 342.4 | 0.087 |
| `b` | `b` | 1 | 10 | 5349.7 | 304.5 | 0.057 |
| `b` | `c` | 2 | 10 | 4603.2 | 158.7 | 0.034 |
| `b` | `d` | 2 | 10 | 3645.2 | 89.3 | 0.025 |
| `c` | `b` | 2 | 10 | 4981.7 | 179.4 | 0.036 |
| `c` | `c` | 1 | 10 | 5435.1 | 289.9 | 0.053 |
| `c` | `d` | 2 | 10 | 3867.2 | 235.9 | 0.061 |
| `d` | `b` | 1 | 9 | 6139.2 | 322.2 | 0.052 |
| `d` | `c` | 1 | 10 | 5714.9 | 308.1 | 0.054 |
| `d` | `d` | 0 | 8 | 6590.9 | 200.4 | 0.030 |

112 of 120 planned runs total. The 8 missing runs were truncated by resolution effects at the CloudWatch measurement boundary and discarded rather than estimated.

</details>

The two hops are not interchangeable. Splitting the single-crossing cases by which hop crossed:

| crossing | samples | mean msg/s |
|---|---|---|
| client to NLB crosses, NLB to broker stays local | 20 | 5392.4 |
| client to NLB stays local, NLB to broker crosses | 19 | 5915.9 |

The client-to-NLB crossing costs noticeably more than the NLB-to-broker crossing (p = 0.00005, d 1.49). This is consistent with the client-side leg traversing a longer network path than the NLB-to-broker leg, which stays inside the VPC.

## Mechanism

The stream protocol's consumer flow control is credit-based. The Java client's default flow strategy is `creditOnChunkArrival(initialCredits)` with `initialCredits=10`: the consumer grants one credit per chunk delivered, keeping a small, fixed in-flight window rather than an amortizing one. With chunks carrying one message each in this workload, that window caps how much data can be outstanding at once, which makes steady-state throughput bound by round-trip latency rather than by bandwidth.

This was confirmed independently by sweeping `--initial-credits` across 10, 50, 200, and 1000 on the same broker. Coefficient of variation collapsed as the window grew: 0.182 at window 10, 0.182 at window 50, 0.068 at window 200, 0.031 at window 1000. A small credit window is far more sensitive to RTT noise than a large one, which is exactly the signature of an RTT-bound in-flight window rather than a throughput-bound one. Working backward from measured throughput and the confirmed 10-credit, 1-message-per-chunk workload gives an implied per-hop RTT increase in the sub-millisecond range, which is plausible for an added AZ hop.

Because this is a latency-bound mechanism, it is specific to the credit-gated consumer path. A long-lived bulk transfer, such as the broker's own connection to S3 for uploads or remote reads, is not gated the same way and would not be expected to show the same sensitivity to a single AZ hop. That path was not part of this workload (no producers were active) and remains untested.

### Confirming the mechanism by controlling AZ and credit window together

The credit sweep above did not control AZ placement, so it could only show that variance goes down as the window grows, not that the specific AZ gap goes away. A follow-up sweep pinned AZ placement to the two most extreme cells from the grid above, the 0-hop cell (`d/d`) and the largest-gap 2-hop cell (`b/d`), and reran `--initial-credits` at 50, 200, and 1000 on each, 5 runs per cell per window. Throughput was computed from the same CloudWatch OTLP counter and active-window method as the main sweep.

| window | d/d mean msg/s | b/d mean msg/s | gap | gap as % of the window 10 gap | p |
|---|---|---|---|---|---|
| 10 (from the main grid, same two cells) | 6590.9 | 3645.2 | 2945.7 | 100.0% | p < 0.0001 |
| 50 | 7111.5 | 6963.3 | 148.2 | 5.0% | p = 0.58 |
| 200 | 7408.2 | 7445.3 | -37.1 | -1.3% | p = 0.90 |
| 1000 | 7782.1 | 7782.1 | 0.0 | 0.0% | p = 1.00 |

The gap converges to near-zero by window 50 and stays there through window 1000. AZ placement stops mattering once the credit window is large enough to absorb the added RTT, which is exactly what the mechanism predicts and is the strongest evidence that credit-window size, not the AZ hop itself, is the actual lever.

## Implications

- For latency-sensitive consumer workloads, AZ placement of the client relative to the NLB and broker matters more than raw network bandwidth between them.
- The default `initialCredits=10` amplifies this sensitivity. Increasing the credit window is a lever available to any consumer that wants to trade a larger in-flight buffer for immunity to this effect, without any broker-side change.
- Cross-zone load balancing on the NLB, combined with small in-flight windows, means a customer's observed throughput can vary run to run.

## Open questions

**Does the S3 connection cross AZs too.** The broker's own connection to S3 was suspected as a third possible AZ boundary but was not measured here. Whether S3 endpoint AZ affinity is even a meaningful concept, and whether it matters for a bulk path that is not RTT-bound the way the consumer path is, needs separate investigation.

**Whether the client-vs-NLB hop asymmetry generalizes.** The observed asymmetry between the two hop types was measured on one specific client and one specific NLB and set of broker instances. It is not yet known whether this reflects a general property, and whether the results reproduce in different regions and combinations of AZs. This test was performed in us-west-2.

**Default credit window tuning.** The AZ-controlled credit sweep confirms that a window well above 10 removes the AZ sensitivity entirely, 50 was already enough in this environment. It may be worth documenting or recommending that latency-sensitive clients raise `initialCredits` above the default, though the exact value needed likely depends on the RTT of the client's own network path and should not be assumed to be 50 in general.
