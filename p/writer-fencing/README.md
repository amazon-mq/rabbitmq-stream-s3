# Writer / epoch fencing

Models the optimistic-lock CAS in `rabbitmq_stream_s3_db:do_put/5` that fences a
deposed writer off the manifest root.

## The property

A manifest-root commit goes through a conditional `adv_put`:

```erlang
[#if_payload_version{version = ExpectedRevision},
 #if_data_matches{pattern = {'_', '$1'}, conditions = [{'>=', Epoch, '$1'}]}]
```

The put succeeds only if **both** hold: the current revision matches the expected
one (the optimistic lock), and the new epoch is `>=` the stored epoch (the
fence). The revision lock alone is not enough: a deposed writer can read the
*current* revision after a newer writer committed and then CAS with that
revision. The epoch fence rejects it because its epoch is lower.

This is the mechanism that makes the committed epoch monotonic - the assumption
the `gc-reset/` epoch axis (and the durability guards generally) rely on. So this
model proves a foundation the others build on.

## What the model captures

- A `KhepriDB` with the CAS: a put commits iff `expectedRev == revision` and
  (`fence` off, or `newEpoch >= storedEpoch`); on success the revision advances
- Two `Writer`s: a new writer (epoch 2) and a deposed writer (epoch 1) that still
  attempts to commit
- `fence` toggles the `>=` epoch condition
- `NoEpochRegression`: the committed epoch never decreases (a decrease means a
  deposed writer overwrote a newer one - split-brain)

## Tests and the validation gate

| Test case | Expectation |
| --- | --- |
| `tcFencingGuarded` | **holds** - the new writer commits (epoch 2); the deposed writer (epoch 1) is rejected by the fence |
| `tcFencingUnguarded` | **fails** - fence removed; the deposed writer's revision-matching commit succeeds and lowers the epoch; `split-brain: committed epoch regressed from 2 to 1` |
| `tcFencingExplore` | **holds** - both writers' reads and commits interleaved freely; no schedule regresses the epoch |

`tcFencingUnguarded` is *expected to fail*; that counterexample is the proof the
model reproduces split-brain.

```bash
p compile
p check -tc tcFencingGuarded   -i 5000   # 0 bugs
p check -tc tcFencingUnguarded -i 2000   # 1 bug: epoch regression (split-brain)
p check -tc tcFencingExplore   -i 5000   # 0 bugs
```
