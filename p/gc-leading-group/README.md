# GC leading-group carve-out

Models the second, independent live-deletion guard in `rabbitmq_stream_s3_gc`: `classify_group` and the `referenced_group_key` carve-out.

## The bug

A group object below `first_offset` is normally a dead orphan and is deleted by the offset heuristic. There is one exception, the leading group. On partial expiry retention advances `first_offset` into the leading group, so that group still sits below the floor yet remains referenced (its surviving fragments are at or above the floor). `leading_group_info/2` records that group's key as `referenced_group_key`, and `classify_group` skips it.

Removing the carve-out treats every group below the floor as a deletable orphan and deletes a live group, a dangling reference: a consumer reading the surviving offsets in that group can no longer fetch it.

This guard is distinct from `still_dangling/1` (the `gc-reset` model): that one re-validates an offset-based finding against the live floor after a reset, this one excludes the referenced leading group from offset-based classification in the first place. The flat-offset `gc-reset` model does not exercise it, so it lives here.

## What the model captures

- Objects carry a `Kind` (`FRAGMENT` or `GROUP`); S3 holds genuine orphans below the floor, the referenced leading group (also below the floor), and a live fragment at the floor
- The manifest records the referenced leading group key
- `guardLeadingGroup` toggles the `classify_group` carve-out
- `NoDanglingReference` (INV#2): GC never deletes a referenced object

This is a classification-correctness property, not a concurrency race, so there is no exploration test.

## Tests

| Test case | Expectation |
| --- | --- |
| `tcLeadingGroupGuarded` | holds: the carve-out protects the leading group, and the genuine orphans are reclaimed with the leading group preserved (anti-vacuity) |
| `tcLeadingGroupUnguarded` | fails: carve-out removed; `INV#2 violated: GC deleted referenced object (offset=80, uid=2)` |

`tcLeadingGroupUnguarded` is expected to fail: that counterexample is the proof the model reproduces the live-group deletion.

```bash
p compile
p check -tc tcLeadingGroupGuarded   -i 5000   # 0 bugs
p check -tc tcLeadingGroupUnguarded -i 2000   # 1 bug: INV#2 on the leading group (80, uid 2)
```
