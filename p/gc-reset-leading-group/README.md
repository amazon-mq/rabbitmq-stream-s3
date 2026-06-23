# GC × reset × leading-group durability seam

Models the three-way interaction that neither sibling model exercises: a **group**
finding flowing through `still_dangling/1`'s offset-only re-check **across a
remote-tier-ahead reset**, when the reset has installed a new leading group below
the live floor.

## The bug

`rabbitmq_stream_s3_gc` has two independent live-deletion guards:

- the **classify-time carve-out** (`classify_group/3` + `referenced_group_key`):
  the leading group, which retention has pushed below `first_offset` while still
  referenced, is excluded from offset-based classification. Validated in
  `../gc-leading-group`
- the **execute-time re-check** (`still_dangling/1`): an offset-based finding is
  re-validated against the *live* floor immediately before deletion, so a
  concurrent reset that lowered the floor and re-tiered live fragments does not
  cause a live delete. Validated in `../gc-reset`

The carve-out's `referenced_group_key` is captured **once**, at the sweep's
snapshot (`build_lookup` / `lookup_entry`), and passed by value through
`list_and_classify`. `still_dangling/1`, for a group finding, re-reads only the
live **floor** (`Offset < first_offset`) — it does **not** re-run
`leading_group_info/2`. So the two guards together leave a hole:

1. GC snapshots at floor `1000` and records the snapshot leading group `(900,1)`
2. A remote-tier-ahead reset lowers the floor to `850` and installs a fresh
   leading group `(850,2)` (a GROUP with a fresh UID)
3. Normal forward retention advances the floor to `870`, so `(850,2)` now
   straddles the floor (partial expiry) and is the **live referenced leading
   group**
4. classify (against the stale snapshot) sees `(850,2)` below the snapshot floor
   `1000` and `≠` the snapshot `referenced_group_key` `(900,1)` → **candidate**
5. `still_dangling` re-reads the live floor `870`; `850 < 870` → deletes the
   **live leading group** → INV#2 dangling reference

No UID collision is required: it is an ordinary GC × reset × retention race. A
consumer reading the surviving offsets in `(850,2)` can no longer fetch it.

## The fix

`still_dangling/1`, for a group finding, re-derives the leading-group carve-out
from the **live** manifest (`live_leading_group/3`, mirroring
`leading_group_info/2` + `classify_group/3`) and keeps the group if it is now the
live referenced leading group or the live manifest is in conservative
`skip_groups` mode.

## What the model captures

- Objects carry a `Kind` (`FRAGMENT` or `GROUP`); the manifest records the live
  leading-group carve-out (`referenced_group_key` + `skip_groups`)
- The snapshot carve-out is captured once and used for the whole sweep, exactly
  as `lookup_entry` does
- `recheckCarveOut` toggles the fix: re-validate the carve-out against the live
  manifest in `still_dangling`
- `NoDanglingReference` (INV#2, headline) keyed on `(offset, uid)`, plus
  `NoLostAckedData` (INV#1) and `MonotonicFrontier` (INV#5)

## Tests and the validation gate

| Test case | Expectation |
| --- | --- |
| `tcLeadingGroupResetBug` | **fails** — offset-only re-check; `INV#2 violated: GC deleted live object (offset=850, uid=2) ... (live leading group)` |
| `tcLeadingGroupResetFixed` | **holds** — re-check re-validates the carve-out; also asserts the deep orphan `(500,0)` is reclaimed and the live leading group `(850,2)` preserved (anti-vacuity) |
| `tcLeadingGroupResetExplore` | **holds** — fix on, reset + retention raced against the sweep across all interleavings |
| `tcLeadingGroupRetentionOnly` | **holds** — forward retention only, offset-only re-check; proves the downward reset, not retention, is what defeats the offset-only re-check |

`tcLeadingGroupResetBug` is *expected to fail*: that counterexample is the proof
the seam is real, so the fixed result is meaningful.

```bash
p compile
p check -tc tcLeadingGroupResetBug       -i 2000   # 1 bug: INV#2 on the live leading group (850, uid 2)
p check -tc tcLeadingGroupResetFixed     -i 2000   # 0 bugs
p check -tc tcLeadingGroupResetExplore   -i 5000   # 0 bugs, multiple timelines
p check -tc tcLeadingGroupRetentionOnly  -i 2000   # 0 bugs
```
