# CP21S-A.2 — Compact economic timeline

**Purpose:** keep Direct2 economic monitoring complete in memory while eliminating the
multi-gigabyte JSON audit volume produced by repeatedly writing routine evaluations.

The economic candidate index, V10 simulation decisions, source freshness checks,
and selection logic are **unchanged**. This change applies only to the audit journal.

## Event retention

- Detailed journal rows remain for \`queued\`, \`v10_submitted\`, \`waiting\`,
  \`not_dispatched\`, \`model_unknown\`, \`retained_ready\`, and any other
  exceptional or actionable economic action.
- Detailed \`catalog_pool\`, \`catalog_reconciliation\`, \`watchset\`,
  \`watchset_snapshot\`, and \`watchset_uncertain\` events are unchanged.
- Routine per-pair actions \`unpriced\`, \`nonpositive\`, \`retained\` are
  counted rather than serialized in compact mode. Their **exact** totals,
  plus per-phase/reason counts for unavailable quotes, are stored in the
  terminal \`economic_rollup\` record and execution worker report.
- A compact journal is **not** a complete historical trace for arbitrary
  excluded pairs. Exact reconstruction needs the original full run or a
  deliberate focus-pool trace, captured while the relevant pool updates occur.

## Configuration

The compact policy is the default whenever \`GRAPH_CP21S_TIMELINE_PATH\` is set.

\`\`\`powershell
$env:GRAPH_CP21S_TIMELINE_MODE = 'compact'
# Optional, comma-separated POOL addresses (not wallet addresses):
$env:GRAPH_CP21S_TIMELINE_FOCUS_POOLS = 'POOL_ADDRESS_1,POOL_ADDRESS_2'
.\scripts\cp21s_a2_timeline_live.ps1
\`\`\`

Use \`GRAPH_CP21S_TIMELINE_MODE=full\` **only when intentionally
collecting the full multi-gigabyte per-pair decision stream**. Never run this
mode during normal live operation. The CP21S-A.2 runner forces compact mode.

For an external wallet reconciliation: extract the pools touched by that
wallet's transactions (CP28), then focus those **pool addresses**. A wallet
address by itself cannot filter graph-level pool evaluation events.

## Evidence checks

\`cp21a1_timeline_emitted == cp21a1_timeline_written\` and
\`cp21a1_timeline_lost == 0\` still verify physical journal completeness.
Compacted routine actions are not counted as lost; they have explicit
\`cp21a2_suppressed_*\` fields. The Python audit reports:

- \`counts.total\`: physical JSONL records.
- \`suppressed_by_action\`: exact compacted routine action counts.
- \`accounted_economic_pair_events\`: saved economic-pair records plus
  exactly counted routine decisions.

### 2026-11-07 baseline

The archived five-minute run on commit \`a812063c\` produced 2,135,260
timeline rows, 2,444,212,161 uncompressed bytes. Of those, 2,130,921
were routine \`unpriced\` / \`nonpositive\` / \`retained\` decisions.
An offline proof-of-concept compaction retained 4,339 original detailed
rows plus one rollup; the complete ZIP became 346,509 bytes.
This **does not certify the size or speed of the updated code**; validate it
with the next five-minute run before merging.

## Validation

\`\`\`powershell
git switch cp21s-a2-compact-timeline
cargo test --locked --release cp21a2_ -- --nocapture
cargo test --locked --release cp21a1_ -- --nocapture
python -m unittest discover -s scripts/tests -p 'test_cp21a1_timeline.py' -v
.\scripts\cp21s_a2_timeline_live.ps1
\`\`\`

No real transactions are broadcast by this runner. Compare the compacted
event counts with the \`execution_candidates.cp21s_pair_refreshes\` counter,
and verify the journal audit has no missing required fields or clock-ordering
errors. All V10 dispatch decisions must remain individually visible.
