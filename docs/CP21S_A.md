# CP21S-A – Complete Incremental Economic Direct2 Signal Evaluation

**Base:** `cp21a-net-profit-accounting`. **Status:** Separate opt-in checkpoint.
Only activate with `GRAPH_CP21S_INCREMENTAL_SIGNALS=true` plus
`GRAPH_CP9C_PACKING_MODE=economic` and `GRAPH_ENABLED_ROUTES=direct2`.

## Invariants

- Jupiter market-data qualification and the mint eligibility predicate are
  unchanged. So are pool TVL/admission, Prepared V10 proofs, WSOL/USDC
  settlements, the existing selected WatchCatalog, the 50-USD direction gate,
  the three enabled DEXs, flashloan use, V10 `trade_size=0`, and simulated
  mainnet **without transaction broadcasts**.
- Every `SignalStateUpdate`/Price revision is consumed in sequence. The
  incrementally maintained identity index `State.pair_pools` maps the
  changed pool to **all** same-token-pair counterparts already admitted to
  the Direct2 signal plan.
- Each affected **directed** ordered pair is re-evaluated once, including
  the two trade directions when both are enabled by the topology. Neither
  the previous best marginal pool nor the `SignalCandidateBatch` is
  needed to invoke the economic quote. This includes revisions in which the
  GraphSignals marginal winner does not change and no CandidateBatch is
  emitted. There is no 3/96/512 candidate-count cutoff.
- Scores use the already implemented CP21A WSOL/USDC USD-nanounit
  normalization, frozen pool-amount curves, known 10% V10 profit share,
  and known network cost. Exact frozen source fingerprints detect unchanged
  economics. Unknown quote states are **accounted for**, not invented.
- A live economic index holds entries for all currently price-valid directed
  combinations. Ranking is updated by direct key in a BTreeSet: no
  iteration over unrelated tokens at each update. Known model-positive
  opportunities are highest priority; Unknown with positive marginal
  witness is eligible. Nonpositive forecasts remain completely evaluated
  and indexed but are not sent to V10 by this mode.
- CP21S-A packages the **specific quoted pair**, with no unrelated greedy
  alternative pools. V10 still makes its own amount decision for that
  pair. This guarantees auditability of source pair vs simulated path.
- Only the V10 channel capacity is bounded (existing 32 concurrent
  simulations plus the channel queue). A full channel does not remove
  pending entries. On the next event or 5ms scheduler tick the worker
  retries in globally ranked order. A newer observation supersedes the
  old entry; stale requests are not sent later.
- Existing CP20S cooldown remains a dedup protection on the **exact
  packed snapshot**, not a limitation on economic discovery.
- Every such replacement or rejection is counted. An input broadcast lag
  is detected, forces resynchronization and fails the complete-run evidence
  contract. There is no silent signal loss.
- Catalog snapshots rebuild the index. Baseline/replay updates contribute to
  scores without being mistaken for new live opportunities. Coverage/removal
  updates invalidate affected scores. WS gaps clear the stale ranking and
  require renewed state.

## Performance and completeness tests

Run:

```powershell
cargo test --locked --release cp21s_a_ -- --nocapture
```

Deterministic tests include:
1. Changing one of 5, 97, 513, or 1,001 peers rechecks *every* incoming and
   outgoing combination; the old arbitrary limits do not apply.
2. 50,000 synthetic ordered updates compared to a separate exhaustive
   full-route reference calculation and global ranking.
3. Real frozen CPMM pool observations: a reserve change updates all and only
   the affected pairs, with each score equal to an independent full economic
   recomputation.
4. 50,000 real varying-reserve CPMM economic optimizer calls, reporting
   distribution of p50/p95/p99 calculation nanoseconds. These timings are
   machine-specific and do not establish network latency.
5. A full V10 channel leaves the highest-priority work pending, and the
   next available capacity can resume it.

Mainnet dry-run (on an environment with the configured wallets and RPC keys):

```powershell
Set-Location C:\dev_projects\liq_graph
git fetch origin
git switch cp21s-a-complete-incremental-direct2
git pull --ff-only origin cp21s-a-complete-incremental-direct2
.\scripts\cp21s_a_incremental_live.ps1
```

The new 300-second runner preserves the previous protocol, quality and
execution settings; it explicitly enables the new path and deactivates
it again after completion.

Inspect `execution.json` → `execution_candidates`:
`cp21s_updates`, `cp21s_pair_refreshes`, `cp21s_entries`,
`cp21s_model_positive`, `cp21s_model_nonpositive`,
`cp21s_model_unknown`, `cp21s_unpriced`,
`cp21s_identical_state_reuses`, `cp21s_state_superseded`,
`cp21s_pending_peak`, `cp21s_pending_at_shutdown`,
`cp21s_queue_full_retries`, `cp21s_seed_rejections`,
`cp21s_amount_exclusions`, `cp21s_dispatched`,
`cp21s_score_timing`, `cp21s_refresh_timing`, and
`cp21s_drain_timing`, with `lost_events=0`.

**Acceptance:** Full market update consumption with zero input stream gaps;
complete per-update directed pair counts; no 3/96/512 cutoffs;
no silent queue-full drops; V10 requests/reports matched; p50/p95/p99
latencies reported; meaningful distinct positive *frozen path* outcomes.
The 50k deterministic test is necessary but not sufficient to prove
line-rate mainnet throughput; the live run must be evaluated separately.

## Out of scope

- Pool universe coverage (e.g. the exact historical AI/Meteora pool) is
  a distinct audit. An excluded pool cannot be economically evaluated.
- More than two legs, token-2022 transfer fees with unresolved model
  semantics, exact on-chain DLMM dynamic fee progression, and matched
  simulation bank freshness are future separate checkpoints.
- No promise that all *temporary versions* can be simulated while there
  are only 32 V10 slots. Every relevant version is evaluated; superseded
  versions yield to newer states and are explicitly accounted for.
- Negative forecasts are not exclusion proofs for the entire topology:
  the approximate economic model determines only advisory V10 priority.
