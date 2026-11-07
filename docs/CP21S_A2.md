# CP21S-A.2 — Delta-gated Direct2 and verified compact evidence archive

**Base**: CP21S-A.1 (`a812063c2`). **Branch**: `cp21s-a2-state-journal-optimization`.
**PR**: #102 (draft, no merge before live review). **Mode**: simulation-only, no broadcasts.

## What is changed

1. **Directional gate history, not redundant partner-wide rejections.**
   Two immutable facts exclude a direction before computing a pool-pair quote:
   absent price or the explicitly configured `dust` direction with no marginal
   weight. There is no new economic filter. The `direction_observation` events
   contain pool address, input/output mint, status, observed slot, component
   slot min/max, and host receipt UTC microseconds. They are retained when
   a gate changes or a new observation arrives. The complete pair index
   does not repeatedly score every partner of a still-blocked direction.
   Previously indexed candidates are invalidated once when the direction
   becomes blocked, and *all* directed peers are checked again when it
   returns to `available`. A priced direction with missing marginal log
   remains eligible for the amount-dependent model; model-unknown is not
   proof of a negative route.
2. **Frozen quote reuse** occurs before the expensive economic optimizer
   when the same complete source fingerprint reappears. Initial live
   dispatch eligibility is still promoted, and future changes still force
   recomputation. The existing 50k-update test remains the full reference.
3. **Pure catalog ID permutation** uses a stable-address O(n) remap of
   entries and ready priorities when every pool's frozen price Arc and
   directed topology are unchanged. Actual content/participant changes use
   the existing conservative reconciliation path, with blocked directions
   skipped rather than rematerializing millions of unavailable pairs.
4. **Prioritize model-unknown candidates** by their observed marginal
   log advantage *within the unknown category*. This value is not a
   fabricated dollar gain. Every positive modeled normalized USD net result
   still outranks unknown and the model-negative entries remain indexed.
   No per-revision pair cap or V10 candidate throttling is introduced.
5. **Pack preemption** happens before expensive assembly if a newer
   market event is pending, with the existing post-pack check kept.
6. **Archive** is produced only after the existing bounded journal writer
   has completed its fsync and event accounting, and the streaming JSONL
   analyzer confirms every record and the writer count. A separate
   streaming archiver independently verifies complete gzip round-trip and
   SHA256 + row count before deleting the raw JSONL. The final artifact
   `economic_timeline.jsonl.gz` is fully readable through
   `analyze_cp21a1_timeline.py`; its metadata are
   `economic_timeline_archive.json`. If any check fails, raw data remain.
7. **V10 Custom(1)** means the completed V10 economic decision
   `no_economic_route`, not a failed executor implementation.
   The execution analyzer reports this separately without treating
   a negative simulation as a successful trade.

## Explicitly unchanged

Direct2, settlement WSOL/USDC, the existing Jupiter mint and TVL rules,
PumpSwap exception, `direction_notional=50 USD`, threshold=0, the three
selected dry-run DEXs, independent V10 trade size (`trade_size=0`),
10% profit share, 32 configured simulation workers, and *zero transactions
sent*. An unknown quote is not discarded because no economic forecast
can be computed. Data outside the live watchset cannot be inferred.

## Reproducible run

```powershell
Set-Location C:\dev_projects\liq_graph
git fetch origin
git switch cp21s-a2-state-journal-optimization
git pull --ff-only origin cp21s-a2-state-journal-optimization
.\scripts\cp21s_a2_live.ps1
```

The script runs Rust economic regressions, CP21S-A/CP21S-A.1 and CP21S-A.2
tests, Python analyzers and archives, then 300 seconds of read-only
mainnet observations and V10 `simulateTransaction`.
Local `.env` and `~/.solana/meteora-arb-test.json` are required.

## Proofs to inspect in the next archive

- Compare `execution.json.execution_candidates.cp21a2_blocked_pair_skips`,
  `cp21a2_direction_observations`, `cp21a2_gate_transitions`,
  `cp21a2_reused_quote_calculations`, `cp21a2_catalog_id_remaps`,
  `cp21a2_prepack_preemptions` and `cp21s_pair_refreshes`.
- Check `economic_timeline_analysis.json` event counts against
  `cp21a1_timeline_written` and
  `economic_timeline_archive.json.integrity_verified`; require lost=0.
- Compare V10 outcome classes in
  `execution_analysis.json.v10_economic_outcomes`. Model positives with
  `Custom(1)` are negative *at simulation bank state*, not missing
  requests. Compare source component slots and V10 bank slots carefully.
- Check p50/p95/p99 refresh/dispatch, selected/qualified pool coverage,
  and the number of distinct positive *frozen* opportunities.

## Limits of evidence

`direction_observation` records the state and gate for each changed
pool direction. Repeated combinations that are structurally impossible
are no longer emitted as separate `economic_pair` events; their exclusion
is reconstructed by combining the two direction histories and catalog
membership. Thus raw pair-refresh counters are **not** directly comparable
with CP21S-A.1: the optimization intentionally avoids redundant checks.
The archive never samples or truncates the economic evaluations that
actually occur. Bank identity and per-subscription WS ACK history are
not proven by this checkpoint; slots are not atomic multi-account snapshots.
