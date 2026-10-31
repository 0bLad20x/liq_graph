# CP9F – MarketState WS hotpath latency, phase 1 (lossless valuation-state elision)

Parent: `cp9e-jupiter-reliable-direct3`. Draft experimental branch, **no broadcast execution, no strategy/routing changes.**

## Evidence from CP9E 60-minute capture (not extrapolated from arbitrary elapsed time)

- 102,807 Account events processed; 28,786,889 pool bindings -> 280 bindings/event on average (shared account fanout).
- `market_state.metrics.processing`: 17.895 ms/event mean, max 491.18 ms, total ~1,840 seconds.
- `market_state.metrics.liquidity.profile_build`: 69,481 calls, mean 26.314 ms, max 491.15 ms, summed ~1,828 seconds. *These per-call durations are recorded inside the same serial MarketState WS consumer and are not independent wall-clock processes.*
- `market_state.metrics.queue`: 10.281 s mean and 98.8 s max, receiver buffer peak 4096; 137 Account inputs lost through broadcast lag. `SignalEvent` delivery loss remained zero.
- DLMM-specific triggers were 42,368 `dlmm_bin_array`, 9,216 `dlmm_bitmap`, 15,520 `dlmm_lbpair`, plus clock revaluation. Profile changes 24,950 vs 69,481 complete profile builds – unchanged resulting profiles are common, but **unchanged output does not prove unchanged input**.
- Market selection `all_fit` (~1943 pools, ~8664 accounts of WS capacity 11500). Provider disconnects and failed reconnect handshakes near end are a separate stability concern, not necessarily caused by one internal CPU defect.
- PumpSwap's common global-fee account resulted in 329,795 field-attributed observations, and 394,613 published fee valuations had unchanged net rates. A high shared-account fanout can generate repeated publication without newly actionable economic state; this is a **next profiling target**, not yet changed in CP9F. Clock updates and profile work must be separated by origin in a measured run.
- The one-hour test's 44 WS gaps (including provider HTTP 429s) remain independent failure modes. No amount of quote-computation optimization guarantees recovery from external rate limits.

## Isolated code change

`Book::pair`, `Book::array`, `Book::bitmap` short-circuit when the entire incoming account byte payload matches the previously validated payload. The underlying subscription still updates slot, received timestamp, and live-state readiness bookkeeping. **Only if the pool was already available and the account bytes are identical** can MarketState skip a redundant DLMM profile build, indicative price/valuation and duplicate signal publication. It never skips initialization, recovery of an unavailable pool, or a changed byte payload; different in-slot writes remain in sequence. This does **not** change the signal threshold, 50 USD gate, account budget, Direct2/Direct3, 4 packing strategies, 32 V10 simulations, or persisted manifests.

New diagnostics in `market_state.json`:
- `metrics.dlmm_identical_account_events_elided`: counts of avoided recalculation by field
- `metrics.processing_by_account_field`: latency distribution attributed to the WS account's first bound field
- `metrics.account_bindings_max_per_event`: maximal shared account fanout

Regression test validates identical payload short circuits and changed valid DLMM account payloads are still accepted.

## Acceptance before choosing a more invasive optimization

Use the same CP9E simulation configuration for **120-second smoke** and (if integration passes) a **5-minute comparative run**. Independent nonstationary market windows cannot prove a percentage speedup; compare per-operation `processing`, `queue`, `profile_build`, eliminated unchanged events and fanout *together with* event load and per-venue mix.

Success criteria: no lost market/signal events, no regression in priced pools for equivalent coverage, exact request/report accounting, positive DLMM/pump signals still observed, profile samples remain calibrated, and strong reduction in DLMM profile rebuild work when duplicate payloads appear. Do not claim that stream reconnect 429s or external latency is solved.

**Further audit**: If profile recompute remains dominant, investigate immutable snapshot/versioning and per-pool background profile workers with slot-monotone generation checks; preserve transient pool updates until an explicit economic policy supports coalescing. Never silently drop WS events to improve p95. Also investigate repeated shared PumpSwap global-fee fanout and separate WS transport from MarketState compute. Dead-code deletion is secondary and requires dependency/consumer proof, not warning suppression.

Existing observed source: `20261031T101050438Z.zip`, `market_state.json` and `CP9E_Stundenlauf_Vollanalyse_20261031.md`.
