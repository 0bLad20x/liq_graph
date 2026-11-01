# CP15 Raydium CLMM implementation and evidence

Branch: `cp15-raydium-clmm-direct2`; PR #94 remains draft until operator's
600-second V10 mainnet simulation is analyzed. Base: CP14 main SHA
`75b0093d19fa47aaebf533aa459569b0fab716f7`.
Contract: [CP15_PLAN.md](CP15_PLAN.md).

## Implemented modules

- `src/workers/completion/raydium_clmm.rs`: Prepared with a validated first
  initialized TickArray on either side; native direction readiness in version-2
  evidence; light/full manifest consistency; invalid direction guarded separately
  by V10. Existing PDA, vault, Mint and bitmap validation remains mandatory.
- `src/workers/execution_v10.rs`: CLMM direction requires at least one TickArray
  in addition to common program, config, bitmap, observation and vault roles.
- `src/workers/market_state/raydium_clmm.rs`: full pool/config/TickState
  decoding, fee_on side, static and time-aware dynamic fee, directional marginal
  net rates, conservative **single active interval** capacity, frozen diagnostic
  approximate base-input quote.
- `src/workers/market_state.rs`: full relevant CLMM subscriptions, Typed
  FeeValuation/PoolLiquidity, Clock-slot guard for dynamic fee, account-set digest,
  update/re-watch; no separate connection transport or Signal Engine.
- `src/workers/execution_candidates.rs`: post-V10 amount model against the
  chosen input. `amounts.rs` explicitly refuses a speculative pre-V10 CLMM
  economic exclusion proof.
- `src/core/market_state.rs`: minimal `ClmmQuoteSnapshot` with only bounded
  snapshot fields needed for diagnostic quote and reference replay.
- `src/workers/pool_registry.rs`: end-of-run native direction classes
  (both, 0->1 only, 1->0 only, neither and unclassified) and selected/priced
  counts from authoritative registry snapshot.
- `scripts/cp15_clmm_reference.py`: separate Python integer reference based
  on Raydium `swap_math::compute_swap`, valid only for input within observed
  active interval. Fee-on-output, ceil/floor and direction are tested; V10
  detail/compact pairing uses request ID + payload SHA-256 + bank context slot.
  Reference-eligible cases must be independently traceable, but a later
  simulation bank can differ from the frozen snapshot.
- `scripts/cp15_clmm_live.ps1`: reproducible 60/300/600-second five-venue
  Direct2-only run, V10 simulation, no broadcast. Writes
  `cp15_clmm_directions.json`, `cp15_clmm_calibration.json`,
  `cp15_clmm_reference.json` in the runtime folder, plus standard evidence.

The portfolio scanner and all existing worker boundaries remain unchanged.
The CLMM active interval is a **partial execution witness**, not a multi-tick
simulator. It may omit routes, but must never claim Dust or an exact quote
outside its bounded coverage. Limit orders at the observed next tick boundary
are excluded from that interval; in-pool fee-group changes require a Clock
revaluation and bounded execution interval. No profit guarantee.

## Tests and operational checks

CI locked build and `cargo test --locked --release cp15_` plus
`python scripts/test_cp15_clmm_reference.py`, alongside all CP10-CP14
regressions and PowerShell syntax checks.

On Windows after the final CI green:
```powershell
Set-Location C:\dev_projects\liq_graph
git fetch origin
git switch cp15-raydium-clmm-direct2
git pull --ff-only origin cp15-raydium-clmm-direct2
.\scripts\cp15_clmm_live.ps1 -DurationSeconds 60
.\scripts\cp15_clmm_live.ps1 -DurationSeconds 300
.\scripts\cp15_clmm_live.ps1 -DurationSeconds 600
```

For each final run compare: unique admitted/prepared/selected/priced CLMM
pools, complete CLMM V10 calls and rates, unknown fee/amount reasons, exact
reference-eligible raw deltas, reference-unknown coverage and duplicated
payload joins (zero), WebSocket provider gaps and actual recovery, and
market/signal event losses (both zero). Four baseline venues are controls.
The 600-second ZIP is required to decide whether to merge PR #94.

## Limitations before live evidence

- Integer reference currently covers no arbitrary multi-tick cross or limit
  order; V10 remains the amount and profitability authority.
- MarketState's approximate directed capacity is deliberately discounted and
  represents only the observed next interval; it is not a strict exact
  mathematical quantity proof. Cases without a complete witness stay Unknown.
- Full multi-account subscriptions consume additional account bandwidth; measure
  provider recovery and impact on other venues before acceptance.
- CI can prove compilation/fixtures, not the economics or stability of a
  mainnet run; no mainnet result should be fabricated.

`main` remains untouched until final merge acceptance.
