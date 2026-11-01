# CP14 — Orca Whirlpool Direct2: research, boundary contract and implementation gates

Status: **PLAN ONLY**. No Orca signal or executable code has been implemented on this branch yet.
Base: liq_graph `main` merge `23fba2e1d0a631ccd79aa47d39c7d342e31f6009`.
Official source baseline: orca-so/whirlpools `cc683ec92acb5e7606caa3d8e514d9383e9c7c11` (2026-11-01).
Scope: add `orca_whirlpool` to existing `meteora_dlmm,pumpswap,raydium_cpmm`, Direct2 only. No changes to Graph or shared signal routing policy. V10 simulation only, no broadcasts.

## Goal and non-goals

**Goal:** for each Prepared Orca Whirlpool publish the same *economic* directional semantics as CP13: validated mint/pool identity, actual fee-adjusted marginal quote in both directions, usable reference-size liquidity status, revision/slot/time provenance, account-set/manifest binding. The shared Direct2 engine compares these without knowing about ticks, adaptive fee or TickArray layouts. Preserve source signal vs offered pool set vs actually chosen V10 route. Attach frozen Orca model evidence and quote **only at the input quantity actually selected by V10** in post-selection calibration.

**Not goals:** full pre-V10 trade size optimization, global Orca depth crawler, new independent graph/signal engine, any new profitable-only route cutoff, mass snapshots in RAM, masking RPC or program failures. V10 retains amount optimization and profitability enforcement.

## Verified primary-source behavior

* `Whirlpool` (653 bytes incl. 8-byte Anchor discriminator) stores `tick_spacing`, `fee_rate` (u16, hundredths of bp = 1e-6 fraction), `protocol_fee_rate` (share of trade fee, not an extra trader fee), `liquidity` (u128), `sqrt_price` (u128, Q64.64), `tick_current_index` (i32), mints and vaults. Relevant observed offsets relative to full account: `fee_rate=45`, `protocol_fee_rate=47`, `liquidity=49`, `sqrt_price=65`, `tick_current_index=81`. Validate against upstream struct in tests; do not trust a 16-byte sqrt-only slice for liquidity/fee events.
* The price is `(sqrt_price / 2^64)^2` (mint B per mint A) with mint decimal conversion. Swap A->B moves sqrt price DOWN, B->A UP. The instantaneous net fraction for the known static fee is `1 - fee_rate/1_000_000` for each direction, but only after verifying direction availability. Fee application to individual amounts uses Orca integer rounding.
* Adaptive-fee Whirlpools have a valid program-owned Oracle PDA. The Oracle contains `trade_enable_timestamp`, `AdaptiveFeeConstants` and `AdaptiveFeeVariables`. Before a swap the fee manager updates its reference based on chain timestamp and tick group; during a swap its volatility accumulator / fee can change with tick traversal. `FeeRateManager` caps total swap fee at `100_000/1_000_000` (10%). A static `whirlpool.fee_rate` quote must **not** be used as the claimed adaptive net rate.
* An uninitialized Oracle (system-owned, empty) is allowed by Orca and indicates the static fee path. Initialized Oracle states require owner, PDA, discriminator, length and parent-pool identity verification; trading may be time-gated.
* `swap` v1 declares Oracle read-only in its normal account list, but adaptive-fee swaps must pass it as writable (including via remaining accounts). `swap_v2` declares it mutable. Our current `make_manifest()` always uses `ro(oracle)`: audit/fix the live V10 contract and mutable account flags before accepting adaptive pools.
* Fixed and Dynamic TickArrays coexist; each covers 88 ticks at the pool tick spacing. Fixed TickArray has 88 × 113-byte ticks and is a fixed-size account. Dynamic TickArray has a 128-bit initialization bitmap and variable per-tick encoding (uninitialized tag 0; initialized tag 1 + 112-byte data). Mint order and negative tick positions must be handled explicitly.
* `swap_v2` supports Token-2022 transfer fees and optional transfer-hook supplemental accounts. These must be supported by our actual V10 account contract and MintProof before admitting them as execution-ready; do not assume the legacy v1 interface covers all tokens. Transfer fees are amount-specific and are separate from the Orca swap fee.
* The official standalone `orca_whirlpools_core` Rust library exposes `swap_quote_by_input_token()` and `compute_swap()` with Whirlpool, optional Oracle, TickArrays, timestamp and optional token transfer fees. Prefer using it as an **independent frozen-quote test oracle** (pin source/version); decide on runtime dependency only after measuring overhead and avoiding model proliferation.

Official code:
- https://github.com/orca-so/whirlpools/blob/main/programs/whirlpool/src/state/whirlpool.rs
- https://github.com/orca-so/whirlpools/blob/main/programs/whirlpool/src/state/oracle.rs
- https://github.com/orca-so/whirlpools/blob/main/programs/whirlpool/src/state/fixed_tick_array.rs
- https://github.com/orca-so/whirlpools/blob/main/programs/whirlpool/src/state/dynamic_tick_array.rs
- https://github.com/orca-so/whirlpools/blob/main/programs/whirlpool/src/manager/fee_rate_manager.rs
- https://github.com/orca-so/whirlpools/blob/main/programs/whirlpool/src/manager/swap_manager.rs
- https://github.com/orca-so/whirlpools/blob/main/programs/whirlpool/src/instructions/swap.rs
- https://github.com/orca-so/whirlpools/blob/main/programs/whirlpool/src/instructions/v2/swap.rs
- https://github.com/orca-so/whirlpools/blob/main/rust-sdk/core/src/quote/swap.rs

## Current repository boundary findings

| Area | Current main | CP14 action |
|---|---|---|
| `src/workers/discovery/orca_whirlpool.rs` | Orca discovery already exists | Keep worker, measure relevant overlap |
| `src/workers/completion/orca_whirlpool.rs` | Owner/mint/vault/Oracle validation and fixed/dynamic arrays; `base_a` (B->A) and `base_b` (A->B) manifests; both initial arrays required; oracle currently read-only | Audit one-direction-only exclusion, time gating, current account order and adaptive writable Oracle. Diff against V10 compiler requirements |
| `src/workers/market_state.rs` | Orca in hydrated path; `State::OrcaWhirlpool{sqrt}`, `Field::OrcaWhirlpoolSqrt` only | Protocol-specific state, WS subscriptions, stable digest, net valuation, directional coverage |
| `PoolInput::of` | Orca `manifest_digest=[0;32]` | Bind observed Orca profile to actual Prepared account-set digest |
| `watch_cost()` | Orca uses fallback 1 subscription | Set correct dynamic account budget cost for pool, Oracle and required TickArrays; dedupe shared resources |
| `src/workers/graph_signals/direction_gate.rs` | Current generic classifier uses `DirectedLiquidity`; `Dust` excluded, `Unknown` retained | Do NOT introduce Orca-specific signal rules. Supply conservative capacities only |
| `src/workers/execution_candidates/amounts.rs` | Exact DLMM/Pump evaluator, CPMM signal-only model returns `Unknown` | Orca signal-only model must not become an unsound amount-based `Excluded` |
| `src/workers/execution_candidates.rs` | Frozen CPMM amount quote during post-V10 calibration | Add Orca post-selection quote variant only; preserve prediction-versus-observed metadata |
| `src/workers/execution_v10.rs` | V10 manifest/compiler and simulation reporting | Audit Orca fixed/dynamic array, writable oracle, swap v1/v2, mint extension, runtime-only compatibility |
| `scripts/runtime_live.ps1` | Stable single runtime, full 600-s simulation, diagnostics | Reuse unchanged runner with four venues, create CP14 wrapper |

## Staged implementation and stop/go gates

### CP14A — Reference audit, market universe and V10 readiness

1. Pin Orca upstream code/SDK and collect on-chain fixtures: static Oracle-absent, adaptive Oracle-present, fixed and dynamic TickArrays, positive/negative ticks, mint SPL and Token-2022 cases.
2. From existing Registry/DB, **measure** Orca Prepared/eligible, qualifying `TOKEN/WSOL` or `TOKEN/USDC`, shared pairs with DLMM/Pump/CPMM, incremental Direct2 pair and option counts; do not infer from DEX-wide volume.
3. Verify the current Prepared gate requiring both directions. Quantify one-way-viable pools dropped by that gate; if meaningful, implement direction-aware admission via the existing manifest policy without polluting the graph with protocol-specific conditions.
4. Prove V10 account meta order, mutable Oracle, `swap` vs `swap_v2`, token-program/mint/memo and supplemental accounts, plus mixed TickArray kinds. Fix only confirmed misalignments.
**Gate:** offline fixed fixtures and V10 compiler/regression tests pass; no unsupported pool silently marked Prepared.

### CP14B — Orca typed state and WS lifecycle

Add `src/workers/market_state/orca.rs` (protocol parser and fee/tick math) and small explicit `market_state.rs` wiring.

- Decode Whirlpool identity and fields `fee_rate, protocol_fee_rate, liquidity, sqrt_price, tick_current_index, tick_spacing` with validated lengths and offsets. Read fee/liquidity/price/tick together (possible bounded slice, not only sqrt).
- Decode/subscribe validated Oracle when initialized. Observe chain timestamp with a justified Clock mechanism; detect trade-enable, adaptive-fee changes **even without sqrt movement**. Do not reuse stale Oracle values as current.
- Subscribe minimum necessary TickArrays for the current executable directional windows; parse both fixed/dynamic, correct tick ordering, bitmap and negative index floor arithmetic. Reconcile/re-watch when an array boundary or manifest changes; do not hold the entire Orca universe in a worker-specific cache.
- Deduplicate shared accounts; account selection/budget reports must count all subscriptions before the WS plan. Use existing baseline plus RPC rewatch. No worker→worker calls.
**Gate:** deterministic restart, WS baseline and rewatch tests; pool/Oracle/TickArray changes produce exactly relevant profile revisions and no phantom signals.

### CP14C — Common directed signal and minimum-liquidity semantics

Orca adapter produces standard `FeeValuation`, `PoolLiquidity`, `DirectedLiquidity[2]`.

- Compute marginal bid/ask from Q64.64 price, mint decimals and **current** static/adaptive swap fee. Protocol fee share is not double charged.
- For each direction, prove whether the configured reference-size anchor trade can execute within observed available active liquidity and next validated boundaries. In narrow/empty active ranges inspect the minimum next reachable initialized ticks as required; do not call an unknown tail empty or `Dust`.
- Set `Eligible` only when economic reference amount is supported; mark `Dust` only on a **complete, verified** insufficient-liquidity proof; otherwise `Unknown`. The generic classifier remains untouched. Marginal price is not a fixed-input quote and must not imply feasible large size.
- Bound computation by a small, explicit execution window, no full TickArray scan on every pool update unless mathematically necessary; if adaptive-fee change with time is not cheaply resolvable, `Unknown` instead of fake static rate.
- Publication must carry slot min/max, receive timestamps, content/account-set digest, each direction's reason, first reachable tick, and an adaptive fee basis. Keep a fixed-size count/latency histogram, not unbounded JSON.
**Gate:** targeted tests show static/adaptive direction correctness, reciprocal mint normalization, zero liquidity, tick boundaries, no false `Excluded`, no graph/signal-specific Orca branching.

### CP14D — V10 execution and frozen output calibration

- Use the already-built common `Selection` / `ExecutionReport` calibration pipeline.
- Freeze a compact Orca-specific quote snapshot containing Whirlpool state, observed Oracle/time, ordered initialized tick data needed for the actual path, mint-extension status, account-set and content digest. Make unknown-range/insufficient tick coverage explicit. Avoid full account payloads in every log event.
- After V10 chooses the amount, compute Orca exact-input expected output with correct tick crossing, adaptive fee changes and Orca integer rounding (using pinned official core math as reference). Never use this model in the pre-V10 exclusion path by default.
- Compare model vs V10 raw output, direction, snapshot age, slot difference and chosen pools. If the actual transaction crosses outside frozen coverage, report `Unknown`, never manufacture a quote.
- Include partial/reverted and zero-profit attempts as separate categories, not realized gains.
**Gate:** fixtures match official reference math; every observed complete supported Orca leg has model output or precise, demonstrable unknown reason; no silent missing values or wrong fee units.

### CP14E — Integration and 600-second test

1. Locked CI: `cargo build --locked --release`, existing CP10–CP13 regression suites and new `cp14_` tests.
2. Short local smoke test only as a gate: prepared >0, selected/priced Orca >0, oracle status, active liquidity and WS updates and some signal delivery.
3. Full 600-second run with:
   - `GRAPH_ENABLED_VENUES=meteora_dlmm,pumpswap,raydium_cpmm,orca_whirlpool`
   - `GRAPH_ENABLED_ROUTES=direct2`, `Stage=Signals`, `SimulateV10=true`, `DurationSeconds=600`
   - `PoolLimit=0`, `AccountBudget=25000`, `MinPoolTvlUsd=500`, `MinDirectionNotionalUsd=50`, `DirectionLiquidityMode=Filter`, `ThresholdBps=0`, `V10Parallelism=32`, `CP9CPackingMode=AllCoverage`, `CP9DMaxPairs=3`, wallet locally configured.
4. Compare against CP13: prepared/admitted, selected/priced, **incremental Direct2 pairs and options**, eligible/unknown/dust by venue/direction, V10 actual Orca leg/pool count, chosen pool versus signal witness, model vs V10 output error (bps), failures by failure class, event loss, P50/P95/P99 frame→request and per-account processing cost, WS subscription/account budget.
5. Reject automatic claims of financial profitability merely from `SimulationSucceeded`. Require net wallet/settlement + network fees for separate economic proof.
**Gate:** runtime 600s complete, all existing delivery/simulation invariants pass, 0 lost events, actual priced Orca and no unsupported fee/Oracle used as valid signal. Complete Orca V10 swap/quote coverage needed for full execution/calibration acceptance; if no Orca swap is selected during the observation, mark that sub-gate **incomplete**, not success.

## Essential tests (named groups)

- `cp14_pool_layout`: mint identity, 653 bytes, price Q64.64/decimals, liquidity, fee/tick offsets, invalid static/config.
- `cp14_tick_arrays`: fixed/dynamic, negative tick, last tick/index boundary, both directions, sparse initialized ticks, unsupported/missing next array => Unknown.
- `cp14_oracle_fee`: no Oracle static, initialized adaptive, timestamp advance without pool update, reference decay, volatility cap and fee scaling, trade enable timestamp, Oracle ownership and writable flag.
- `cp14_liquidity_gate`: exact reference anchor positive/insufficient, counter-direction, incomplete observed window never Dust, no missed valid route.
- `cp14_v10_accounts`: account order, owner/memo/Oracle writable, v1/v2 compatibility, Token-2022 accepted/rejected with proof.
- `cp14_calibration`: frozen tick execution amount quote versus official reference and constructed simulation traces, overflow/rounding; same-slot-not-same-bank caveat.
- `cp14_regression`: existing DLMM/PumpSwap/CPMM frozen CP10–CP13 tests unchanged.

## Definition of done

Code only in CP14 branch until CI and full 600-second live evidence. Do not merge if the active price model uses only pool sqrt price but omits adaptive Oracle/active liquidity, if V10 adaptive Oracle is not writable, if a time/array update can leave stale eligibilities, if valid one-way pools are silently dropped, if Orca requires special branching in the Graph/SignalEngine, or if unsupported amount-model errors cause false candidate exclusion. The calibration and subsequent timing comparison must remain auditable without buffering complete streams in memory.

Only after CP14 closure should CP15 Raydium CLMM proceed; any reusable CLMM math may be extracted **after** observing the Orca/CLMM requirements rather than imposing a speculative shared model.
