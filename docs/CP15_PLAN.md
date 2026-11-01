# CP15 — Raydium CLMM Direct2: implementation contract and acceptance

Status: **PLAN ONLY**. The base is main after merged CP14, commit \`75b0093d19fa47aaebf533aa459569b0fab716f7\`. No CLMM runtime changes should be made without the validation gates below. Target branch: \`cp15-raydium-clmm-direct2\`.

Official primary-source pin: \`raydium-io/raydium-clmm\` commit \`6d7a0f82d2f293c553c69aaf943377367c6c4cc6\`. **Verify source vs deployed on-chain execution/V10 observations before assuming a branch or optional feature is active.** Relevant files in this pinned upstream:
- \`programs/amm/src/states/{pool.rs,config.rs,tick_array.rs,tickarray_bitmap_extension.rs}\`
- \`programs/amm/src/instructions/swap.rs\`
- \`programs/amm/src/libraries/{swap_math.rs,sqrt_price_math.rs,liquidity_math.rs,tick_math.rs}\`
- \`programs/amm/src/states/dynamic_fee.rs\` (resolve actual module/name from upstream tree before implementation).
- Reference: https://github.com/raydium-io/raydium-clmm/tree/6d7a0f82d2f293c553c69aaf943377367c6c4cc6

## 1. Objective and invariants

Complete the protocol path **Discovery -> PoolRegistry -> Completion/Prepared V10 manifests -> RAM MarketState -> unchanged shared Graph/Direct2 Signal Engine -> Candidate Preparation -> V10 simulation -> measured calibration** for \`raydium_clmm\`, alongside \`meteora_dlmm,pumpswap,raydium_cpmm,orca_whirlpool\`.

Same economic semantics as CP13/CP14: directional **marginal DEX-net rate** (no gross fallback), verified per-direction execution account manifest, bounded conservative direction liquidity for the existing $50 notional gate, source slot/time/manifest provenance, frozen amount quote **at actual V10-selected gross input**. V10 owns trade sizing and profitability decisions. This CP is NOT authorization for broadcasting.

Do not introduce:
- a second graph, candidate selector, RPC/WS client, protocol-specific signal heuristics, parallel Worker->Worker wiring, unbounded snapshots or per-event RAM queues;
- exclusion of routes on a speculative CLMM full-size quote, arbitrary gross price thresholds, new unrelated DEX implementations;
- duplicate reference model in the hot path; reference tests/diagnostics may operate off-hot-path.

Keep existing runtime mode \`Stage=Signals\`, \`Direct2\`, \`threshold_bps=0\`, \`gross_fallback=false\`, fixed SOL=100 USD, minimum $50 direction notional, pool TVL >=$500 (PumpSwap exempt), unknown/exclusions as currently configured; four existing venues remain a control cohort.

## 2. Existing code and exact gap

| Path | Present | CP15 responsibility |
|---|---|---|
| \`src/workers/discovery/raydium.rs\` | REST Raydium venue discovery | Verify pool IDs, order, config/TVL relevance; no redesign |
| \`src/workers/completion/raydium_clmm.rs\` | Pool/config/observation/bitmap/60-tick-array owner/PDA/full-read checks; 1544-byte pool, 117-byte config, 10240-byte TickArray, 1832-byte extension; V10 base_a/base_b manifests; both directions currently required | Accurate first **initialized** array in each direction using bitmap/extension and actual V10 order, one-sided admission, current-source fee/feature validation, light/full refresh equivalence, coherent context |
| \`src/workers/market_state.rs\` | \`State::RaydiumClmm{sqrt}\`, 16-byte price slice, no CLMM \`FeeValuation\` or \`PoolLiquidity\` | Full relevant pool/config and tick snapshot, directed net fees, conservative capacity, account-set watches/reconciliation, freeze semantics and lifetime |
| \`src/workers/market_state/raydium_clmm.rs\` | New protocol-specific module | Typed decoders, validated fee regimes, CLMM capacity math and diagnostic quote; no connection ownership |
| \`src/core/market_state.rs\` | Typed common observation, fee and liquidity contract | Minimal optional typed CLMM frozen diagnostic fields; preserve existing DEX semantics |
| \`src/workers/execution_candidates/amounts.rs\` | CPMM/Orca light profile => Unknown for pre-V10 proof | Same safe \`Unknown\` for CLMM until exact execution-window proof exists; no false insufficient/dust |
| \`src/workers/execution_candidates.rs\` | Post-V10 frozen Orca/CPMM comparison | Diagnostic CLMM quote at chosen V10 amount, snapshot/manifest/fee proof; reason on unsupported/out-of-window |
| \`src/workers/execution_v10.rs\` | Common manifest/compiler path | Reject missing first direction TickArray/invalid CLMM account count, order, owner/roles and required writable bits; regression |
| \`src/workers/graph_signals/*\` | Common Direct2 and notional gate | **No protocol-specific rules**; only protocol-independent fix if failing invariant proven |
| \`scripts/cp15_clmm_live.ps1\`, \`scripts/cp15_clmm_calibration.py\`, \`scripts/test_cp15_clmm_reference.py\` | New acceptance harness | 60/300/600 second runs, bounded streamed details, independent integer-reference and V10 comparison, final JSON |
| \`.github/workflows/locked-build.yml\`, \`docs/CP15_IMPLEMENTATION.md\` | Locked checks and evidence | Targeted regression + reference, actual run facts and limitations |

## 3. Upstream protocol behaviors to prove before coding

### 3.1 Pool / Config
- Raydium CLMM price is Q64.64 \`sqrt_price_x64\` for native token1/token0; decimal scaling and canonical base/quote inversion happen **exactly once**. Tick index can be negative. TickArrayState has **60** ticks; verify signed index, start/end, bitmap/default/extension logic and initialized tick count against pinned source.
- \`AmmConfig.trade_fee_rate\`, \`protocol_fee_rate\`, \`fund_fee_rate\` are scaled by 1,000,000. Protocol and fund are portions of trade fee, **not additional trader charges**. Verify config PDA/index, owner, length, spacing, rate bounds, and update slots. Do NOT use a fixed fee tier inferred from REST.
- Pool \`fee_on\`: 0=FromInput; 1=Token0Only; 2=Token1Only. Derive the **effective fee side separately for each trading direction**. Fee-on-output and fee-on-input have different exact integer steps even if simple marginal continuous-rate factor can coincide.
- Pool \`dynamic_fee_info\` is not the separate Oracle account used by Orca. Its timestamp-sensitive fee state comes from the pool, on-chain Clock and \`swap.rs\`; verify feature-enabled encoding, reference/decay/volatility, hard cap and tick-spacing group transitions.
- TickStates may contain limit-order inventory at boundaries. Their effects on a size quote cannot silently be approximated by pure constant-liquidity mathematics.

### 3.2 Account-set / execution
- Current Completion checks tick arrays in **both** directions, and assumes a local radius. Compare \`PoolState::first_tick_array_index_with_extension_info\` and \`next_tick_array_index_with_extension_info\` with existing \`array_initialized\`, including sparse arrays, negative indices, current tick at boundary, and extension pages. A first initialized array may be beyond the current center. A missing array in one direction must not kill the other.
- V10 account order and flags: correct native mint orientation, config, observation writable, bitmap extension, vaults, Token-2022/Memo conditions and ordered tick accounts. Confirm with V10 actual compiler and test cases rather than only assuming on-chain instruction ordering.
- Pool open/disabled flags and Token-2022 fee/transfer hooks must be decoded. Upstream marks \`open_time\` currently disabled; **do not silently equate wall-clock on the host with on-chain executable time**. Check deployed behavior and use explicit unknown/unsupported for ambiguity.
- Explicitly classify unsupported source / fee / limit-order cases. A pool may be structurally Prepared but an **unsupported direction must never publish a positive net rate or compile to executable V10**.

## 4. Implementation sequence and gates

### Gate A: Base inventory and protocol fixture contract (no signal change)
1. Verify branch is descendant of CP14 main. Compare current Completion layouts/offsets against pinned upstream and actual on-chain sample accounts from relevant Prepared pool IDs. Record ABI contract and source SHA.
2. Gather pool samples covering classic fee-on-input, fee-on-output, dynamic fee enabled, static fee, negative/current boundary tick, sparse/default+extension bitmap, Token-2022, possible limit orders, single-sided or empty directions.
3. For each sample output a **bounded** layout/feature classification, not full raw records in RAM. Record whether actual V10 supports that combination. Unknown source-feature combinations fail closed.
Acceptance: deterministic golden data/fixtures and exact owner/PDA/binary offset tests; old CP10-CP14 pass.

### Gate B: Completion/V10 directional readiness
1. Normalize first executable tick arrays from on-chain bitmap/extension using correct traversal. Validate account ownership/start index/discriminator, count and pool identity, and all required V10 block roles.
2. Admit if >=1 direction has a valid execution path; save readiness flags in versioned CompletionEvidence, preserve unsupported direction as explicit empty array block (no invented account).
3. Full and light-refresh paths must calculate **identical** direction flags, start indices and manifest digest; on layout/state changes reprepare/rebuild; test neither/one/both.
4. V10 compiler guard rejects invalid/missing arrays on chosen direction regardless of upstream selection.
Acceptance: A-only, B-only, both, neither, negative/boundary, missing initialized arrays, bitmap extension and stale/reordered/account-writable faults all tested; none can sneak through.

### Gate C: MarketState full decoding and subscriptions
1. Replace CLMM 16-byte sqrt-only watch with the validated full pool state. Observe validated config plus required 60-tick arrays and bitmap extension per Prepared manifest; observation only where needed for V10/quote-state validity, not duplicated without rationale. Deduplicate subscription accounts, cost all watches in existing WS budget.
2. Use existing Engine hydration/install/rewatch; map \`Field::RaydiumClmm...\` updates to native typed pool/config/tick/clock changes, recompute only on material changes, maintain content digest and manifest account-set digest. A new manifest/tick window invalidates the old frozen execution profile.
3. Static fee regime should not unnecessarily subscribe to Clock. Dynamic regime must read positive chain timestamp; at least \`clock_slot >= max(pool_slot, config_slot)\` for time-derived marginal fee (slot ordering is only a guard, **not an atomic-bank proof**). Reevaluate on Clock changes. No synthetic host timestamp.
4. Directional marginal net rate and fee side, canonical orientation, \`FeeValuation\` and \`PoolLiquidity\` populated; missing/invalid/unsupported/clock-lag => \`None\`/Unknown, **never gross fallback or fee=0**.
5. Directed minimum-notional capacity is bounded by the next initialized liquidity/fee-regime/limit-order boundary **actually covered** by accounts, with conservative integer rounding and evidence. A small incomplete window is \`Unknown\`, never \`Dust\` unless fully exhausted bitmap/known execution window proves absence. Do not claim arbitrary multitik depth.
Acceptance: positive direction net rates with matched frozen manifests, no invalid-side net rate, no false Dust, correct dynamic time withheld/recovery, matched reload/re-watch, no duplicate subscriptions or unrelated Signal Engine changes.

### Gate D: Amount diagnostic and independent exact reference
1. Keep V10 sizing authoritative. Make pre-V10 amount evaluator explicitly \`Unknown("amount_model_not_implemented")\` for lightweight CLMM profile so it never vetoes an economically viable route.
2. Build \`RaydiumClmmQuoteSnapshot\` containing only the frozen fields required for replay: native mints/decimals, sqrt, liquidity, tick/spacings, fee source/fee_on, dynamic parameters and chain time where applicable, bounded ordered initialized tick/limit-order witness, covered accounts, slot/manifest digest and transfer-fee status. Use only when selected V10 input and data are trusted; if unavailable emit a precise reason.
3. Implement a *separate* exact integer **single-active-interval** quote test oracle using the pinned \`swap_math::compute_swap\` semantics (including fee-on-output). Cases crossing initialized ticks, fee-group boundaries, limit-order matching or Token-2022 transfer fees return explicit Unknown unless proven exact. Avoid presenting a float estimate as exact.
4. Diagnostics join **request ID + payload digest + simulation context slot**, not payload hash alone (CP14 bug). Compare frozen model vs independent integer reference vs actual V10 raw per-leg amounts. Report gross input, observed output, fee-on side, per-direction provenance, raw diff and bps; repeated payloads across slots must never cross-match.
Acceptance: exact reference fixtures for both directions, small roundings, static fee-on-input, fee-on-output, zero/dust/overflow, dynamic fee if supported, no-tick/tick-cross refusal, negative boundaries, stale/wrong manifests, duplicated payload joins. All reference-eligible cases must be byte/atom exact with frozen integer oracle; live V10 differences must be explicitly classified (bank/time changes vs math) and never masked.

### Gate E: Reproducible production-like simulation and comparison
1. Add \`scripts/cp15_clmm_live.ps1 -DurationSeconds 60|300|600\` mirroring CP14 wrapper. Enable \`meteora_dlmm,pumpswap,raydium_cpmm,orca_whirlpool,raydium_clmm\`; only \`direct2\`, no broadcast, fixed $50 minimum reference amount, $500 TVL floor, \`marginal_dex_net\`, AllCoverage, \`cp9d_max_pairs=3\`, 32 in-flight and unchanged engine/account budgets unless sourced evidence justifies change.
2. Run unit/integration test suite, 60s structural smoke, 300s price/fee/signal/V10 confirmation, 600s final operator run. Avoid unintentional transport setting changes. If WS gaps recur, record provider and exact affected scope; do not confuse with protocol-math regression.
3. Generate \`cp15_clmm_completion.json\` (both/A-only/B-only/neither, reasons, prepared/admitted/selected/priced, unique pools), \`cp15_clmm_calibration.json\`, \`cp15_clmm_reference.json\` plus standard runtime \`summary.json\`, \`market_state.json\`, \`execution*.jsonl\`.
4. Compare against **same-window four-venue control** or time-normalized CP14 sample with market/provider caveats. Report incremental Raydium CLMM pool/pair/Direct2 route opportunity counts, candidate chosen vs offered, and V10 actual CLMM invocation/output; measure potential displaced venue coverage due to WS budget.
5. Record p50/p95/p99 frame->V10, market decode/compute, preparation, RPC runtime/429 and reconnects, endpoint gaps and recovery, no receiver queues.

## 5. Hard merge acceptance gates

Required, not aspirational:
- Locked CI and all CP10-CP15 regressions green on exact final SHA; no new Rust warnings from CP15 code, no silent suppression.
- At least one unique CLMM pool \`Prepared + selected + priced\`; at least one *CLMM-containing* Direct2 candidate reaches V10, with complete CLMM leg(s) and calibration, not merely account hydration. The run must identify distinct leg count, direction and amount; if market opportunities absent, evidence is **incomplete** rather than failure disguised as success.
- Only trusted supported regimes may generate positive CLMM marginal DEX-net signal; unsupported fee-on/dynamic/limit-order/Token-2022 behaviors have explicit \`Unknown\` reasons; no wrong-direction compiler acceptance, no invalid first TickArray, no unexplained CLMM-specific V10 account failures.
- 100% of **reference-eligible same-frozen-state integer cases** exact at raw-atom level; outside coverage is counted as Unknown. Any nonzero observed V10 deviation must be inspectable at request/slot/state level, not silently declared identical.
- \`lost_market_events=0\`, \`lost_signal_events=0\` and complete streaming evidence; WS gaps may occur due to provider but must have explicit observed recovery/account-coverage evidence. If coverage is not restored, separate provider issue must be documented and acceptance of price-quality affected by it must be honest.
- No unbounded diagnostics/memory queues, no duplicate structure paths, no throughput collapse. Baseline p95 frame->V10 was ~8.47 ms on CP14 600s; compare like-for-like and investigate significant regressions (do not require wall-clock exact same under varying network conditions).
- V10 \`Custom(1)\` is an expected economic abort class; evaluate other errors separately. Successful simulation is NOT proof of realized profitability; broadcasts stay **false**.
- \`main\` untouched until operator uploads the final 600-second artifacts and accepts the merge. Keep PR draft until review.

## 6. Implementation deliverables and done definition

Files: \`docs/CP15_PLAN.md\` (this contract), \`docs/CP15_IMPLEMENTATION.md\`, dedicated CLMM decoder/math module, targeted existing MarketState/Completion/Core/Candidate/V10 changes, locked CI, CP15 wrapper and reference/regressions. All commits on \`cp15-raydium-clmm-direct2\`.

Done means Raydium CLMM contributes **truthful, fee-net, directionally executable prices** to the existing Direct2 stream, V10 can simulate CLMM-containing routes and output complete trace evidence, independently reproducible integer cases match the pinned math, and unsupported cases are explicitly Unknown. None of this claims profitable net broadcasts or exact arbitrary multitik/limit-order routing without supporting tests.

## 7. Review checklist

Before PR merge answer with evidence: How many discovered/admitted/prepared CLMM pools? How many two-way/one-way? How many selected/priced? What are fee-on modes, dynamic fee coverage and Unknown reasons? Did observed CLMM paths win candidate selection and reach V10? What is the output difference at the actual input? Are wrong-direction, tick crossings, and missing account cases rejected? Were zero market/signal events lost? Did WS account budget displace other venues? Are end-to-end latency and provider recovery acceptable? Are all baseline four venues still healthy?
