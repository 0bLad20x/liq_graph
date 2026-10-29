# CP8B: directed DLMM liquidity in the existing pipeline

Base: merged main `b7327a44f197fbd8bd7323957fedbe8394d9676a` (CP8A.2).
Scope: DLMM and PumpSwap, one MarketState, one directed net index, one V10
simulation path. PumpSwap math, automatic V10 sizing and all topology kinds stay.
No pool blacklist, minimum trade size, new profitability threshold, opportunity
deduplicator, RPC-on-signal path, worker, crate, runtime or dependency is added.

## Problem and resulting behavior

CP8A.2 treated the active DLMM bin's price as a marginal price in both directions
without observing that bin's output liquidity. A mathematically positive net edge
could therefore describe an empty side or require traversal to another bin at a
different price. The 258 repeated Triangle4 payloads identify a concrete case to
inspect; their historical BinArray bytes are absent. Those failures alone do not
prove the exact historical bin composition or that every repetition had one cause.

MarketState now hydrates and watches the BinArrays declared in each current
Prepared execution manifest, plus its optional bitmap extension. Starting at the
active bin it walks down IDs for native X -> Y (requires Y), and up IDs for Y -> X
(requires X). Empty bins are skipped. The first reachable positive integer output
level supplies that direction's marginal DEX-net rate. The active-bin spot remains
evidence only. Each observed level records raw fill amounts, integer-rounded fees,
minimum positive-output input, marginal rate and full-level effective net rate.
These last two rates can differ materially for tiny, highly quantized balances.

All reachable levels inside the supplied execution window are processed; no top-K
or extra route/calculation cap is introduced. An initialized but unobserved closer
array is a boundary, never skipped to a more distant known array. `available`
means positive output was established before that boundary; `empty` means no such
output in the exhausted observed bitmap; `uncovered` means the boundary prevented
establishing output; `unknown` means invalid or unsupported inputs. Exhaustion is
relative to the observed bitmap, not a promise about unobserved extension accounts
or the pool's future state. `next_required_account` identifies a missing next array.
Unknown directions have no net weight and cannot fabricate a reciprocal price.

This is still a marginal signal, not an amount-specific route quote. A positive
first-bin balance can be economically tiny, and even the corrected marginal edge
can exceed the achievable integer/amount-specific return. CP8B exposes that gap;
it does not invent a minimum economic amount or promise elimination of all 258-like
requests. Full-level fill records are not a multi-bin exact-in simulation. Network
costs, account deposits, transfer-fee effects, wallet profit and host rebates remain
separate. No profitable simulation is promised by the implementation.

## Ownership, continuity and account costs

- Core adds only typed protocol-independent liquidity output and a stable digest
  of the two execution account blocks, pool/mint identity and bound signer.
- MarketState owns the private DLMM layout decoder, raw book, traversal, integer
  math and profile. It derives subscriptions from the typed manifest; it does not
  parse Completion's private evidence JSON or maintain a second Completion state.
- Completion still owns structural execution readiness. A renewed validation slot
  alone preserves MarketState indices/watches. A changed account block replaces
  that pool's structure through the existing installation path. No new DB schema.
- Shared HTTP transport supplies initial and post-ACK/resync baselines; the same
  existing WS transport then supplies full array updates. There is no added RPC
  lookup for each price, candidate or request. Slot floors, invalidation, replay
  and non-atomic component provenance remain mandatory.
- Selection now budgets one DLMM root plus its actual array count and optional
  bitmap, with shared Clock counted once. The existing 1..6 array manifest bound
  is a V10 account contract, not a new route limit. The selected pool count can
  fall when the same account budget must also cover these necessary accounts.
- A material liquidity content digest survives baseline revision restarts. Depth
  changes at an unchanged marginal price update affected signal candidates through
  the existing pair index; unchanged rehydration does not create a false change.
  Baseline/catalog updates still do not dispatch execution. Existing WS/revaluation
  dispatch eligibility, bounded queue and busy-revision skipping remain.
- ExecutionCandidates checks each DLMM profile against the current manifest's
  account digest and required direction, for witnesses and alternatives. It records
  `liquidity_window_checked`; old-window observations cannot be packed with a newer
  account window. The independent executor request validation remains.

`indicative_seed_input_at_first_depth_boundary` converts first-level input capacities
back through preceding marginal rates into settlement-token UI units. It covers
only legs with observed depth, is linearized, and is neither a chosen trade amount
nor a lower/upper bound on profitable route size. `liquidity_profile_legs` states
how many legs supplied those inputs. It never filters or ranks a candidate. V10
continues receiving `trade_size=0` unless the existing parameters specify otherwise.

## Protocol reference and integer rules

Pinned official Meteora SDK commit:
[`af36dd53ab180783ccecda306926189a4700953c`](https://github.com/MeteoraAg/dlmm-sdk/tree/af36dd53ab180783ccecda306926189a4700953c).
Sources: `idls/dlmm.json`, `ts-client/src/dlmm/helpers/{bin,binArray,fee,lbPair}.ts`
and `commons/src/extensions/lb_pair.rs`.

LbPair is 904 bytes; BinArray is 56 + 70 * 144 bytes. Parent identity, owner,
discriminator, array index/PDA and supported version are checked. Negative IDs use
Euclidean division by 70. Internal bitmap covers array indices -512..511; the
optional extension maps positive bits from 512 and negative bits from -513.
The raw bin price is Q64.64 **Y atoms per X atom**. Balances, order amounts and
fee calculations remain token atoms; decimals enter only human-unit rates.
MM balances already exclude protocol fees; those fees are not subtracted again.

For supported limit-order pools, matching-side processed and open orders follow
MM liquidity in the SDK's fill order. Liquidity-mining pools do not add them.
Undetermined pools support orders only with both reward mints zero, as in the SDK.
Input cost uses ceil division/multiplication per liquidity component; no floating
point enters raw fill arithmetic. Input fees gross up with ceil(excluded * 1e9 /
(1e9-fee)); output fees use ceil(output * fee / 1e9). Total fee stays capped at 10%.
Volatility reference reset uses actual active ID and chain Clock; each reached bin
then uses its distance from that reference. When LbPair arrives before Clock,
the signed negative time delta retains the existing reference, as in the SDK;
it does not invalidate the quote. A nonrepresentable full u64 input is explicitly absent, never wrapped.

## Ten-minute live proof and analysis

Locked exact-head release build and PowerShell syntax are prerequisites. They
are not functional acceptance. This implementation environment has no Rust,
PowerShell, configured project DB/RPC access or wallet; its actual 600-second
simulation proof must run in the configured Windows checkout. No live run or
improvement in success/profit/latency is claimed before those artifacts exist.

```powershell
cd C:\dev_projects\liq_graph
git fetch origin
if ($LASTEXITCODE -ne 0) { throw 'Fetch fehlgeschlagen.' }
git switch --track origin/cp8b-dlmm-directed-liquidity
if ($LASTEXITCODE -ne 0) { throw 'Branchwechsel fehlgeschlagen.' }
.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp8b-dlmm-directed-liquidity `
    -EnabledVenues 'meteora_dlmm,pumpswap' `
    -Stage Signals -DurationSeconds 600 `
    -PoolLimit 0 -AccountBudget 25000 `
    -ProfileSignals -ThresholdBps 0 `
    -RequireFeeEvidence -RequireLiquidityEvidence `
    -LiquidityEvidencePools '3w6Bk1xj4wAVG7ZPHRgpT5SW77hB2ofs3iYYvE5gezLN' `
    -SimulateV10 -SimulationLimit 0 `
    -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
```

For an existing local branch, use `git switch cp8b-dlmm-directed-liquidity` then
`git pull --ff-only origin cp8b-dlmm-directed-liquidity`, checking both exit codes.
The diagnostic focus does not pin or filter selection. If the pool is no longer
admitted/selected/hydrated, its missing evidence is explicit; no historical state
is fabricated. This run keeps the previous 25,000 account budget for comparison,
while reporting the new account demand and actual selected/hydrated population.
No broadcasts, artificial graph audit load or forced Rewatch in this measurement.

Existing delivery/fee/net checks remain. `-RequireLiquidityEvidence` additionally
requires real array WS updates, available profiles consumed by Signal Engine,
no unchecked DLMM request windows, and snapshots for explicitly requested focus
pools. `liquidity_evidence_passed` is a structural necessary check, not proof of
all formulas, rare array boundaries or profitable arbitrage. Missing real events
remain unproven rather than being replaced by synthetic tests.

Inspect these existing-run artifacts:

| Artifact | Analysis |
|---|---|
| `liquidity_evidence.json` | Owner counts, profile build latency, status/coverage counts, up to eight observation samples; requested pools' first/latest full LbPair/array/bitmap bytes and component provenance |
| `market_state.json` | Selection/hydration, actual account load, loss/decoder errors, initial/baseline/processing/install timings |
| `signal_production.json` | `liquidity_inputs`, `liquidity_only_updates`, known directions, candidate windows, ordinary/initialization/revaluation timings |
| `selection-NN.json` / `execution.json` | Observed levels and rates for each packed pool, checked manifest window, indicative first-depth boundary, actual request/report pairing |
| `repeated_request_sets.json` | SHA256 of request payload without ID and associated IDs, for recurring failure analysis; equality does not establish unchanged pool state |
| `simulation-NN.json` / `summary.json` | Actual DEX calls, chosen pools, return/failure codes and aggregate timing/delivery results |

Compare the focused pool's two directions and raw bin balances to the pinned SDK,
including empty side, adjacent arrays, negative IDs, fee mode, quantity rounding
and first-reachable versus active price. Check what actually occurred; one run
cannot prove unobserved boundary/mode transitions. Match any recurring failed
payload to its selected liquidity profile and actual simulation, rather than
attributing all Custom1 errors to one presumed cause. Compare latency populations
and account/price coverage as well as simulation counts to the earlier run;
different duration or subscription workload is not an isolated speed benchmark.

Checkpoint completion and merge readiness require the real artifacts to pass review.

Ordinary observation/request JSON includes `levels.count` and `levels.first`, plus
directional totals, coverage and the material content digest. Complete level vectors
stay in the live typed contract and are included in explicitly focused raw snapshots
as `accounts.complete_levels`. This bounds evidence duplication across thousands of
requests; it does not limit traversal, signal calculation or pool alternatives.
