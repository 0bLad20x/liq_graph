# CP9A – Directed minimum-notional gate (DLMM + PumpSwap)

## Goal and strict scope

Prevent a one-atom directed output (such as `BP4Wr3XnsAWxcWMRRNJSchEs4Pc59QuoYZ8gFYc1ffs2`, X -> USDC, output capacity one USDC atom) from endlessly winning marginal-net route ranking. Do not blacklist pools/tokens, delete topologies, invent a profitability threshold, change V10 sizing or request a new RPC call per signal.

- `GRAPH_MIN_DIRECTION_NOTIONAL_USD=1` (default), any finite value 0..1,000,000; `0` disables the threshold.
- `GRAPH_DIRECTION_LIQUIDITY_MODE=filter` (default), or `shadow` / `off`.
- **Fixed reference for this experiment:** USDC 1 USD, WSOL 100 USD. One dollar is 1,000,000 USDC atoms or 10,000,000 lamports. No price oracle.
- Two independent directions per pool. Unknown data **passes** even in filter mode.
- Does **not** prove a profitable arbitrage or any particular V10 trade size.

## Evaluation

Runs on each existing MarketState `PoolPriceObservation` as it enters the directed signal index. It does not scan complete graph routes or issue new HTTP/RPC requests. Existing DLMM and PumpSwap profiles are reused.

- **Output anchor** (X -> USDC or X -> WSOL): compare the protocol's known spendable `output_capacity_raw` with the required native amount. If insufficient, mark `dust` **only** for the exhaustively observed DLMM bitmap or known Pump reserve curve. For an incomplete DLMM execution window, mark `unknown`.
- **Input anchor** (USDC/WSOL -> X): for DLMM compare `input_capacity_raw` (complete cost of the known direction's levels). For Pump, check that the reference-sized anchor input yields a positive raw token output using the existing reserve-curve and fee data; a failed/zero diagnostic quote is **unknown**, not proof of insufficient capacity.
- **Neither anchor known**: mark `unknown`. No conversion of arbitrary token valuation via a possibly fictitious pool spot.
- Any missing profile, unsupported window, token-cost caveat or overflow must not generate a proved `dust` from a guess.

The observation's direction labels are mapped to canonical directed signal slots. Filtering removes `dust` weights, **not** pool watches or graph topology. This allows dust -> eligible on the next observed BinArray/vault update (or fee/quote state change) to enter the index. `shadow` records the same verdicts but preserves all weights.

The contract `SignalPoolPrice.direction_liquidity_status` records `eligible/dust/unknown`. Each packed `PoolEvidence` records the status of its own directed slot, so unknown V10 dispatches remain identifiable. `signal_production.json.direction_gate` contains mode, threshold, verdict populations, dust/unknown with nonzero marginal rates, filtered direction observations and bounded samples.

The gate is a cheap **minimum available volume** test, not a guarantee of 1 USD net output after all swap fees, a full route quote, a worst-case economic upper bound, or a proof that the route's profit exceeds execution/network costs.

## One-minute Windows live acceptance (no TX broadcasts)

On the Windows checkout after updating the branch, with configured `.env` and V10 simulation wallet:

```powershell
Set-Location C:\dev_projects\liq_graph
git status --short
git fetch origin
git switch cp9-economic-signal-classification
git pull --ff-only origin cp9-economic-signal-classification
cargo test --locked
if ($LASTEXITCODE -ne 0) { throw 'Tests failed; do not run live.' }

.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp9-economic-signal-classification `
    -EnabledVenues 'meteora_dlmm,pumpswap' `
    -Stage Signals -DurationSeconds 60 `
    -PoolLimit 0 -AccountBudget 25000 -MinPoolTvlUsd 500 `
    -MinDirectionNotionalUsd 1 -DirectionLiquidityMode Filter `
    -ProfileSignals -ExecutionDetailLimit 16 `
    -SimulateV10 -SimulationLimit 0 `
    -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
```

The script writes `checkout.json` including the exact gate mode, threshold and fixed 100 USD/SOL assumption. Analyze `signal_production.json.direction_gate`, `execution.json`, V10 outcomes and runtime integrity. For an apples-to-apples short diagnostic, rerun with `-DirectionLiquidityMode Shadow`; do not attribute all differences to the gate across different live time windows. Keep the longer run separate and only after reviewing the 60-second result.

Important: a 60-second run can spend most of its time hydrating accounts. Zero dispatched requests or no actual liquidity updates is inconclusive, not a passed economic acceptance. Execution of these commands needs the user-configured Windows RPC, Postgres and wallet; GitHub CI cannot perform a real mainnet simulation.

## Acceptance invariants

1. X -> USDC capacity of one USDC atom is `dust` at 1 USD.
2. A fully known 1 USD output or one USD input capacity is `eligible`.
3. For WSOL, 10,000,000 lamports are required at the fixed reference price.
4. Incomplete DLMM window and non-anchor pair stay `unknown`.
5. Unknown never loses marginal signal eligibility.
6. A liquidity-only update can change a slot's effective weight from None to Some and trigger candidate reconciliation; no permanent blacklist.
7. The same directed net rate and V10 request parameters are preserved whenever eligible; no new scoring function.
