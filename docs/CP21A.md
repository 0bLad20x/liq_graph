# CP21A – Exact Net-Profit Accounting (checkpoint)

Parent branch: `cp20-direct2-economic-predictor` (CP20/CP20R/CP20S).
Scope: corrected ranking economics and complete V10 fee reconciliation.
**Not in scope:** pairwise combinatorial packing (CP21B), fresh/active
scheduling policy redesign (CP21C), new DEXs or changing V10 amount choice.

## Verified contract from CP20R/CP20S evidence

The V10 executor's **already defined** settlement profit share is ten
percent of *positive* gross DEX cycle profit, with integer truncation:

```
gross_dex_gain_raw = route_output_raw - route_input_raw
v10_profit_fee_raw = floor(max(gross_dex_gain_raw, 0) / 10)
post_v10_gain_raw  = gross_dex_gain_raw - v10_profit_fee_raw
predicted_net_raw  = post_v10_gain_raw - network_cost_settlement_raw
```

Fee logic reconciles **2,174 of 2,174** complete successful
simulation cycles in the two archived 300-second runs from 2026-11-05
(`20261105T053606125Z.zip` and `20261105T064131638Z.zip`).
This verifies the expected calculation for those traces, **not**
that every different V10 invocation or configuration must match.
DEX fees already included in the pool quote are never subtracted twice.

The network cost in the forecast remains the **reference** transaction
fee of 5,000 lamports plus the configured absolute priority fee.
For USDC this lamport cost is converted to USDC base units by exact integer
arithmetic and rounded up. In the observed calibration the independent
`simulateTransaction.fee` is used, not the predicted reference fee.
Tips, unmodelled loan costs and other external costs remain **unknown**
unless explicitly verified; refundable rent funding remains capital
lock-up rather than permanent fee. Nonzero `additional_fee_bp` or
an additional fee collector fail closed as Unknown instead of pretending
their charge is zero.

## Cross-asset ranking contract

**Never sort raw WSOL lamports against raw USDC micro-units.**
All fresh, active and packing-alternative `Economic` priority scores use
signed **USD nanounits** (1 unit = 10^-9 USD), at the existing fixed
reference SOL = 100 USD:

- 1 WSOL lamport = 100 USD nanounits
- 1 USDC base unit (10^-6 USDC) = 1,000 USD nanounits

Normalization uses `i128.checked_mul` and returns Unknown on
unsupported settlements/overflow. The original `predicted_net_gain_raw`
stays in settlement atoms for direct V10 comparisons. Its extra field
`predicted_net_gain_usd_nano` is a comparable ranking diagnostic.

The size optimizer still maximizes gross swap profit on the frozen pool
curves, using no fixed trading input amounts. Subtracting a monotonic
10% profit share and a fixed transaction fee preserves its economic
optimum in the continuous approximation. Integer fee rounding can add
ties; neither quote nor optimizer is claimed to be exact V10 execution.
`trade_size=0` is unchanged; V10 chooses its own amount.

## Measurement and source of truth

The compact CP20S journal stays at **schema 7**. Small optional economic
fields are added without altering its column layout:

- `selection.economic.dex_gain_raw`
- `selection.economic.v10_profit_fee_raw`
- `selection.economic.post_v10_gain_raw`
- `selection.economic.reference_network_cost_raw`
- `selection.economic.predicted_net_gain_raw`
- `selection.economic.predicted_net_gain_usd_nano`
- `calibration.economic_at_v10_input`: forecast fees, post-V10 gain,
  net at the **V10-chosen quantity**, plus expected V10 share on observed
  gross swap profit. Only two-leg continuous closed cycles are compared.

`execution_analysis.json.cp21a_profit_accounting` reports each
settlement separately, class shifts from pre-fee positive to net
nonpositive, the expected-vs-observed V10 fee mismatch count and
simulated net after actual transaction fee, in USD nanounits.

Summing repeated simulation observations is **not** a realistic
achievable profit. Distinct frozen states and actually broadcast
transactions remain separate concepts. Historical journals can be
re-evaluated **without inventing** CP21A fields that were not recorded.

## Verification and short simulation

Rust:
```powershell
cargo test --locked --release cp21a_
```
Evidence:
```powershell
python -m unittest discover -s scripts/tests -p 'test_cp21a_profit_audit.py'
```
Mainnet, simulation only:
```powershell
Set-Location C:\dev_projects\liq_graph
git fetch origin
git switch cp21a-net-profit-accounting
git pull --ff-only origin cp21a-net-profit-accounting
.\scripts\cp21a_profit_live.ps1
```

This 300-second runner preserves CP20S protocol set, Direct2 route,
10,000 WS account budget, 32 concurrent simulations, no broadcasts,
no fixed trade size and the same economic scheduler. The short test
must demonstrate complete request/reply accounting and materially
correct profit classification and fee reconciliation; economic
improvement in newly discovered opportunities cannot be assumed.
Compare independently observed positive *frozen route states*,
not only the number of successful simulations.

Do not merge this checkpoint into CP20S/main without reviewing CI
and the new real-run evidence.
