# CP8A: directed fee assessments for DLMM and PumpSwap

Base: main 1c7559bab88e9a95358f23d242753c4b03608053. One runtime,
one Graph/CompactIndex and one V10 simulation path. No broadcasts.

## Contract and selection

MarketState publishes `valuation` alongside the unchanged indicative gross price.
The two `estimated_net_rates` are canonical **base -> quote / quote -> base**,
expressed in human token units. GraphSignals maps them to the existing canonical
Pubkey slots. They are marginal DEX-fee assessments, not amount quotes or a
prediction of wallet profit. `scope` states this explicitly. Token-2022 transfer
fees/caps or unresolved transfer behavior remain visible in `token_fee_status`
and `unknown_reason`; they are not silently modelled as free transfers.

The existing optimistic gross index remains the candidate admission screen.
There is no extra net threshold, pool cut, combination budget or second graph.
A witness additionally carries its current estimated net log sum and return.
ExecutionCandidates prioritizes candidate witnesses by that estimate when known,
with the gross score as an explicit fallback when unknown. It does not reject a
candidate merely because its marginal net estimate is negative. Alternatives
continue to be packed under the existing V10/account constraints. The best gross
witness is not claimed to be the globally best net or amount-dependent witness.
Quantity/depth-aware alternatives and busy-request retry remain CP8B/CP8C.

A fee-only change recalculates affected topology witnesses even when gross
weights stay equal. A relevant fee change also republishes active affected candidates when the
best gross witness is unchanged, so a fee change on a parallel alternative
can reach V10. This does not require that alternative to win the gross index.
Catalog reconciliation remains state maintenance, not an execution trigger.
Clock/mint revaluation has its own PriceOrigin and can trigger a new request;
initialization replay and baseline-only observations do not request execution.
V10 `trade_size=0`, profit parameters, signer and simulation logic are unchanged.

## Protocol references and rules

DLMM: MeteoraAg/dlmm-sdk `idls/dlmm.json`,
`commons/src/extensions/lb_pair.rs`, `ts-client/src/dlmm/helpers/fee.ts` and
`helpers/lbPair.ts`. LbPair is the verified 904-byte layout. MarketState now
watches the full pool account. Static parameters start at 8, variable parameters
at 40; active_id/bin_step remain 76/80. Fee references are recomputed using the
chain Clock's timestamp, not the host wall clock. Total base + variable rate is
capped at 100,000,000 / 1,000,000,000 (10%). Protocol share is a split, not an
extra fee. InputOnly and OnlyY collection modes are recorded by direction.
The marginal estimate deducts the total trading fee, conservatively omitting
any host rebate. It does not predict the fee evolution across multiple bins.

PumpSwap: pump-fun/pump-public-docs `idl/pump_amm.json`, `idl/pump_fees.json`,
and the official published @pump-fun/pump-swap-sdk 2.1.0
`src/sdk/{fees,buy,sell,util}.ts`:
https://app.unpkg.com/@pump-fun/pump-swap-sdk@2.1.0/files/src/sdk/fees.ts

Canonical classification uses the pool PDA creator, not coin_creator. The fee
config's SOL-like tiers, stable tiers (legacy fallback), noncanonical flat fees,
and exotic flat fees (zero/unset fallback) are decoded from chain. Mayhem cap
uses the SDK's fixed 1,000,000,000,000,000 supply; ordinary cap uses the watched
base Mint supply. Cap uses effective quote reserves, with checked integer math.
A gated nonzero pool creator override replaces only the creator rate; absent
coin_creator zeroes it. Buyback/holder/cashback routing does not add another copy
of the same creator/protocol charge. Buy/sell disable flags are reflected in
rate availability.

For native base/quote gross rate p and fee fraction f:
- Sell base -> quote: p * (1-f).
- Buy quote -> base: (1/p) / (1+f).

Effective quote = signed vault amount + virtual quote reserves. Fee buckets
must not be subtracted a second time. The default/short append-only pool layout
reads absent trailing creator fields as zero. Missing or malformed mandatory
fee accounts fail initialization or withdraw availability; no zero-fee fallback.

Shared GlobalConfig/FeeConfig/Clock watches are deduplicated. Shared fee tables
are parsed once per account observation and Arc-shared across pool bindings.
Pump costs four per-pool accounts conservatively (pool, two vaults, base Mint)
plus shared accounts; DLMM one full pool plus shared Clock. Existing subscription
continuity/baseline guards still apply. Clock writes only emit a valuation when
a fee boundary/epoch or unknown-to-known assessment changes; they do not emit
one price per pool per slot. Mint facts remain owned by the central Mint Registry.

## Evidence and live acceptance

`market_state.json.metrics.fee_valuation` contains per-model observed/known fee
counts, unknown reasons, change fields, revaluation counts, asymmetric-rate
counts and fee calculation timing. At most eight samples per protocol include
the observation and the exact decoded fee inputs for independent comparison
against the pinned official SDK. These are bounded evidence samples, not a cap
on live calculations. Existing request artifacts preserve each selected leg's
assessment, provenance, component slots and indicative price.

`-RequireFeeEvidence` requires observed known DEX fees for **both** models and
non-reciprocal rates. This check is necessary, not sufficient: inspect the real
samples/formulas, unknown states, fee-only changes, delivery and latency tails.
A run that never observes a particular configuration change cannot prove that
change path live. Simulation handoff, successful swaps and profit are separate.

The uploaded successful V9 transaction 2VZZJEZpk6juj8ZxZ7mAb2TKMYaneJTHq9u7q6JsPMF9rLgDCmBtyibWJmgp6NSPESKQYkqEoh4GiFNvdXXSnPci
is reference evidence, not a replay fixture. Its Pump sell charged 20 + 5 + 18
bps; buyback was part of protocol fee. Its positive virtual reserves materially
changed the price. Its DLMM host fee was credited to the wallet. Historical
DLMM pre-state is absent, so no full historical signal reproduction is claimed.

After the locked feature build, run one short smoke check, then the requested
1800 seconds in the configured Windows environment (no transaction broadcast):

```powershell
git switch cp8a-directed-fee-signals
if ($LASTEXITCODE -ne 0) { throw 'Branchwechsel fehlgeschlagen.' }
git pull --ff-only origin cp8a-directed-fee-signals
if ($LASTEXITCODE -ne 0) { throw 'Synchronisierung fehlgeschlagen.' }
.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp8a-directed-fee-signals `
    -EnabledVenues 'meteora_dlmm,pumpswap' `
    -Stage Signals `
    -DurationSeconds 1800 `
    -PoolLimit 0 `
    -AccountBudget 25000 `
    -ProfileSignals `
    -RequireFeeEvidence `
    -SimulateV10 `
    -SimulationLimit 0 `
    -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
```

Live acceptance is pending until real input/output artifacts have been evaluated.
This implementation environment has no Rust/PowerShell, DB/RPC configuration or
wallet; GitHub CI supplies the locked build, not the live proof. No new automated
unit/mock/regression suites or historical checkpoint scripts are introduced.

## CP8A.1: Registry maintenance and request read timing

Continues on `cp8a-directed-fee-signals`, based on 2af5e94. The two real
600-second runs 20261028T080230472Z and 20261028T084147169Z used that exact head.
The second eliminated the first run's native-SOL account funding failures and
recorded 28 successful simulations, 18 involving both DLMM and PumpSwap.
All 32 emitted PumpSwap swap events matched the recorded LP/protocol/creator
fee rates. Successful requests are not necessarily economically profitable.

The second run's three largest request builds took 290.6, 292.8 and 208.5 ms
at approximately 311, 371 and 431 seconds after process start. Two considered
zero alternatives. These are steady-running stalls, not startup/shutdown samples.
They coincided with minute maintenance; the old reports did not measure Registry
lock waits and therefore do not prove the exact contribution of each cause.

Registry remains the only writer. The minute task is awaited on the blocking
pool before the owner accepts another source/completion/qualification update.
The reference-pair diagnostic scan also runs there. TVL preparation uses a read
lock on entries, so execution readers can proceed during the full quote scan and
cleanup. Only changed TVL/provenance values are installed, in write sections of
at most 64 pools, with a scheduler yield between sections. This is installation
granularity, not a pool/route limit. All patches are applied before publication.
No alternate Registry, legacy mode, new worker, crate or dependency is introduced.

TTL, source precedence, persistence coalescing, historical observations and
native-TVL admission rules are preserved. Canonical TVL expiry does not withdraw
native admission. MarketState can still use canonical TVL when budgets bind.
Execution manifests and mint proofs are untouched by TVL installation.

Measurements:
- One `pool registry tvl_refresh={...}` log per maintenance cycle: scan count,
  changed/persisted pools, preparation time, lock wait, total/max write hold and
  actual maximum batch size. `scan_ns` on the existing reference-pair log records
  that separate diagnostic scan. No per-pool maintenance logs are added.
- Each selection stores `registry_reads`: call count, total read-lock wait/hold,
  and maximum individual wait/hold in ns. This includes seed attempts and
  alternatives, including failed lookups. The aggregate is in
  `execution.json.execution_candidates.registry_reads`. Ordinary untimed Registry
  consumers continue using `get()`.
- `selection_ns` remains total wall time, including Registry access. A large
  selection time with small measured Registry wait must not be labelled a lock
  stall without further evidence; scheduling/other work can still contribute.
- `summary.json` includes `registry_maintenance`, `execution_registry_reads`,
  `registry_timing_observed` and `execution_skipped_busy_revisions`. The timing flag
  means at least two maintenance cycles and actual request reads were measured;
  it does not assert absence of contention or replace functional acceptance.

Run the command above with `-DurationSeconds 600`, retaining the same venues,
budgets, wallet, automatic sizing, minimum profit and flashloan configuration.
Inspect several maintenance cycles, actual read waits, request tails, coverage,
delivery and simulation results. `changed=0`/`write_batches=0` correctly means a
scan needed no installation; that run cannot prove mass-expiry installation.
Large expiry batches require real observations of such expiry, not synthetic
inputs or a claim inferred from the configured duration. Other source/completion
write sections are not refactored by this checkpoint; measured waits include them.
The new head still requires its own real-input live acceptance.

### Deferred candidate/swap analysis

Dispatch is unchanged: a full output channel skips a revision with no retry,
and at most one packable candidate is sent per revision. `skipped_busy_revisions`
counts revisions, not unique pools, executable swaps or missed profitable trades.
No pending queue, new threshold or duplicate-opportunity filter is installed.
Accepted requests and failed simulations remain individually available in the
existing request/selection/simulation artifacts. Busy revision identities are
not retained by the existing counter, so those cannot be reconstructed later.

The later analysis should compare actual pool/direction/route and time, both
negative-estimate successful requests (graph-32-47837-315 and graph-35-51873-384),
three-swap successes, slippage failures and repeated pool sets. Equality of pool
sets alone does not prove equal opportunities: direction, state revision and
available liquidity matter. Complete historical DLMM BinArray pre-states are
absent from the current evidence. Request route labels also do not prove the
executed hop count: successful Direct3/Triangle4 requests in the second run
executed two swaps. Real four-swap execution remains unproven.
