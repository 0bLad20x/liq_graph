# CP8F: Prepared pool structure -> watches -> one live baseline

## Scope and ownership

Builds on the tested CP8E protocol scope. DLMM and PumpSwap no longer fetch pool,
Mint and detail accounts to rediscover their structure before subscribing.
Completion owns validated structure, Registry owns accepted proofs, Persistence
stores them, and MarketState owns subscriptions and dynamic valuation.
No new worker, table, dependency, parallel registry or cache service is added.

DLMM already declares arrays and optional bitmap in its execution manifest. PumpSwap
now publishes `PoolVaults` in mint_a/mint_b orientation with the same CompletionResult.
Registry checks distinct nonzero vaults and their writable presence in both manifest
blocks. Persistence stores/restores this typed optional field inside the existing
`pools.completion` JSON. PumpSwap evidence v2 stops duplicating the vault addresses;
the light checker compares the typed roles instead of decoding those JSON fields.
Mint decimals and token owners continue to come from the authoritative Mint Registry.

Old Prepared PumpSwap rows without vault roles remain restorable. The lifecycle
immediately schedules a full refresh for an admitted row missing them; the light
path cannot postpone this upgrade. MarketState waits for those roles instead of
issuing a second private structure lookup. Subsequent restarts reuse persisted roles.
Existing completion refresh/backoff and qualification policy otherwise remain intact.

## Runtime change

Before: pool RPC -> detail RPC -> watches -> ACKs -> dynamic baseline -> catch-up.
After, for these two venues: prepared structure -> watches -> ACKs -> dynamic
baseline -> catch-up. A structure alone never publishes a price. Initial dynamic
state and reconnect recovery use the same baseline implementation and update decoders.
The staged baseline invalidates component flags before decoding; a pool is evaluated
only after every required component was decoded successfully.

DLMM baseline: LbPair, declared BinArrays, optional bitmap extension and Clock.
PumpSwap baseline: pool, both vaults, base Mint supply, GlobalConfig, FeeConfig and
Clock. Contents of these accounts stay dynamic. No DLMM Mint or PumpSwap quote Mint
is requested by MarketState. PumpSwap base Mint remains necessary for current supply.
Current vault owner/mint/authority and pool/vault role checks protect the new path.

Addresses are deduplicated in each baseline batch and in the existing shared watch
plan. There is deliberately no additional cache across independent jobs or sessions.
Component slot floors, buffered WS catch-up, provider continuity, version tickets,
selection removals and manifest dependency changes retain their protections.
Other venues keep their existing structure hydration; the redundant Mint reads in
that path are removed because their decimals already come from Mint Registry.

## Evidence

`market_state.json` and the existing starter summary include:

- `prepared_structures_by_venue`: direct structure installations, including replacements.
- `prepared_structure_build`: CPU time to build/install the static bindings.
- `prepared_to_first_baseline`: wall time from binding construction to the first
  accepted baseline, including subscription and baseline queue/RPC waiting.
- `structure_rpc` / `baseline_rpc`: logical Multiple RPC batches, requested account
  entries and successfully decoded account-data bytes from finished preparation jobs.
  Split/retry work in those jobs counts again. Transport-internal retries, unfinished
  canceled jobs, HTTP/base64/TLS overhead and other workers are outside these counters.
- `preparation`: mutually exclusive final counts of selected pools waiting for structure,
  mints, subscriptions or baseline, plus unpriceable and priced pools. No selection
  denominator is reduced to claim complete coverage.
- Persistence startup log: restored typed vault roles and old PumpSwap rows still
  missing them. After a first upgrade run, a restart must show restored roles.

`-RequirePreparedEvidence` requires direct installation and known-fee output for each
selected venue, no initial structure-hydration jobs or structure RPC calls, a real
baseline, zero decoded-account errors/lost inputs, and a complete stage-count partition.
It does not claim 100% stable WS coverage, profitable simulations or full readiness
of every selected pool. Existing delivery and orchestration checks still apply.

## Live proof

Next run: the normal pipeline, 600 seconds, one executor and one queued request,
no lifetime simulation cap, no broadcasts.
Use the existing `.env`, RPC configuration and test wallet. No extra provider budget.

```powershell
.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp8f-prepared-market-bootstrap `
    -EnabledVenues 'meteora_dlmm,pumpswap' `
    -Stage Signals `
    -DurationSeconds 600 `
    -PoolLimit 0 `
    -AccountBudget 25000 `
    -ProfileSignals `
    -RequirePreparedEvidence `
    -SimulateV10 `
    -SimulationLimit 0 `
    -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
```

A later short restart with the same command and `-DurationSeconds 60` checks actual
DB reuse. Subscription recovery/rewatch can be inspected with the existing `-Rewatch`;
initial bootstrap measurement should first use the uninterrupted selection above.
A successful locked release build is required before handoff. A real live run and
its artifacts are still required for acceptance of the follow-up below.

## Follow-up: preserve packed exclusions and explain readiness failures

The 600-second run on `d288a4e` proved direct structure installation without
MarketState structure RPC calls. It did not prove stable readiness: the final
5,072 installed pools split into 4,151 waiting for subscription ACKs and 921
waiting for baseline. The three-request cap was reached after roughly 45 seconds;
all three simulations returned Custom1 before a DEX call. This cannot establish
market weakness or the meaning of Custom1.

Two selections exposed a pre-existing CP8C bug: their actual packed pool set was
excluded, but unknown alternatives outside that request replaced the exclusion
with `unknown`. The follow-up preserves the exclusion. The existing alternative
seed path may still produce a different request, but that rebuilt packed set must
pass its own assessment. No new quantity search, threshold, RPC query or change
to Flashloan, auto trade size or V10 minimum profit is introduced.
`amounts.packed_exclusions` counts preserved packed exclusions when the wider
universe was not excluded and seed repair did not produce an admissible request.
Existing global exclusions remain counted separately within `amounts.excluded`.
Retained packed exclusion samples describe the packed inputs, not unknown pools
outside that request. This can correctly result in fewer simulations.

The same MarketState owner now retains bounded diagnostic evidence:

- `metrics.preparation_samples`: last 24 samples, one every 30 seconds plus shutdown,
  each with its timestamp and sampling duration (`build_ns`). They partition the
  selected set using the existing preparation stages. `net_direction_pools[0..2]`
  additionally counts ready/available pools with a fee valuation by known net
  directions; these counts do not redefine the existing indicative `priced` field.
- Each sample splits baseline waiting into in-flight, retry backoff and queued
  pools, and reports the oldest in-flight baseline age among those waiting pools.
- Per-source component-account counts show acknowledged and missing accounts.
  `blocked_pools` is unique within each source; pools can be blocked by several
  sources, so these counts must not be summed as unique pools.
- Up to eight missing accounts and eight shared accounts show address, field,
  binding fan-out, intended source and acknowledged source. Assignment metadata is
  published through the existing account contract in one batch per changed socket,
  never one event per account and never as a subscription ACK.
- `metrics.subscription_gaps` counts gap events per source. The last eight gaps
  that actually removed active accounts retain affected pool counts, previously
  ready counts and up to eight shared affected accounts. Reconnect lifecycle
  events can describe the same outage more than once.
- Up to four failed preparation samples preserve pool, phase and exact reason.
- `metrics.fee_valuation.invalid_reference_samples` keeps at most four examples
  per DLMM error: `DLMM clock behind fee reference` versus `DLMM reduction factor
  invalid`, with Clock timestamp, fee parameters and component slots/timestamps.
  Guards and fee math remain enforced. No assumed Clock correction is applied.

The transport logs per-socket counts every 30 seconds and the oldest pending
request/address when a reply times out. It does not log account notifications or
credentials. Allocation, full readiness projections and sample serialization are
outside ordinary price updates, except bounded samples of the actual fee error.
There are no new diagnostic consumers, database tables or services.

Use the command above for one new 600-second run. `market_state.json` holds the
bounded evidence; `summary.json` includes the final readiness sample and gap counts.
Inspect `amount_evidence.json` for preserved exclusions and `execution.json` for
the requests actually simulated. Existing `-RequirePreparedEvidence` still proves
bootstrap only; it does not certify full coverage or successful trading.
The follow-up is not functionally accepted until this new live evidence exists.
