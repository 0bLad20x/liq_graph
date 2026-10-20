# CP2 — Account bytes to normalized pool prices

Branch: `agent/cp2-market-state-decode`, based on `agent/cp1-account-transport`.

## Responsibility and boundaries

The RPC worker already decodes WebSocket JSON/base64. Market State receives the
typed `AccountEvent` with shared, immutable account bytes. It selects the protocol
fields, updates the affected pool state, calculates an indicative price and emits
the existing `MarketStateEvent` contract. No route calculation, profitability
model, signal threshold, transaction construction or execution is added here.

The existing ten venue decoders and price formulas remain in Market State. Reserve
prices can combine component accounts from different slots; the output retains
that consistency classification. Manifest supplies a book midpoint and BBO.
These observations are price indicators, not executable trade quotations.

## Incremental initialization

The global `ready`/baseline barrier and the loop that discarded all notifications
while HTTP was pending have been removed. One preparation task handles at most 64
pools at a time; the receive loop continues to process initialized pools.

* Additions hydrate only missing identities and establish their account watches.
* A pool receives a second, post-subscription HTTP baseline once its own account
  ACKs are present. There is no one-second ACK-settling delay and no wait for all
  other selected pools to become ready.
* Unchanged pools retain their state, account bindings and subscriptions. Removed
  identities receive `Removed` and their unused watches are released.
* Preparation results are checked against current identities and per-pool resync
  revisions. A result made obsolete by removal or a stream gap is discarded.
* A source gap invalidates pools using that source's accounts. Other sources'
  pools continue. Local receiver lag invalidates all affected state explicitly.
* Only initializing/unavailable-for-resync pools keep a latest-account catch-up
  buffer. Normal price updates are processed individually. Catch-up is marked
  `initialization_replay` and excluded from steady-state latency histograms.
* Each subscription has its own HTTP slot floor. A strictly newer slot establishes
  WebSocket ordering; subsequent same-slot writes are processed in stream order.
  HTTP and WebSocket writes within the baseline slot cannot be ordered reliably.

The account-to-binding index is built when membership/baselines change. A normal
account event performs an address lookup and visits that address's bindings; it
does not search or rebuild the selected-pool catalog. Output uses synchronous
broadcast publication and never awaits a receiver. Consumer loss remains visible.

## Independent real-data run

```powershell
& {
    git fetch origin
    if ($LASTEXITCODE -ne 0) { throw "Fetch fehlgeschlagen." }
    git switch agent/cp2-market-state-decode
    if ($LASTEXITCODE -ne 0) { throw "Branchwechsel fehlgeschlagen." }
    git pull --ff-only
    if ($LASTEXITCODE -ne 0) { throw "Synchronisierung fehlgeschlagen." }

    .\scripts\rpc_account_stream_live.ps1 -DecodePrices `
        -DurationSeconds 600 -PoolLimit 750
}
```

The shared CP1.2 runner keeps the existing per-provider `.env` configuration and
restores process variables afterwards. It builds `cargo build --locked --release`
on the displayed branch/HEAD, stops on build errors or tracked changes, and saves
`checkout.json`, `websocket_plan.json`, `preparation.json`, `accounts.json`,
`report.json` and `worker.log` under `market_state_live/<timestamp>/`.

This isolated run deliberately uses **persisted Prepared proofs**, exactly as the
CP1.2 account plan does. It establishes no current Registry TVL admission and does
not modify the database or production admission rules. It starts no mint/TVL
enrichment, graph, signal or execution workers. Provider selection is still the
RPC worker's responsibility. Full runtime retains live Registry admission.

With `PoolLimit 750`, the four phases select 375 / 750 / 375 / 750 pools. Each
measurement lasts 150 seconds **after** that selection's account ACKs and per-pool
baselines are ready. Preparation and transition times are additional. Existing
pools continue to process updates during additions and removals. A quiet transition
does not establish that processing stopped: retained-pool observations are counted
separately in transition and measurement intervals.

## Measurements and evidence

All reported durations are nanoseconds; divide by 1,000,000 for milliseconds.
Percentiles are histogram upper bounds. Empty histograms report null, not zero.

| Report field | Boundary |
|---|---|
| `transport_to_market` | Transport decoded timestamp → Market State receives event |
| `protocol_decode` | Protocol owner/length checks and field decoding; monotonic clock |
| `price_compute` | Normalized price calculation; monotonic clock |
| `market_to_publish` | Market State receives event → price prepared for publication |
| `price_publish` | Producer's Arc construction and broadcast send; monotonic clock |
| `publish_to_consumer` | Publication timestamp → independent price consumer |
| `frame_to_consumer` | Local complete WebSocket text frame → price consumer |
| `account_processing` | Handling one active mapped account event, including its fan-out |
| `preparation_install`, `watch_reconcile` | Control-plane installation/indexing and watch reconciliation |

Cross-worker boundaries use local microsecond-resolution timestamps. They include
scheduling/queueing but exclude the network/provider delay before the socket yields
a complete frame. Protocol decode starts after extracting the field's byte view;
`market_to_publish` and `account_processing` include that extraction and dispatch.

The producer reports received, inactive/unmapped, initializing, baseline-floor,
out-of-order and processed bindings, plus errors/unavailable prices by venue.
One account can update multiple pool bindings, so account and price totals need
not match. Published and consumed **price** totals must match.

The consumer separates initial/add/remove/restore transitions and steady intervals.
HTTP baseline prices have their own count. Catch-up replay is counted but excluded
from the latency histograms. One real output example per observed venue records
pool, mint orientation, price kind, component slots and pipeline timestamps.

`decoding_evidence_passed` / `live_evidence_passed` refer to this decoder checkpoint:
real WebSocket prices in every steady phase, exact producer/consumer price totals,
no local account/price receiver loss, sequence discontinuity, protocol decode or
clock errors. A venue with no observed update has no live decoder proof yet.
`continuity_passed` separately reports source gaps/rejections; this run does not
reclassify or close the outstanding CP1 transport continuity/load acceptance.

No new performance result is claimed before the build and real run. Subscription
count alone is not throughput evidence. This workspace has no Rust/PowerShell
toolchain or RPC/database credentials, so the implementation requires the local
locked build and live report above before acceptance. No dependency/lockfile
changes, formatter run or artificial test suite are involved.
