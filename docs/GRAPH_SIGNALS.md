# Graph signals: full reference calculation

## Scope

`graph_signals` consumes the existing `WatchCatalog` and `MarketStateEvent` stream. It joins pool addresses to the current graph generation and keeps the last valid price observation for each observed pool. Its output is the typed `core::signals::SignalEvent` stream, consumed by a separate diagnostic consumer in the same worker module. Orchestration owns the channels and tasks.

Registry remains the canonical pool-state writer. Graph construction, Market-State decoders, transport, Completion lifecycle, manifests and V10 execution are unchanged. This checkpoint neither packs an `ExecutionRequest` nor signs, simulates or broadcasts transactions.

The graph contains V10 role templates, not packed transactions. Its alternative lists can exceed the execution limits. Pool selection, up to four execution groups, the 16-declaration cap including conversion, and concrete transaction account/byte budgets remain the subsequent execution-job checkpoint. Four route shapes and four execution groups are different concepts.

## Reference semantics

For **every accepted Price event**, including an unchanged price, visit all graph groups indexed by that pool. Enumerate each group's concrete directed pool combinations and fully recalculate every route containing the updated pool. Unaffected routes are not recalculated. All latest valid prices are available by address; there is no wait for simultaneous updates or a common slot.

Both directions are covered for all four existing forms:

| Form | Forward route | Reverse route |
| --- | --- | --- |
| Direct2 | S -> T -> S, two distinct pools | Opposite ordering of the two pools |
| Direct3 | S -> T -> Q -> S | S -> Q -> T -> S |
| Triangle3 | S -> T -> B -> S | S -> B -> T -> S |
| Triangle4 | S -> T -> B -> Q -> S | S -> Q -> B -> T -> S |

S is WSOL or USDC; Q is the other settlement. Intermediate and conversion pools come from the graph's fixed group roles. Alternatives are enumerated with nested loops. Every visited group's combination count is checked against its published `route_options`. Pool reuse in one concrete route is rejected. A concrete path can occur in different graph templates; no global cross-template deduplication is introduced.

There is **no best-price pruning, unchanged-price shortcut, cached logarithm, cached partial sum, event coalescing or candidate ranking**. Existing membership indices only establish which groups depend on the changed pool. Entire groups are enumerated before the containing-pool check; `combinations_visited` therefore includes combinations that do not contain the changed pool, while `routes_affected` counts the concrete routes that do. This distinction exposes reference enumeration cost instead of hiding it.

A new catalog generation triggers a full evaluation using the available observations, including newly valid routes. Group IDs never cross generation boundaries. Pool/mint identities are checked when observations join the catalog. `Gap` clears all price coverage; `Unavailable` and `Removed` withdraw the relevant pool. Selection changes retain only selected prices. Input or output-consumer lag fails explicitly; no successful evidence is produced from a lossy run.

Cooperative yields every 4,096 visited combinations and between groups let other runtime tasks proceed. They do not skip events or calculations. Large enumerations can still build a backlog, which is part of the measurement.

## Calculation and meaning

Market-State provides human quote-token units per one base token. For each route leg:

- base -> quote: rate = p; weight = -ln(p)
- quote -> base: rate = 1/p; weight = +ln(p)

Each concrete route reads all its available leg observations and recomputes every logarithm and the full sum W. The indicative roundtrip return is `expm1(-W) * 10_000` basis points. A candidate qualifies strictly when `W < -ln1p(threshold_bps / 10_000)`.

Default threshold is 0 bps. No hidden economic threshold or rounding tolerance is applied. Floating-point effects close to zero remain visible; such a candidate is not evidence of executable profit. Nonfinite directional rates/results are counted separately and do not produce candidates. Missing observations are counted separately and cannot be assigned a fabricated price.

The calculation uses the existing indicative price only, including the current Manifest midpoint. It does not model swap fees, amount-dependent liquidity, slippage, orderbook fills, transfer fees or execution costs. Component slots and consistency metadata are preserved in every emitted leg. Equal slots do not prove atomic multi-account observations. An unchanged but still observed pool is not expired by a new arbitrary TTL.

## Output and measurements

- `Candidate`: every qualifying concrete route, including generation/group ID, trigger, pool observations, mint direction, each rate/weight, full sum and indicative return. No qualifying candidate is silently sampled out of the typed stream.
- `RouteSample`: the first configured number of nonqualifying complete calculations, separately for WebSocket and other triggers, so a run can show real arithmetic even without a positive candidate.
- `Evaluated`: one accounting/measurement event per complete price or catalog evaluation.
- `Invalidated`: loss of pool coverage or replacement of the graph generation. Candidates are event observations, not durable execution approvals.

The consumer counts all events. By default it prints the first 16 candidates and input evaluations plus the first 16 of each triggered by WebSocket; nonqualifying route samples are limited to 16 per trigger category, and invalidations to the first 16. This keeps actual live examples visible after initial baselines. `-TraceSignals` prints all emitted events; it does not expand nonqualifying route sampling. Terminal/file IO is measured separately from worker publishing. Candidate production and per-route evidence allocation still contribute to worker output cost.

The final `signal_report` includes:

- visited groups/combinations, maximum groups and affected routes per input, affected/scored/missing/nonfinite routes; scored routes by each of the four shapes, separately for real WebSocket triggers;
- all candidates and WebSocket-triggered candidates, invalidations, and observed input/output queue depths;
- calculation, evidence/publishing and total processing nanoseconds per input;
- local account receipt to worker receipt, candidate creation and diagnostic consumer receipt in microseconds;
- diagnostic formatting/output wall time.

Timing histograms have bounded memory and logarithmic buckets. `p50_upper_bound`, `p95_upper_bound` and `p99_upper_bound` are bucket upper bounds, not exact percentiles. Min/max/mean are recorded. Processing time includes identity joining, enumeration, cooperative scheduling and publishing. Calculation time includes per-route lookups, direction resolution, logs and sums; it excludes evidence construction and downstream output. Summing many short measurements adds measurement overhead, which is retained in this reference implementation.

Account-age measurements use the local transport wall-clock timestamp, whose input resolution is milliseconds. They are not provider/network latency measurements. No separate pure channel-wait measurement is claimed. A future optimization comparison requires identical observations and graph generations; two unrelated market periods do not establish equal results or a speedup.

## Local live run

Use the existing database, CP1 admission/enrichment and RPC configuration from `MARKET_STATE.md`. No wallet is required. The standalone runner reuses that exact wiring: persisted proof seed, live admission, graph/catalog, real subscriptions and Market-State, with the signal worker and consumer added. It starts no pool-cache persistence, Completion or execution workers and does not refresh cached manifests.

For the already exercised RPC 5/6 configuration with 24 account subscriptions each, begin with **16 pools**. Raising the pool limit does not raise provider capacity. Existing provider flags must still be enabled in the process or environment.

```powershell
.\scripts\graph_signals_live.ps1 -DurationSeconds 600 -PoolLimit 16 -ThresholdBps 0
```

Add `-Rewatch` for the existing controlled subscription pause/resume. The existing Market-State post-rewatch diagnostic counter limitation remains tracked in issue #34; this checkpoint does not change it or claim a provider disconnect test.

The small entry script delegates to `market_state_live.ps1 -Signals`, sharing checkout/build guards and environment restoration. It checks branch, exact HEAD and tracked/untracked status, requires a clean tracked checkout and `cargo build --locked --release` without lockfile changes, then records checkout metadata and parameters. Artifacts are written to `graph_signals_live/<UTC timestamp>/checkout.json`, `worker.log` and `report.json`. Process environment values are restored afterwards. Culture-independent decimal formatting is used for the threshold.

`signal_calculation_passed` requires complete route calculations triggered by real WebSocket observations. `signal_candidate_passed` additionally requires a real WebSocket-triggered positive candidate to reach the consumer. Overall `live_evidence_passed` requires these and the existing Market-State evidence. If the market produces no candidate above the configured threshold, arithmetic can be demonstrated while candidate delivery remains unproven; the report states that distinction and the script does not claim full acceptance. Early task failures may have only a worker log, rather than a completed report.

Inspect `websocket_scored_by_kind`: a Direct2 observation does not prove the other three forms. Large pool-count performance, unobserved protocol decoders, production-wide runtime composition and the future execution handoff require their own real evidence. Build and live acceptance of this new branch are pending until the local run succeeds.

## Normal runtime

```dotenv
MARKET_STATE_ENABLED=true
GRAPH_SIGNALS_ENABLED=true
GRAPH_SIGNALS_THRESHOLD_BPS=0
GRAPH_SIGNALS_SAMPLE_LIMIT=16
GRAPH_SIGNALS_TRACE=false
```

`GRAPH_SIGNALS_ENABLED` defaults to false. Enabling it without Market-State fails configuration explicitly. Both workers and their consumers are wired within the existing process; no additional service or network hop is introduced.
