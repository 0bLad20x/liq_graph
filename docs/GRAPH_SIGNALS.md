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

The consumer counts all events. By default it prints the first 16 candidates and input evaluations plus the first 16 of each triggered by WebSocket; nonqualifying route samples are limited to 16 per trigger category, and invalidations to the first 16. This keeps actual live examples visible after initial baselines. `-TraceSignals` prints all emitted events; it does not expand nonqualifying route sampling. Terminal/file IO is measured separately from worker publishing. Candidate construction, reference-count operations and per-event publishing still contribute to worker output cost.

The final `signal_report` includes:

- visited groups/combinations, maximum groups and affected routes per input, affected/scored/missing/nonfinite routes; scored routes by each of the four shapes, separately for real WebSocket triggers;
- all candidates and WebSocket-triggered candidates, invalidations, and observed input/output queue depths;
- calculation, evidence/publishing and total processing nanoseconds per input;
- local account receipt to worker receipt, candidate creation and diagnostic consumer receipt in microseconds;
- diagnostic formatting/output wall time.

The report also separates calculation, output and processing timings for WebSocket-triggered inputs (`websocket_calculation_ns_per_input`, `websocket_output_ns_per_input`, `websocket_processing_ns_per_input`). `websocket_account_to_worker_us` measures age when the signal worker accepts the input. `websocket_account_to_evaluation_consumer_us` measures age when the consumer receives that input's completion marker, after all its candidates have reached the same consumer. The latter includes preceding queue wait and worker processing; it is not pure output-channel latency. Zero-candidate inputs are included. Periodic timing lines retain WebSocket output/processing means and maximum processing/completion ages in `worker.log` even if shutdown later fails before writing the report. Candidate counts are checked at every completion marker as well as at shutdown. `candidate_leg_references` counts all delivered candidate legs, including references to older frozen observations.

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

Inspect `websocket_scored_by_kind`: a Direct2 observation does not prove the other three forms. Large pool-count performance, unobserved protocol decoders, production-wide runtime composition and the future execution handoff require their own real evidence.

The uploaded 600-second run at `2ecb489ad28f8474d23695e17eed08329fd3ba94` established the Direct2 dataflow with 16 selected pools, 32 active accounts, 17 WebSocket price events, 34 complete WebSocket-triggered route calculations and 17 WebSocket-triggered candidates at the consumer. All 64 printed route calculations were independently recomputed without a mismatch. The other three route shapes were not price-covered.

The subsequent uploaded run at `cf5f553b419fa07c3c04033ed539dc14e10001ed` used only RPC 5/6 and reached 5,100 selected pools and 9,706 active account subscriptions. Its printed WebSocket evaluation counters covered all four route shapes. Market-State produced 8,851 WebSocket price observations; the last signal-consumer log accounted for only 2,516 completed WebSocket evaluations, with an observed maximum price queue depth of 6,805. Shutdown failed at the unchanged 30-second signal-worker deadline, so there was no successful final report or large-load acceptance. One printed evaluation visited 488,668 combinations, scored 80,092 routes and emitted 40,046 candidates: 27.5287 ms calculation, 172.8301 ms output, 299.3262 ms total processing. These are one input's measurements, not whole-run averages. The 64 printed full route examples were independently recomputed without a mismatch.

## Output optimization checkpoint 1

`signal_report.output_layout` and the worker startup log identify `shared_fixed_legs_v1`:

- The signal worker owns one immutable `Arc<PoolPriceObservation>` snapshot per accepted Price input. The incoming observation is copied once when joining the price cache, not once per candidate leg. Total per-input processing time includes that copy. Replacing/removing the cached price or changing the catalog cannot mutate or invalidate an observation already held by an emitted route; reference counts release it after the cache, output channel and consumers no longer hold it.
- Every route and the completion marker share one immutable trigger per input evaluation. Group generation, IDs, numeric values and binary group addresses remain self-contained in the output; no Registry/DB access is needed to interpret it.
- Route mint/context addresses are `Pubkey` values internally. The existing JSON serializers format Base58 only when the diagnostic consumer actually serializes a selected example. Price-observation metadata retains its existing representation and values.
- `SignalLegs::Two/Three/Four` hold fixed arrays inline; candidate construction allocates no leg vector. Their untagged serialization preserves the previous `legs` JSON array with exactly 2, 3 or 4 entries. Shared triggers/observations serialize to the same JSON objects and addresses remain strings.
- Each qualifying candidate is still published immediately as its own typed event. There is no whole-evaluation buffer, batch wait, candidate suppression, additional lookup service or legacy output path. One event allocation and channel publication per candidate remain part of this checkpoint's cost.

This changes only `core/signals.rs` and `workers/graph_signals.rs` plus this documentation. Enumeration, per-route lookup/logarithms/sum/threshold, yields, invalidations, sample limits, provider settings, Market-State decoding and shutdown deadlines remain as before. Registry ownership and the Core / Workers / Orchestration boundaries remain unchanged. There are no new dependencies or lockfile changes.

For a scoped load comparison, repeat the RPC 5/6 command below with the same pool limit, account budget, duration and 0-bps threshold; leave `-TraceSignals` and `-TracePrices` off. Upload `checkout.json`, `websocket_plan.json`, `report.json` and `worker.log` from that run. A successful locked build and real consumer delivery are required before acceptance. Inspect WebSocket-only timings, route/candidate counts and queues together, and require clean completion without lag or timeout. A shorter output time alone does not establish a shorter complete pipeline. Different live inputs do not prove a numerical speedup on identical work. The overall 1-2 ms objective is not an acceptance claim for this change.

The agent environment has no Cargo/Rust compiler or PowerShell, RPC credentials or database access. This revision is prepared for the existing local locked-build-gated runner; compilation and live performance remain unverified until it is run there. No test suites or formatting checks are added.

## Larger observation scope and provider selection

The existing transport remains the sole owner of RPC configuration. The script exposes these options for both Market-State and Graph-Signals; values are temporary process overrides and are restored after the run:

| Option | Meaning |
| --- | --- |
| `-WsProviders Configured` | Default. Only providers with `RPC_N_WS_ACCOUNT_SUBSCRIBE=true`; original per-provider capacities apply without a budget override. |
| `-WsProviders All` | Select every configured, nonempty `RPC_N_WS_URL`, including providers whose account flag is false. This requests account subscriptions; it does not prove endpoint support. |
| `-WsProviders "5,6"` | Select only these WebSocket provider indices and require their WS URLs to exist. The individual account flags are overridden for selection. |
| `-WsAccountBudget 30000` | Explicit aggregate **local test** account budget, shared evenly across selected providers in index order. Overrides their configured account/per-connection capacities for this run. Default 0 preserves per-provider capacities. |
| `-WsConnectionsPerProvider 1` | Connection limit per selected provider for an explicit account budget. Default 0 preserves each provider's configured connection count; per-connection capacity is calculated from its budget share. |
| `-PlanOnly` | After the locked build, resolve and save configuration without opening RPC connections or reading the database. |

The corresponding normal-runtime environment keys are `RPC_WS_PROVIDER_SELECTION` (`configured`, `all` or comma-separated indices), `RPC_WS_ACCOUNT_BUDGET` (default 0) and `RPC_WS_CONNECTIONS_PER_PROVIDER` (default 0). Without these overrides the original RPC configuration behavior is retained. HTTP reads continue to use the existing eligible HTTP providers, independently of WebSocket selection. Subscription pacing, timeouts and reconnect settings remain per provider.

Before every live run, `--rpc-ws-plan` resolves the configuration through the same Rust transport parser that the actual runner uses. The script prints index/name/socket/account limits and saves `websocket_plan.json`; the plan is also included in `checkout.json` and the final `report.json`. No endpoint URL or access key is serialized. The plan describes eligibility and local capacity, not successful subscription ACKs; those are measured during the live run.

For the previously used RPC 5/6, a 10,000-pool test can be requested as follows:

```powershell
.\scripts\graph_signals_live.ps1 -DurationSeconds 600 -PoolLimit 10000 -ThresholdBps 0 `
    -WsProviders "5,6" -WsAccountBudget 30000 -WsConnectionsPerProvider 1
```

This sets a local capacity of 15,000 accounts on one socket per selected provider. It is not a verified OrbitFlare service limit. To distribute the same aggregate test budget across every configured WS endpoint, replace `"5,6"` with `All`. To inspect either plan first without subscriptions, append `-PlanOnly`.

Pool count is an upper bound. The runner selects only pools currently admitted by the existing Registry; it does not create additional Prepared pools or bypass admission. Thirty thousand distinct account slots cover the current maximum of three price accounts per selected pool at a 10,000-pool limit, before shared-account deduplication. Only actual selected accounts are subscribed. Providers can reject subscriptions or throttle connections; local budgets do not change their service limits. No automatic migration from a permanently unavailable provider is introduced, and unavailable providers can prevent complete observation coverage.

The route reference calculation, complete enumeration and existing loss detection are unchanged. New multi-hop coverage is accepted only when the corresponding `websocket_scored_by_kind` counters are positive and the real calculation evidence is present. The local plan does not establish throughput or successful large-scope operation.

## Normal runtime

```dotenv
MARKET_STATE_ENABLED=true
GRAPH_SIGNALS_ENABLED=true
GRAPH_SIGNALS_THRESHOLD_BPS=0
GRAPH_SIGNALS_SAMPLE_LIMIT=16
GRAPH_SIGNALS_TRACE=false
```

`GRAPH_SIGNALS_ENABLED` defaults to false. Enabling it without Market-State fails configuration explicitly. Both workers and their consumers are wired within the existing process; no additional service or network hop is introduced.
