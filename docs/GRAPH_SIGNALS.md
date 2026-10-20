# Graph signals: compiled role plan and complete route sums

## Scope

`graph_signals` consumes the existing `WatchCatalog` and `MarketStateEvent` stream. It joins pool addresses to the current graph generation and keeps the last valid price observation for each observed pool. Its output is the typed `core::signals::SignalEvent` stream, consumed by a separate diagnostic consumer in the same worker module. Orchestration owns the channels and tasks.

Registry remains the canonical pool-state writer. Graph construction, Market-State decoders, transport, Completion lifecycle, manifests and V10 execution are unchanged. This checkpoint neither packs an `ExecutionRequest` nor signs, simulates or broadcasts transactions.

The graph contains V10 role templates, not packed transactions. Its alternative lists can exceed the execution limits. Pool selection, up to four execution groups, the 16-declaration cap including conversion, and concrete transaction account/byte budgets remain the subsequent execution-job checkpoint. Four route shapes and four execution groups are different concepts.

## Evaluation semantics

For **every accepted Price event**, including an unchanged price, use the compiled pool-to-role bindings to visit exactly the concrete directed routes containing that pool. Each affected route retains its full ordered two/three/four-leg sum. Unaffected routes are not recalculated. All latest valid prices are available; there is no wait for simultaneous updates or a common slot. The allowed route universe, strict threshold, sample rules and candidate order remain the same as in the original reference calculation.

Both directions are covered for all four existing forms:

| Form | Forward route | Reverse route |
| --- | --- | --- |
| Direct2 | S -> T -> S, two distinct pools | Opposite ordering of the two pools |
| Direct3 | S -> T -> Q -> S | S -> Q -> T -> S |
| Triangle3 | S -> T -> B -> S | S -> B -> T -> S |
| Triangle4 | S -> T -> B -> Q -> S | S -> Q -> B -> T -> S |

S is WSOL or USDC; Q is the other settlement. Intermediate and conversion pools come from the graph's fixed group roles. On generation compilation, role mint pairs, unique alternatives, distinct cycle mints, full route counts and pool-to-group bindings are validated against the catalog/graph. Distinct mint pairs prevent pool reuse between the roles of 3/4-leg routes; Direct2 explicitly excludes the same pool twice. A concrete path can occur in different graph templates; no global cross-template deduplication is introduced.

There is **no best-price pruning, unchanged-price shortcut, cached partial route sum, event coalescing or candidate ranking**. Directed logarithmic weights are prepared once per accepted price, then reused wherever that pool occurs. A private CSR index maps each pool directly to group/role/position bindings. A variable-role update fixes that position and enumerates the other alternatives. An intermediate/conversion update still evaluates the full group, because all its combinations are affected. `combinations_visited` now counts actual affected-route visits; `combinations_available` records the full group combinations that the former enumerator would have visited. They must not be interpreted as the same metric across revisions.

A new catalog generation compiles a replacement plan and triggers a full evaluation using the available observations, including newly valid routes. Compilation is measured as `plan_build_ns`; it and the full catalog evaluation still happen in the signal worker, so generation changes can still delay subsequent price inputs. This checkpoint does not claim to have removed all pipeline backlog. Group IDs never cross generation boundaries. Pool/mint identities are checked when observations join the catalog. `Gap` clears all price coverage; `Unavailable` and `Removed` withdraw the relevant pool. Selection changes retain only selected prices. Input or output-consumer lag fails explicitly; no successful evidence is produced from a lossy run.

The worker calculates blocks of at most 256 routes with fixed 2/3/4-leg kernels and flushes the required outputs after each block, in the former filtered nested-loop order. The reusable staging buffer holds at most 256 outputs; it never waits for another price, a timer or a full block. Every candidate is still its own typed event. Cooperative yields after at least 4,096 actual route visits (at block boundaries) let other runtime tasks proceed. No price events or affected routes are coalesced or skipped. Large calculations/output volumes can still build a backlog, which remains part of the measurement.

## Calculation and meaning

Market-State provides human quote-token units per one base token. For each route leg:

- base -> quote: rate = p; weight = -ln(p)
- quote -> base: rate = 1/p; weight = +ln(p)

Each accepted price prepares one logarithm and both directed rates/weights. Each concrete route reads its directed weights from dense arrays and recomputes the full sum W, starting at zero in the original leg order. No route sum is maintained by adding/subtracting a price delta. Missing/nonfinite directional states are checked in leg order, preserving the reference classification when several legs are unavailable or invalid. The indicative roundtrip return is `expm1(-W) * 10_000` basis points. A candidate qualifies strictly when `W < -ln1p(threshold_bps / 10_000)`.

Default threshold is 0 bps. No hidden economic threshold or rounding tolerance is applied. Floating-point effects close to zero remain visible; such a candidate is not evidence of executable profit. Nonfinite directional rates/results are counted separately and do not produce candidates. Missing observations are counted separately and cannot be assigned a fabricated price.

The calculation uses the existing indicative price only, including the current Manifest midpoint. It does not model swap fees, amount-dependent liquidity, slippage, orderbook fills, transfer fees or execution costs. Component slots and consistency metadata are preserved in every emitted leg. Equal slots do not prove atomic multi-account observations. An unchanged but still observed pool is not expired by a new arbitrary TTL.

## Output and measurements

- `Candidate`: every qualifying concrete route, including generation/group ID, trigger, pool observations, mint direction, each rate/weight, full sum and indicative return. No qualifying candidate is silently sampled out of the typed stream.
- `RouteSample`: the first configured number of nonqualifying complete calculations, separately for WebSocket and other triggers, so a run can show real arithmetic even without a positive candidate.
- `Evaluated`: one accounting/measurement event per complete price or catalog evaluation.
- `Invalidated`: loss of pool coverage or replacement of the graph generation. Candidates are event observations, not durable execution approvals.

The consumer counts all events. By default it prints the first 16 candidates and input evaluations plus the first 16 of each triggered by WebSocket; nonqualifying route samples are limited to 16 per trigger category, and invalidations to the first 16. This keeps actual live examples visible after initial baselines. `-TraceSignals` prints all emitted events; it does not expand nonqualifying route sampling. Terminal/file IO is measured separately from worker publishing. Candidate construction, reference-count operations and per-event publishing still contribute to worker output cost. Block staging changes output timing but preserves the candidate/sample sequence and the immutable source observations.

The final `signal_report` includes:

- `execution_layout=indexed_role_plan_v1`, full group `combinations_available`, actual `combinations_visited`, analytical `routes_expected`, `logarithms_prepared`, visited groups, maximum groups and affected routes per input, affected/scored/missing/nonfinite routes; scored routes by each of the four shapes, separately for real WebSocket triggers;
- all candidates and WebSocket-triggered candidates, invalidations, and observed input/output queue depths;
- price preparation, block calculation, evidence/publishing and total processing nanoseconds per input; catalog plan compilation is reported separately;
- local account receipt to worker receipt, candidate creation and diagnostic consumer receipt in microseconds;
- diagnostic formatting/output wall time.

The report also separates calculation, output and processing timings for WebSocket-triggered inputs (`websocket_calculation_ns_per_input`, `websocket_output_ns_per_input`, `websocket_processing_ns_per_input`). `websocket_account_to_worker_us` measures age when the signal worker accepts the input. `websocket_account_to_evaluation_consumer_us` measures age when the consumer receives that input's completion marker, after all its candidates have reached the same consumer. The latter includes preceding queue wait and worker processing; it is not pure output-channel latency. Zero-candidate inputs are included. Periodic timing lines retain WebSocket output/processing means and maximum processing/completion ages in `worker.log` even if shutdown later fails before writing the report. Candidate counts are checked at every completion marker as well as at shutdown. `candidate_leg_references` counts all delivered candidate legs, including references to older frozen observations.

Per-evaluation accounting requires `combinations_visited == routes_expected == routes_affected == routes_scored + routes_missing_prices + routes_nonfinite`, plus full candidate delivery. Every accepted Price must prepare exactly one logarithm and perform no plan compilation. WebSocket-only visit/expected/logarithm counters make these checks visible without mixing in baselines/catalogs.

Timing histograms have bounded memory and logarithmic buckets. `p50_upper_bound`, `p95_upper_bound` and `p99_upper_bound` are bucket upper bounds, not exact percentiles. Min/max/mean are recorded. Processing time includes price preparation or plan compilation, enumeration, calculation, cooperative scheduling and publishing. Price preparation includes address parsing, identity validation, one logarithm, both directional states and the frozen observation. Calculation time is measured per block and includes iteration, dense-array reads, full ordered sums, return conversion, counters and bounded output staging. Output time is measured per nonempty block and includes candidate/sample construction and publication, excluding downstream formatting. These component timings have changed boundaries from the previous per-route timers; compare total processing and input/completion age as well. Plan compilation is measured for catalog-triggered evaluations, separately from price preparation.

Account-age measurements use the local transport wall-clock timestamp, whose input resolution is milliseconds. They are not provider/network latency measurements. No separate pure channel-wait measurement is claimed. A future optimization comparison requires identical observations and graph generations; two unrelated market periods do not establish equal results or a speedup.

## Local live run

Use the existing database, CP1 admission/enrichment and RPC configuration from `MARKET_STATE.md`. No wallet is required. The standalone runner reuses that exact wiring: persisted proof seed, live admission, graph/catalog, real subscriptions and Market-State, with the signal worker and consumer added. It starts no pool-cache persistence, Completion or execution workers and does not refresh cached manifests.

For the already exercised RPC 5/6 configuration with 24 account subscriptions each, begin with **16 pools**. Raising the pool limit does not raise provider capacity. Existing provider flags must still be enabled in the process or environment.

```powershell
.\scripts\graph_signals_live.ps1 -DurationSeconds 600 -PoolLimit 16 -ThresholdBps 0
```

Add `-Rewatch` for the existing controlled subscription pause/resume. The existing Market-State post-rewatch diagnostic counter limitation remains tracked in issue #34; this checkpoint does not change it or claim a provider disconnect test.

The small entry script delegates to `market_state_live.ps1 -Signals`, sharing checkout/build guards and environment restoration. It checks branch, exact HEAD and tracked/untracked status, requires a clean tracked checkout and `cargo build --locked --release` without lockfile changes, then records checkout metadata and parameters. Artifacts are written to `graph_signals_live/<UTC timestamp>/checkout.json`, `worker.log` and `report.json`. Process environment values are restored afterwards. Culture-independent decimal formatting is used for the threshold.

`signal_calculation_passed` requires complete route calculations triggered by real WebSocket observations. `signal_candidate_passed` additionally requires a real WebSocket-triggered positive candidate to reach the consumer. Overall `live_evidence_passed` requires these, producer/consumer delivery agreement and the existing Market-State evidence. If the market produces no candidate above the configured threshold, arithmetic can be demonstrated while candidate delivery remains unproven; the report states that distinction and the script does not claim full acceptance. The runner also writes partial reports on returned setup, task and shutdown errors; unavailable worker reports remain null. A crash or forced termination that returns control to the script gets an explicit failed-report placeholder, not invented measurements.

## Output-path profiling checkpoint

The uploaded `adf14a9` run ended with `signal consumer lost 6306 events` before a final report was written. Those were overwritten signal-output events; that message alone does not identify the relative cost of construction, broadcast publication, consumer bookkeeping, console IO or scheduling. This checkpoint measures those existing operations. It does not claim to fix throughput or reach the latency objective.

Run the same large observation scope with explicit profiling enabled:

```powershell
.\scripts\graph_signals_live.ps1 -DurationSeconds 900 -PoolLimit 10000 -ThresholdBps 0 `
    -WsProviders "5,6" -WsAccountBudget 30000 -WsConnectionsPerProvider 1 -ProfileSignals
```

Keep both trace options off. The same limited real arithmetic examples remain visible. The new switch sets `GRAPH_SIGNALS_PROFILE=true` temporarily and records it in `checkout.json`. Default operation still stops on diagnostic receiver lag. With profiling enabled, this receiver counts each `Lagged` notification and continues to the end of the observation period; a price-input gap/lag or calculation error still stops the worker. **Any lost output events invalidate delivery and live acceptance**, even when `run_complete=true`. The script prints report locations and producer/receiver totals before returning failure. It does not replay lost events, mask failure or backpressure the producer.

The producer still publishes individual typed events through the existing 16,384-entry broadcast channel. No receiver acknowledgement, blocking capacity wait, additional channel, output ranking, route filtering or larger queue is introduced. To time construction separately from publication without putting clock reads around every candidate, the existing 256-route block now first constructs its events into a reused vector of at most 256 `Arc` pointers, then sends them in the same order. This changes within-block publication timing and adds bounded staging; it is not an evaluation-wide buffer. Route arithmetic and the set/order of qualifying outputs are preserved. `emitted_at_us` remains the event's creation timestamp and precedes this staging delay.

Inspect the two independent reports:

| Field in `report.json` | Measurement |
| --- | --- |
| `signal_production_report.all_inputs` / `.websocket_inputs` | Completed-input distributions recorded directly by the producer, independent of received output events. |
| `preparation_ns`, `calculation_ns` | Existing preparation and block calculation boundaries. |
| `event_build_ns` | Route evidence construction, frozen-observation references and event/Arc allocation per nonempty block. |
| `publication_ns` | Synchronous broadcast sends, publication counters and a queue-depth sample per block; includes contention and any overwritten-event destruction. No receive acknowledgement is awaited. |
| `cooperative_yield_ns` | Wall time across the existing `yield_now` calls. This includes rescheduling delay, not just the yield instruction's CPU cost. Other preemptions remain included in their enclosing wall-time measurements. |
| `processing_ns` | Worker acceptance through calculation, output construction/publication and accounting checks; ends before recording producer aggregates and publishing the completion marker. Includes loop overhead and scheduling. |
| `signal_production_report.catalog_plan_build_ns` | Generation-plan compilation, separated from price-input work. |
| `signal_report.consumer_bookkeeping_sample_ns` | One event in every 257 received events when profiling; histogram of bookkeeping/checks before JSON/printing. Not a total CPU-time estimate and not the channel receive/select cost. |
| `signal_report.consumer_formatting_ns` / `.consumer_periodic_logging_ns` | Total wall time in sampled-example JSON/printing and periodic log calls respectively; includes blocking output/console IO. |
| `signal_report.completion_to_consumer_us` / `.websocket_completion_to_consumer_us` | Completion-marker creation to receiver bookkeeping, including producer aggregate recording, marker allocation/send, queueing and receiver scheduling. Not pure channel latency. |
| `signal_report.first_lag`, `.lost_events`, `.lag_incidents` | First loss context and all reported output losses. After a loss, consumer counters/timings cover only surviving events; producer completed-input measurements remain available. |
| `signal_delivery_passed` | Producer completion, zero consumer loss/errors and agreement of published/received events, evaluations and candidates, including WebSocket totals. |

`max_output_queue_depth` is sampled at producer block boundaries, so it is an observed maximum rather than an exact queue high-water mark. Histograms remain fixed-size logarithmic distributions. Component measurements are wall time and carry instrumentation overhead; their percentiles cannot be added to obtain an end-to-end percentile. Baseline ages must be separated from real WebSocket ages. Producer aggregates include completed inputs only; a failed or forcibly aborted evaluation is not represented as a completed one.

`run_complete=false` marks early/setup/shutdown failure; partial reports retain the data that could be collected. Timeout/panic/forced termination can leave a worker report unavailable, and process/file-system failure can prevent persistence. `run_complete=true` establishes that the requested duration and shutdown finished, while `live_evidence_passed` remains a separate check. Upload `checkout.json`, `websocket_plan.json`, `report.json` and `worker.log` even if the script reports failure. The next decision is based on these separated measurements, not an assumed queue or consumer bottleneck. No build or new real run has been demonstrated in the agent environment; Cargo and PowerShell are unavailable here.

Inspect `websocket_scored_by_kind`: a Direct2 observation does not prove the other three forms. Large pool-count performance, unobserved protocol decoders, production-wide runtime composition and the future execution handoff require their own real evidence.

The uploaded 600-second run at `2ecb489ad28f8474d23695e17eed08329fd3ba94` established the Direct2 dataflow with 16 selected pools, 32 active accounts, 17 WebSocket price events, 34 complete WebSocket-triggered route calculations and 17 WebSocket-triggered candidates at the consumer. All 64 printed route calculations were independently recomputed without a mismatch. The other three route shapes were not price-covered.

The subsequent uploaded run at `cf5f553b419fa07c3c04033ed539dc14e10001ed` used only RPC 5/6 and reached 5,100 selected pools and 9,706 active account subscriptions. Its printed WebSocket evaluation counters covered all four route shapes. Market-State produced 8,851 WebSocket price observations; the last signal-consumer log accounted for only 2,516 completed WebSocket evaluations, with an observed maximum price queue depth of 6,805. Shutdown failed at the unchanged 30-second signal-worker deadline, so there was no successful final report or large-load acceptance. One printed evaluation visited 488,668 combinations, scored 80,092 routes and emitted 40,046 candidates: 27.5287 ms calculation, 172.8301 ms output, 299.3262 ms total processing. These are one input's measurements, not whole-run averages. The 64 printed full route examples were independently recomputed without a mismatch.

## Previous output optimization checkpoint (5e0c4ef)

`signal_report.output_layout` and the worker startup log identify `shared_fixed_legs_v1`:

- The signal worker owns one immutable `Arc<PoolPriceObservation>` snapshot per accepted Price input. The incoming observation is copied once when joining the price cache, not once per candidate leg. Total per-input processing time includes that copy. Replacing/removing the cached price or changing the catalog cannot mutate or invalidate an observation already held by an emitted route; reference counts release it after the cache, output channel and consumers no longer hold it.
- Every route and the completion marker share one immutable trigger per input evaluation. Group generation, IDs, numeric values and binary group addresses remain self-contained in the output; no Registry/DB access is needed to interpret it.
- Route mint/context addresses are `Pubkey` values internally. The existing JSON serializers format Base58 only when the diagnostic consumer actually serializes a selected example. Price-observation metadata retains its existing representation and values.
- `SignalLegs::Two/Three/Four` hold fixed arrays inline; candidate construction allocates no leg vector. Their untagged serialization preserves the previous `legs` JSON array with exactly 2, 3 or 4 entries. Shared triggers/observations serialize to the same JSON objects and addresses remain strings.
- At that revision each qualifying candidate was published immediately as its own typed event, without an evaluation buffer or block staging. The current compiled kernel retains individual typed events and adds the bounded staging described above.

That earlier checkpoint changed only `core/signals.rs` and `workers/graph_signals.rs` plus documentation. It preserved per-route lookups/logarithms and enumeration; the current compiled checkpoint replaces those internals. Shared observations, fixed leg arrays, immutable output and the Core / Workers / Orchestration boundaries are retained.

For a scoped load comparison, repeat the RPC 5/6 command below with the same pool limit, account budget, duration and 0-bps threshold; leave `-TraceSignals` and `-TracePrices` off. Upload `checkout.json`, `websocket_plan.json`, `report.json` and `worker.log` from that run. A successful locked build and real consumer delivery are required before acceptance. Inspect WebSocket-only timings, route/candidate counts and queues together, and require clean completion without lag or timeout. A shorter output time alone does not establish a shorter complete pipeline. Different live inputs do not prove a numerical speedup on identical work. The overall 1-2 ms objective is not an acceptance claim for this change.

## Compiled execution checkpoint

The implementation changes `workers/graph_signals.rs`, adds accounting fields to the existing `core/signals.rs` output contract, updates the existing live runner checks and this documentation. There is one production calculation path. Its plan and numeric caches are private to the signal worker; no business logic moves into Core, no worker-private state is shared, and no new worker, dependency or lockfile change is introduced.

Pool identities have stable private array slots for the worker lifetime. Graph-dependent group IDs, roles and positions belong to exactly one catalog generation. Role arrays are shared for identical directed mint pairs; bindings are linear in role references, not in the concrete Cartesian route count. Observations arriving before catalog inclusion are retained and joined when a generation includes the pool. Withdrawal/Gap clears price coverage without reusing slot identities; already emitted observations remain immutable.

For n left and m right alternatives, the planned affected counts are:

| Form / updated role | Directed routes |
| --- | --- |
| Direct2, catalog | n * (n - 1) |
| Direct2, one pool | 2 * (n - 1) |
| Direct3/Triangle3/Triangle4, left | 2 * m |
| Direct3/Triangle3/Triangle4, right | 2 * n |
| Direct3/Triangle3/Triangle4, fixed role or catalog | 2 * n * m |

During the local live run, require:

- `signal_report.execution_layout` is `indexed_role_plan_v1` and the recorded checkout is the intended commit;
- `websocket_combinations_visited == websocket_routes_expected`, and `websocket_logarithms_prepared == websocket_evaluations`;
- per-evaluation full accounting and candidate delivery checks pass; no channel lag or shutdown timeout;
- real WebSocket calculations/candidates reach the consumer; inspect each `websocket_scored_by_kind` separately;
- `websocket_price_preparation_ns_per_input`, calculation/output/processing distributions, account-to-worker age and complete evaluation-consumer age are reviewed together. Under-five-millisecond pipeline latency remains a live measurement objective, not a source-review claim.

The latest uploaded run at `5e0c4ef` completed with 466 WebSocket inputs, 3,023,492 scored WebSocket routes and 1,511,720 delivered WebSocket candidates. It covered all four route forms, but mean receipt-to-worker age was about 1.502 seconds, maximum completion age about 4.685 seconds, and maximum WebSocket processing 138.532 ms. It contained only about ten seconds of actual WebSocket price traffic. This is historical functional evidence, not acceptance of the new compiled kernel or steady-state latency.

Source arithmetic was also inspected against the 64 complete route examples already uploaded in `worker(3).log`: 156 directed legs reproduced the logged rates, weights and ordered sums without a difference; the maximum basis-point difference from Python return conversion was approximately 1.14e-13. This checks the directed-slot arithmetic on historical real observations, not compilation, enumeration or live execution of the Rust revision.

The agent attempted `cargo build --locked --release`, but Cargo/Rust are unavailable here (`cargo: command not found`); PowerShell, DB and live RPC credentials are also unavailable. This implementation must pass the existing local build gate and real live run. No automated test suite or formatter checks are introduced.

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

The route universe, full ordered sums, candidate delivery and existing loss detection are retained. The compiled role plan reduces visits to affected combinations; inspect both `combinations_available` and `combinations_visited`. New multi-hop coverage is accepted only when the corresponding `websocket_scored_by_kind` counters are positive and the real calculation evidence is present. The local plan does not establish throughput or successful large-scope operation.

## Normal runtime

```dotenv
MARKET_STATE_ENABLED=true
GRAPH_SIGNALS_ENABLED=true
GRAPH_SIGNALS_THRESHOLD_BPS=0
GRAPH_SIGNALS_SAMPLE_LIMIT=16
GRAPH_SIGNALS_TRACE=false
```

`GRAPH_SIGNALS_ENABLED` defaults to false. Enabling it without Market-State fails configuration explicitly. Both workers and their consumers are wired within the existing process; no additional service or network hop is introduced.
