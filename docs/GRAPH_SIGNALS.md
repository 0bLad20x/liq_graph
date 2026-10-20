# Graph signals: indexed input and compact route output

## Scope and boundaries

The existing Registry owns canonical pool state. Prepared/admitted pools feed the graph catalog and Market-State. `rpc_transport` owns connections/subscriptions, `market_state` owns protocol decoding and normalized indicative prices, and `graph_signals` owns route evaluation. Orchestration provides channels and tasks. The shared Core contracts contain data and lossless block expansion, not routing/admission logic.

This checkpoint replaces repeated input scans and individual route-event construction/publication. It introduces no additional parallelism, worker, service, dependency or lockfile change. It does not pack, sign, simulate or broadcast transactions. V10 group/declaration/account/byte limits remain part of the subsequent execution-job handoff: graph templates are not packed transactions.

## Indexed input

`rpc_transport::ws_session` keeps a pending subscription queue and an address membership set. Target changes reconcile the desired set; ACKs maintain membership incrementally. Ordinary account messages do not rebuild the active/busy sets or scan desired subscriptions. The existing 32-pending limit, per-provider subscribe spacing, capacities, view negotiation, rejection/gap handling and reconnect behavior remain. Notifications for retired subscriptions are still ignored. An address removed while its subscribe ACK is pending is unsubscribed after that ACK; a desired address can be resubscribed after its outstanding unsubscribe reply.

Market-State builds private binary-key indices after hydration: account -> CSR subscription range and pool -> component indices. Shared accounts retain every field/pool binding in the former subscription order. Normal updates inspect only those bindings and the affected pool's components. Owner/address comparisons use `Pubkey`; component slots/timestamps use one local pass without a temporary component vector. Completeness, invalid components, older-slot rejection, baseline floors and protocol price formulas retain their meaning.

Registry identity/admission changes, HTTP hydration and ACK-baseline coordination retain their existing lifecycle. In particular, selection changes still establish a fresh global baseline; incremental pool hydration is not implemented here. Transport/frame timing begins when `ws.next()` yields text, not at provider generation, network arrival or OS-buffer entry.

## Route calculation

Every accepted Price event, including an unchanged price, recalculates every permitted concrete directed route containing the updated pool. Both directions of Direct2, Direct3, Triangle3 and Triangle4 retain the full ordered two/three/four-leg sum. A new catalog generation compiles a replacement private role plan and evaluates its full route universe using available prices. Compilation and catalog evaluation remain inline and can delay later price inputs.

For a base-to-quote price p, the forward rate/weight are p and -ln(p); reverse values are 1/p and +ln(p). Exactly one logarithm is prepared per Price. Each route recomputes W from zero in leg order. The indicative return is `expm1(-W) * 10000` bps, qualifying strictly when `W < -ln1p(threshold_bps / 10000)`. There is no partial-sum cache, ranking, best-price pruning, unchanged-price shortcut or input coalescing.

Calculation retains the existing 256-route blocks and cooperative yield points after at least 4096 visited routes. Missing/nonfinite prices keep their separate classifications. No arbitrary price TTL is added. A stream Gap clears coverage; Unavailable/Removed withdraw the pool. Group IDs belong to one graph generation. Distinct role mint pairs prevent repeated pools in 3/4-leg routes; Direct2 excludes the same pool twice. Different graph templates can represent the same concrete path.

| Form / updated role | Expected directed visits |
| --- | --- |
| Direct2, catalog | n * (n - 1) |
| Direct2, one pool | 2 * (n - 1) |
| Other forms, left alternative | 2 * m |
| Other forms, right alternative | 2 * n |
| Other forms, fixed role or catalog | 2 * n * m |

These are indicative spot comparisons. Fees, sizing, slippage, depth, transfer fees, execution costs and executable profit are not modeled. At threshold zero, approximately half of nonzero reciprocal forward/reverse comparisons qualifying is expected; it is not a profitable-trade rate.

## Compact output contract

`signal_report.execution_layout=indexed_role_plan_v1` and `output_layout=compact_route_blocks_v1` identify this revision.

The typed stream has one route representation, `SignalEvent::Routes(SignalRouteBlock)`, plus Evaluated and Invalidated. Each existing calculation block publishes its required outputs immediately in slices of at most 64 rows. It never waits for a timer, another input, a full output block or a consumer acknowledgement. Broadcast publication remains synchronous without waiting for receiver capacity.

A block shares generation, group, route context, trigger and threshold. Rows retain sequence, directed leg indices, full sum, return and qualifying flag in the former output order. Only qualifying routes and the existing bounded nonqualifying samples are materialized, exactly as before. A private epoch/index array deduplicates identical directed legs within the block. Each leg holds an immutable observation reference and its rate, weight and mint direction. Later price updates, removals or catalog replacements cannot alter queued results. There is no reference into mutable worker state.

`SignalRouteBlock::route(index)` expands any row into the previous complete RouteEvaluation data. Diagnostic JSON still prints Candidate/RouteSample examples; the consumer expands only rows selected for printing. Other rows remain fully present and reconstructable. The old per-candidate typed event path is removed.

One timestamp is taken at the start of block construction. Expanded rows share that `emitted_at_us`; `account_to_emitted_us` therefore measures age at block construction rather than an individual row's creation. Consumer candidate ages use one block-receipt timestamp and preserve candidate-weighted histograms via counted additions. This reduces per-row clocks and bookkeeping. Numeric route values and information needed to interpret them remain unchanged; timing boundaries differ from the previous output layout.

The output channel holds 256 block/control events. At 64 rows per block, at most 16384 route rows are retained in the channel, matching the previous maximum route-event count. Control events reduce that bound. Each block can contain at most 256 directed legs; two vectors and one shared event allocation replace per-row event allocations. These are payload bounds, not a claim of identical allocator/RSS usage. In-flight blocks and diagnostic row expansions also consume memory.

## Accounting, errors and measurements

The producer owns `signal_production_report`; the consumer owns `signal_report`. Output loss cannot erase completed-input producer measurements. Acceptance compares published/received channel events, route rows, evaluations and candidates, including WebSocket evaluation/candidate totals. Per-evaluation accounting requires:

`combinations_visited == routes_expected == routes_affected == routes_scored + routes_missing_prices + routes_nonfinite`

Every accepted Price prepares one logarithm and uses the existing plan. Completed-input counters omit an evaluation interrupted by an error. Producer errors and consumer integrity errors are checked by standalone and normal-runtime orchestration.

`published_events`, `received_events`, `lost_events` and queue depths now count **block/control events**, not individual candidates. Compare `published_route_rows` against `received_route_rows`, and producer candidate counts against consumer candidate counts. A lost block can contain multiple candidates; its row count cannot be inferred from a Lagged notification alone. Every loss invalidates acceptance.

`-ProfileSignals` temporarily sets `GRAPH_SIGNALS_PROFILE=true`. The diagnostic receiver counts output lag and continues recording; ordinary mode stops on lag. Price-input loss and calculation errors remain fatal. `run_complete=true` only means the requested duration and shutdown finished. `live_evidence_passed` additionally requires lossless delivery and the existing live Market-State/calculation/candidate evidence. A quiet market with no qualifying WebSocket candidate cannot prove candidate delivery.

Producer `all_inputs` and `websocket_inputs` contain fixed-memory histograms:

| Field | Boundary |
| --- | --- |
| `transport_decode_us` | Text-frame read to decoded transport notification. |
| `transport_to_market_us` | Decoded notification to Market-State acceptance, including queue/scheduling. |
| `market_processing_us` | Market-State acceptance to price publication boundary. For a shared account this includes preceding affected bindings. |
| `price_queue_us` | Price publication boundary to signal-worker acceptance, including allocation/send/queue/scheduling. |
| `frame_to_worker_us` | Text-frame read to signal-worker acceptance. |
| `preparation_ns` | Signal price parsing, validation, logarithm and frozen observation storage. |
| `calculation_ns` | Block enumeration, dense-array ordered sums, return conversion, classification and pending output staging. |
| `event_build_ns` | Shared block context, frozen-leg deduplication, rows and event allocation. |
| `publication_ns` | Synchronous broadcast publication, counters and queue-depth sample. |
| `cooperative_yield_ns` | Wall time across the existing yields, including rescheduling. |
| `processing_ns` | Signal acceptance through calculation/output/accounting checks; ends before producer aggregates and completion-marker publication. |

`websocket_frame_to_evaluation_consumer_us` adds complete processing and marker handoff to the frame boundary. Existing account-age fields retain the decoded local transport timestamp with millisecond resolution. `completion_to_consumer_us` includes producer aggregate recording, marker allocation/send, queueing and consumer scheduling; it is not pure channel latency. Histograms use wall time: other preemptions remain in their enclosing measurements. Clock reversals omit affected durations; complete WebSocket timing counts are required for delivery acceptance.

Profile logs include `graph signals window` at the existing five-second consumer ticker. Each window summarizes received WebSocket evaluation markers, pipeline stages and processing maxima/means, then resets. Use these windows to distinguish startup from continuing load. Output loss makes consumer windows incomplete; the final independent producer report remains authoritative for completed work. Final histograms aggregate the run and do not themselves separate steady state.

Bookkeeping is sampled once per 257 received block/control events, excluding measured row expansion/JSON/printing time. It is a per-event sample, not per-row CPU time, and excludes channel receive/select and event destruction. Diagnostic example formatting/output and periodic logging wall time are reported separately. Histogram percentiles are logarithmic bucket upper bounds, not exact percentiles. Maxima/percentiles of separate stages cannot be added. Do not compare raw queue depths/event rates across the changed output units.

The runner writes partial reports on returned setup/task/shutdown errors. Timeout/panic/forced termination may leave worker reports null. If the worker exits without a report, the PowerShell runner writes an explicit failed placeholder. Process/file-system failure can still prevent persistence. Script failures retain diagnostics and restore temporary process environment values.

## Local build and live run

Use the existing database, admission/enrichment and RPC configuration from MARKET_STATE.md. No wallet is needed. The standalone mode starts no execution, pool-cache writer or Completion worker and does not refresh cached manifests.

```powershell
.\scripts\graph_signals_live.ps1 -DurationSeconds 900 -PoolLimit 10000 -ThresholdBps 0 `
    -WsProviders "5,6" -WsAccountBudget 30000 -WsConnectionsPerProvider 1 -ProfileSignals
```

Leave TracePrices/TraceSignals off for a comparable load run. The script requires a clean tracked checkout, builds with `cargo build --locked --release`, checks Cargo.lock, and saves checkout.json, websocket_plan.json, report.json and worker.log. Upload all four even on failure. Pool/account limits are upper bounds; successful ACKs and actual price coverage determine observed scope.

| WebSocket option | Meaning |
| --- | --- |
| `Configured` | Only providers enabled by RPC_N_WS_ACCOUNT_SUBSCRIBE, with configured capacities. |
| `All` | Every configured nonempty WS URL; endpoint support is still unproven. |
| `"5,6"` | Only those configured indices, overriding their account-subscribe selection flags. |
| `-WsAccountBudget 30000` | Aggregate local test capacity, evenly shared across selected providers. Zero preserves configured capacities. |
| `-WsConnectionsPerProvider 1` | Socket count per selected provider with an explicit budget; zero preserves configuration. |
| `-PlanOnly` | Build and save the resolved plan without DB reads/subscriptions. |

These are local settings, not verified provider service limits. HTTP provider selection remains independent. Existing pacing/timeouts/reconnect settings apply. The plan omits endpoint URLs/credentials and proves no subscription ACKs. `-Rewatch` exercises the existing pause/resume; issue #34 tracks its diagnostic counter limitation.

Normal-runtime keys remain MARKET_STATE_ENABLED=true, GRAPH_SIGNALS_ENABLED=true, GRAPH_SIGNALS_THRESHOLD_BPS=0, GRAPH_SIGNALS_SAMPLE_LIMIT=16 and GRAPH_SIGNALS_TRACE=false. Signal processing requires Market-State. GRAPH_SIGNALS_PROFILE defaults to false.

## Evidence and acceptance status

The uploaded f5c1942 run processed all 16674 published prices, including 11668 WebSocket inputs, and scored 183107098 WebSocket routes. Its old individual-event output lost 36167 events in 117 incidents. Within the signal worker, event construction/publication averaged 3.328 ms of 4.526 ms total WebSocket processing. Mean pre-worker age was 32.084 ms; complete-consumer age was 37.233 ms. The first price output followed roughly 605 seconds of startup, leaving about 295 seconds of actual WebSocket processing. These are historical measurements, not performance proof for this revision.

Source review covers subscription transitions, multi-pool account bindings, frozen-leg mapping, ordered row expansion and accounting. The agent attempted cargo build --locked --release, but Cargo/Rust are unavailable here; PowerShell and live DB/RPC access are also unavailable. No automated test suite or formatter is introduced. The current revision requires the local locked build and a successful lossless real run. Check all four websocket_scored_by_kind counters and the real arithmetic examples separately. Stable sub-five-millisecond processing and pipeline latency remain measurement objectives; no achieved speedup or completed acceptance is claimed.
