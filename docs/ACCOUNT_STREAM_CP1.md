# CP1: independent account reception

Scope: source configuration + desired account watches -> real account bytes -> independent consumer.
No database, Registry, admission/enrichment, Market-State, graph, wallet or execution runs in this mode.

## Contract and implementation

`core/account_stream.rs` owns `AccountWatch`, `AccountView`, `AccountData`, `AccountEvent`,
`AccountEventStream` and the single desired-watch controller. Commitment is source configuration
(`RPC_COMMITMENT`, default `confirmed`). Account events carry a source/socket identity, local
session and notification sequence, slot, requested/actual view, raw bytes or deletion and local times.
The local sequence is not an on-chain write version or a total order across sources.

The RPC worker reuses that contract for HTTP account values. Its WebSocket implementation is
private in `workers/rpc_transport/ws.rs`. `Market-State` now receives stream control and events
explicitly from orchestration. Its pool selection, global baseline, decoders and price formulas
are unchanged; the global-baseline problem belongs to CP2.

The socket's read and write halves are polled independently. The reader never awaits a subscription
sleep, socket send/flush or downstream receiver. A private writer applies subscription pacing,
handles ping/pong flushing and reports control timings. At most 32 subscription/unsubscription
requests are pending. Reply deadlines are absolute from local request enqueue and are not restarted
by ordinary notifications. Target changes reconcile metadata; ordinary account updates only look
up their subscription ID. A required view change reconnects and emits a continuity gap.

JSON-RPC/base64 framing is decoded in the transport worker with typed borrowed fields. Pool bytes
are not interpreted. Each account payload is wrapped once in `Arc`; a broadcast receiver clones
the reference, not the byte vector. The existing 4096-event broadcast remains non-waiting and
reports lag to receivers. This is not a lock-free or allocation-free claim. Any gap, rejection,
decode failure, sequence error, lag or unequal producer/consumer totals fails this live audit.

All socket tasks are owned by a JoinSet. Dropping/stopping the RPC task cancels them and their
internal writer futures; socket task errors propagate to the RPC task.

## Real live run on Windows

```powershell
.\scripts\rpc_account_stream_live.ps1 -DurationSeconds 300 -WsProviders "5,6"
```

The default watch file contains 15 real pool addresses from the previously uploaded live evidence.
Its first two addresses had actual WebSocket observations in the f5c1942 run. It is a small transport
proof, not a 10000-account load proof. Account activity can change; an ACK-only or quiet run fails.
Only providers 5 and 6 are selected by default, with one socket each and 64 aggregate account slots.
These settings are local limits, not verified provider allowances. The standalone path performs no
HTTP RPC requests even if HTTP providers are configured.

For larger real watch lists:

```powershell
.\scripts\rpc_account_stream_live.ps1 -DurationSeconds 900 `
    -AccountsPath .\my_accounts.json -WsProviders "5,6" -WsAccountBudget 30000
```

The JSON file is an array. An account may request a full value or a nonempty slice:

```json
[
  {"address":"REAL_ACCOUNT_ADDRESS"},
  {"address":"ANOTHER_REAL_ACCOUNT","offset":76,"length":6}
]
```

Use at least four distinct real addresses; the example above only illustrates the file format.
No fixture values or invented notifications are injected into the worker.

Four equal timed phases subscribe half the list, add the remainder, remove the added half, and
restore the full list. Existing source assignments are retained. A final drain removes all watches
and continues consuming until unsubscribe ACKs and a 1.5-second metrics settlement, up to 60 seconds.
The reported duration excludes that final drain. Each timed phase must acknowledge its full desired
set and deliver real notifications. Add/remove/restore must also deliver notifications for retained
accounts. Every selected socket must publish real account updates.

`updates_before_transition_complete` additionally shows notifications received before the phase's
ACK transition finished. A zero value does not prove a stall: a quiet account may not update in
that short interval. The audit does not claim uninterrupted delivery during a transition without
those observations; use a larger active watch set to collect that evidence.

## Evidence and timing

The script requires a clean tracked checkout, builds `--locked --release`, verifies Cargo.lock,
restores temporary process variables, and saves `checkout.json`, `websocket_plan.json`, `accounts.json`,
`report.json` and `worker.log` in `rpc_account_stream_live/<timestamp>`.

The producer separately counts text frames, account notifications, published accounts, retired
notifications, subscription/unsubscription ACKs, rejected requests, decode errors, gaps and sessions.
The consumer reports source totals, sequence/lag/clock errors, observed queue depth and bounded real
payload examples. Producer publication totals must equal consumer account totals.

Times are nanoseconds; percentile fields are histogram bucket upper bounds:

* `producer.sources[].decode`: text read through transport decode and payload creation.
* `publication`: immediate channel send and publication bookkeeping.
* `control_pacing` / `control_write`: writer wait/send times, independent of the reader.
* `watch_reconcile` / `control_enqueue`: CPU time spent on desired-watch reconciliation and bounded
  request staging in the session; these are not part of the per-account decode interval.
* `consumer.decode_to_consumer`: decode timestamp through consumer acceptance, including scheduling.
* `consumer.frame_to_consumer`: complete local text-read -> consumer interval.
* `transition_frame_to_consumer` / `steady_frame_to_consumer`: before/after the current phase's
  first complete ACK set. These are per-phase readiness classifications, not provider latency.

Local wall timestamps have microsecond resolution; producer durations use Instant. Provider,
network, TLS/OS buffering and time before the socket yields a text message are not measured by
`frame_to_consumer`. No zero-latency guarantee or achieved throughput is inferred from the code.
`live_evidence_passed` proves continuity/activity in the observed run; latency values and the
tested input volume must be assessed separately before scaling. This is not a full pipeline proof.

Agent verification: Cargo/Rust and PowerShell are unavailable in the editing environment.
The local locked build and real live run are pending. No formatter, automated test suite,
dependency change or manual lockfile change is included.
