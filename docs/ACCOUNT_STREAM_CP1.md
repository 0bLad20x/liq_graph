# CP1.2: real account reception under load

The RPC transport receives a typed `Vec<AccountWatch>` and publishes immutable account bytes through
`core/account_stream.rs`. It owns sockets, subscription distribution, framing, reconnects and provider
policy. Market-State owns the protocol-specific account requirements, slices and price decoding.
Graph/route evaluation and execution are outside this checkpoint.

## Windows run

```powershell
.\scripts\rpc_account_stream_live.ps1 -PoolLimit 1000 -DurationSeconds 600
```

Default input is the persisted Prepared pool population. `PoolLimit=0` means no pool-count limit;
`10000` is a larger explicit run. Provider capacity always remains enforced. Pools and account
subscriptions are different quantities: reserve-based pools also require vault accounts.

Enable the intended providers in `.env`, for example `RPC_5_WS_ACCOUNT_SUBSCRIBE=true` and
`RPC_6_WS_ACCOUNT_SUBSCRIBE=true`; disable unwanted providers there. Endpoints and these limits
are read per provider:

* `RPC_N_WS_MAX_CONNECTIONS`
* `RPC_N_WS_MAX_SUBSCRIPTIONS` (aggregate account capacity for this provider)
* `RPC_N_WS_SUBSCRIPTIONS_PER_CONNECTION`
* `RPC_N_WS_SUBSCRIBE_SPACING_MS`
* `RPC_N_WS_TIMEOUT_MS` / `RPC_N_WS_RECONNECT_MS`

The script changes no `RPC_N_*` variable. It disables the older global test overrides for its lifetime
(`RPC_WS_PROVIDER_SELECTION=configured`, `RPC_WS_ACCOUNT_BUDGET=0`,
`RPC_WS_CONNECTIONS_PER_PROVIDER=0`) so provider ENV flags and limits are authoritative. Previous
process variables are restored in `finally`. The effective plan omits URLs/credentials and is saved
before preparation. There is no automatic capacity increase, account truncation or provider switch.
The current configuration identifies providers by `RPC_N`, not by a shared API-key identity; operators
must keep the aggregate connections of entries sharing a key inside that key's allowance.

HTTP RPS, in-flight, timeout and retry settings apply to HTTP preparation. They do not pace incoming
WebSocket updates. `WS_SUBSCRIBE_SPACING_MS` paces outgoing Subscribe requests only.

## Prepared account plan

Orchestration restores the existing persistence snapshot into a private Registry and asks Market-State
for the account plan. Market-State selects persisted Prepared identities and reuses its production
HTTP account discovery, identity/owner checks, subscription bindings and slice planner. The same
hydration implementation is used, with baseline price calculation/publication disabled for the probe.
Required accounts are deduplicated; overlapping slices are combined. Missing/unsupported pools fail
preparation with their reasons; they are not silently dropped to produce a successful smaller run.
The plan reports pool counts by venue, pool addresses, account references, unique accounts, preparation
time and whether ENV capacity is sufficient. HTTP preparation ends before measured reception starts.

Persisted Prepared proofs are a historical address source for this transport experiment. They do not
establish live TVL admission, refresh completion manifests or promote any pool in the production
Registry. No admission/enrichment source, graph, signal worker, wallet or transaction execution runs.
Market-State's production global-baseline lifecycle is unchanged and remains CP2.

An explicit real account file remains available without database/Market-State preparation:

```powershell
.\scripts\rpc_account_stream_live.ps1 -AccountsPath .\scripts\rpc_account_stream_accounts.json -DurationSeconds 300
```

The checked-in file has 15 real pool addresses and is a small smoke run only. Custom input is a JSON
array of distinct `address` strings with optional paired `offset` / `length` integers. Omit both for
full accounts. At least four real accounts are required. No fake notifications or prices are injected.

## Measurement phases

The same consumer receives initial half / add all / remove half / restore all / final drain phases.
Halves refer to the deduplicated account list, not complete pool bundles. No prices are evaluated.
Existing account-to-socket assignments are retained across list changes.

`DurationSeconds` now specifies the sum of the four post-ACK observation periods. Each period starts
only after the full desired account set has been acknowledged. Preparation and subscription transitions
are additional elapsed time, with separate timings. A transition has its own timeout (default 600 s,
configurable using `-TransitionTimeoutSeconds`). The final drain waits for all unsubscribe ACKs and
then 1.5 s for the producer's one-second metrics publication. The process keeps consuming throughout
subscription changes. The readiness set is checked until first completion, not scanned per steady update.

Each phase reports `transition` and `steady` separately:

* account updates, distinct updated accounts, decoded account bytes, updates/s and decoded bytes/s;
* peak accepted update counts and decoded bytes in fixed 10-ms and 1-s windows;
* text-read -> consumer and transport-decode -> consumer latency histograms;
* ACK transition time, actual observation time and active account count at the end;
* `continuing_updates_during_transition`: updates for retained accounts before full ACK completion.

The burst windows are fixed, aligned to the start of the respective phase interval, not rolling
windows. They describe acceptance at the consumer; they are not exchange traffic or network packet
measurements. Aggregate `transport_text_messages` and `transport_text_bytes` count incoming JSON text,
including ACKs, before base64 decoding; they exclude TLS/TCP overhead. Per-phase byte rates describe
only decoded account payloads. No per-update printing or receiver-waiting send was introduced.

The consumer compares producer publication totals, source/session sequences, actual payload identities
and lag/clock/gap counters. All phases must reach the desired ACK set; the four observation periods
must receive real account updates and add/remove/restore must retain real ongoing activity. Every
reported source must publish real account updates without framing failures. Any gap, rejection, lag,
sequence/clock error or unequal local producer/consumer account totals fails the live evidence.

`transition_activity_observed` separately requires retained-account notifications during all three
control-change transitions. False means that specific evidence is absent, not that a stall was proved.
A continuity pass alone does not prove load capacity, uninterrupted transition delivery, a latency
ceiling or full-pipeline performance. Assess actual traffic and the reported phase distributions.

## Evidence and limits

The script checks the committed checkout, builds `--locked --release`, verifies Cargo.lock and saves
`checkout.json`, `websocket_plan.json`, `preparation.json` (Prepared mode), `accounts.json`, `report.json`
and `worker.log` in `rpc_account_stream_live/<timestamp>`. The exact account input gets a SHA-256 hash.
Preparation failures leave their plan/log; unexpected early exits get an explicit incomplete report.

Latency values are nanoseconds; percentile fields are histogram bucket upper bounds. Local wall
measurements have microsecond resolution; producer durations use Instant. Timing begins when the
WebSocket stream yields a complete text frame. Provider/network latency, TLS/OS buffering and time
before that yield are not measured. `last_read_end` preserves read errors or close status/reason.
Reconnection and matching local totals do not establish delivery of chain updates missed while offline.

The transport keeps its non-waiting 4096-event broadcast; slow consumers receive explicit lag. Read
and write halves are independently polled; only the writer awaits subscription spacing and writes.
Socket tasks are JoinSet-owned and cancelled on RPC shutdown. At most 32 RPC control requests are
pending per socket. Metadata reconciliation occurs on desired-list changes. This is not a lock-free
or allocation-free claim.

Previous Windows evidence: `fbf9269` built successfully and passed a 300-second, 15-account run:
177/177 locally published/received updates, no gaps/lag, mean 0.097311 ms and maximum 0.181 ms from
text-read to consumer. That run did not reproduce the earlier disconnect; its cause remains unknown.
The CP1.2 changes require a new Windows build and real load run. Cargo/Rust and PowerShell are absent
in the editing environment. No formatter, automated test suite, dependencies or lockfile changes.
