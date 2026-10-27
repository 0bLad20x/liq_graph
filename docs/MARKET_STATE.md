> CP7A current run instructions and acceptance: [CP7A_RUNTIME.md](CP7A_RUNTIME.md).
> Finite and continuous full-pipeline runs now share one orchestration implementation.
> Earlier acceptance records below remain historical evidence for their stated commits.

# Market-State stream

## Scope and boundaries

`market_state` receives the canonical Registry state and its typed update stream. It selects Prepared, admitted pools, reads their live accounts through the existing `rpc_transport`, decodes indicative prices and emits `core::market_state::MarketStateEvent`. It never changes pool admission, completion state, manifests or database pool rows.

The Registry remains the sole pool-state writer. Completion lifecycle continues to refresh execution manifests. Market-State subscribes to the accounts required for price observations; those are a separate concern from manifest freshness.

`rpc_transport` owns provider configuration, HTTP limits/retries, WebSocket connections, account assignment, ACKs and reconnects. Its extension accepts an account plus a requested view (full account or byte slice). There is one desired watch-set owner per transport instance. The runtime assigns it to Market-State; existing Completion workers continue to use HTTP. Do not run an additional watch-set owner on that same handle.

The consumer joins prices to the existing generational `WatchCatalog` and reports price coverage of actual Direct2 pairs. The topology graph still contains Prepared pools without prices. Future priced route evaluation must restrict itself to pools with current observations. This checkpoint does not yet implement logarithmic edge weights, cycle detection, thresholds, candidate ranking or V10 execution jobs.

## Input and selection

- Production `--run`: current Registry snapshot, followed by Registry notifications. A five-second reconciliation rechecks selected pools; a lagged notification stream rebuilds the derived identity index from the current Registry. Losing Prepared/admitted status removes a selected pool and its derived price. Newly eligible pools enter the bounded selection.
- Finite `--run` uses the same persisted Registry proof seed, the existing CP1 mint qualification/Jupiter/DexScreener admission flow, and a real RPC/WebSocket price stream. PostgreSQL restore intentionally leaves live TVL unknown, so cached Prepared status alone cannot enter the admitted watch set or graph. The single Registry writer establishes current admission from real enrichment results; the common gate remains unchanged. Startup logs distinguish `persisted_prepared` from the admitted Prepared count in `Selection.prepared`. An initial empty selection is expected during enrichment warmup; it must advance once qualified pools receive current TVL.
- CP7A finite runs use the normal discovery, CompletionLifecycle and Persistence workers.
  `-Stage MarketState` disables downstream signals/simulation, not Completionrefresh.
  Historic TVL is never promoted to current TVL.
- `MARKET_STATE_POOL_LIMIT=1000` by default. `0` removes this pool limit; configured provider/account limits remain in force.
- Selection is deterministic by unordered mint pair, then address. This keeps pair alternatives together but is not a profitability ranking. It does not guarantee that a capped selection contains every venue or a trade opportunity.
- The limit counts pools. Required subscriptions count distinct account addresses. Reserve-based venues may require three accounts per pool; shared accounts are deduplicated. Insufficient configured WS capacity fails explicitly with the required and available counts. There is no silent capacity truncation.
- The initial RPC 5/6 setup uses 24 account subscriptions per provider, 48 total. Use `-PoolLimit 16` for that acceptance run. Raising only the pool limit to 10,000 does not expand provider account capacity. Increase observation scope only with matching configured and provider-permitted capacity.
- A manifest refresh with unchanged pool identity does not rebuild subscriptions. A changed selection does re-establish a fresh baseline.

## Output contract

`Price` includes pool/venue, on-chain mint order, normalized base/quote order, price, origin, price kind, consistency, component-slot range, component receipt-time range, receipt timestamp and sequence. Prices use human quote-token units per one human base-token unit. Quote preference is USDC, then WSOL, then the lexicographically larger mint address. The inverse is used when on-chain orientation differs. Thus prices of the same pair are comparable across venues.

`previous_price` and `delta_bps` describe adjacent observations of the same pool. Recovery from an unavailable state starts with no previous price. These fields are not a signal rule.

`Unavailable` removes a pool's usable derived price. `Gap` invalidates consumer coverage until fresh baselines/updates arrive. `Removed` withdraws a pool from observation. `Selection.prepared` counts admitted Prepared identities. `Selection` and `Stats` expose the selected addresses, account demand, active ACK state and stream counters. Sequence numbers belong to a producer run. The broadcast stream is not durable; a lagging consumer fails explicitly rather than retaining apparently valid coverage.

Freshness is observable through slots and receipt times. There is no new arbitrary TTL: a quiet pool can legitimately remain unchanged. Stream gaps, decode failures and removals invalidate observations. Reserve-component updates can span slots; even equal slots do not prove an atomic multi-account snapshot. Consumers must retain the quality metadata.

## Protocol prices

| Venue | Indicative price | Account watches |
|---|---|---|
| Meteora DLMM | Active bin and bin step, decimal normalization | Pool field slice |
| Meteora DAMM V2 | Squared Q64.64 sqrt price | Pool field slice |
| Raydium CLMM | Squared Q64.64 sqrt price | Pool field slice |
| Orca Whirlpool | Squared Q64.64 sqrt price | Pool field slice |
| PancakeSwap CLMM | Squared Q64.64 sqrt price | Pool field slice |
| Byreal CLMM | Squared Q64.64 sqrt price | Pool field slice |
| PumpSwap | Effective quote/base reserves, including the supplied layout's virtual quote adjustment | Pool field and two vault amount slices |
| Raydium AMM V4 | Vault reserves minus accrued PnL | Pool PnL and two vault amount slices |
| Raydium CPMM | Vault reserves minus protocol, fund and creator fee accruals | Pool fee and two vault amount slices |
| Manifest | Midpoint of unexpired, two-sided, uncrossed best bid/ask | Full variable-size market account |

Decoders and price formulas derive from the attached standalone `market_state_producer(1).rs`. Transport, independent pool lists and standalone startup from that prototype are replaced by framework contracts. The decoder rejects an unknown owner/layout; unsupported pools remain unavailable instead of receiving a fabricated price. Raydium Stable has no decoder in that input and is explicitly unavailable.

These are price indicators, not amount-dependent executable quotes. They omit swap fees, slippage, DLMM bin traversal, tick liquidity traversal, transfer-fee execution effects and other execution costs. AMM V4 does not reconstruct OpenOrders inventory. Manifest midpoint is not a directional fill quote. Successful live coverage of one venue does not validate every other venue's layout; the final report lists observed WebSocket venues.

## Bootstrap, recovery and removal

1. Resolve selected pool identities, mint decimals and necessary reserves over shared HTTP; isolate failed pools. A failed pool retains a root-account watch and is retried every 30 seconds.
2. Deduplicate account watches and request slices where supported. Providers that explicitly reject a slice with invalid-parameter errors are retried with full accounts; the worker extracts its own fields. Providers that ignore slices and return full data are also handled when response metadata identifies the full account.
3. Wait for subscription ACKs and establish an HTTP baseline after subscriptions. Consume incoming transport notifications while hydration is pending. Only pools with all required active subscriptions can publish usable prices.
4. Discard queued notifications at or below each component's baseline slot until the first strictly newer slot. Thereafter preserve WS order within a slot and reject older-slot updates. This avoids overwriting the HTTP baseline with an ambiguously ordered notification.
5. A disconnect invalidates coverage and requires ACKs plus a fresh baseline. Missing, deleted or invalid components remove the usable pool price. An invalid component must itself recover before another component can restore that pool.
6. Removing Prepared eligibility or changing the observation selection updates the desired subscriptions and withdraws prices. Stop cancels hydration as well as idle streaming and clears the desired watch set.

Provider assignment preserves existing addresses and balances new accounts across configured sockets. It does not promise automatic migration from a permanently unavailable provider to another provider. Failed sockets reconnect with the existing retry policy, including handshake HTTP 429 backoff.

## Configuration

Use the existing `DATABASE_URL` (or `LIQ_GRAPH_DATABASE_URL`), `JUPITER_SEARCH_API_KEYS`, CP1 mint-catalogue/HTTP-proxy configuration and `RPC_N_*` configuration. The finite entrypoint is the production pipeline; DexScreener requires verified working proxies. No signer or wallet is needed. An RPC WebSocket URL alone does not enable account subscriptions. Each provider used for WS must have explicitly configured account capabilities and capacity, for example:

```dotenv
RPC_1_WS_ACCOUNT_SUBSCRIBE=true
RPC_1_WS_MAX_CONNECTIONS=1
RPC_1_WS_MAX_SUBSCRIPTIONS=100
RPC_1_WS_SUBSCRIPTIONS_PER_CONNECTION=100
RPC_1_WS_SUBSCRIBE_SPACING_MS=20
```

The numbers above are an example configuration, not a statement about a provider's permitted limits. Set them to your actual plan. Additional `RPC_2_*`, `RPC_3_*` providers use the same mechanism. HTTP and WS need to refer to the same Solana network and compatible commitment. Existing HTTP capability flags, rate limits and `RPC_ACCOUNT_BATCH_SIZE` are respected.

```dotenv
# Enable in the common runtime; runtime_live.ps1 sets this for finite proofs.
MARKET_STATE_ENABLED=true
MARKET_STATE_POOL_LIMIT=1000
MARKET_STATE_TRACE=0
```

`Stats.ws_text_bytes` counts received WS text-frame payload bytes, including JSON/base64 overhead. It excludes HTTP traffic, TLS/TCP framing and provider-specific billing multipliers. It is not a billing estimate.

## Local live run

Use the committed feature checkout. Preserve any local tracked changes before switching; the script never resets, cleans or stashes user files.

```powershell
git fetch origin
if ($LASTEXITCODE -ne 0) { throw "Fetch fehlgeschlagen." }
git switch --track origin/cp7a-unified-runtime
if ($LASTEXITCODE -ne 0) { throw "Branchwechsel fehlgeschlagen; lokale Änderungen zuerst sichern." }
git pull --ff-only
if ($LASTEXITCODE -ne 0) { throw "Synchronisierung fehlgeschlagen." }
.\scripts\runtime_live.ps1 -Stage MarketState -DurationSeconds 900 -PoolLimit 1000
```

The script records branch, exact HEAD and working-tree status, runs `cargo build --locked --release`, verifies lockfile hygiene and captures `runtime.log`, `checkout.json`, `runtime.json` and `market_state.json` under `runtime_live/<UTC timestamp>/`. Default output samples the first 16 baselines, first 16 live pool updates and first 16 unavailable/removal events; counters cover the entire stream. `-TracePrices` prints every event.

Orchestration retains the price sender and RPC handle until the producer result is joined, then releases them for normal consumer/transport shutdown. This prevents an early producer failure from being reported only as an unexpectedly closed consumer or RPC task. Setup errors are retained in `runtime.json` where possible; inspect the original error in `runtime.log`.

During startup, expect `mint qualification published qualified=...`, `dexscreener cp1 continuous started=true`, Registry TVL updates, then a non-empty Market-State selection and subscription ACKs. If qualification or proxy initialization produces no usable results, the run must not be accepted merely because it stays alive. The runtime log includes bootstrap and admission counts; `market_state.json` contains the observer totals.

A successful finite orchestration proof requires real prices and WS price updates. Direct2 pair coverage remains a reported metric rather than a requirement for this general dataflow proof. With signals enabled, a fully priced topology and complete signal delivery remain mandatory. Inspect `websocket_venues` to see which protocol decoders received real live updates; unobserved venues remain unproven.

To exercise removal and resubscription using the same real pools:

```powershell
.\scripts\runtime_live.ps1 -Stage MarketState -DurationSeconds 900 -PoolLimit 1000 -Rewatch
```

At half-time this disables observations for six seconds and enables them again. It does not alter Registry states or invent a pool update. The run requires reselection, removals and subsequent WS prices; The common observer verifies removals, reselection and post-rewatch WebSocket prices. This verifies observation-control removal, not a live Prepared-to-Incomplete transition. That transition uses the same production selection reconciliation but needs a real Registry lifecycle event for a separate live proof.

## Confirmed live acceptance

The user supplied a successful 600-second `-PoolLimit 16 -Rewatch` run and its `checkout.json`, identifying branch `agent/market-state-stream`, application commit `9ed2e8fa20a4a3fffa967a0af341e135f406cbcd`. The recorded checkout contains only untracked local tools; no tracked modifications are recorded. The live script enforces `cargo build --locked --release` and an unchanged lockfile before starting the worker. This is user-local build/live evidence, not a build performed in the implementation environment.

| Evidence | Result |
|---|---|
| Final result | `live_evidence_passed=true`, `error=null`, regular shutdown |
| Watched pools / active accounts | 16 / 32 |
| Price observations | 138: 32 baseline observations across both phases, 106 WS observations |
| WS pool coverage | 7 pools |
| WS venue coverage | Meteora DLMM 36, Raydium AMM V4 36, Raydium CLMM 6, Raydium CPMM 28 |
| Actual unsubscribe ACKs | 32: 16 on each of RPC 5 and 6 |
| Actual subscribe ACKs after re-enable | 32: 16 on each of RPC 5 and 6 |
| Actual WS observations after re-enable | 78, derived from cumulative counters: 106 final minus 28 before the pause |
| Maximum price-covered Direct2 pairs | 3; 2 included a real WS observation |
| Unavailable pools / Registry lag | 0 / 0 |
| Final fresh TVL / admitted Prepared | 13,441 / 5,075 |

Observation control was disabled at `1792447093773` ms and enabled at `1792447099776` ms. During that pause the consumer had zero prices and the transport had zero active accounts. New baselines and WS price observations followed re-enable. The run completed at `1792447397053` ms. The supplied log excerpt is `Eingefügter Text(20261019-220540).txt` (SHA-256: `33411d18954a4bb95df77e4d116b45b3f883a9b169f9839658df543baca9bdc8`). The separate user-supplied checkout metadata binds this run to the exact application commit above.

This accepts the shared Registry -> RPC accounts -> normalized price stream -> graph-catalog consumer flow and the controlled subscription lifecycle for the observed scope. Other protocol decoders, a real network-disconnect recovery, a real Prepared-to-Incomplete withdrawal, larger observation sets and the complete production `--run` composition remain outside this live proof. There is no claim of arbitrage detection or execution handoff.

Known diagnostic defect: `post_rewatch_websocket_observations` reports 106 because the initial empty startup selection activates its rewatch counter. The correct post-rewatch count is 78, independently established from the ACK timeline and cumulative counters above. This is tracked in [issue #34](https://github.com/0bLad20x/liq_graph/issues/34). Acceptance uses that actual timeline. Application code is kept at the live-tested version; this acceptance record changes documentation only.
