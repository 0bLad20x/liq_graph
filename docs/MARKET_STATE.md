# Market-State stream

## Scope and boundaries

`market_state` receives the canonical Registry state and its typed update stream. It selects Prepared, admitted pools, reads their live accounts through the existing `rpc_transport`, decodes indicative prices and emits `core::market_state::MarketStateEvent`. It never changes pool admission, completion state, manifests or database pool rows.

The Registry remains the sole pool-state writer. Completion lifecycle continues to refresh execution manifests. Market-State subscribes to the accounts required for price observations; those are a separate concern from manifest freshness.

`rpc_transport` owns provider configuration, HTTP limits/retries, WebSocket connections, account assignment, ACKs and reconnects. Its extension accepts an account plus a requested view (full account or byte slice). There is one desired watch-set owner per transport instance. The runtime assigns it to Market-State; existing Completion workers continue to use HTTP. Do not run an additional watch-set owner on that same handle.

The consumer joins prices to the existing generational `WatchCatalog` and reports price coverage of actual Direct2 pairs. The topology graph still contains Prepared pools without prices. Future priced route evaluation must restrict itself to pools with current observations. This checkpoint does not yet implement logarithmic edge weights, cycle detection, thresholds, candidate ranking or V10 execution jobs.

## Input and selection

- Production `--run`: current Registry snapshot, followed by Registry notifications. A five-second reconciliation rechecks selected pools; a lagged notification stream rebuilds the derived identity index from the current Registry. Losing Prepared/admitted status removes a selected pool and its derived price. Newly eligible pools enter the bounded selection.
- `--market-state-live`: persisted Registry proof seed, the existing CP1 mint qualification/Jupiter/DexScreener admission flow, and a real RPC/WebSocket price stream. PostgreSQL restore intentionally leaves live TVL unknown, so cached Prepared status alone cannot enter the admitted watch set or graph. The single Registry writer establishes current admission from real enrichment results; the common gate remains unchanged. Startup logs distinguish `persisted_prepared` from the admitted Prepared count in `Selection.prepared`. An initial empty selection is expected during enrichment warmup; it must advance once qualified pools receive current TVL.
- This finite mode starts no protocol pool discovery, Completion, execution or pool-cache persistence writer. It does not refresh cached manifests. Existing targeted Jupiter processing may update its mint catalogue; existing persistence schema initialization is unchanged. Historic TVL is never promoted to current TVL.
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

Use the existing `DATABASE_URL` (or `LIQ_GRAPH_DATABASE_URL`), `JUPITER_SEARCH_API_KEYS`, CP1 mint-catalogue/HTTP-proxy configuration and `RPC_N_*` configuration. The standalone entrypoint now runs the same existing admission producers that production uses; DexScreener requires verified working proxies. No signer or wallet is needed. An RPC WebSocket URL alone does not enable account subscriptions. Each provider used for WS must have explicitly configured account capabilities and capacity, for example:

```dotenv
RPC_1_WS_ACCOUNT_SUBSCRIBE=true
RPC_1_WS_MAX_CONNECTIONS=1
RPC_1_WS_MAX_SUBSCRIPTIONS=100
RPC_1_WS_SUBSCRIPTIONS_PER_CONNECTION=100
RPC_1_WS_SUBSCRIBE_SPACING_MS=20
```

The numbers above are an example configuration, not a statement about a provider's permitted limits. Set them to your actual plan. Additional `RPC_2_*`, `RPC_3_*` providers use the same mechanism. HTTP and WS need to refer to the same Solana network and compatible commitment. Existing HTTP capability flags, rate limits and `RPC_ACCOUNT_BATCH_SIZE` are respected.

```dotenv
# Enable in the normal full runtime; standalone live mode starts it directly.
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
git switch agent/market-state-stream
if ($LASTEXITCODE -ne 0) { throw "Branchwechsel fehlgeschlagen; lokale Änderungen zuerst sichern." }
git pull --ff-only
if ($LASTEXITCODE -ne 0) { throw "Synchronisierung fehlgeschlagen." }
.\scripts\market_state_live.ps1 -DurationSeconds 600 -PoolLimit 1000
```

The script records branch, exact HEAD and working-tree status, runs `cargo build --locked --release`, verifies lockfile hygiene and captures `worker.log`, `checkout.json` and `report.json` under `market_state_live/<UTC timestamp>/`. Default output samples the first 16 baselines, first 16 live pool updates and first 16 unavailable/removal events; counters cover the entire stream. `-TracePrices` prints every event.

Orchestration retains the price sender and RPC handle until the producer result is joined, then releases them for normal consumer/transport shutdown. This prevents an early producer failure from being reported only as an unexpectedly closed consumer or RPC task. A failed startup may have no final `report.json`; inspect the original producer error in `worker.log`.

During startup, expect `mint qualification published qualified=...`, `dexscreener cp1 continuous started=true`, Registry TVL updates, then a non-empty Market-State selection and subscription ACKs. If qualification or proxy initialization produces no usable results, the run must not be accepted merely because it stays alive. `report.json` includes restored Prepared and live TVL counts.

A successful finite run requires real WS price updates and at least one actual graph Direct2 pair with prices for two selected pools, including a WS observation on that pair. Lack of an arbitrage opportunity is irrelevant here. Inspect `websocket_venues` to see which protocol decoders received real live updates; unobserved venues remain unproven.

To exercise removal and resubscription using the same real pools:

```powershell
.\scripts\market_state_live.ps1 -DurationSeconds 600 -PoolLimit 1000 -Rewatch
```

At half-time this disables observations for six seconds and enables them again. It does not alter Registry states or invent a pool update. The run requires reselection, removals and subsequent WS prices; `rpc_transport` debug ACK logs additionally expose actual `unsubscribed=...` and `subscribed=...` responses. This verifies observation-control removal, not a live Prepared-to-Incomplete transition. That transition uses the same production selection reconciliation but needs a real Registry lifecycle event for a separate live proof.

A build is only a prerequisite. The user successfully built committed `9364b64` with `cargo build --locked --release`; its live run exposed the missing standalone admission wiring (23,722 persisted Prepared pools, zero admitted identities). The follow-up corrects orchestration using the existing admission producers. This follow-up still needs its own build/live evidence; no Cargo or real credentials are available in the implementation environment. Accept this checkpoint only after reviewing the actual live report/logs.
