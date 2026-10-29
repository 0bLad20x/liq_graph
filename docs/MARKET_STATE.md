# MarketState

[CP8C](CP8C.md) extends the same liquidity contract with exact level fractions,
ordered raw components and PumpSwap reserve curves. Physical quote liquidity
excludes accrued fee buckets; effective pricing reserves keep the signed virtual
adjustment. Same-rate reserve changes update the existing signal consumers.

The worker owns selected pool identities, account bindings and indicative prices.
Registry is the sole owner of admission and Completion; `rpc_transport` owns HTTP,
subscriptions, ACKs and reconnections. MarketState never modifies manifests or SQL.
Current launch and acceptance instructions: [RUNTIME.md](RUNTIME.md).
[CP8F](CP8F.md) replaces DLMM/PumpSwap HTTP structure hydration with direct
installation from Completion and Mint Registry, followed by the existing post-ACK baseline.

## Selection and installation

The candidate set consists of currently admitted Prepared Registry identities that
match the supplied WatchCatalog generation and canonical mint pair. Selection runs
outside the async price loop. If every eligible pool fits the pool cap and conservative
account budget, `all_fit` selects all of them. Otherwise `budgeted` prioritizes route
packages using completeness, breadth, account cost, retained pools and TVL. Actual
route coverage is computed once. There is no alphabetical historical comparison.

`MARKET_STATE_POOL_LIMIT` defaults to 15000; zero removes only the pool-count cap.
Effective account capacity is the minimum of `MARKET_STATE_WS_ACCOUNT_BUDGET` and
transport capacity. DLMM costs its root plus declared arrays and optional bitmap;
PumpSwap costs four per-pool accounts. Reserve venues cost three, other supported
venues one, plus shared fee/Clock accounts. The actual plan deduplicates addresses.
An account-budget violation fails explicitly. Selection is not a profitability rule.

Structural installation updates pools and account indices once. Dynamic baseline
installation retains pool positions, static account bindings and desired watches.
HTTP baselines remain necessary after subscription ACKs and resyncs. Ticket revisions,
identity checks, component slot floors and cached WebSocket catch-up protect against
stale initializer results. A renewed manifest validation slot keeps static watches;
changed DLMM account dependencies require structural replacement.

## Output and measurement

`core::market_state::MarketStateEvent` contains Price, Unavailable, Removed, Gap,
Selection and Stats. Price carries mint orientation, indicative price, origin,
consistency, component slots, receipt times and pipeline timing. The canonical quote
preference is USDC, then WSOL, then the lexicographically larger mint address.
Recovery after unavailability starts without an adjacent previous price. Removed events distinguish selection changes from shutdown for downstream duration censoring.

MarketState reports counters and bounded timing histograms directly from its owner.
`market_state.json` records published event/price/WS totals, post-rewatch WS prices,
selected/hydrated/priced pool counts before shutdown, account loss, decode failures,
hydration/install/index/watch counters and timings. There is no diagnostic price map
or Direct2 coverage mirror. `TracePrices` explicitly logs events at the publisher;
normal operation does no per-price JSON logging.

With signals active, the actual Signal Engine consumes MarketState output and its
received event/price/WS totals must match. A standalone MarketState proof uses one
stateless counting consumer, which retains no pool or price state.

## Protocol prices

| Venue | Indicative price | Account watches |
|---|---|---|
| Meteora DLMM | Active-bin raw indicator; directed net uses first reachable liquid bin | Full pool, declared BinArrays, optional bitmap, shared Clock |
| Meteora DAMM V2 | Squared Q64.64 sqrt price | Pool field slice |
| Raydium CLMM | Squared Q64.64 sqrt price | Pool field slice |
| Orca Whirlpool | Squared Q64.64 sqrt price | Pool field slice |
| PancakeSwap CLMM | Squared Q64.64 sqrt price | Pool field slice |
| Byreal CLMM | Squared Q64.64 sqrt price | Pool field slice |
| PumpSwap | Effective quote/base reserves, including the supplied layout's virtual quote adjustment | Full pool, two vault amount slices, base Mint supply, shared GlobalConfig/FeeConfig/Clock |
| Raydium AMM V4 | Vault reserves minus accrued PnL | Pool PnL and two vault amount slices |
| Raydium CPMM | Vault reserves minus protocol, fund and creator fee accruals | Pool fee and two vault amount slices |
| Manifest | Midpoint of unexpired, two-sided, uncrossed best bid/ask | Full variable-size market account |

Decoders and price formulas derive from the original standalone producer. Transport, independent pool lists and standalone startup from that prototype are replaced by framework contracts. The decoder rejects an unknown owner/layout; unsupported pools remain unavailable instead of receiving a fabricated price. Raydium Stable has no decoder in that input and is explicitly unavailable.

These are price indicators, not amount-dependent executable route quotes. For
DLMM/PumpSwap, CP8A publishes directed marginal DEX-fee estimates and provenance
(see [CP8A](CP8A.md)). [CP8B](CP8B.md) adds observed DLMM traversal, raw level fill
amounts and integer fee rounding inside the execution account window. Other venues
retain their indicators. Full route slippage, tick traversal, transfer-fee execution
effects and other execution costs remain outside the signal. AMM V4 does not
reconstruct OpenOrders inventory. Manifest midpoint is not a directional fill quote.
One venue's coverage does not validate other unobserved decoders.

## Bootstrap, recovery and removal

1. For DLMM/PumpSwap, install the validated account structure from Registry/manifest and mint metadata from Mint Registry without HTTP. Old PumpSwap proofs missing vault roles receive a full Completion refresh. Other supported venues retain their structure hydration; its failed pools keep a root watch. Failed dynamic baselines keep the complete watch set and retry every 30 seconds.
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


CP8A watches full DLMM/PumpSwap pool accounts plus shared chain Clock, Pump GlobalConfig/FeeConfig and Pump base Mint supply. Shared addresses are deduplicated; see [CP8A](CP8A.md) for fee-only revaluation, unknown token-transfer costs and live acceptance.

CP8A.2 Signal Engine consumes the directed fee-adjusted rates exclusively. Raw
indicative prices remain protocol evidence; they are not a fallback selection
score. Other venues retain their raw indicators, but require a fee model before
participating in net signal evaluation. Rate-equality telemetry is
`fee_valuation.unchanged_net_rate_updates`; it does not suppress observations.
