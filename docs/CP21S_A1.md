# CP21S-A.1 – Full Economic Coverage and Historical Timeline Evidence

**Branch:** `cp21s-a1-signal-timeline-evidence`, based on CP21S-A.
**Mode:** Direct2 + Economic, simulation-only, no broadcasts.
**Explicitly unchanged:** Jupiter market-data filter and qualification, pool TVL
and PumpSwap exception, settlement WSOL/USDC, three test DEXs, 50-USD
direction-notional Filter, CP21A 10% V10 fee calculation, V10 trade_size=0,
V10 pack/account capacity and all min-profit configuration.

## Resolved signal/discovery issues

1. **Economics independent of marginal winner/log:** Every affected
   ordered Direct2 pool pair reaches the amount-dependent model when both
   directed price/liquidity observations are present, even if one or both
   marginal log weights are missing. When marginal data is not available
   the legacy SignalLeg weight field contains 0 **only as a representation
   placeholder**; `marginal_signal_bps` and
   `estimated_net_return_bps` are **None**, never made-up 0 bps.
   Deliberately filtered `dust` directions remain excluded.
2. **Exact cause-specific records:** Missing pool identity/observations,
   unavailable liquidity/directions, explicit direction-notional dust,
   missing fees, unresolved token transfer fees, unsupported economic
   quote models, empty CPMM reserves or DLMM windows, and unmodelable
   quote/depth are independently counted and journaled.
   Counters distinguish `live` from `catalog_or_warmup`.
   A model-unknown result is **not proof of non-profitability**.
3. **Non-destructive catalogs:** An identical catalog state has an O(n)
   identity/Arc-equality path with *zero* pair requotes. A changed graph
   generation maps old economic entries by stable
   `(settlement,target,direct_pool_address,partner_pool_address)`; unchanged
   frozen fingerprints retain their scores, rank and pending V10 state
   even if compact pool IDs are reordered. Changed/new entries are re-evaluated.
4. **Auditable scheduling:** Every evaluated direction/source pair, including
   nonpositive and unknown, records its business identity and causal outcome.
   The journal additionally records V10 submitted `request_id`, rejections,
   new-market-state preemption, and CP20S cooldown. Queue-pressure episodes
   remain counted; their first occurrence is recorded.
5. **Historical live selection:** `MarketStateEvent::Selection` is passed
   unchanged through `SignalStateUpdate.selected_watch_pools` as a selection
   set. Its per-pool enter/exit events have layer
   `market_state_selected_not_ws_ack`. Graph catalog membership uses a
   separate `catalog_pool` kind and MUST NOT be conflated with actual
   MarketState-selected pools. Neither is a per-subscription ACK.
6. **Complete V10 time/slot report:** Every successful-or-failed simulated
   V10 request in `execution_events.jsonl` carries the actual RPC
   `min_context_slot_requested`, local UTC microsecond
   `v10_worker_received_at_us`, `rpc_sent_at_us`, and
   `rpc_answer_at_us`, plus provider and reported simulation context
   slot, quantity, route and outcome.

## Economic timeline format

`runtime_live/<UTC stamp>/economic_timeline.jsonl` is newline-delimited
JSON, `schema=1`. One record per economic pair evaluation or recorded
decision, plus catalog/selection membership transitions and reconciliation
summaries. Important fields:

- Identity: `kind`, `action`, `reason`, `settlement`, `target`,
  `direct_pool`, `partner_pool`, `graph_generation`, `revision`,
  `trigger` and optional V10 `request_id`.
- State: `direct_slot`, `partner_slot`,
  `direct_component_slot_min/max`, `partner_component_slot_min/max`,
  local observation receipt times and `frame_received_at_us`.
- Economics: `economic_score_usd_nano`, `optimal_input_raw`,
  `gross_gain_raw`, marginal availability flags. Missing fields mean
  **unknown**, never zero.
- UTC: `at_us` represents the local machine's microsecond timestamp,
  not the network's block time. Two observations at one slot do **not**
  imply one atomic Bank snapshot.

A single dedicated buffered file-writer owns this output. Economic workers
never perform disk writes; they use a bounded **audit-only** channel.
Queue overflow/disconnection increments `cp21a1_timeline_lost` and
**fails the run**; it does not reduce the set of economically checked pairs.
On clean shutdown, writer flushes and `sync_data` is called. Every emitted
line must match the writer's persisted count.

## Automatic analysis

The run writes `economic_timeline_analysis.json`, produced by:

```powershell
python .\scripts\analyze_cp21a1_timeline.py `
  --timeline .\runtime_live\<run>\economic_timeline.jsonl `
  --output .\runtime_live\<run>\economic_timeline_analysis.json
```

Focused later audit of a named competitor pool:

```powershell
python .\scripts\analyze_cp21a1_timeline.py `
  --timeline .\runtime_live\<run>\economic_timeline.jsonl `
  --pool <POOL_ADDRESS> `
  --output .\runtime_live\<run>\focused_pool_timeline.json
```

The analyzer streams the full timeline; it never samples or silently
truncates economic evaluations. It also checks JSON schema, basic
required fields, live watchset enter/exit membership and clock-order
anomalies. The full-run PowerShell script verifies the exact number of
records against `execution.json.execution_candidates.cp21a1_timeline_written`.

## Regressions and controlled next test

```powershell
Set-Location C:\dev_projects\liq_graph
git fetch origin
git switch cp21s-a1-signal-timeline-evidence
git pull --ff-only origin cp21s-a1-signal-timeline-evidence
.\scripts\cp21s_a1_timeline_live.ps1
```

The script first runs CP20, CP21A, CP21S-A, CP21S-A.1 Rust/analysis tests;
then starts the same 300s three-DEX read-only mainnet V10 workload.
No private keys or RPC URL credentials are logged.

Unit regressions include:
- CPMM pair with missing marginal weights remains economically scoreable.
- Dust-filtered direction is still excluded, preserving qualifications.
- No-op catalog never recomputes all pairs.
- Pool-ID reorder preserves economic scores and stable pool keys.
- 10,000 journal entries are durably written and in the original order.
- Full historical watchset/price/action timeline is stream-validated.
- Prior 50,000-event and full-quote benchmarks continue passing.

### Exit criteria for merging

**Required:** green CI; `lost_events=0`;
`cp21a1_timeline_lost=0`; persisted=emitted; no parse errors;
verifiable `watchset` selection membership; cause-specific
`cp21a1_unpriced_reasons`, `cp21a1_model_unknown_reasons`; fewer
avoidable full catalog re-quotes; real p50/p95/p99 refresh latency; and
the ability to correlate every simulated `request_id` with economic
source-pair events, V10 RPC request/answer times and simulation slot.

Actual WS subscription ACK times and validator bank-identity equality are
**not** supplied by this checkpoint. Historical pools outside this run's
watchset still have no live price evidence. Comparing with successful
transactions requires matching the same pool accounts and contextual
slots, not only wall-clock proximity. A green CI build alone cannot prove
live runtime performance or profitable arbitrage.
