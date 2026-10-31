# CP9G – bounded streaming V10 evidence (Direct2 + Direct3)

Base: \`cp9f-market-state-hotpath\`. **No trading broadcasts**.

## Motivation
The prior CP9E/CP9F orchestration accumulated both full \`Vec<Selection>\`
and \`Vec<ExecutionReport>\` until exit, then serialized large overlapping
evidence into \`execution_events.jsonl\`. This created RAM proportional to
completed simulation count; a crash before shutdown could lose the entire
execution evidence archive. Large JSONL evidence is **not ordinary
\`runtime.log\`** and was written mainly after the live interval.

## Runtime design
- Candidate producer still uses \`try_reserve()\` on the V10 request channel.
  After creating a valid selection, it uses **\`try_send()\`** to a bounded
  1,024-item evidence queue. Never performs filesystem I/O and never awaits
  file writer capacity in the Signal worker. Each report is sent through this
  same bounded evidence channel without awaiting disk.
- Dedicated blocking-thread writer pairs Selection and V10 Report by request
  id, validates metrics/hand-off, generates the frozen calibration, writes the
  joined record *during* the run and immediately releases both objects.
  Unmatched join maps are bounded to 1,024 entries.
- Four files: \`execution_events.jsonl\` (compact full outcome + calibration),
  \`execution_payloads.jsonl\` (content-addressed complete V10 manifests),
  \`execution_snapshots.jsonl\` (full unique pool observations by digest),
  and \`execution_details.jsonl\` (bounded audited full examples).
- Repeated selected pool observations in events are replaced with stable
  content-addressed refs, retaining inline venue and presence of liquidity
  for the existing Python invariants. Full frozen observations remain in
  \`execution_snapshots.jsonl\`; the existing per-leg calibration is computed
  **before** compaction.
- A bounded 32,768-digest cache accelerates dedup; after a cache eviction
  the same digest can legally be emitted again, so \`unique_payloads\`
  represents *distinct payloads written under bounded-cache semantics*.
  The JSONL consumer can deduplicate by stable digest offline.
- Write buffers flush every 64 paired records or after 5 seconds on the
  next completed pair. Successful graceful shutdown explicitly syncs data.
- Summary \`execution.json.streaming_evidence\` reports matched, report
  successes/failures, written snapshots and payloads, maximum unmatched
  entries and max evidence queue depth.

## Failure policy, important limitation
No bounded system can guarantee all three simultaneously under arbitrary
disk slowdown: (a) exact lossless evidence, (b) never blocking the
producer and (c) never failing. CP9G chooses **fail visibly** if the
writer cannot keep pace, rather than quietly lose audit records or stall
MarketState/Signals. A nonblocking queue overflow produces a fatal candidate
or collector error; runtime does not declare complete handoff. Successfully
flushed records survive a later process failure; some buffered entries may
still be lost before OS flush/sync, so this is **not** database durability,
exactly-once transactional persistence or crash-safe WAL.

All V10 strategies, thresholds, quantity rules and graph scope are untouched.

## 15-minute live test
Do not interpret CPU latency improvement unless **Meteora DLMM TVL
admission is nonzero**. The previous CP9F five-minute sample had
\`meteora_dlmm prepared/admitted=0\` because the DexScreener enrichment
worker repeatedly reported \`no verified DexScreener HTTP proxies\`.
Do not bypass the TVL>=500 gate to make this test look faster. Check the
DexScreener source and availability before judging DLMM hotpath.

Use prior CP9F settings, changing just the branch and duration to 900:

\`\`\`powershell
Set-Location C:\dev_projects\liq_graph
git fetch origin
if ($LASTEXITCODE -ne 0) { throw 'Git fetch failed' }
git switch cp9g-streaming-evidence
if ($LASTEXITCODE -ne 0) { throw 'Branch switch failed' }
git pull --ff-only origin cp9g-streaming-evidence
if ($LASTEXITCODE -ne 0) { throw 'Git pull failed' }
cargo test --locked
if ($LASTEXITCODE -ne 0) { throw 'Rust tests failed' }
python -m unittest discover -s scripts/tests -p 'test_*.py'
if ($LASTEXITCODE -ne 0) { throw 'Python tests failed' }
$previousCommitment=$env:RPC_COMMITMENT
try {
    $env:RPC_COMMITMENT='confirmed'
    .\scripts\runtime_live.ps1 \`
        -ExpectedBranch cp9g-streaming-evidence \`
        -EnabledVenues 'meteora_dlmm,pumpswap' \`
        -Stage Signals -DurationSeconds 900 -PoolLimit 0 \`
        -AccountBudget 25000 -MinPoolTvlUsd 500 \`
        -MinDirectionNotionalUsd 50 -DirectionLiquidityMode Filter \`
        -Direct2Direct3Only -RouteKind Direct2Direct3 \`
        -ThresholdBps 0 -V10Parallelism 32 \`
        -CP9DMaxPairs 3 -CP9CPackingMode AllCoverage \`
        -ProfileSignals -ExecutionDetailLimit 64 \`
        -RequireCalibrationEvidence -SimulateV10 -SimulationLimit 0 \`
        -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
    if ($LASTEXITCODE -ne 0) { throw 'CP9G live test failed' }
} finally {
    $env:RPC_COMMITMENT=$previousCommitment
}
\`\`\`

## Acceptance
1. Exact \`request_count == report_count == streaming_evidence.matched\`;
   all Python amount/delivery/calibration invariants pass, no missing pairs.
2. No dropped writer evidence, bounded max queue/unmatched, no V10 orphan
   responses, no MarketState or signal event loss.
3. During the run, JSONL files grow (check midway), not only at shutdown;
   byte volume and peak process memory are lower at similar request counts.
4. No impact on signal thresholds and pool coverage; if DLMM TVL remains
   unavailable, classify latency result as **PumpSwap-only**.
5. Repeat any economic comparison only with appropriately matched
   token-pair/revision/market-state samples; no conclusions from raw profit
   counts between different clock windows.
