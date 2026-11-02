# CP15 — Raydium TVL Coverage and Freshness

## Problem and established limits

A 600-second CP15 live run showed 50,131 Raydium CLMM and 15,118 Raydium CPMM identities, but current TVL for only a small subset. Pool-TV​​L from the protocol REST API and `liquidity.usd` from DexScreener are **pool-level USD observations**, not proof that concentrated liquidity is in-range or immediately executable. The 500 USD TVL minimum and the global mint qualification remain unchanged.

The historical `pool.tvl_usd` field could survive Registry restart without restoring its source quote into the freshness index. This allowed old TVL to retain admission. The current code admits restored TVL only when a persisted observation matches its value, valid protocol/DexScreener source and timestamp within the 3,600-second freshness limit. Missing/stale proof clears the live TVL; the original historical `last_observation` remains for diagnostics.

## Live source strategy

1. Raydium global paginated REST discovery still finds new pool addresses and metadata.
2. The same worker also queries `/pools/info/ids?ids=...` for known, mint-qualified Raydium pools whose TVL is currently unavailable. This is **not** a second Registry or HTTP client: it uses the same `reqwest::Client`, endpoint rate gate, source `DataBatch`, and Registry writer. Small 10-ID batches and bounded 512-pool scan cycles constrain load. Negative/unavailable responses do not manufacture TVL or admission.
3. The DexScreener source prioritizes qualified mints attached to known pools with unavailable TVL before routine mints at scheduling time. Identity must match a known Registry pool. The existing precedence remains: fresh native REST TVL first, then fresh DexScreener `liquidity.usd`, else unavailable.

Targeted REST results are best-effort: the provider may not index all historical pools. The confirmed number of missing/eligible pools **after** the run, not a predicted improvement percentage, measures impact.

## One-hour coverage-only acceptance

Run on Windows with the existing local `.env`, PostgreSQL and HTTP/RPC access:

```powershell
Set-Location C:\dev_projects\liq_graph
git fetch origin
git switch cp15-raydium-clmm-direct2
git pull --ff-only origin cp15-raydium-clmm-direct2
.\scripts\cp15_tvl_coverage_1h.ps1
```

The runner uses 3600 seconds, Stage=Graph, GraphAudit=true, `raydium_cpmm,raydium_clmm,orca_whirlpool`, Direct2, minimum pool TVL=500 USD, and **no V10 simulation or transaction broadcast**.

Compare `summary.json` (`registry_tvl.by_venue`) and `runtime.log` counters `raydium targeted_tvl`, `dexscreener cp1 continuous`, and `pool registry discovery_sources`. Record per venue:
- known identities, known TVL, qualified-but-missing TVL, below-500 TVL
- prepared and admitted, prepared-but-not-admitted
- native API versus DexScreener source coverage
- overall Graph membership and Direct2 topology counts
- targeted attempts, IDs returned, batches failed (not unique success unless verified)

Success requires no execution of broadcasts, no lost source batches, preserved identity/graph membership and valid TVL provenance; a particular number of newly admitted pools is **not** hard-coded as a requirement.

A one-hour live Mainnet run requires user-controlled provider credentials and the local database and is not available in GitHub CI. Rust tests and PowerShell syntax checks are performed by `.github/workflows/locked-build.yml`.

## CP15R — Raydium REST fault isolation

The May/November 2026 five-minute audit showed six restart events in the
Raydium discovery supervisor caused by a page response-body timeout. These
previously terminated all ten cursor lanes and the targeted-ID job together.

The CP15R change keeps all requests on the existing Raydium HTTP client and
shared RPS gate:

- Each page request has three attempts, including transport errors, interrupted
  response bodies, malformed JSON, HTTP 408/429 and 5xx; 429 respects bounded
  Retry-After with the existing shared cooldown.
- On exhausted transient retries, the owning route pauses and retries the **same
  cursor**, with bounded backoff. It does not restart other routes.
- Nonretryable 4xx restart only the offending route; the two liquidity
  directions may restart as a pair because they share overlap evidence.
- Head refresh failures are isolated per route; targeted-ID fetches continue.
- Failed targeted-ID batches are eligible again after 45 seconds instead of
  the 30-minute successful-refresh interval.
- Test `cp15r_rest_fault_tests` injects a truncated HTTP body then verifies that
  the retry carries the identical `nextPageId`; permanent 400 is identified
  separately without a retry storm.

Before another hour-long acceptance, use the 300-second smoke test:

```powershell
Set-Location C:\dev_projects\liq_graph
git fetch origin
git switch cp15-raydium-clmm-direct2
git pull --ff-only origin cp15-raydium-clmm-direct2
.\scripts\cp15_tvl_coverage_1h.ps1 -DurationSeconds 300
```

`cp15r_transport_report.json` is produced alongside `summary.json` and
`runtime.log`, with counts of whole-worker restarts, page retries, cursor
preservation pauses, local restarts, head-refresh errors, targeted-ID success/
failure, and DexScreener progress. A whole-worker Raydium restart invalidates
the CP15R smoke acceptance; Graph membership and TVL admission evidence must
also pass. Other sources (e.g., Orca) remain out of scope.
