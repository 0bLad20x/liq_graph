# CP1 – Mint-Qualifizierung und Pool-TVL-Enrichment

Umfang: Jupiter (gezielte neue/unvollständige Mints), SQL-Qualifizierung,
dauerhafter DexScreener-Consumer, kanonischer Pool-TVL mit Provenienz,
gebündelte Persistenz. **Keine Completion- oder Graph-Zulassung:** Das ist CP2.

## Lokaler Live-Checkpoint ohne Completion-RPC

PowerShell im Repository:

```powershell
git fetch origin
git switch --track origin/agent/cp1-market-qualification
cargo check --locked

$env:CP1_PROBE_DURATION_SECONDS = "300"
$env:MINT_QUALIFICATION_REFRESH_SECONDS = "60"
$env:DEXSCREENER_CP1_MINTS_PER_CYCLE = "100"
$env:DEXSCREENER_REFRESH_SECONDS = "60"
$env:DEXSCREENER_PROXY_WAIT_SECONDS = "60"
$env:RUST_LOG = "info"

cargo run --locked --release -- --cp1-live 2>&1 |
    Tee-Object -FilePath cp1_live.log
```

Benötigt: `LIQ_GRAPH_DATABASE_URL` oder `DATABASE_URL`,
`JUPITER_SEARCH_API_KEYS` sowie die bestehenden
`http_transport`-Proxy-Einstellungen; DexScreener nutzt die
getestete `ProxyOnly`-Route aus dem Diagnose-Worker.
Der CP1-Checkpoint startet weder DEX-Discovery noch Completion-RPC.
Der Jupiter-Vollbackfill ist für diesen isolierten Test explizit deaktiviert.

## Kontrollquery

```sql
SELECT
    venue,
    COUNT(*) AS pools,
    COUNT(*) FILTER (
      WHERE last_observation->>'tvl_usd' IS NOT NULL
    ) AS pools_with_persisted_tvl,
    COUNT(*) FILTER (
      WHERE last_observation->>'source' = 'dexscreener_token_pairs'
    ) AS last_tvl_from_dexscreener,
    COUNT(*) FILTER (
      WHERE last_observation->>'source' IS NOT NULL
        AND last_observation->>'source' <> 'dexscreener_token_pairs'
    ) AS last_tvl_from_other_sources
FROM public.liq_graph_pool_cache
GROUP BY venue ORDER BY venue;
```

Die Jupiter-Qualifizierung setzt die sechs abgesprochenen Kriterien um:
90 Tage `updated_at`, 30-Tage-Ein-Pool-Regel, `holder_count >= 10`
(Null bleibt zulässig), vorhandene Jupiter-Liquidität und Market Cap,
Jupiter-Liquidität `>= 2000` und Market Cap `>= 10000` USD.

Regulärer Runtime-Test nach bestandenem isoliertem Checkpoint:

```powershell
$env:CP1_MARKET_ENRICHMENT_ENABLED = "true"
$env:JUPITER_BACKFILL_ON_START = "false"
cargo run --locked --release -- --run
```

Achtung: `--run` startet weiterhin die bisherigen unbeschränkten
Completion-Worker. Die Einschränkung auf qualifizierte Pools und
`POOL_MIN_TVL_USD >= 500` ist ausdrücklich **CP2**, noch nicht Teil von CP1.

## Invarianten

- Die Pool-Identitäten und `Unknown`/`Incomplete`/`Prepared` ändern sich nicht.
- DexScreener ist nur für bekannte Pool-Adressen mit passender Mint-Paarung
  zugelassen; keine Venue-Ableitung allein aus `dexId`.
- REST-TVL geht bei gültiger Frische (60 Minuten) vor DexScreener-TVL.
  Fehlt frische REST-TVL, darf frische DexScreener-TVL den Wert ergänzen.
- `last_observation` enthält Herkunft und Beobachtungszeitpunkt.
  Persistenzänderungen werden je Pool auf maximal einen Refresh alle
  15 Minuten gedrosselt, abgesehen vom ersten TVL.
- Historische Postgres-Werte sind keine aktuellen Live-TVL-Quotes.
- Die Qualifikationsliste wird aus der Datenbank rekonstruiert und
  zusätzlich nach Pool-/Jupiter-Änderungen mit Debounce aktualisiert.
- Gezielte Jupiter-Mints werden in der bestehenden Worker-API-Lane
  verarbeitet, nicht in einem zweiten HTTP-Scheduler.

### Abnahmemessung

1. Logs enthalten `mint qualification published qualified=N`.
2. DexScreener-Logs melden `cp1 target_mints` und erfolgreiche Anfragen.
3. Die Kontrollquery zählt gespeicherte DexScreener-TVL pro Venue.
4. Unbekannte `pairAddress` und abweichende Mint-Paarungen erzeugen
   keine neuen Pool-Identitäten.
5. `--cp1-live` meldet `completion_workers=0` und endet
   mit `cp1 live complete=true persisted=true`.

Ein lokaler Rust-Build und RPC-/Proxy-Livetest müssen noch ausgeführt
werden. Stand der Implementierung: Feature-Branch, nicht `main`.
