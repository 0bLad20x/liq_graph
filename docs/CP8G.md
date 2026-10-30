# CP8G: TVL-500-Vergleich und kompakte Laufartefakte

## Ziel und Zulassung

Aufbau auf dem getesteten CP8F-Commit `3211d045`. Der bisherige Code hatte
**keine gemeinsame 100-USD-Grenze**: Er verwarf bekannte protokolleigene TVL
unter 10 USD; fuer PumpSwap fehlte eine gleichwertige native TVL-Quelle.
Der neue Versuch ist daher kein isolierter 100-gegen-500-Vergleich der alten Regel.

`POOL_MIN_TVL_USD=500` gilt in der Registry fuer alle aktivierten Pools, auch
WSOL/USDC-Konverter. Verwendet wird die vorhandene kanonische TVL: frische
Protokollquelle zuerst, sonst frische DexScreener-Quelle. Quellenalter maximal
eine Stunde. Keine neue RPC-Abfrage, kein zweites Poolranking.

- Unter der Grenze: `TvlBelowMinimum`.
- Keine gueltige aktuelle TVL: `TvlUnavailable`; auf Enrichment warten.
- Identitaet/Mintqualifikation bleiben Voraussetzung. Das bestehende WSOL/USDC-
  Paar bleibt nur von der Non-Reference-Mintqualifikation ausgenommen.
- Prepared-Proofs und DB-Zeilen bleiben erhalten. Gespeicherte TVL bleibt beim
  Restore historische Evidenz; eine neue Beobachtung ist erforderlich. Dadurch
  kann sich die Anlaufphase veraendern.
- Ueber-/Unterschreiten und Quellenablauf erzeugen normale Registry-Updates.
  Der Graph folgt der Zulassung; kurze Wartungsschreibabschnitte bleiben erhalten.

Der Starter bietet `-MinPoolTvlUsd`, Standard 500; direkter Runtime-Start liest
die ENV. `0` deaktiviert die neue Regel, rekonstruiert aber nicht die alte native
10-USD-Regel. Eine spaetere Gegenprobe mit 100 verwendet dieselbe neue Regel.

`runtime.json`/`summary.json`: `registry_tvl.by_venue` zaehlt Gesamtzahl,
Prepared, zugelassen, Prepared+zugelassen, fehlende TVL und zu geringe TVL.
Die Bereiche `<100`, `100..<500`, `>=500` betreffen bekannte aktuelle TVL von
bereits Prepared/mintqualifizierten Pools. Fehlende Werte sind kein Null-TVL.
Graphkatalog, ausgewaehlte und bepreiste Pools bleiben getrennte Populationen.
Nicht jeder zugelassene Pool bildet eine Arbitrageroute.

## Aufzeichnung

| Datei | Inhalt |
|---|---|
| `execution.json` | Kleine Besitzerzaehler, Request-/Reportanzahl, Uebergabe und Simulationsergebnisse; Schema 2 ohne volle Request-/Reportarrays |
| `execution_events.jsonl` | Ein kompakter Datensatz je Versuch: Signal, Mengenurteil, Pools, Preise/Gebuehren, Packing, Antwort, Fehler, CU, Gebuehr, Salden und Zeiten |
| `execution_payloads.jsonl` | Vollstaendiger Request einmal je Payload-Hash; nur die Request-ID ist vom Hash ausgenommen |
| `execution_details.jsonl` | Begrenzte vollstaendige Auswahl-/Antwortfaelle verschiedener Ergebnis-/Fehlerklassen und Ausschlussbeispiele |
| `execution_analysis.json` | Streaming-Auswertung aller Zaehler, Fehlerklassen, Routenformen, Wiederholungen, Zeitquantile und der zehn groessten Gewinne je Settlement |

Keine drei Einzeldateien je Simulation mehr. Dedupliziert werden nur Payload-
Kopien auf der Platte, keine Ausfuehrungen oder Marktbeobachtungen. Jeder Versuch
bleibt im Ereignisstrom. Binprofile behalten ihre bestehende kompakte Form mit
Anzahl/erstem Level. Vollstaendige RPC-Traces, Inner Instructions und Accounts
bleiben nur fuer Detailfaelle. DEX-Erkennung und Requestvalidierung erfolgen
weiterhin auf vollstaendigen Originaldaten vor der Verdichtung.

`RUN_EXECUTION_DETAIL_LIMIT=16` bzw. `-ExecutionDetailLimit 16`: maximal 0..256
Detailfaelle insgesamt. Bis zu vier bestehende Mengenausschlussbeispiele
verbrauchen ebenfalls dieses Budget. `0` deaktiviert Vollkopien.
Per-Request-Logs sind Debug; deren Zeiten bleiben in JSONL/Analyse verfuegbar.
Warnungen, Verbindungsereignisse und Owner-Zaehler bleiben erhalten. Optionale
Mengen-/Liquiditaetsberichte entstehen nur bei angefordertem Nachweis.
Im Terminal erscheinen wenige Statuszeilen statt der kompletten Summary.

Die bestehende Execution-RAM-Historie bleibt bis Shutdown erhalten. Hier werden
Dateivervielfachung und PowerShell-Nachverarbeitung reduziert; konstanter
RAM-Verbrauch fuer unbegrenzte Laufdauer ist nicht Gegenstand dieses Checkpoints.
Python 3.10+ analysiert zeilenweise; PowerShell liest nur kleine Abschlussberichte
(maximal 16 MiB je Datei). Verletzungen von Uebergabe, Netto-Witness, DLMM-Fenster
oder Mengenkette fallen durch. Keine alten Gesamt-JSONs mehr im Speicher.

Alte echte Lauf-ZIPs lassen sich ohne ihre grosse `execution.json` analysieren:

```powershell
python .\scripts\execution_analyze.py --archive .\alter_lauf.zip --output .\execution_analysis.json
```

Fuer Replay den Hash eines Ereignisses im Payload-Strom suchen, das `request`-
Objekt exportieren und bei Bedarf seine ID auf die Ereignis-ID setzen.
`v10_simulation_live.ps1` akzeptiert diese unveraenderte Request-Struktur.

## Stundenlauf und Abnahme

Getestete Providerkapazitaeten unveraendert aus der privaten ENV uebernehmen.
Keine aggregierte Kapazitaet auf einzelne Orbitflare-Sockets umverteilen.

```powershell
.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp8g-tvl500-compact-evidence `
    -EnabledVenues 'meteora_dlmm,pumpswap' `
    -Stage Signals `
    -DurationSeconds 3600 `
    -PoolLimit 0 `
    -AccountBudget 25000 `
    -MinPoolTvlUsd 500 `
    -ExecutionDetailLimit 16 `
    -ProfileSignals `
    -RequirePreparedEvidence `
    -SimulateV10 `
    -SimulationLimit 0 `
    -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
```

Signalmathematik und Poolranking bleiben gleich: Netto-Threshold 0 BPS,
`minimum_profit=0`, `trade_size=0`, Flashloan verpflichtend, kein Broadcast.
TVL garantiert weder handelbare Tiefe noch Profit. Ein Lauf ohne relevante Gewinne
bleibt ein Befund. Bestehende strikte Uebergabenachweise bleiben erforderlich.

Vergleichen: zugelassene/Katalog-/ausgewaehlte Pools, Preisabdeckung nach Anlauf,
WS-Luecken/Reconnects, Signal-/Requestzeiten, Busy-Verwerfungen, Fehlerklassen,
Erfolge je Routenform und Settlementgewinne. WSOL-Gewinne, Netzwerkgebuehren und
neue Kontoeinlagen getrennt ausweisen; USDC-Rohwerte nicht mit Lamports summieren.
Gleiche Payload beweist keinen gleichen Marktstand. Andere Marktlage verhindert
einen allein kausalen Schluss von Gewinnveraenderung auf die TVL-Grenze.

Locked Build ist Voraussetzung. Fachliche Abnahme erst mit dem echten Stundenlauf
auf dem neuen Commit: `pool_tvl_evidence_passed=true`, keine Zulassungsverletzung,
durchgaengige Datenlieferung, passende Request-/Reportzahlen und keine
Analyseverletzungen. Altlaeufe pruefen nur das Analysewerkzeug, nicht die neue
Zulassung. Kein Merge vor dieser Auswertung.
