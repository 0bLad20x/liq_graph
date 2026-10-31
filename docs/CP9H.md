# CP9H – Kandidatenaufbereitung ohne Aenderung der Auswahl

Basis: CP9G `ff3db817ff95f5a28e4791d025571723c2ac146f` und dessen echter
900-Sekunden-Lauf `20261031T161650835Z`.

## Umfang

- ExecutionCandidates haelt einen privaten Mintpaar-Index auf seine bereits
  veroeffentlichten Poolidentitaeten. Ein Snapshot baut ihn einmal auf; neue
  Identitaeten werden angehaengt. Preisupdates veraendern ihn nicht.
  Die Kandidaten eines Paars werden in derselben Pool-ID-Reihenfolge besucht
  wie beim bisherigen Vollscan. Richtungen, Preisverfuegbarkeit, Prepared-
  Pruefung, Nettofilter und Ausschluss bereits benutzter Pools bleiben gleich.
- Coverage baut die bereits angebotenen Poolpaarungen einmal pro Ranking auf
  und berechnet den Neuigkeitswert einmal je Alternative. Sortierschluessel
  und Tie-Breaks sind unveraendert.
- Kleine Histogramme zaehlen im bestehenden Worker; sie werden ausschliesslich
  im Abschlussbericht serialisiert. Keine neuen Worker, RPC-Abfragen,
  Diagnosekonsumenten oder unbeschraenkten Laufhistorien.
- Alle vier Packing-Strategien bleiben verfuegbar. Es gibt keine neue Heuristik,
  Wiederholungssperre oder wirtschaftliche Zulassungsgrenze.

## Zeitmessung und Arbeitszaehler

`execution.json.execution_candidates.preparation` und
`summary.json.execution_preparation` enthalten:

| Feld | Bedeutung |
|---|---|
| `pair_index_rebuilds`, `pair_index_appends` | Pflege der Identitaeten, keine Preisarbeit |
| `indexed_pools`, `indexed_pairs` | Letzter Umfang des privaten Index |
| `alternative_scans`, `indexed_pool_visits` | Tatsaechlich besuchte Paarlisten und Identitaeten |
| `full_scan_pool_visits_equivalent` | Identitaetsbesuche, die der bisherige Vollscan bei denselben Aufrufen benoetigt haette; keine gemessene alte Laufzeit |
| `alternative_options` | Nach den bestehenden Filtern erstellte gerichtete Alternativen |
| `capacity_checks`, `capacity_rejections` | Exakte V10-Kapazitaetspruefungen |
| `coverage_rankings`, `coverage_key_evaluations` | Ein vorbereiteter Coverage-Aufbau je Ranking und ein Schluessel je Alternative |
| `pair_index_ns` | Pflegezeit des Index |
| `alternative_lookup_ns` | Paarzugriff und Aufbau der Alternativenprofile |
| `ranking_ns` | Rangfolge einschliesslich Coverage-Vorbereitung |
| `pool_resolution_ns` | Einzelne Registry-/Manifestaufloesungen |
| `capacity_validation_ns` | Einzelne Aufrufe der unveraenderten V10-Validierung |
| `pack_build_ns` | Ein kompletter Aufruf von `build_compact`, auch bei Fehler oder nachtraeglicher Duplikaterkennung |
| `signal_to_round_start_ns` | Vom publizierten Signalzustand bis zum Beginn seiner Packing-Runde; enthaelt Zustellung und vorherige Verbraucherarbeit |
| `round_work_ns` | Ganze Runde mit mehreren Paaren/Strategien, Duplikaten und Mengenpruefungen |

Jede Zeitverteilung enthaelt Anzahl, Summe, Mittelwert, Maximum und logarithmische
Perzentil-Obergrenzen. Unterphasen sind in `pack_build_ns` enthalten; sie duerfen
nicht nochmals zur Packzeit addiert werden. Die bestehende Mengenverteilung steht
weiter in `execution_candidates.amounts` beziehungsweise `summary.amount_evaluation`.
Eine Mengenpruefung kann bei Bedarf zusaetzliche Reparatur-Packs enthalten.

Der bisherige Ereigniswert `selection_ns` war die bis zur jeweiligen Uebergabe
vergangene Zeit der gesamten Runde. Er heisst jetzt eindeutig `round_elapsed_ns`;
`summary.execution_round_elapsed_ns` ist dessen Verteilung. Er ist weder eine
exklusive Packzeit noch ein zusaetzlicher Summand zu `frame_to_request_us`.
Journal-Schema 5 fuehrt diesen Namen; der Analyzer normalisiert historische
Schema-4- und Vollformate. Es gibt keinen zweiten Ausgabealias.

## Auswahlvergleich aus vorhandenen Daten

`execution_analysis.json.cp9c_strategy_comparison.baseline_path_coverage`
beziehungsweise `summary.execution_baseline_path_coverage` vergleichen jede
erfolgreich ausgefuehrte Poolfolge mit der Baseline-Poolmenge derselben
Graphgeneration, Signalrevision, Routenidentitaet und Settlement-Mint.

`path_present` bedeutet enthalten, `path_absent` bedeutet nicht enthalten.
`baseline_not_dispatched` bleibt ungepaart und wird nicht als Baseline-Misserfolg
ausgegeben. Erfolg, Ergebnis oberhalb der Netzwerkgebuehr und positiver nativer
SOL-/WSOL-Saldo werden getrennt gezaehlt. Unterschiedliche Poolfolgen werden
zusaetzlich ohne Wiederholungen gezaehlt; Beispiele bleiben auf acht begrenzt.
Die Mitgliedschaft beweist keine identische Gegenfaktum-Ausfuehrung der Baseline.
Simulationen sind keine realisierten Gewinne. Die normale Python-Auswertung
verwendet weiter exakte, datenmengenabhaengige Offline-Mengen; der Rust-Livepfad
baut keine solche Historie auf.

Im CP9G-Basislauf wurden 24.501 Requests vollstaendig ausgewertet. Alle drei
positiven Gesamtsalden (+567, +2.686, +11.830 Lamports) nutzten Poolfolgen, die im
zugehoerigen Baseline-Paket fehlten. Das ist ein Auswahlbefund, kein Nachweis
einer allgemein profitablen Strategie. Der schnellere Paarzugriff verspricht
keine neuen wirtschaftlichen Signale.

Die aktualisierte Auswertung wurde auf allen 24.501 realen Ergebniszeilen
erneut ausgefuehrt. Alle bisherigen Wirtschafts-, Fehler-, Kalibrierungs- und
Zeitaggregate bleiben erhalten; der kumulative Zeitname wird normalisiert.
Die Uebertragung desselben echten Ereignisstroms von Schema 4 auf Schema 5
liefert dieselbe Auswertung. Insgesamt 506 erfolgreiche Beobachtungen aus
Mixed/Contrarian/Coverage nutzen Pfade ausserhalb ihres jeweils vorhandenen
Baseline-Pakets; ungepaarte Beobachtungen sind darin nicht enthalten.
Dies validiert die Offline-Auswertung, nicht den neuen Rust-Packingpfad.

## Lokaler Live-Nachweis: 600 Sekunden

```powershell
Set-Location C:\dev_projects\liq_graph
git fetch origin
if ($LASTEXITCODE -ne 0) { throw 'Fetch fehlgeschlagen' }
git switch cp9h-candidate-preparation
if ($LASTEXITCODE -ne 0) { throw 'Branchwechsel fehlgeschlagen' }
git pull --ff-only origin cp9h-candidate-preparation
if ($LASTEXITCODE -ne 0) { throw 'Synchronisierung fehlgeschlagen' }
$previousCommitment = $env:RPC_COMMITMENT
$previousDexMode = $env:DEXSCREENER_TRANSPORT_MODE
try {
    $env:RPC_COMMITMENT = 'confirmed'
    $env:DEXSCREENER_TRANSPORT_MODE = 'direct'
    .\scripts\runtime_live.ps1 `
        -ExpectedBranch cp9h-candidate-preparation `
        -EnabledVenues 'meteora_dlmm,pumpswap' `
        -EnabledRoutes 'direct2' `
        -Stage Signals -DurationSeconds 600 -PoolLimit 0 `
        -AccountBudget 25000 -MinPoolTvlUsd 500 `
        -MinDirectionNotionalUsd 50 -DirectionLiquidityMode Filter `
        -ThresholdBps 0 -V10Parallelism 32 `
        -CP9DMaxPairs 3 -CP9CPackingMode AllCoverage `
        -ProfileSignals -ExecutionDetailLimit 64 `
        -RequireCalibrationEvidence -SimulateV10 -SimulationLimit 0 `
        -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
    if ($LASTEXITCODE -ne 0) { throw 'CP9H-Lauf fehlgeschlagen' }
} finally {
    $env:RPC_COMMITMENT = $previousCommitment
    $env:DEXSCREENER_TRANSPORT_MODE = $previousDexMode
}
```

Der Starter fuehrt weiterhin einen locked Release-Build aus. Der Live-Lauf muss
reale Markt-/Signaleingaenge, Paketbildung, Simulation und vollstaendige kompakte
Ergebnisse nachweisen. Auszuwerten sind reduzierte Identitaetsbesuche, getrennte
Phasenzeiten, Queue-Spitzen und erfolgreiche Pfade je Packing-Strategie. Vergleiche
unterschiedlicher Live-Zeitfenster sind kein kontrollierter Performance- oder
Profitabilitaetsnachweis. Flashloan bleibt verpflichtend; `minimum_profit=0`
und automatische Handelsgroesse bleiben gleich. Kein Broadcast.
