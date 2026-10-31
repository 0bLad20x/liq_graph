# CP9G – schlanke Streaming-Evidenz und gemeinsame Routenfreigabe

Der darauf aufbauende [CP9H](CP9H.md) benennt die kumulative `selection_ns`
korrekt in `round_elapsed_ns` um (Journal-Schema 5). Der Analyzer liest beide
Formate; exklusive Packzeiten kommen aus dem aggregierten Workerbericht.

Basis des Nachlaufs: `06e8f033668902386baf6e71d4da645b3f75b280`.
Der Checkpoint bleibt simulationsbasiert; es wird nichts gesendet.

## Eine Routenfreigabe

```dotenv
GRAPH_ENABLED_VENUES=meteora_dlmm,pumpswap
GRAPH_ENABLED_ROUTES=direct2
```

`GRAPH_ENABLED_ROUTES` erlaubt beliebige nichtleere Kombinationen aus `direct2`,
`direct3`, `triangle3`, `triangle4`. Unbekannte und doppelte Eintraege werden vor
Workerstart abgewiesen. Ohne Einstellung gilt `direct2`. Zum Hinzuschalten die
Liste aendern und den Prozess neu starten; keine Codeaenderung erforderlich.

Der Graph erstellt nur freigegebene Topologien, einschliesslich Delta-Neubildung
und optionalem Referenzvergleich. Signal und Execution lesen denselben Core-Vertrag.
Der Signalworker weist einen Graph ausserhalb dieser Freigabe ab; Execution
uebergibt nur freigegebene Quellkandidaten. Der konfigurierte Umfang steht in
`plan.json`, `checkout.json`, `runtime.json` und `summary.json`.

Die alten separaten Einstellungen `GRAPH_DIRECT2_ONLY`,
`GRAPH_DIRECT2_DIRECT3_ONLY`, `GRAPH_EXECUTION_ROUTE_KIND` haben keine Funktion mehr.
Die Starterparameter `Direct2Only`, `Direct2Direct3Only`, `RouteKind` entfallen.
Einziger optionaler Override: `-EnabledRoutes 'direct2'`.

Die Route bezeichnet den Quellkandidaten, nicht zwingend die vom Executor gewaehlte
Poolfolge. Der Analyzer zaehlt die tatsaechliche Anzahl ausgefuehrter Swaps separat.
Rueckwaerts gerichtete historische Direct3-Witnesses werden korrekt zugeordnet:
bei Umrechnung am Anfang ist der Zielmint die Ausgabe der zweiten Swap-Stufe.
Neue Evidenz speichert den typisierten Topologie-Zielmint direkt.

## Was dauerhaft geschrieben wird

- `execution_events.jsonl`: genau ein kompaktes Ergebnis pro angenommenem Request.
  Die erste Zeile enthaelt Schema 4, Spaltennamen und unveraenderliche Wallet-/
  Request-Parameter. Jede weitere Zeile enthaelt fuenf Felder: Payload-Hash,
  Selection, Report, Kalibrierung und Timing. Namen werden nicht pro Zelle wiederholt.
- `execution_details.jsonl`: begrenzte komplette Selection-/RPC-Beispiele,
  einschliesslich Manifeste, eingefrorener Beobachtungen und Rohbalanzen.
  `ExecutionDetailLimit` (Standard 16, maximal 256) gilt fuer die gesamte Datei,
  auch fuer Mengen-Ausschlussbeispiele. Ab Limit 4 wird ein Viertel der Plaetze
  fuer erstmals beobachtete WSOL-Ergebnisse oberhalb der Netzwerkgebuehr reserviert.
- `execution.json`: kleiner Abschlussbericht (Schema 5), Zaehler und Writer-Metriken.
  Der bestehende Python-Analyzer schreibt `execution_analysis.json`.

Die separaten unbegrenzten Dateien `execution_payloads.jsonl` und
`execution_snapshots.jsonl` entfallen. Ein Payload-Hash dient weiterhin zum
Gruppieren identischer Requests; er verspricht keinen gespeicherten vollstaendigen
Replay fuer jeden Versuch. Komplette Requests gibt es nur in den Detailbeispielen.
Wer mehr Rohdaten benoetigt, erhoeht gezielt deren Limit; Standardlaeufe bewahren
nicht jede RPC-Kontobalanz und jedes Modell immer wieder auf.

Erhalten bleiben fuer jeden Versuch: exakte Identitaet, Quellroute/Zielmint,
Paketstrategie und Poolauswahl, Mengenentscheidung, marginale Bewertung,
RPC-Fehler/Programmfehlername, Provider/Slot/CU, Netzgebuehr, beobachtete
Wallet-Differenzen und neue Accountfinanzierung sowie getrennte Zeiten.
Bei beobachteten DEX-Aufrufen bleiben deren Reihenfolge, gerichtete Rohmengen,
Modellausgaben, Zustandsfingerprints und Slot-/Zeitbezug erhalten.

Native SOL und Token-Rohmengen werden niemals ungeprueft addiert. Fehlende
Balance-Evidenz bleibt unbekannt. Fehlgeschlagene Simulationen erhalten keinen
realisierten Wallet-Gewinn. Marginale Rendite, mengenbezogener DEX-Ueberschuss,
Executor-Abzuege, Netzwerkgebuehr und Accountfinanzierung bleiben getrennt.

## Laufende Verarbeitung und Grenzen

Der Producer stellt Selection und Report mit `try_send()` in eine Queue von 1024
Eintraegen. Ein eigener Blocking-Thread fuehrt sie anhand der Request-ID zusammen,
prueft die originalen Packing-Metriken, kalibriert vorhandene DEX-Aufrufe,
schreibt das Ergebnis und gibt beide Objekte frei. Keine unbeschraenkten
Request-/Report-Historien und keine Payload-/Snapshot-Digest-Caches.

Der Writer haelt maximal 1024 noch nicht zusammengefuehrte Objekte. Queue- oder
Dateifehler fuehren sichtbar zum Laufabbruch; sie werden weder verborgen noch
mit Warten im Preis-/Signalpfad abgearbeitet. Die beiden Dateien werden nach
64 Ergebnissen beziehungsweise nach 5 Sekunden beim naechsten Ergebnis geflusht,
nach 256 Ergebnissen und beim sauberen Abschluss synchronisiert. Dies ist kein
transaktionales WAL; ein Prozess-/Systemabbruch kann den letzten ungesicherten
Abschnitt verlieren. Bereits geschriebene Zeilen sind ohne normalen Shutdown lesbar.

`streaming_evidence` zaehlt `matched`, Ergebnisarten, `detail_samples`,
`event_bytes`, `detail_bytes`, `max_backlog`, `max_unmatched`, Flushes und Syncs.
Die Python-Auswertung liest alte und neue Formate zeilenweise. Exakte Distinct-
Mengen und Verteilungen fuer die Offline-Auswertung bleiben datenmengenabhaengig;
sie sind keine Zustaende der laufenden Rust-Pipeline.

## Vorliegender Nachweis

Der echte 900-Sekunden-Lauf `20261031T141111325Z` wurde offline in das neue
Schema uebertragen und mit demselben Analyzer erneut ausgewertet:

- 35.123 Requests und Berichte, 3.874 Erfolge, 31.249 Fehlschlaege.
- Gleiche Gebuehren-/Wallet-Ergebnisse, Fehler, Routen-/Strategieauswertung,
  Payload-Gruppierung und komplette Kalibrierstatistiken; keine Vertragsverletzung.
- Ereignisdatei 37.784.894 statt 731.339.270 Bytes: 94,83 % weniger.
- Historische Direct3-Zuordnung korrigiert: 227 aktive und 180 frische
  unterschiedliche Quellrouten-Identitaeten statt 184 beziehungsweise 133.

Dieser Replay prueft Datenprojektion und Auswertung anhand realer Ergebnisse.
Er ersetzt keinen neuen Live-Nachweis der geaenderten Rust-Verkabelung. Der
locked Release-Build und Syntaxpruefungen werden auf dem exakten Branch-HEAD
geprueft; keine formalen oder synthetischen Test-Suiten.

## Naechster lokaler Lauf: nur Direct2

Die vorherigen Provider-/Liquiditaetseinstellungen bleiben fuer den Vergleich
bestehen. Direct3 und beide Triangle-Formen werden durch die zentrale Liste
abgeschaltet; Packing-Strategien und V10-Parallelitaet sind davon getrennt.

```powershell
Set-Location C:\dev_projects\liq_graph
git switch cp9g-streaming-evidence
if ($LASTEXITCODE -ne 0) { throw 'Branchwechsel fehlgeschlagen' }
git pull --ff-only origin cp9g-streaming-evidence
if ($LASTEXITCODE -ne 0) { throw 'Synchronisierung fehlgeschlagen' }
$previousCommitment = $env:RPC_COMMITMENT
$previousDexMode = $env:DEXSCREENER_TRANSPORT_MODE
try {
    $env:RPC_COMMITMENT = 'confirmed'
    $env:DEXSCREENER_TRANSPORT_MODE = 'direct'
    .\scripts\runtime_live.ps1 `
        -ExpectedBranch cp9g-streaming-evidence `
        -EnabledVenues 'meteora_dlmm,pumpswap' `
        -EnabledRoutes 'direct2' `
        -Stage Signals -DurationSeconds 900 -PoolLimit 0 `
        -AccountBudget 25000 -MinPoolTvlUsd 500 `
        -MinDirectionNotionalUsd 50 -DirectionLiquidityMode Filter `
        -ThresholdBps 0 -V10Parallelism 32 `
        -CP9DMaxPairs 3 -CP9CPackingMode AllCoverage `
        -ProfileSignals -ExecutionDetailLimit 64 `
        -RequireCalibrationEvidence -SimulateV10 -SimulationLimit 0 `
        -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
    if ($LASTEXITCODE -ne 0) { throw 'Direct2-Lauf fehlgeschlagen' }
} finally {
    $env:RPC_COMMITMENT = $previousCommitment
    $env:DEXSCREENER_TRANSPORT_MODE = $previousDexMode
}
```

Abnahme: exakte Request-/Report-/Writer-Zaehler, keine Markt-/Signalverluste,
ausschliesslich Direct2-Quellkandidaten, fehlerfreie kompakte Auswertung,
begrenzte Writer-Queues und nachvollziehbare Bytegroessen. Preisabdeckung und
Profitabilitaet sind eigene Ergebnisse; gruenes Orchestrierungs-Evidence bedeutet
weder 100 % Preisabdeckung noch wirtschaftliche Tragfaehigkeit.
