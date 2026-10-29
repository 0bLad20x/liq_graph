# Gemeinsame Runtime und Live-Nachweis

Der aktuelle Mengencheckpoint [CP8C.md](CP8C.md) verwendet denselben Starter mit
`RequireAmountEvidence` fuer einen einzelnen 1800-Sekunden-Simulationslauf.
`amount_evidence.json` enthaelt Mengenentscheidungen, begruendete Ausschluesse,
positive Modellmengen und neue Zeitmessungen. V10 waehlt die Handelsgroesse weiterhin.

Der aktuelle DLMM-Liquiditaetscheckpoint und sein einzelner 600-Sekunden-Lauf
stehen in [CP8B.md](CP8B.md). `RequireLiquidityEvidence` verlangt echte BinArray-
WS-Eingaben, Signalverbrauch und konsistente Request-Fenster. Optionale
`LiquidityEvidencePools` sichern begrenzte Rohdaten ohne Auswahl-Pinning oder
zusaetzlichen Diagnosekonsumenten. Die Ausgabe bleibt im Laufverzeichnis.

`--run` ist der einzige vollständige Aufbau: Discovery, Mintversorgung, Registry,
Persistence, CompletionLifecycle, Graph, WatchCatalog, MarketState, Signal Engine und
optionale V10-Simulation. Endliche und dauerhafte Läufe verwenden dieselbe Verkabelung.
`RUN_DURATION_SECONDS=0` bedeutet Dauerbetrieb; `q` plus Enter stoppt kontrolliert.
Worker bleiben über ihre vorhandenen CLI-Einstiege eigenständig ausführbar.
Der Registry-Nachlauf zu CP8A misst Request-Lesewartezeiten und entfernt den
langen TVL-Wartungsschreiblock; Messfelder und der 600-Sekunden-Nachweis stehen
in [CP8A.md](CP8A.md#cp8a1-registry-maintenance-and-request-read-timing).
Discovery-Module, gemeinsame Startverkabelung und begrenzte Einzelaufrufe beschreibt
[DISCOVERY.md](DISCOVERY.md).
Completion-Module und der gemeinsame Einzelstart sind in [COMPLETION.md](COMPLETION.md) beschrieben.

## Werkzeuge

Unter `scripts/` bleiben genau drei wiederverwendbare Dateien:

| Werkzeug | Zweck |
|---|---|
| `runtime_live.ps1` | Ein Starter für Graph-, MarketState-, Signal- und Simulationsläufe |
| `graph_live_analyze.py` | Offline-Auswertung eines ausdrücklich angeforderten Graphaudits |
| `v10_simulation_live.ps1` | Eine vollständige eingefrorene Request-Datei erneut simulieren |

Historische Checkpoint-Starter, Flashloan-Presets, DB-Wrapper, SQL-Vergleiche,
Migration und Reset-Werkzeuge sind entfernt. Benötigte Workerfunktionen bleiben
erhalten, darunter `--cp1-live`, `--completion-live PROTOCOL` und
`--jupiter-mint-backfill`. Schema und Completion-Serialisierung beschreibt
[PERSISTENCE.md](PERSISTENCE.md). Es wird keine Datenbankmigration oder Löschung durch
diesen Cleanup ausgeführt.

## Diagnoseumfang und Korrektheit

Der Simulationsbetrieb startet **null zusätzliche Diagnosekonsumenten**. Es gibt
keinen MarketState-Preisbestand im Beobachter, keinen zweiten Signalvertragsspiegel
und keine zusätzlichen Catalog-, Discovery-, Registry- oder Jupiter-Beobachter.
MarketState liefert seinen Abschlussbericht selbst. Die Signalübergabe wird am
wirklichen ExecutionCandidates-Eingang geprüft; dessen Zustandsvalidierung bleibt.

Ein Signallauf ohne Execution ist ein ausdrücklich aktivierter Funktionsnachweis:
Der Starter setzt `RUN_SIGNAL_PROOF=true` und startet genau einen Signalprüfkonsumenten.
Ein reiner MarketState-Nachweis verwendet genau einen zustandslosen Zählkonsumenten.
Graphläufe benötigen keinen zusätzlichen Runtime-Beobachter. Eigenständige Worker-
CLI-Nachweise dürfen weiterhin den jeweils benötigten Ausgangsprüfer verwenden.

Für einen endlichen erfolgreichen Lauf sind echte Preis- und WebSocket-Eingaben,
vollständige Mengenübergabe, null Account-/Signalverlust und ein sauberer Abschluss
erforderlich. Rewatch verlangt Entfernen, Neuauswahl und danach echte WS-Preise.
Simulation verlangt zusätzlich eindeutig zugeordnete Antworten aller angenommenen
Requests. Ein On-Chain-Fehler darf diesen Übergabenachweis erfüllen; erfolgreiche
Swaps, DEX-Aufrufe je Routenform und Profitabilität sind eigene Ergebnisse für CP8.

## CP7E/F: Implementierung und Live-Evidenz

Basis: gemergte CP7D-`main` mit `bc511df4894598731f80d909838da436824159cc`.
Branch: `cp7ef-runtime-tooling-cleanup`. CP8 bleibt separat.

Entfernt sind unbenutzte persisted-/Poollimit-Varianten, die unerreichbare initiale
Preisberechnung, der wirkungslose Jupiter-Startschalter und doppelte Signal-Ausgaben.
Die HTTP-Baseline, ACK-/Slot-/Replay-Sicherung, Kandidatenregeln und unabhängige
Executor-Requestvalidierung bleiben aktiv. Der verworfene MarketState-JSON-Bericht
ist durch einen tatsächlich ausgewerteten typisierten Eigentümerbericht ersetzt;
ungenutzte Protokoll-Diagnosehistogramme und die Paarprojektion ihres alten
Beobachters entfallen.

Der locked Release-Build und die Syntaxprüfung sind Voraussetzungen. Erst reale
Artefakte auf dem tatsächlich übergebenen Commit schließen diesen Checkpoint ab.
In der Implementierungsumgebung fehlen Rust/PowerShell und konfigurierte DB/RPC-
Zugänge; der Build wird über Repository-CI geprüft.

Der reale Lauf `20261027T114819833Z` auf `0d0e5f7` belegt 60,005 Sekunden,
null Diagnosekonsumenten und vollständige Übergabe: 28.689 MarketState-Ereignisse,
546 Preise einschließlich eines WS-Preises sowie 57.519 Signalereignisse und
475 Kandidatenänderungen kamen jeweils beim tatsächlichen Folgekonsumenten an.
Keine Verluste, Decoderfehler oder unverifizierten Execution-Eingaben; sauberer Shutdown.
Entfernen und Neuauswahl nach Rewatch sind sichtbar, ein nachfolgender WS-Preis
und ein echter V10-Request fehlen. Diese beiden Nachweise bleiben unvollständig.
Eine einzelne gewöhnliche WS-Auswertung ist kein belastbarer Geschwindigkeitstest.

Die erste Zusammenfassung setzte `candidate_delivery_passed` fälschlich aus dem
Gesamtabnahmestatus ab. Das ist korrigiert: Markt-/Signalübergabe, Rewatch und
Request-Handoff werden unabhängig geprüft. `evidence_errors` nennt alle fehlenden
Nachweise. Die Gesamtabnahme bleibt bei einem fehlenden angeforderten Nachweis falsch;
die erfolgreiche Kandidatenübergabe wird dadurch nicht mehr als gescheitert angezeigt.

Ein kurzer gemeinsamer Lauf prüft die Verschlankung am echten Execution-Eingang:

```powershell
git fetch origin
if ($LASTEXITCODE -ne 0) { throw 'Fetch fehlgeschlagen.' }
git switch --track origin/cp7ef-runtime-tooling-cleanup
if ($LASTEXITCODE -ne 0) { throw 'Branchwechsel fehlgeschlagen.' }

.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp7ef-runtime-tooling-cleanup `
    -DurationSeconds 60 -PoolLimit 1000 -AccountBudget 25000 `
    -Rewatch -SimulateV10 -SimulationLimit 3 `
    -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
```

Bei bereits vorhandenem lokalem Branch einmal `git switch` und `git pull --ff-only`
verwenden. Kein Reset, Clean oder Stash. Der Starter akzeptiert jeden sauberen Branch;
`ExpectedBranch` kann den vorgesehenen Checkout ausdrücklich absichern. Er prüft
Branch/HEAD/Working Tree, baut `cargo build --locked --release` und prüft das Lockfile.

Für diesen gezielten Nachweis werden nur 1000 Pools beobachtet, damit Rewatch nicht
mit der erneuten Initialisierung der gesamten Auswahl konkurriert. Der Graph behält
sein vollständiges Routenuniversum. Dies ist kein Leistungsnachweis für 15.000
vollständig hydrierte Pools.

Kein zusätzlicher zehn- oder fünfzehnminütiger Pflichtlauf. Liefert der kurze Lauf
keine benötigten echten Ereignisse, bleibt der entsprechende Nachweis unvollständig;
die Laufdauer garantiert keine Gelegenheit. Der Graph-/Referenzaudit ist optional
und für diese Geschwindigkeitsmessung deaktiviert. Signalläufe ohne Wallet verwenden
dieselbe Runtime ohne `SimulateV10` und genau einen ausgewiesenen Prüfkonsumenten.

## Optionen und Artefakte

`Stage` ist Graph, MarketState oder Signals. Graph verlangt `GraphAudit`; bei Signals
kann `SimulateV10` zugeschaltet werden. `PlanOnly` löst Einstellungen ohne DB/RPC auf.
`RouteKind` begrenzt nur Execution-Auswahl. `ExecutionParametersPath` übernimmt die
vorhandene Parameter-JSON. Simulationen verwenden verpflichtend `use_flashloan=true`;
ein ausdrückliches `false` wird abgewiesen. Das Handelskapital stammt aus dem
Settlement-Flashloan-Vault, native SOL auf der Wallet bezahlt Gebühren und nötige
Kontoeinlagen. Rewatch pausiert Beobachtungen
nach halber Laufzeit für sechs Sekunden. Trace und Signalprofil sind ausdrücklich
optional; sie gehören nicht zur normalen Geschwindigkeitsmessung.

Providerkapazitäten stammen aus `.env`; `WsProviders`, `WsAccountBudget` und
`WsConnectionsPerProvider` sind optionale Overrides. Gesicherte Prozessvariablen und
das Arbeitsverzeichnis werden auch nach Fehlern wiederhergestellt.

Alle Ergebnisse liegen in `runtime_live/<UTC-Zeitstempel>/`:

| Artefakt | Inhalt |
|---|---|
| `checkout.json`, `plan.json`, `websocket_plan.json` | Commit, Optionen, Kapazitäten und Diagnoseanzahl |
| `runtime.log`, `runtime.json`, `summary.json` | Originalfehler, Abschluss, Datenfluss, ausgewählte Zeitmessungen |
| `market_state.json` | Zähler und Timing des tatsächlichen Preisworkers |
| `signal_production.json` | Signalproduktion und getrennte Initialisierungs-/WS-Zeiten |
| `signal_consumer.json` | Ausschließlich beim ausdrücklichen Signalnachweis ohne Execution |
| `execution.json`, `request-NN.json`, `selection-NN.json`, `simulation-NN.json` | Tatsächliche Übergaben und Simulationen |
| Audit-JSONL, `graph_summary.json`, CSV-Dateien | Nur bei angefordertem Graphaudit |

Im vorgesehenen Simulationslauf müssen `diagnostic_consumers=0`,
`signal_proof=false`, `orchestration_evidence_passed=true` und
`candidate_delivery_passed=true` gelten. MarketState-Event/Preis/WS-Zähler müssen
zur Signalproduktion passen; Signal-Event/State/Batch/Kandidaten/Evaluation-Zähler
müssen zum wirklichen ExecutionCandidates-Bericht passen. Es darf kein
`signal_consumer.json` entstehen. Fehlende Messwerte werden nicht als Nullerfolg ausgegeben.
`market_delivery_passed`, `signal_delivery_passed` und `rewatch_evidence_passed`
zeigen ihre jeweils eigenen Prüfungen; deaktivierte Prüfungen bleiben null.

`summary.json` enthält Account-Verarbeitung und Queue in ns, gewöhnliche
WS-Signalverarbeitung in ns und Frame-bis-Signalzustand in µs. P95/P99 sind
Histogrammobergrenzen. Graphbuild, Catalogbuild und Auswahlzeiten bleiben im Log
und bei Audit in der Offline-Auswertung. Vergleiche benötigen ähnliche Last und
Auswahlgröße; dieser Cleanup verspricht keinen vorab erfundenen Beschleunigungsfaktor.

## Completion-Abschluss und 30-Minuten-Integration

Der vollstaendige Aufruf mit allen laufenden Stufen und V10-Simulation steht in
[COMPLETION.md](COMPLETION.md#registry-bootstrap-correction-and-30-minute-integration).
`CompletionRefreshSeconds` und `CompletionRecheckSeconds` sind optionale Overrides;
0 bedeutet die bestehenden Einstellungen uebernehmen. Der Runtime-Starter setzt
keinen Completion-Kohortenfilter. PumpSwap verwendet im Simulationslauf den aus
`WalletPath` lokal ermittelten oeffentlichen Signer; die Cache-Kompatibilitaetspruefung
bleibt aktiv. Ein einmaliger Full-RPC-Inventarscan ist weiterhin ein eigener
Bootstrap, kein regelmaessiger Runtime-Worker.

Die Zusammenfassung enthaelt zusaetzlich `completion_protocols`,
`completion_protocol_evidence_passed`, `completion_issues`,
`discovery_records_by_source`, `log_bytes` und `log_levels`.
Registry-Abhaengigkeitsupdates werden unveraendert veroeffentlicht, ohne
Einzellogging unter Schreibsperren. Der optionale Graph-Audit behaelt seine
Membership-Uebergaenge; normale Laeufe verwenden Zusammenfassungen.

`SimulationLimit=0` ist der Standard und bedeutet kein Gesamtlimit. Die reale
Parallelitaet bleibt bei einer Simulation plus einem wartenden Request. Positive
Limits bleiben fuer gezielte Kurzlaeufe moeglich. Das begrenzte Eingangsangebot
bleibt erhalten; ausgelastete Revisionen werden gezaehlt und uebersprungen.

Registry publiziert den restaurierten Prepared-Anfangsbestand mit berechneter
Admission vor den ersten Writer-Deltas. Nach dem Drain prueft `graph_membership`
konkrete Pooladressen und Mintpaare gegen die finale Registry. Fehlende,
unberechtigte oder falsch zugeordnete Graph-Pools verhindern die endliche Abnahme.
Der Vergleich laeuft einmalig beim Abschluss und startet keinen Diagnosekonsumenten.

MarketState unterscheidet im Abschlussbericht `selected_pools`, strukturell
installierte `hydrated_pools`, nach Baseline betriebsbereite `ready_pools` und
`priced_pools`. Der alte Final-Logname `prepared` fuer installierte Pools entfaellt;
Completion-Prepared und Preisbereitschaft werden dadurch nicht verwechselt.

For the two-protocol 30-minute fee checkpoint, use the existing starter with
`-EnabledVenues meteora_dlmm,pumpswap -RequireFeeEvidence`; the exact PowerShell
command and acceptance limits are in [CP8A](CP8A.md).


## CP8A.2: reine Netto-Pruefung

Signalindex, Kandidaten und Poolalternativen verwenden ausschliesslich die
bekannten gerichteten DEX-Nettoraten. `ThresholdBps` bzw.
`GRAPH_SIGNALS_THRESHOLD_BPS` bezeichnet jetzt den geschaetzten Vorteil nach
DEX-Gebuehren; Standard 0 verlangt einen positiven Wert. Fehlende Nettoraten
haben keinen Bruttofallback. Fuer diesen Checkpoint werden DLMM/PumpSwap aktiviert.
Netzwerkgebuehren, Liquiditaetstiefe und Mengenwirkung bleiben gesondert.

Der einzelne 1800-Sekunden-Lauf und die Abnahmekriterien stehen in
[CP8A.md](CP8A.md#cp8a2-net-only-selection-and-duration-telemetry).
`summary.json` enthaelt `net_evidence_passed`, `signal_score_basis`, die Nettoschwelle,
`observations_by_known_net_directions`, `net_candidate_windows`, getrennte
Signalstufen und Revaluation-Zeiten sowie Request-Aufbauzeiten. Signalfenster am
Laufende sind rechtszensiert und keine gemessenen wirtschaftlichen Abbrueche.
Die Telemetrie benoetigt keinen zusaetzlichen Diagnosekonsumenten.
