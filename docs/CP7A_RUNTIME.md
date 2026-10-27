# CP7A: ein vollständiger Pipeline-Aufbau

Basis ist CP7 `3c3aa6fed407f827060a7d452b7a5e0c0eeefae5`.
Feature-Branch: `cp7a-unified-runtime`.

## Ergebnis und Grenzen

`--run` ist der einzige vollständige Aufbau von Registry, Persistence, Mintversorgung,
CompletionLifecycle, Graph, WatchCatalog, MarketState, Signalen und optionaler V10-Simulation.
Ein begrenzter Lauf verwendet dieselbe Verkabelung, Admission, Completionaktualisierung
und Shutdownreihenfolge wie der Dauerbetrieb. `RUN_DURATION_SECONDS=0` bedeutet Dauerbetrieb.

`src/orchestration/market_state_live.rs` und seine beiden CLI-Einstiege sind entfernt.
`ExecutionTasks` liegt im bestehenden V10-Orchestrierungsmodul. Eigenständige Worker-
Nachweise und manueller `--v10-simulate`-Request-Replay bleiben verfügbar; sie sind kein
zweiter vollständiger Pipeline-Aufbau.

Es gibt einen Starter: `scripts/runtime_live.ps1`. Er ersetzt `graph_live.ps1`,
`market_state_live.ps1`, `graph_signals_live.ps1` und `full_runtime_v10_live.ps1`.
Der historische Flashloan-Presetstarter ruft ebenfalls diesen gemeinsamen Starter auf.
Der Offline-Graphanalyzer bleibt erhalten. Kein Workeralgorithmus, Decoder,
Admissiongrenzwert, Kandidatentrigger oder V10-Accountlayout wird in CP7A verändert.

## Lokaler Checkout und Plan

```powershell
git fetch origin
if ($LASTEXITCODE -ne 0) { throw 'Fetch fehlgeschlagen.' }
git switch --track origin/cp7a-unified-runtime
if ($LASTEXITCODE -ne 0) { throw 'Branchwechsel fehlgeschlagen; lokale Änderungen zuerst sichern.' }
git branch --show-current
git rev-parse --short HEAD
git status --short

.\scripts\runtime_live.ps1 -PlanOnly -PoolLimit 15000 -AccountBudget 25000
```

Wenn der lokale Branch bereits existiert: einmal `git switch cp7a-unified-runtime` und
`git pull --ff-only` mit derselben Fehlerprüfung verwenden. Keine automatischen Stashes,
Resets oder Änderungen an `.env`.

Der Starter prüft Branch/HEAD und unveränderte versionierte Dateien, baut mit
`cargo build --locked --release` und prüft anschließend Lockfile und Checkout.
Er verwendet das Binary dieses Builds. `--run-plan` prüft die Aktivierungsabhängigkeiten
und löst dieselben Provider-/WS-Einstellungen auf, ohne Datenbank oder RPC zu öffnen.
Der Plan ist kein Live-Nachweis; eine Wallet wird für PlanOnly nicht benötigt.

## Reale Abnahme ohne Simulation

```powershell
.\scripts\runtime_live.ps1 `
    -DurationSeconds 60 -PoolLimit 15000 -AccountBudget 25000 -Rewatch
```

Dieser Lauf startet reguläre Discovery, Mintversorgung und Completionrefresh.
Nach der Hälfte der Laufdauer werden MarketState-Beobachtungen sechs Sekunden
deaktiviert und anschließend wieder aktiviert. Das erzeugt keine künstlichen
Registryzustände. Die vorhandenen Consumer prüfen echte Removals, Reselection und
nachfolgende WebSocket-Preise.

Die Standardlaufdauer ist 60 Sekunden. Fuer die Abschlussbereinigung genuegt dieser
eine kurze Nachweis; ein erneuter Simulations- oder 15-Minuten-Lauf ist nicht vorgesehen.
Die allgemeine MarketState-Abnahme verlangt echte Preise und WebSocket-Updates.
Eine spezielle Direct2-Paarabdeckung oder eine aus WS-Preisen vollstaendig bepreisbare
Route ist dafuer keine Voraussetzung. Ein Live-Preisupdate darf korrekt auf noch
fehlende Preise anderer Routenbeine treffen. Die Signalpruefung verlangt weiterhin
die vollstaendige Uebernahme der echten Preis-/WS-Eingaben, saemtliche Pipeline-
Zeitmessungen, vollstaendige Producer-/Consumer-Paritaet und null Signalverluste. Rewatch
verlangt weiterhin Entfernen, Neuauswahl und nachfolgende echte WebSocket-Preise.

`-GraphAudit` ist ein optionaler, gesonderter Routen-/Referenznachweis. Er ist fuer
die CP7A-Abschlussbereinigung nicht erforderlich. Sein Ergebnis wird unabhaengig als
`graph_audit_passed` ausgewiesen; ein angeforderter, unvollstaendiger Graphaudit
beendet den Starter weiterhin mit einem Fehler.

`-RequireCatalogReconciliation` kann zusätzlich zum GraphAudit verlangt werden.
Wie bei CP7 muss der reale Lauf dann die geforderten Katalogänderungen tatsächlich
enthalten; eine nicht beobachtete Änderung wird nicht als erfolgreich geprüft ausgegeben.

Der Starter verlangt einen beendeten Lauf, vollständige Signalübergabe, reale
WS-Preisbewertung, gestarteten Lifecycle und mindestens einen beobachteten Prepared-
Refresh. Ein kurzer Lauf ohne diese tatsaechlichen Ereignisse bleibt unvollstaendig;
eine feste Dauer garantiert keine Live-Evidenz.

## Begrenzte reale Simulation auf demselben Aufbau

```powershell
.\scripts\runtime_live.ps1 `
    -DurationSeconds 60 -PoolLimit 15000 -AccountBudget 25000 `
    -SimulateV10 -SimulationLimit 3 -RouteKind All `
    -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
```

Dies ist eine optionale Aufrufvariante, kein zweiter Pflichtlauf fuer die
Abschlussbereinigung. Der vom Nutzer gelieferte Lauf auf `d68ee47` belegt bereits
32.987 vollstaendig uebergebene Signalereignisse und drei zugeordnete echte
V10-Simulationsantworten. Alle drei endeten mit `Custom(1)` ohne DEX-Aufruf.
Das ist ein Orchestrierungsnachweis, kein erfolgreicher Swapnachweis.

Für einen eng begrenzten Protokoll-/Routennachweis kann `-RouteKind Direct2`,
`Direct3`, `Triangle3` oder `Triangle4` gewählt werden. Der Filter gilt nur für die
Execution-Auswahl. `All` garantiert keine Simulation jeder Routenform.

`-ExecutionParametersPath` übernimmt eine vorhandene ExecutionParameters-JSON-Datei.
Es wird nichts gebroadcastet. Nach dem Stopp werden bereits angenommene Requests
mit noch verfügbarem RPC abgearbeitet. Die bestehende Grenze von 300 Sekunden für
den Executor-Drain bleibt erhalten; der Runtime-Drain erlaubt 360 Sekunden.

CP7A verlangt einen tatsächlichen, eindeutig zugeordneten V10-Simulationsaufruf.
Ein abgeschlossener Aufruf kann einen On-Chain-Fehler zurückgeben. Deshalb werden
`execution_handoff_observed`, `execution_integration_passed`, erfolgreiche/fehlgeschlagene
Simulationen und beobachtete DEX-Aufrufe getrennt ausgegeben. CP7A-Abnahme ist keine
Behauptung erfolgreicher Swaps, Profitabilität oder erfolgreicher Simulation aller vier
Routenformen; dieser fachliche Nachweis bleibt CP8.

## Einstellungen und Artefakte

- `-Stage MarketState` verwendet denselben Aufbau mit deaktivierten Signalen/Simulationen.
- `-WsProviders Configured` respektiert die bisherigen Providerflags; explizite Indizes
  sowie `WsAccountBudget` und `WsConnectionsPerProvider` bleiben verfügbar.
- `AccountBudget` begrenzt die MarketState-Auswahl. `WsAccountBudget` ist der davon
  getrennte optionale globale Transportkapazitäts-Override.
- Trace- und Profilparameter sind ausdrücklich wählbar. Die Diagnoseconsumer werden in
  diesem Checkpoint beibehalten; deren spätere Bereinigung ist ein eigener Schritt.
- Gesicherte Prozessvariablen werden auch bei Fehlern wiederhergestellt. Ein Abbruch vor
  dem Setzen von Variablen löscht keine bestehenden Prozesswerte.

Alle Artefakte liegen in `runtime_live/<UTC-Zeitstempel>/`:

| Datei | Zweck |
|---|---|
| `checkout.json`, `plan.json`, `websocket_plan.json` | Exakter Commit, Optionen und aufgelöste Kapazitäten |
| `runtime.log` | Ungefilterte Runtime-Ausgaben einschließlich Fehlern |
| `runtime.json` | Abschluss und gemeinsame Orchestrierungspruefung der Workerberichte |
| `market_state.json` | Bestehender MarketState-Observerbericht einschließlich Rewatch |
| `signal_production.json`, `signal_consumer.json` | Bestehende Signalberichte; Mengen- und Deliveryvergleich |
| `execution.json`, `request-NN.json`, `selection-NN.json`, `simulation-NN.json` | Bei aktivierter Simulation: tatsächliche Übergaben und Ergebnisse |
| `graph_pool_audit.jsonl`, `graph_summary.json`, CSV-Dateien | Bei GraphAudit: vorhandener Graphnachweis |
| `summary.json` | Orchestrierungsnachweis sowie getrennte Graph-/Simulationsergebnisse |

Bei Fehlern bleiben vorhandene Artefakte erhalten. Setup-/Crashfehler können einzelne
Workerberichte verhindern; das wird niemals als erfolgreicher Nachweis gewertet.
`orchestration_evidence_passed=true` bezeichnet ausschliesslich den nachgewiesenen
Datenfluss und Abschluss der konfigurierten Pipeline. Die allgemeinen Felder
`checkpoint_evidence_passed` und `live_evidence_passed` entfallen in diesen beiden
CP7A-Laufberichten. `execution_handoff_observed`, `execution_integration_passed`,
Simulationsfehler und DEX-Aufrufe behalten ihre getrennte Bedeutung.
PlanOnly schreibt ausdruecklich `orchestration_evidence_passed=false`.

Der Abschlusslauf auf `57147fa` lieferte 628 Preisbeobachtungen, davon zwei echte
WS-Updates nach Rewatch, 57.942 vollstaendig uebergebene Signalereignisse und 83
Kandidatenaenderungen. Producer und Consumer stimmen in allen Zaehlern ueberein;
es gab keine Verluste, Resynchronisierungen oder unverifizierten Ereignisse.
Entfernung, Neuauswahl, regulaerer Completionrefresh und Shutdown sind belegt.
Der damalige Fehlerstatus entstand ausschliesslich durch die zusaetzliche
WS-Routenabdeckungsbedingung; deren Entfernung aendert keine Verarbeitung und keine
der gemessenen Eingaben/Ausgaben. Diese Aufzeichnung bleibt die reale Evidenz fuer
die Abschlussbewertung; es wird kein weiterer Live-Lauf zur Wiederholung verlangt.

Der Build und die Syntaxprüfung sind Voraussetzungen. Erst die realen Läufe auf dem
tatsächlichen Feature-Commit schließen den Checkpoint fachlich ab.
