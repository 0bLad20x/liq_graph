# CP7B: dynamische Baselines ohne globalen Zustandsneuaufbau

Basis: `main@f117dc7653bcaea9dd708050a1c28f0f2a60cfaf`, nach Merge von CP7 und CP7A.
Feature-Branch: `cp7b-market-state-baseline-install`.

## Aenderung

MarketState unterscheidet private strukturelle Installation von dynamischer
Baselineinstallation. Es gibt weiterhin genau einen Besitzer des laufenden Zustands.

- `install_structure()` entfernt/ersetzt alte Poolstrukturen, fuegt den vorbereiteten
  Batch hinzu und baut die Account-/Poolindizes einmal auf. Der vorherige doppelte
  Aufbau ueber `retain()` und anschliessendes `index()` entfaellt.
- `install_baseline()` uebernimmt nur dynamischen Poolzustand und pro Account Slot,
  Empfangszeit, Baseline-Floor und Gueltigkeit. Poolpositionen, statische Deskriptoren,
  Accountbereiche und Subscription-Bindungen bleiben erhalten.
- Ein privater Pooladressindex wird zusammen mit den anderen strukturellen Indizes
  gepflegt. Die Installation verwendet direkte Zugriffe auf betroffene Pools. Auch
  Jobplanung und Baselineausschnitte verwenden diesen Index, statt eigene globale
  Hilfsmaps oder vollstaendige Pool-/Subscriptionscans fuer jeden Batch aufzubauen.
- Reine Baselineergebnisse verlangen keinen neuen Watchplan. Fehlgeschlagene erste
  Hydrationen planen weiterhin den benoetigten Root-Account-Watch ein; fehlgeschlagene
  Baselines behalten ihre bereits vorhandenen Account-Watches.
- Unveraenderte Retain-Aufrufe bauen keine Indizes auf. Strukturentfernung und neue
  Hydration loesen weiterhin die erforderliche Index-/Watchanpassung aus.

Die Installation einer Baseline arbeitet am betroffenen Batch. Auswahl und periodische
Statistik koennen weiterhin ueber den Poolbestand laufen; CP7B behauptet keinen
vollstaendig scanfreien Scheduler und keine bereits gemessene Beschleunigung.

## Erhaltene Absicherungen

Vor jeder Installation werden wie bisher Poolidentitaet und Revisionstickets gegen
den aktuellen Zustand geprueft. Veraltete oder nicht mehr ausgewaehlte Ergebnisse
werden verworfen. Danach prueft die Baselineinstallation vor jeder Mutation den
gesamten verbliebenen Batch: Pool muss existieren und noch auf eine Baseline warten;
Venue, Mints, Decimals, kanonische Richtung und Accountbindungen muessen unveraendert
sein. Eine unerwartete Strukturabweichung wird als Fehler gemeldet.

Der Baseline-HTTP-Abruf nach Subscriptionbestaetigung bleibt notwendig. Er verwendet
weiterhin vorhandene Accountdeskriptoren. Fehlerisolierung durch Batchaufteilung,
aktive Subscriptions, Reconnectinvalidierung, Baseline-Floors, Replay der gepufferten
Updates und Invalidierung nicht mehr verfuegbarer Preise bleiben erhalten.
Die Preis-/Unavailable-Ausgabe und Replay beruehren nur die tatsaechlich installierten
Baseline-Pools; gemeinsam genutzte Accounts behalten den bisherigen Replayfilter.

Keine Aenderung an Graph, Signalbewertung, Admission, Decodern, V10, CORE-Vertraegen,
Dependencies oder Cargo.lock. Keine neue Runtime, kein zusaetzlicher Consumer oder
Monitoringdienst.

## Kurzer realer Nachweis

Nach erfolgreichem Release-Build auf dem Feature-Commit:

```powershell
git fetch origin
if ($LASTEXITCODE -ne 0) { throw 'Fetch fehlgeschlagen.' }
git switch --track origin/cp7b-market-state-baseline-install
if ($LASTEXITCODE -ne 0) { throw 'Branchwechsel fehlgeschlagen; lokale Aenderungen zuerst sichern.' }

.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp7b-market-state-baseline-install `
    -DurationSeconds 60 -PoolLimit 15000 -AccountBudget 25000 -Rewatch
```

Der gemeinsame Starter prueft Branch, Commit und Checkout und baut das richtige
Binary. Dieser eine kurze Lauf prueft Erstinstallation, Entfernung/Wiederaufnahme
und dynamische Baselines mit realen Daten. Kein Graphaudit, kein V10-Aufruf und kein
zweiter Lauf sind standardmaessig erforderlich. Falls die benoetigten realen Ereignisse
nicht auftreten, bleibt der entsprechende Nachweis offen; Zeitablauf ist keine Evidenz.

In `runtime_live/<Zeitstempel>/runtime.log` enthalten die vorhandenen Hydrations-
Fortschritts-/Abschlusszeilen zusaetzlich:

| Feld | Bedeutung im laufenden Engine-Zustand |
|---|---|
| `live_index_rebuilds` | Aufbauten der strukturellen Pool-/Accountindizes |
| `structure_installs` | Nichtleere angenommene Struktur-Batches |
| `baseline_installs` | Nichtleere angenommene dynamische Baseline-Batches |
| `baseline_pools_installed` | Summe installierter Baseline-Pools, keine Unique-Poolzahl |
| `watch_plans` | Tatsaechliche Watchplanberechnungen |

Diese Werte zaehlen nur den laufenden Besitzer; private vorbereitete Batchindizes
sind darin nicht enthalten. `baseline_jobs` zaehlt auch Ergebnisse, die wegen einer
neueren Revision verworfen werden; deshalb muss es nicht `baseline_installs` entsprechen.

Erwartet werden Baselineinstallationen ohne strukturellen Neuaufbau durch diesen
Pfad, gueltige Baseline-/WS-Preise, vollstaendige Signalzaehler und ein erfolgreicher
Rewatch/Shutdown. Gleichzeitig eintreffende echte Strukturereignisse duerfen die
Index-/Watchzaehler erhoehen. Eine zeitliche Geschwindigkeitsverbesserung wird erst
anhand echter Laufdaten bewertet. Der Checkpoint bleibt bis zur realen Abnahme offen.
