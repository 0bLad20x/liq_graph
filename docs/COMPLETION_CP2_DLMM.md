# Completion CP2.1 + CP2.2: Lifecycle und DLMM

Basis: `agent/cp1-completion-contracts`, Commit
`7d9c9eb9da004f12c8d4e2e3730270d18c85b962`.
Testbranch: `agent/cp2-dlmm-lifecycle`.

## Umfang und Grenzen

CP2.1 plant anhand echter Registry-Snapshots und Updates fuer alle beobachteten,
aktuell zugelassenen Pools. CP2.2 fuehrt diese Planung **nur fuer DLMM** aus.
Die anderen neun Completion-Worker behalten ihre bisherige Steuerung; ihre
Lifecycle-Zusammenfassungen tragen `executing=false`. Keine weiteren Decoder,
Protokolle, Mint-Regeln oder TVL-Quellen werden umgebaut.

Der Lifecycle liegt in einem Worker, nicht im Core. Der Core enthaelt die
gemeinsamen Auftrags-/Antwortvertraege. Die Registry bleibt alleiniger
Pool-State-Writer, PostgreSQL hat weiterhin genau den bestehenden Writer und
dieselbe Tabelle. Der Lifecycle interpretiert keine protokollspezifische
Evidence und besitzt keine RPC-Verbindung. Der DLMM-Worker erhaelt explizite
Auftraege; sein lokaler Scheduler ist entfernt.

Diese Stufe aktualisiert Prepared-Manifeste regelmaessig, fuehrt aber **noch
keinen Entzug wegen maximalen Pruefalters** ein. Der Graph und seine Logik
bleiben unveraendert. TVL-Zulassung und TVL-Expiry kommen unveraendert aus der
Registry. Eine Frischefreigabe ist Teil des spaeteren CP2-Abschlusses.

## Vorgang und Regeln

1. Die Orchestrierung abonniert Registry-Updates vor dem Start-Snapshot. Auch
   Prepared-Pools sind enthalten. Die Registry stellt nach DB-Restore noch keine
   frische TVL bereit; erst aktuelle Source-Daten koennen einen Pool zulassen.
2. Ein zugelassener Unknown-Pool wird als `Initial` eingeplant. Ein Prepared-Pool
   wird als `Refresh` nach `checked_at_ms + refresh` eingeplant. Ein historischer
   Pruefstand ohne Zeitstempel ist nach Zulassung sofort faellig.
3. Wiederpruefbares Incomplete wird als `Recheck` nach dem Recheck-Intervall
   eingeplant. `unsupported_configuration` und `configuration_required` pausieren;
   dieser Pilot fuehrt keinen automatischen Support-/Konfigurationswechsel ein.
4. Faellige Auftraege laufen nach Faelligkeit und Pool-Adresse. Es gibt pro Pool
   hoechstens einen laufenden Auftrag, begrenzte MPSC-Kanaele und je Executor eine
   Obergrenze fuer laufende Pools. Eine volle Queue blockiert nicht die Annahme
   von Antworten oder Registry-Updates. Es gibt keinen permanenten Vorrang neuer
   Pools vor alten Refresh-Terminen.
5. DLMM fuehrt seinen vorhandenen vollstaendigen Pruefpfad aus: LbPair, Bitmap,
   ausgewaehlte BinArrays, Mint-Pruefung, Evidence und gerichtete Accountbloecke.
   Ergebnisse werden nach Pool-Adresse den Auftrags-IDs zugeordnet, weil interne
   RPC-Partitionen die Reihenfolge aendern koennen.
6. Terminale Ergebnisse gehen ueber den bestehenden Ergebnisvertrag zur Registry.
   Erst deren passender Slot-/Zeitstempel-/Status-Update bestaetigt den Auftrag.
   Ein verschwundener oder abgelehnter Abschluss fuehrt nach der Ack-Frist zu
   einem ausdruecklichen Fehler. Die naechste Pruefung wird nicht vor der
   Registry-Uebernahme gestartet.
7. Retryable geht ebenfalls zur Registry, behaelt dort bestehende technische
   Evidence und Pruefzeit. Der Lifecycle braucht dafuer keine Pool-Aenderung als
   Bestaetigung, sondern terminiert anhand der expliziten Executor-Antwort erneut.
   Backoff waechst bis zum Maximum, plus bis knapp 10 Prozent Pool-Jitter. Es
   gibt keinen Abbruch nach zwei Versuchen. Ein identischer/aelterer Slot erneuert
   ebenfalls keinen Pruefstand und wird mit Backoff erneut angefragt.
8. TVL-Entzug entfernt wartende Arbeit. Laufende Ergebnisse duerfen weiterhin
   technische Evidence aktualisieren, stellen die TVL-Zulassung aber nicht her.
   Eine erneute Zulassung macht den dann aktuellen Pruefbedarf wieder planbar.
9. Bei Neustart werden Fälligkeiten aus gespeicherten Pruefstaenden rekonstruiert.
   Queue, Auftrags-IDs und transiente Fehlerzaehler sind RAM-Zustand; ihr exakter
   Backoff wird nicht persistiert. Alte Statuswerte werden nicht zurueckgesetzt.

Ein Executor- oder Lifecycle-Fehler ist fuer die DLMM-Anbindung fail-fast. Es
gibt keinen stillen Rueckfall auf den entfernten lokalen Scheduler. Die anderen
neun Worker behalten ihren vorhandenen Supervisor. Beim Stoppen werden neue
Auftraege eingestellt und laufende Antworten samt Registry-Uebernahme abgearbeitet.

## Einstellungen

Alle Zeiten sind Sekunden; erlaubt sind 1 bis 86400.

| Variable | Standard | Bedeutung |
| --- | ---: | --- |
| `COMPLETION_LIFECYCLE_REFRESH_SECONDS` | 900 | Abstand nach erfolgreicher Prepared-Pruefung |
| `COMPLETION_LIFECYCLE_RECHECK_SECONDS` | 300 | Abstand nach wiederpruefbarem Incomplete |
| `COMPLETION_LIFECYCLE_RETRY_SECONDS` | 60 | Initialer transienter Backoff |
| `COMPLETION_LIFECYCLE_RETRY_MAX_SECONDS` | 900 | Backoff-Obergrenze vor Jitter |
| `COMPLETION_LIFECYCLE_ATTEMPT_SECONDS` | 180 | Timeout eines realen Executor-Batches |
| `COMPLETION_LIFECYCLE_ACK_SECONDS` | 120 | Warten auf Registry-Uebernahme |
| `COMPLETION_LIFECYCLE_POOL_LIMIT` | 0 | Optional feste reale DLMM-Kohorte, 0 = alle |

Die Standardintervalle sind konfigurierbare Startwerte, kein nachgewiesenes
Durchsatzversprechen. `oldest_overdue_ms` zeigt eine eventuell unzureichende
Verarbeitungskapazitaet. Protokollspezifische Read-Optimierung folgt spaeter.

## Lokal und live pruefen

Mit bestehender PostgreSQL-/RPC-Konfiguration und funktionierendem DLMM-REST:

```powershell
git fetch origin
git switch --track origin/agent/cp2-dlmm-lifecycle
.\scripts\completion_lifecycle_live.ps1 -DurationSeconds 300 -PoolLimit 16
```

Das Skript prueft Checkout/HEAD, baut mit `cargo build --locked --release` und
verwendet den echten Logger `LIQ_GRAPH_LOG`. Es startet
`--completion-lifecycle-live`: DLMM-REST, Lifecycle, DLMM-Executor, Registry,
RPC-Transport und PostgreSQL-Writer. Andere Completion-Worker und Graph starten
in diesem isolierten Lauf nicht. `--run` und der bestehende DLMM-Einzelmodus
verwenden denselben Lifecycle-/Executor-Datenfluss.

Die erste reale zugelassene Kohorte ist fuer den Lauf fest; unsupported-Pools
belegen beim Eintritt keinen Platz. Der Live-Aufruf verwendet Refresh=30,
Recheck=45, Retry=5, Retry-Maximum=30 und zwei parallele DLMM-Batches. Er erzeugt
keine Testpools, resettet keine Statuswerte und sendet keine Transaktionen.
Die Environment-Aenderungen des Skripts werden anschliessend wiederhergestellt.

Ausgaben:

- `completion_lifecycle_live.log`: Branch, voller HEAD, Eingang/Planung,
  Auftrags-IDs, Ergebnisse, Registry-Bestaetigungen und Abschlusszaehler.
- `completion_lifecycle_verify.sql`: automatisch aus dem letzten bestaetigten
  Ergebnis je Pool dieses Laufs erzeugte **rein lesende** SQL-Abfrage. Auf
  derselben Datenbank unmittelbar nach dem Lauf ausfuehren. Erwartet:
  `proofs_expected = exact_matches`, `mismatches = 0`.
- `scripts/completion_lifecycle_inspect.sql`: zusaetzliche Bestands-/Fehleruebersicht.

Der Skripterfolg verlangt mindestens zwei erfolgreiche Prepared-Refresh-Auftraege
fuer denselben echten Pool, keine verbleibenden laufenden Auftraege, saubere
Registry-Zaehler und Empfang beim Persistenz-Writer. Die SQL-Abfrage belegt die
konkret akzeptierten abschliessenden Slots, Statuswerte und Zeitstempel in der DB.

Initial/Recheck/Retry, Reaktivierung, TVL-Entzug und Neustart werden nur dann als
live nachgewiesen bewertet, wenn sie tatsaechlich beobachtet wurden. Fehlende
reale Ereignisse werden nicht simuliert oder als bestanden ausgegeben. Ein
Refresh-Lauf allein nimmt diese weiteren Uebergaenge nicht ab.

## Stand bei Uebergabe

Die Bearbeitungsumgebung hat keine Rust-Toolchain und keine bereitgestellten
Live-Zugangsdaten. Ein erfolgreicher Build und die fachliche Live-Abnahme sind
hier deshalb nicht belegt. Es werden keine Unit-/Mock-/Regressionstests und
keine Formatierungspruefungen eingefuehrt. Erst der lokale Build, reale Logs und
die zugehoerige SQL-Auswertung erlauben die Abnahme dieser Stufe.
