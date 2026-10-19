# Completion Lifecycle: DLMM und DAMM v2

Basis dieser Erweiterung: `agent/cp2-dlmm-lifecycle`, Commit
`1dc0b96c55b4ebfba457dc7a967a2c129829ed89`.
Testbranch: `agent/cp2-damm-v2-lifecycle`.

## Umfang und Grenzen

CP2.1 plant anhand echter Registry-Snapshots und Updates fuer alle beobachteten,
aktuell zugelassenen Pools. CP2.2 hat DLMM angeschlossen; diese Erweiterung schliesst **DAMM v2 an dieselbe
Lifecycle-Instanz** an. Die anderen acht Completion-Worker behalten ihre bisherige Steuerung; ihre
Lifecycle-Zusammenfassungen tragen `executing=false`. Keine weiteren Decoder,
Protokolle, Mint-Regeln oder TVL-Quellen werden umgebaut.

Der Lifecycle liegt in einem Worker, nicht im Core. Der Core enthaelt die
gemeinsamen Auftrags-/Antwortvertraege. Die Registry bleibt alleiniger
Pool-State-Writer, PostgreSQL hat weiterhin genau den bestehenden Writer und
dieselbe Tabelle. Der Lifecycle interpretiert keine protokollspezifische
Evidence und besitzt keine RPC-Verbindung. Die beiden Meteora-Worker erhalten explizite
Auftraege; ihre lokalen Scheduler sind entfernt. Auch die Versuchsgrenze ist aus
ihren Protokoll-Settings entfernt und wird zentral im Lifecycle gesetzt. Es gibt
eine gemeinsame Registrierung fuer beide Executor-Receiver und einen gemeinsamen
Antwortkanal, keine zweite Lifecycle-Instanz fuer DAMM v2.

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
   DAMM v2 verwendet unveraendert seinen kohärenten Pool-/Mint-/Vault-Pruefpfad
   und seine vorhandene Manifest-Erstellung. Ergebnisse werden nach Pool-Adresse den Auftrags-IDs zugeordnet, weil interne
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

Ein Executor- oder Lifecycle-Fehler ist fuer beide Anbindungen fail-fast. Es
gibt keinen stillen Rueckfall auf den entfernten lokalen Scheduler. Die anderen
acht Worker behalten ihren vorhandenen Supervisor. Beim Stoppen werden neue
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
| `COMPLETION_LIFECYCLE_POOL_LIMIT` | 0 | Optional feste reale Kohorte je Executor, 0 = alle |

Die Standardintervalle sind konfigurierbare Startwerte, kein nachgewiesenes
Durchsatzversprechen. `oldest_overdue_ms` zeigt eine eventuell unzureichende
Verarbeitungskapazitaet. Protokollspezifische Read-Optimierung folgt spaeter.

## Lokal und live pruefen

Mit bestehender PostgreSQL-/RPC-Konfiguration und erreichbaren REST-Quellen
der ausgewaehlten Protokolle:

```powershell
git fetch origin
git switch --track origin/agent/cp2-damm-v2-lifecycle
.\scripts\completion_lifecycle_live.ps1 -Venue Both -DurationSeconds 300 -PoolLimit 16
```

Das Skript prueft Checkout/HEAD, baut mit `cargo build --locked --release` und
verwendet den echten Logger `LIQ_GRAPH_LOG`. Es startet
`--completion-lifecycle-live`: die ausgewaehlten REST-Quellen und Executor-Worker,
einen gemeinsamen Lifecycle, Registry, RPC-Transport und PostgreSQL-Writer.
`-Venue DammV2` prueft DAMM v2 allein (Standard), `-Venue Dlmm` DLMM allein,
`-Venue Both` beide zugleich. Der Pool-Limit-Wert gilt je Protokoll; Both mit
16 bedeutet maximal 32 Pools. Andere Completion-Worker und Graph starten in
diesem isolierten Lauf nicht. `--run` und die bestehenden Einzelmodi verwenden
denselben Lifecycle-/Executor-Datenfluss. Im kombinierten endlichen Einzelmodus
begrenzt `--completion-limit` die Gesamtzahl der Lifecycle-Auftraege beider
angeschlossener Executor-Worker; es gibt keine getrennten Protokoll-Limits.

Die Auswahl erfolgt ausschliesslich in der Orchestrierung ueber
`COMPLETION_LIFECYCLE_LIVE_VENUE=dlmm|damm_v2|both`; der Lifecycle enthaelt keine
DAMM-/DLMM-Fallunterscheidung und unveraendert dieselben Zeitregeln.

Die erste reale zugelassene Kohorte ist fuer den Lauf fest; unsupported-Pools
belegen beim Eintritt keinen Platz. Der Live-Aufruf verwendet Refresh=30,
Recheck=45, Retry=5, Retry-Maximum=30 und zwei parallele Batches je Protokoll. Er erzeugt
keine Testpools, resettet keine Statuswerte und sendet keine Transaktionen.
Die Environment-Aenderungen des Skripts werden anschliessend wiederhergestellt.

Ausgaben:

- `completion_lifecycle_<selection>_live.log`: Branch, voller HEAD, Eingang/Planung,
  Auftrags-IDs, Ergebnisse, Registry-Bestaetigungen und Abschlusszaehler.
- `completion_lifecycle_<selection>_verify.sql`: automatisch aus dem letzten bestaetigten
  Ergebnis je Pool dieses Laufs erzeugte **rein lesende** SQL-Abfrage. Auf
  derselben Datenbank unmittelbar nach dem Lauf ausfuehren. Erwartet:
  `proofs_expected = exact_matches`, `mismatches = 0`.
- `scripts/completion_lifecycle_inspect.sql`: zusaetzliche Bestands-/Fehleruebersicht.

Der Skripterfolg verlangt mindestens zwei erfolgreiche Prepared-Refresh-Auftraege
fuer denselben echten Pool **je ausgewaehltem Protokoll**, keine verbleibenden laufenden Auftraege, saubere
Registry-Zaehler und Empfang beim Persistenz-Writer. Die SQL-Abfrage belegt die
konkret akzeptierten abschliessenden Slots, Statuswerte und Zeitstempel in der DB.

Initial/Recheck/Retry, Reaktivierung, TVL-Entzug und Neustart werden nur dann als
live nachgewiesen bewertet, wenn sie tatsaechlich beobachtet wurden. Fehlende
reale Ereignisse werden nicht simuliert oder als bestanden ausgegeben. Ein
Refresh-Lauf allein nimmt diese weiteren Uebergaenge nicht ab.

Registry-Debug wurde im Skript abgeschaltet. Auftrags-/Ergebnislogs des Lifecycle
und der beiden Executor-Worker sowie die SQL-Abnahme bleiben verfuegbar. Es werden
keine separaten DAMM-v2-Pruefskripte oder Tabellen eingefuehrt.

## Stand bei Uebergabe

DLMM ist auf Basiscommit `1dc0b96` bereits live nachgewiesen: 160 Auftraege,
155 akzeptierte/persistierte terminale Ergebnisse, fuenf erfolgreiche Retry-
Wiederaufnahmen, 142 Prepared-Refreshes und SQL 16/16/0. Dieser Nachweis gilt
fuer den damaligen Stand. Die DAMM-v2-Erweiterung und der gemeinsame Betrieb
beider Executor-Worker brauchen den neuen Build-/Live-/SQL-Nachweis.

Die Bearbeitungsumgebung hat keine Rust-Toolchain und keine bereitgestellten
Live-Zugangsdaten. Ein erfolgreicher Build und die fachliche Live-Abnahme sind
hier deshalb nicht belegt. Es werden keine Unit-/Mock-/Regressionstests und
keine Formatierungspruefungen eingefuehrt. Erst der lokale Build, reale Logs und
die zugehoerige SQL-Auswertung erlauben die Abnahme dieser Stufe.
