# Completion Lifecycle: Meteora und Raydium

Basis dieser Erweiterung: `agent/cp2-damm-v2-lifecycle`, Commit
`5514e21aacb1d31b349bb6dbf0d837393e941e79`.
Testbranch: `agent/cp2-raydium-lifecycle`.

## Umfang und Grenzen

CP2.1 plant anhand echter Registry-Snapshots und Updates fuer alle beobachteten,
aktuell zugelassenen Pools. DLMM und DAMM v2 sind bereits angeschlossen;
diese Erweiterung schliesst **Raydium AMM v4, CPMM und CLMM an dieselbe
Lifecycle-Instanz** an. Die anderen fuenf Completion-Worker behalten ihre bisherige Steuerung; ihre
Lifecycle-Zusammenfassungen tragen `executing=false`. Keine weiteren Decoder,
Protokolle, Mint-Regeln oder TVL-Quellen werden umgebaut.

Der Lifecycle liegt in einem Worker, nicht im Core. Der Core enthaelt die
gemeinsamen Auftrags-/Antwortvertraege. Die Registry bleibt alleiniger
Pool-State-Writer, PostgreSQL hat weiterhin genau den bestehenden Writer und
dieselbe Tabelle. Der Lifecycle interpretiert keine protokollspezifische
Evidence und besitzt keine RPC-Verbindung. Die fuenf angeschlossenen Worker erhalten explizite
Auftraege; ihre lokalen Scheduler sind entfernt. Auch die Versuchsgrenze ist aus
ihren Protokoll-Settings entfernt und wird zentral im Lifecycle gesetzt. Es gibt
eine gemeinsame Registrierung fuer alle fuenf Executor-Receiver und einen gemeinsamen
Antwortkanal. Raydium Stable hat keinen Completion-Worker und ist nicht Teil
dieser Migration.

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
   und seine vorhandene Manifest-Erstellung. Auch Raydium behaelt seine Decoder,
   Mint-/Vault-/Konfigurationspruefungen und Manifest-Erstellung unveraendert;
   CLMM liest dabei die aktuellen TickArray-/Bitmap-Accounts erneut ein.
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

Ein Executor- oder Lifecycle-Fehler ist fuer alle fuenf Anbindungen fail-fast. Es
gibt keinen stillen Rueckfall auf den entfernten lokalen Scheduler. Die anderen
fuenf Worker behalten ihren vorhandenen Supervisor. Beim Stoppen werden neue
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
git switch --track origin/agent/cp2-raydium-lifecycle
.\scripts\completion_lifecycle_live.ps1 -Venue Raydium -DurationSeconds 300 -PoolLimit 16
```

Das Skript prueft Checkout/HEAD, baut mit `cargo build --locked --release` und
verwendet den echten Logger `LIQ_GRAPH_LOG`. Es startet
`--completion-lifecycle-live`: die ausgewaehlten REST-Quellen und Executor-Worker,
einen gemeinsamen Lifecycle, Registry, RPC-Transport und PostgreSQL-Writer.
`-Venue Raydium` prueft die drei Raydium-Worker gemeinsam (Standard).
`RaydiumAmmV4`, `RaydiumCpmm` und `RaydiumClmm` pruefen sie jeweils einzeln.
`Dlmm`, `DammV2` und `Both` bleiben verfuegbar; `All` startet alle fuenf.
Es laeuft genau eine gemeinsame Raydium-REST-Quelle, auch mit drei Raydium-Executoren.
Der Pool-Limit-Wert gilt je Protokoll; Raydium mit 16 bedeutet maximal 48 Pools,
All maximal 80. Andere Completion-Worker und Graph starten in
diesem isolierten Lauf nicht. `--run` und die bestehenden Einzelmodi verwenden
denselben Lifecycle-/Executor-Datenfluss. Im kombinierten endlichen Einzelmodus
begrenzt `--completion-limit` die Gesamtzahl der Lifecycle-Auftraege aller
angeschlossener Executor-Worker; es gibt keine getrennten Protokoll-Limits.

Die Auswahl erfolgt ausschliesslich in der Orchestrierung ueber
`COMPLETION_LIFECYCLE_LIVE_VENUE=dlmm|damm_v2|both|raydium_amm_v4|raydium_cpmm|raydium_clmm|raydium|all`;
der Lifecycle enthaelt keine Protokoll-Fallunterscheidung und unveraendert dieselben Zeitregeln.

Die erste reale zugelassene Kohorte ist fuer den Lauf fest; unsupported-Pools
belegen beim Eintritt keinen Platz. Der Live-Aufruf verwendet Refresh=30,
Recheck=45, Retry=5 und Retry-Maximum=30. DLMM und DAMM v2 verwenden dabei
zwei parallele Batches. Die Raydium-Worker behalten ihre vorhandene technische
Obergrenze von 16 parallelen Batches und ihre Account-Batchgroessen (AMM v4 bis
16, CPMM bis 12, CLMM bis 7 Pools, abhaengig von `RPC_ACCOUNT_BATCH_SIZE`).
Die feste reale Kohorte begrenzt zusaetzlich die insgesamt laufenden Pools.
Der Lauf erzeugt
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
und der ausgewaehlten Executor-Worker sowie die SQL-Abnahme bleiben verfuegbar.
Alle Protokolle nutzen dasselbe Pruefskript und dieselbe Tabelle.

## Stand bei Uebergabe

DLMM ist auf Basiscommit `1dc0b96` bereits live nachgewiesen: 160 Auftraege,
155 akzeptierte/persistierte terminale Ergebnisse, fuenf erfolgreiche Retry-
Wiederaufnahmen, 142 Prepared-Refreshes und SQL 16/16/0. Dieser Nachweis gilt
fuer den damaligen Stand. Auf Basiscommit `5514e21` ist auch der gemeinsame
DLMM-/DAMM-v2-Betrieb live nachgewiesen: 304 akzeptierte und persistierte
Prepared-Ergebnisse bei 32 Pools, wiederholte Refreshes je Protokoll und
SQL 32/32/0. Die Raydium-Erweiterung braucht ihren eigenen Build-/Live-/SQL-Nachweis.

Der erste Raydium-Lauf auf `cd2f8d4` belegt Build-Erfolg und 464 von der Registry
akzeptierte Auftraege: AMM v4 160 Prepared, CPMM 160 Prepared, CLMM 116 Prepared
und 28 Incomplete-Rechecks. Alle 44 Prepared-Pools wurden mehrfach aktualisiert;
vier CLMM-Pools blieben Incomplete. Der Lauf scheiterte jedoch beim Shutdown:
abgetrennte Raydium-Head-Refresh-Tasks hielten Source-Sender offen. Registry und
Persistenz konnten ihren Abschluss deshalb nicht bestaetigen. Die Head-Refreshs
gehoeren jetzt einem JoinSet und werden beim Abbruch der Discovery mit beendet.
Auch der unbenutzte AMM-v4-Import ist entfernt. Fuer diesen Fix ist ein erneuter
Live-Lauf mit Abschlusszaehlern und anschliessender SQL-Pruefung erforderlich.

Die Bearbeitungsumgebung hat keine Rust-Toolchain und keine bereitgestellten
Live-Zugangsdaten. Ein erfolgreicher Build und die fachliche Live-Abnahme sind
hier deshalb nicht belegt. Es werden keine Unit-/Mock-/Regressionstests und
keine Formatierungspruefungen eingefuehrt. Erst der lokale Build, reale Logs und
die zugehoerige SQL-Auswertung erlauben die Abnahme dieser Stufe.
