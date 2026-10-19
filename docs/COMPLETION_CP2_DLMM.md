# Gemeinsamer Completion Lifecycle

Basis des Abschlusscheckpoints: `agent/cp2-manifest-lifecycle`, Commit
`0856fda37184db4b7e508f2fda34f7118b6d4847`.
Gemeinsamer Build-/Live-/SQL-Nachweis: `agent/cp2-lifecycle-final`, Commit
`ddc6f5ec4c35410050f5bb74820181ff702c241d`, am 19.10.2026 abgenommen.

## Umfang und Grenzen

Der gemeinsame Lifecycle plant anhand echter Registry-Snapshots und Updates fuer
alle beobachteten, aktuell zugelassenen Pools. **Alle zehn Completion-Worker sind
an dieselbe Lifecycle-Instanz angeschlossen.** Auch Manifests lokale Ablaufsteuerung
ist entfernt, ebenso die
nur dafuer verbliebene Completion-Weiterleitung mit eigener Start-/Restart-Steuerung
in der Orchestrierung. Es gibt keinen parallelen Legacy-Completion-Pfad mehr.
Mint-Regeln und TVL-Quellen bleiben bei ihren vorhandenen CP1-Workern.

Der Lifecycle liegt in einem Worker, nicht im Core. Der Core enthaelt die
gemeinsamen Auftrags-/Antwortvertraege. Die Registry bleibt alleiniger
Pool-State-Writer, PostgreSQL hat weiterhin genau den bestehenden Writer und
dieselbe Tabelle. Der Lifecycle interpretiert keine protokollspezifische
Evidence und besitzt keine RPC-Verbindung. Die zehn angeschlossenen Worker erhalten explizite
Auftraege; ihre lokalen Scheduler sind entfernt. Auch die Versuchsgrenze ist aus
ihren Protokoll-Settings entfernt und wird zentral im Lifecycle gesetzt. Es gibt
eine gemeinsame Registrierung fuer alle zehn Executor-Receiver und einen gemeinsamen
Antwortkanal. Raydium Stable hat keinen Completion-Worker und ist nicht Teil
dieser Migration.

Orcas bisherige Sonderwarteschlange fuer Prepared-Evidence v1/v2 ist entfernt.
Auch diese Pools werden jetzt nach den gemeinsamen Refresh-Terminen neu geprueft;
der unveraenderte Orca-Pruefpfad erzeugt dabei Evidence v3. Es gibt keine
Orca-spezifische Faelligkeitsregel oder zweite Upgrade-Steuerung.

Prepared-Manifeste werden regelmaessig erneut geprueft. Bleiben die Informationen
gueltig, wird der Pruefzeitstempel auch bei unveraendertem Inhalt erneuert.
Erfuellen sie die Manifest-Anforderungen nicht mehr, uebernimmt die Registry
`Incomplete` und entfernt das Manifest; der Graph nimmt diesen Pool heraus.
Ein technischer Fehler liefert `Retryable`, behaelt den bisherigen Pruefstand
und fuehrt zentral zu einem neuen Versuch. Es gibt keine zusaetzliche Alters-
freigabe, keinen neuen Status und keinen automatischen Entzug allein durch
verstrichene Zeit. TVL-Zulassung und TVL-Expiry bleiben bei der Registry.

Dieser Abschlusscheckpoint verdrahtet im gemeinsamen Live-Lauf auch den bereits
vorhandenen Graph-Worker samt Verbraucher und vervollstaendigt das gemeinsame
Pruefskript. Er aendert weder Completion-Pruefpfade noch Status-/Graph-Logik.
Manifest ist strukturell abgeschlossen; eine verwertbare TVL-Quelle und sein
Prepared-Refresh-Nachweis sind auf Nutzerentscheidung zurueckgestellt.

## Vorgang und Regeln

1. Die Orchestrierung abonniert Registry-Updates vor dem Start-Snapshot. Auch
   Prepared-Pools sind enthalten. Die Registry stellt nach DB-Restore noch keine
   frische TVL bereit; erst aktuelle Source-Daten koennen einen Pool zulassen.
2. Ein zugelassener Unknown-Pool wird als `Initial` eingeplant. Ein Prepared-Pool
   wird als `Refresh` nach `checked_at_ms + refresh` eingeplant. Ein historischer
   Pruefstand ohne Zeitstempel ist nach Zulassung sofort faellig.
3. Wiederpruefbares Incomplete wird als `Recheck` nach dem Recheck-Intervall
   eingeplant. `unsupported_configuration` pausiert. `configuration_required`
   pausiert, solange die benoetigte Executor-Konfiguration fehlt. Meldet die
   Registrierung beim Prozessstart `configuration_ready=true`, gilt auch hier
   das gemeinsame Recheck-Intervall. PumpSwap leitet diese Bereitschaft aus
   seiner vorhandenen Wallet-Konfiguration ab; fuer die anderen neun Executoren
   ist `false` registriert. Der Lifecycle liest keine Protokoll-Evidence.
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
   Orca behaelt Pool-/Mint-/Vault-/Oracle-Pruefungen, gerichtete TickArray-Auswahl
   und Evidence v3; ein Refresh liest diese Accounts erneut ueber denselben RPC-Transport.
   PancakeSwap behaelt seinen kohärenten Pool-/Mint-/Vault-/Konfigurations-/
   Observation-/Bitmap-/TickArray-Pruefpfad und seine gerichteten Accountbloecke.
   Dasselbe gilt fuer Byreals vorhandenen Pruefpfad einschliesslich seiner
   TickArray-Layouts und Fee-Konfigurationspruefung. Seine Klassifizierung als
   `unsupported_configuration` bleibt beim Protokoll; die Pause steuert der Lifecycle.
   PumpSwap behaelt Pool-/GlobalConfig-/Mint-/Vault-/Fee-Account-Pruefungen und
   seinen bisherigen walletgebundenen V10-Manifest-Aufbau. Ohne Wallet liefert
   der Worker weiterhin Evidence und `Incomplete/ConfigurationRequired`.
   Manifest behaelt seinen vollstaendigen MarketFixed-/Mint-/Vault-Pruefpfad,
   den kohaerenten zweiten RPC-Snapshot und die gerichteten V10-Accountbloecke.
   Auch bei Refresh wird die bestehende Pruefung auf zwei gueltige freie
   Orderknoten erneut ausgefuehrt. Fehlen sie, liefert der Worker weiterhin
   `Incomplete/MissingExecutionAccounts`; die Wiederpruefung plant der Lifecycle.
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

Ein Executor- oder Lifecycle-Fehler ist fuer alle zehn Anbindungen fail-fast. Es
gibt keinen stillen Rueckfall auf einen entfernten lokalen Scheduler.
Beim Stoppen werden neue
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
git switch main
git pull --ff-only origin main
.\scripts\completion_lifecycle_live.ps1 -Venue All -DurationSeconds 600 -PoolLimit 16
```

Das Skript prueft Checkout/HEAD, baut mit `cargo build --locked --release` und
verwendet den echten Logger `LIQ_GRAPH_LOG`. Es startet
`--completion-lifecycle-live`: die ausgewaehlten REST-Quellen und Executor-Worker,
einen gemeinsamen Lifecycle, Registry, RPC-Transport und PostgreSQL-Writer.
`-Venue All` ist der Standard: alle zehn Executoren, die bestehenden Quellen und
zusaetzlich der vorhandene Graph samt Verbraucher. Die Laufzeit ist standardmaessig
600 Sekunden. Wiederholte Prepared-Refreshes werden fuer die neun Protokolle mit
nachgewiesener Versorgung verlangt. Manifest startet mit, bleibt aber hinsichtlich
TVL/Prepared-Refresh ausdruecklich zurueckgestellt. Seine Laufzeitfehler werden
weiterhin fatal behandelt; tatsaechlich akzeptierte Manifest-Pruefstaende gehen
ebenfalls in die SQL-Pruefung ein.
`-Venue Manifest` prueft Manifest DEX allein; dieser gezielte Nachweis verlangt
weiterhin echte Prepared-Refreshes und wird bei fehlender TVL nicht bestanden.
`-Venue PumpSwap` prueft PumpSwap allein.
`-Venue Byreal` prueft Byreal CLMM allein.
`-Venue PancakeSwap` prueft PancakeSwap CLMM allein.
`-Venue Orca` prueft Orca Whirlpool allein.
`-Venue Raydium` prueft die drei Raydium-Worker gemeinsam.
`RaydiumAmmV4`, `RaydiumCpmm` und `RaydiumClmm` pruefen sie jeweils einzeln.
`Dlmm`, `DammV2` und `Both` bleiben verfuegbar; `All` startet alle zehn.
Es laeuft genau eine gemeinsame Raydium-REST-Quelle, auch mit drei Raydium-Executoren.
Der Pool-Limit-Wert gilt je Protokoll; Manifest, PumpSwap, Byreal, PancakeSwap oder Orca
mit 16 bedeutet maximal 16 Pools, Raydium maximal 48, All maximal 160.
Bei einer einzelnen Protokollauswahl starten andere Completion-Worker und Graph
nicht. `--run` und die bestehenden Einzelmodi verwenden
denselben Lifecycle-/Executor-Datenfluss. Im kombinierten endlichen Einzelmodus
begrenzt `--completion-limit` die Gesamtzahl der Lifecycle-Auftraege aller
angeschlossener Executor-Worker; es gibt keine getrennten Protokoll-Limits.

Die Auswahl erfolgt ausschliesslich in der Orchestrierung ueber
`COMPLETION_LIFECYCLE_LIVE_VENUE=dlmm|damm_v2|both|raydium_amm_v4|raydium_cpmm|raydium_clmm|raydium|orca|pancakeswap|byreal|pumpswap|manifest|all`;
der Lifecycle enthaelt keine Protokoll-Fallunterscheidung und unveraendert dieselben Zeitregeln.

Die erste reale zugelassene Kohorte ist fuer den Lauf fest; unsupported-Pools
belegen beim Eintritt keinen Platz. Der Live-Aufruf verwendet Refresh=30,
Recheck=45, Retry=5 und Retry-Maximum=30. DLMM und DAMM v2 verwenden dabei
zwei parallele Batches. Die Raydium-Worker behalten ihre vorhandene technische
Obergrenze von 16 parallelen Batches und ihre Account-Batchgroessen (AMM v4 bis
16, CPMM bis 12, CLMM bis 7 Pools, abhaengig von `RPC_ACCOUNT_BATCH_SIZE`).
Orca behaelt ebenfalls 16 parallele Batches mit bis zu 8 Pools pro Batch
(`RPC_ACCOUNT_BATCH_SIZE / 12`) und TickArray-Radius 2. Es verwendet die bestehende
REST-Konfiguration `ORCA_API_RPS` und den gemeinsamen RPC-Transport;
es kommen keine Orca-spezifischen RPC-Variablen hinzu.
PancakeSwap und Byreal behalten je 16 parallele Batches mit bis zu 7 Pools pro
Batch (`RPC_ACCOUNT_BATCH_SIZE / 13`) und TickArray-Radius 2. Ihre REST-Quellen
verwenden unveraendert `PANCAKESWAP_API_RPM` bzw. `BYREAL_API_RPM` (Standard je 57);
die Completions verwenden denselben gemeinsamen RPC-Transport und dieselben Lifecycle-Zeiten.
PumpSwap behaelt acht parallele Batches mit bis zu zehn Pools pro Batch
(`(RPC_ACCOUNT_BATCH_SIZE - 1) / 5`). Fuer Prepared-Manifeste verwendet es
unveraendert `PUMPSWAP_V10_WALLET` als oeffentliche Wallet-Adresse.
Manifest behaelt 16 parallele Batches mit bis zu zehn Pools pro Batch
(`RPC_ACCOUNT_BATCH_SIZE / 5`) und verwendet denselben gemeinsamen RPC-Transport.
Es kommen keine Manifest-spezifischen RPC- oder Lifecycle-Variablen hinzu.
Die feste reale Kohorte begrenzt zusaetzlich die insgesamt laufenden Pools.
Der Lauf erzeugt
keine Testpools, resettet keine Statuswerte und sendet keine Transaktionen.
Die Environment-Aenderungen des Skripts werden anschliessend wiederhergestellt.

### Manifest und PumpSwap: frische TVL

PumpSwap-RPC-Discovery liefert Identitaeten, keine TVL. Auch die Manifest-Ticker-
Discovery liefert keine verwertbare TVL; ihr gemeldetes `liquidity_in_usd=0`
wird unveraendert nur im Source-Payload bewahrt. Deshalb startet der isolierte
Manifest- oder PumpSwap-Lauf zusaetzlich die bestehenden Worker `mint_qualification`,
`source_jupiter_mint::run_targeted` und `source_dexscreener::run_live`. Auch bei
`All` laeuft diese gemeinsame Anreicherung genau einmal. Manifest verwendet
seine bestehende Ticker-Discovery, PumpSwap seine rollierende RPC-Discovery.
Es gibt keinen Jupiter-Vollbackfill,
keine zusaetzliche TVL-Berechnung und keine Uebernahme historischer DB-TVL.
Die Registry entscheidet weiter anhand frischer kanonischer TVL >= 500 USD.
Der angegebene 600-Sekunden-Lauf laesst auch Zeit fuer diese vorgeschaltete Anreicherung.

Fuer Manifest sind die bestehende `JUPITER_SEARCH_API_KEYS`, PostgreSQL,
erreichbare/verifizierte DexScreener-Proxys, der Manifest-Ticker-Endpunkt und
ein RPC-Transport mit `getMultipleAccounts` erforderlich. Eine Wallet oder
RPC-V2-Discovery wird fuer den isolierten Manifest-Lauf nicht benoetigt.
Liefert DexScreener keine frische TVL >= 500 USD fuer einen Markt, bleibt er
unzulaessig; der Live-Test umgeht diese Grenze nicht. Historische Prepared-
Eintraege allein sind kein Ersatz fuer aktuelle Zulassung.

PumpSwap benoetigt zusaetzlich die bestehende `PUMPSWAP_V10_WALLET` sowie
`getProgramAccountsV2` und `getSlot`. Sein Prepared-Refresh-Test verlangt eine Wallet;
der normale PumpSwap-Executor
behaelt den walletunabhaengigen Evidence-Modus. Die bestehende Cache-Pruefung
verhindert, dass Prepared-Manifeste einer anderen Wallet weiterverwendet werden.
Ein Wallet-Wechsel wird nicht im laufenden Prozess vorgenommen.

In `--run` bleibt die vorhandene CP1-Verdrahtung ueber
`CP1_MARKET_ENRICHMENT_ENABLED` zustaendig. Die bisherigen Manifest-/PumpSwap-
Einzelmodi behalten ihre bisherigen Discovery-Eingaenge; ein Identitaetsfund allein
erzeugt dort ebenfalls keine TVL-Zulassung. Fuer die isolierte Lifecycle-Abnahme ist der oben
angegebene gemeinsame Live-Aufruf vorgesehen. Beim Stoppen werden die
Live-Eingangsproduzenten beendet; Lifecycle, Registry und Persistenz drainieren.

Ausgaben:

- `completion_lifecycle_<selection>_live.log`: Branch, voller HEAD, Eingang/Planung,
  Auftrags-IDs, Ergebnisse, Registry-Bestaetigungen und Abschlusszaehler.
- `completion_lifecycle_<selection>_verify.sql`: automatisch aus dem letzten bestaetigten
  Ergebnis je Pool dieses Laufs erzeugte **rein lesende** SQL-Abfrage. Auf
  derselben Datenbank unmittelbar nach dem Lauf ausfuehren. Erwartet:
  `proofs_expected = exact_matches`, `mismatches = 0`.
  Bei Prepared-PumpSwap-Ergebnissen wird auch die gespeicherte Wallet-Bindung
  gegen die tatsaechlich vom Executor verwendete oeffentliche Adresse geprueft.
- `scripts/completion_lifecycle_inspect.sql`: zusaetzliche Bestands-/Fehleruebersicht.

Der Skripterfolg verlangt genau eine Lifecycle-Instanz mit allen ausgewaehlten
Executoren und mindestens zwei erfolgreiche Prepared-Refresh-Auftraege fuer
denselben echten Pool je erforderlichem Protokoll. Nur im gemeinsamen `All`-Lauf
ist Manifests Refresh-Nachweis zurueckgestellt. Hinzu kommen keine verbleibenden
laufenden Auftraege, saubere Registry-Zaehler, Empfang beim Persistenz-Writer und
im gemeinsamen Lauf der abgeschlossene Graph-Datenfluss. Die Zahl der aus dem
Log gelesenen Pruefstaende muss dem terminalen Lifecycle-Abschlusszaehler entsprechen.
Die SQL-Abfrage belegt die konkret akzeptierten abschliessenden Slots, Statuswerte,
Zeitstempel und vorhandenen/entfernten Manifeste in der DB. Eine Protokoll-
Zusammenfassung und beobachtete Refresh-zu-Incomplete-Wechsel stehen am Logende.
Es werden maximal 160 letzte Pruefstaende erwartet, bei weiterhin inaktivem
Manifest maximal 144; die tatsaechliche Zahl stammt ausschliesslich aus dem Lauf.

Initial/Recheck/Retry, Prepared-zu-Incomplete, Reaktivierung, TVL-Entzug und Neustart werden nur dann als
live nachgewiesen bewertet, wenn sie tatsaechlich beobachtet wurden. Fehlende
reale Ereignisse werden nicht simuliert oder als bestanden ausgegeben. Ein
Refresh-Lauf allein nimmt diese weiteren Uebergaenge nicht ab.

Registry-Debug wurde im Skript abgeschaltet. Auftrags-/Ergebnislogs des Lifecycle
und der ausgewaehlten Executor-Worker sowie die SQL-Abnahme bleiben verfuegbar.
Alle Protokolle nutzen dasselbe Pruefskript und dieselbe Tabelle. Das Skript
akzeptiert einen sauberen, committeten Abschlussbranch und nach dessen Merge
auch `main`. Vor dem Merge muessen der gemeinsame Live-Lauf und die erzeugte
SQL-Abfrage nachgewiesen sein; ein beendeter Prozess allein reicht nicht.

## Stand bei Uebergabe

DLMM ist auf Basiscommit `1dc0b96` bereits live nachgewiesen: 160 Auftraege,
155 akzeptierte/persistierte terminale Ergebnisse, fuenf erfolgreiche Retry-
Wiederaufnahmen, 142 Prepared-Refreshes und SQL 16/16/0. Dieser Nachweis gilt
fuer den damaligen Stand. Auf Basiscommit `5514e21` ist auch der gemeinsame
DLMM-/DAMM-v2-Betrieb live nachgewiesen: 304 akzeptierte und persistierte
Prepared-Ergebnisse bei 32 Pools, wiederholte Refreshes je Protokoll und
SQL 32/32/0. Der Raydium-Lauf auf `109620f` belegt Build-Erfolg, 408 von Registry
und Persistenz uebernommene Auftraege (400 Prepared, 8 Incomplete), wiederholte
Refreshes fuer alle drei Raydium-Protokolle, sauberen Shutdown und SQL 48/48/0.
Der Orca-Lauf auf `1d09651` belegt 161 Auftraege, 160 akzeptierte/persistierte
Prepared-Ergebnisse, einen erfolgreich wiederaufgenommenen Retryable-Fall,
wiederholte Refreshes aller 16 Pools, sauberen Shutdown und SQL 16/16/0.
Der PancakeSwap-Lauf auf `86afa60` belegt 160 akzeptierte/persistierte
Prepared-Refreshes aller 16 Pools, sauberen Shutdown und SQL 16/16/0.
Der Byreal-Lauf auf `2232d68` belegt 160 akzeptierte/persistierte Prepared-Refreshes
aller 16 Pools, sauberen Shutdown und SQL 16/16/0.
Der PumpSwap-Lauf auf `939d4fc` belegt 312 akzeptierte/persistierte Prepared-
Refreshes bei 16 Pools, jeweils 19 bis 20 Aktualisierungen mit steigenden Slots
und Pruefzeiten, vollstaendigen Abschluss und SQL 16/16/0 einschliesslich Wallet-
Bindung. Initial/Recheck/Retry und TVL-Entzug wurden dort nicht beobachtet.
Der Manifest-Lauf auf `0856fda` belegt Build-Erfolg, Start/Stop der Anbindung und
386 ueber die Ticker-Quelle erfasste Maerkte je Durchlauf. Die Registry erhielt
keine frische TVL fuer Manifest, deshalb gab es null zugelassene Pools, null
Completion-Auftraege und keinen Prepared-Refresh-Nachweis. Die Migration ist
strukturell abgeschlossen; TVL-Versorgung und dieser Funktionsnachweis werden
auf Nutzerentscheidung nicht weiterverfolgt.

Der gemeinsame 600-Sekunden-Lauf auf `ddc6f5e` belegt genau eine Lifecycle-Instanz
mit allen zehn Executoren und wiederholte Prepared-Refreshes fuer jedes der neun
erforderlichen Protokolle. 2.221 Auftraege ergeben 2.203 akzeptierte terminale
Pruefstaende (2.119 Prepared, 84 Incomplete) und 18 Retryable-Versuche. Alle 2.203
terminalen Pruefstaende wurden persistiert; die Registry meldet null abgelehnte
Completions, null Konflikte und null verlorene Eingangsbatches. Nach Abschluss
sind keine Auftraege mehr in Bearbeitung oder ueberfaellig. Alle beobachteten
terminalen Folgepruefungen besitzen steigende Slots und Pruefzeiten.

Nachgewiesen sind 2.092 erfolgreiche Prepared-Refreshes, davon 1.968 wiederholte
Refreshes. Ein echter DLMM-Pool wechselte bei einer Refresh-Pruefung von Prepared
auf Incomplete/MissingExecutionAccounts und wurde danach zweimal erneut als
Incomplete geprueft. Initial-Auftraege und 17 erfolgreiche Retry/Prepared-
Auftraege sowie ein TVL-Zulassungsentzug wurden ebenfalls beobachtet. Die
rein lesende SQL-Auswertung bestaetigt die letzten Pruefstaende aller 144 Pools
mit `proofs_expected=144`, `exact_matches=144`, `mismatches=0`, einschliesslich
Manifest-Vorhandensein/-Entfernung und PumpSwap-Wallet-Bindung.

Der Graph verarbeitete 376.640 Pool-Updates und erzeugte 189 Generationen.
Graph-Verbraucher, Registry, Persistenz und Runtime beendeten den Lauf
vollstaendig; `lifecycle live complete=true persisted=true` liegt vor. Die
Quell-Neustarts bei Raydium/Orca und einzelne RPC-Timeouts sowie ein HTTP 401 bei
`shyft-02/getSlot` sind beobachtete Betriebshinweise. Sie fuehrten in diesem Lauf
zu keinem abgelehnten Completion-Ergebnis. Manifest blieb mit null Auftraegen
entsprechend der dokumentierten Zurueckstellung ohne Refresh-Nachweis.

Damit ist CP2 im vereinbarten Umfang fachlich abgenommen. Die Abnahme beruht
auf dem lokal ausgefuehrten Build, `completion_lifecycle_all_live.log` und der
zugehoerigen SQL-Auswertung. Die Bearbeitungsumgebung selbst besitzt keine
Rust-/PowerShell-Toolchain oder Live-Zugangsdaten. Es wurden keine Unit-/Mock-/
Regressionstests und keine Formatierungspruefungen eingefuehrt.
