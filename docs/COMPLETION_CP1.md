# Completion CP1: gemeinsame Zulassung und Ergebnisvertraege

Basis: `main` bei `d98def9829e9b43ea0b09c7943791f3f88554a68`.
Branch: `agent/cp1-completion-contracts`.

## Umfang

Alle zehn Completion-Worker konsumieren `PoolState.completion_admission`.
Die Registry leitet diese Entscheidung aus vollstaendiger Mint-Identitaet und
ihrem aktuellen kanonischen `tvl_usd` ab. Die gemeinsame Grenze ist inklusive:
**TVL >= 500 USD**. Unbekannte, negative oder nicht endliche Werte qualifizieren
nicht. Beim DB-Restore bleibt die Live-TVL unbekannt, bis frische Quelldaten
eintreffen. Es gibt keinen protokollspezifischen Grenzwert mehr.

REST-/DexScreener-Beschaffung, Quellenprioritaet, TVL-Expiry und Jupiter-
Mintqualifizierung bleiben unveraendert. Dieser Checkpoint fuehrt kein zweites
wirtschaftliches Mint-Gate ein. Ein TVL-Entzug aendert die Zulassung, ohne den
technischen Completion-Status, das vorhandene Manifest oder Evidence zu loeschen.
Auch spaet eintreffende Completion-Ergebnisse stellen eine entzogene Zulassung
nicht wieder her.

Der Graph konsumiert dieselbe veroeffentlichte Freigabe und verlangt weiterhin
`Prepared` mit passendem, nicht leerem Manifest. `GRAPH_MIN_TVL_USD` ist damit
abgeloest; eine noch gesetzte Variable wird mit Warnung ignoriert. Der Aufbau
der Routengruppen wird nicht geaendert.

## Technischer Vertrag

| Ergebnis | Bedeutung |
| --- | --- |
| Prepared | Vollstaendiges technisch validiertes Manifest; kein Fehlercode |
| Incomplete | Technische Pruefung abgeschlossen, Voraussetzung fehlt/ist unzulaessig; typisierter Grund |
| Retryable | Versuch ohne neuen belastbaren Pruefstand; `transient_failure`; bestehende Evidence und Pruefzeit bleiben erhalten |

Fehlercodes: `transient_failure`, `validation_failed`,
`missing_execution_accounts`, `unsupported_configuration`,
`configuration_required`, `legacy_unclassified`.

Fehlende initialisierte Bin-/TickArrays und fehlende Manifest-Free-List-Nodes
werden als `missing_execution_accounts` klassifiziert. Explizit erkannte
Unterstuetzungsgrenzen erhalten `unsupported_configuration`; eine fehlende
PumpSwap-Wallet `configuration_required`. Sonstige abgeschlossene technische
Validierungsfehler erhalten `validation_failed`; ihr genauer Grund bleibt im
diagnostischen Freitext. Scheduler-Entscheidungen lesen keine Fehlertexte mehr.
Die bisherigen protokollspezifischen Token-/Extension-Pruefungen bleiben bestehen.

`checked_at_ms` bezeichnet das Ende einer abgeschlossenen technischen Pruefung,
nicht den letzten Schreibzeitpunkt eines On-Chain-Accounts. Die Registry
uebernimmt den Zeitwert zusammen mit dem akzeptierten Slot. Retryable erneuert
ihn nicht. Die Felder `reason_code` und `checked_at_ms` werden im vorhandenen
Descriptor-JSON gespeichert; keine neue Tabelle und kein Status-Reset.
Alte Deskriptoren bleiben lesbar: unbekannter Pruefzeitpunkt bleibt `None`;
alte Incomplete-Gründe werden ohne Textinterpretation als `legacy_unclassified`
eingelesen, ausser vorhandene Mint-Proofs belegen explizit inkompatible Extensions.

## Lokaler Build und Live-Nachweis

Voraussetzungen: bestehende `.env` mit PostgreSQL, Jupiter-API-Keys, RPC-
Konfiguration und funktionierendem DexScreener-HTTP-Transport. Es werden echte
Daten gelesen und ueber den vorhandenen Writer persistiert. Keine Transaktionen
werden gesendet, keine Statuswerte zur Erzeugung von Testfaellen zurueckgesetzt.

```powershell
git fetch origin
git switch --track origin/agent/cp1-completion-contracts
.\scripts\completion_contracts_live.ps1 -DurationSeconds 300
```

Der Befehl prueft Branch/HEAD/Working Tree, fuehrt `cargo build --locked` aus und
startet die reale Runtime mit dem vorhandenen CP1-Markt-Enrichment. Log:
`completion_contracts_live.log`. Anschliessend die rein lesenden Abfragen aus
`scripts/completion_contracts_inspect.sql` auf der bestehenden DB ausfuehren.

Ein Lauf ohne neues terminales Completion-Ergebnis ist keine Completion-Abnahme:
Sind alle geeigneten Pools bereits Prepared, kann dieser CP1-Lauf nur Zulassung,
Restore und Source-Updates zeigen. Regelmaessige Nachpruefung bestehender
Prepared-Pools wird erst mit CP2 angeschlossen. Vorhandene Einmal-/Worker-Modi
ohne TVL-Enrichment koennen Pools ohne aktuelle TVL entsprechend nicht zulassen.

Fachlich nachzuweisen sind neue typisierte Completion-Ergebnisse, deren
Persistenz und aktuelle zugelassene Prepared-Pools. Bei realen TVL-Grenzuebertritten
zusaetzlich Graph-Entzug bei erhaltener technischer Evidence belegen. Fehlende
Beobachtungen werden nicht als bestandener Nachweis gewertet.

## Abgrenzung und Stand

Lokale Versuchshistorien, begrenzte Retry-Anzahl und Ausschluss normaler
Prepared-Pools aus der erneuten Bearbeitung bestehen bis CP2 weiter. Dieser
Checkpoint etabliert keine Manifest-TTL und keinen periodischen Refresh.
CP2 uebernimmt Erstpruefung, Reaktivierung, Retry und Prepared-Nachpruefung in
einem gemeinsamen Lifecycle-Worker. CP3 optimiert protokollspezifische Reads.

Build-/Live-Abnahme steht aus. In der Bearbeitungsumgebung ist keine Rust-
Toolchain installiert; der Download-Endpunkt ist nicht erreichbar. Ein
GitHub-Actions-Buildversuch startete keine Build-Schritte. Der lokale
Abnahmebefehl ist vorbereitet; ohne erfolgreichen Build und echten Live-Lauf
ist der Checkpoint fachlich nicht abgeschlossen.
