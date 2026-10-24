# CP1: RPC-sparender Completion-Refresh – Implementierungsstand

Basis: `cp-mint-native-tvl-completion-admission`. Implementierungsbranch: `cp1-completion-state-aware-refresh`.

## Änderungen

Die bestehenden `CompletionWork`-Varianten und `CompletionState`-Status bleiben unverändert. Die neuen `CompletionOutcome::{Proof,Unchanged}` transportieren entweder einen vollständigen Registry-Proof oder die Bestätigung eines aktuellen Kurzchecks. `Unchanged` enthält keinen erneuerten Full-Proof und wird ausschließlich vom Lifecycle verarbeitet. Der unveränderte Registry-/Persistence-/Graph-Pfad empfängt ihn nicht. `CompletionRequest.force_full` erzwingt den bisherigen vollständigen Pfad.

Der Lifecycle führt den Zeitpunkt erfolgreicher Kurzchecks nur im RAM. Die nächste Fälligkeit wird daraus abgeleitet; eine vollständige Validierung wird trotzdem spätestens nach `COMPLETION_LIFECYCLE_FULL_REFRESH_SECONDS` (Standard 3600) angefordert. Nach einem Neustart wird nur die historisch persistierte Full-Proof-Zeit rekonstruiert. Transiente Fehler, Admission-Entzug und veraltete Antworten werden weiterhin im Lifecycle behandelt.

In allen zehn Protokoll-Workern bleibt `process()` der vollständige Builder. `process_optimized()` führt bei vorhandenem Prepared-Proof einen protokollspezifischen Kurzcheck aus und fällt bei Strukturänderung, fehlender Evidence oder Unsicherheit auf `process()` zurück. Keine eigenen RPC-Clients; die bestehenden `rpc_transport`-Batches bleiben zuständig.

| Venue | Kurzcheck |
| --- | --- |
| DLMM | LbPair + BitmapExtension; initialisierte Account-Auswahl gegen beide V10-Blöcke prüfen |
| DAMM v2 | Pool-State + Mint-/Vault-Pfade und Ergebnis-Metas |
| Raydium AMM v4 | 752-Byte-AmmInfo inkl. Swap-Status und gerichteten Metas |
| Raydium CPMM | PoolState inkl. Swap-Status/Open-Time und Config-/Observation-/Vault-Bindungen |
| Raydium CLMM | Parent, Bitmap und Kandidaten-Arrays; Richtungslisten und Account-Count |
| Orca Whirlpool | Parent und geslicte 12-Byte-Array-Header; Konsekutivität beider Richtungen |
| PancakeSwap CLMM | Parent und geslicte 44-Byte-Array-Header; alle existierenden Kandidaten |
| Byreal CLMM | Wie Pancake, zusätzlich Gebührenmodusprüfung |
| Manifest | Market einmal vollständig lesen, Free-List aktuell prüfen; Mint-/Vault-Metas aus Proof wiederverwenden |
| PumpSwap | GlobalConfig + Pool und alle relevanten Fee-/Buyback-ATA-Kandidaten erneut auswerten |

Alle Kurzprüfungen verwenden aktuelle echte RPC-Kontexte, sind aber kein Beweis für aktuelle Swap-Profitabilität und ersetzen keine vollständige strukturelle Revalidierung. Bei Pool-Accounts, die sich ohne Änderung ihrer Adresse ungültig verändern können, ist die periodische Full-Prüfung entscheidend.

Byreal: Neue durch dynamischen Gebührenmodus unzulässige Pools erhalten `DynamicModeUnsupported` und werden erneut überprüfbar. Historische `UnsupportedConfiguration`-Proofs können nur mit explizit gesetztem `COMPLETION_LIFECYCLE_RECHECK_PAUSED_ONCE=true` in einem auf Byreal und eine endliche Kohorte beschränkten Live-Lauf neu klassifiziert werden. Der Produktionsmodus `--run` lehnt diese Option ab.

## Lokal synchronisieren

```powershell
cd C:\dev_projects\liq_graph
git status --short
git fetch origin
git switch --track origin/cp1-completion-state-aware-refresh
git rev-parse --short HEAD
git status --short
cargo build --locked --release
git diff -- Cargo.lock
```

Bei bereits lokal angelegtem Branch statt `--track` direkt `git switch cp1-completion-state-aware-refresh` und `git pull --ff-only origin cp1-completion-state-aware-refresh`. Ein blockierter Checkout darf nicht mit pauschalem `reset --hard` oder `clean` erzwungen werden.

## Echter lokaler Live-Nachweis

```powershell
.\scripts\completion_lifecycle_live.ps1 -Venue All -DurationSeconds 600 -PoolLimit 16
.\scripts\cp3_db.ps1 -Action SqlFile -SqlFile .\scripts\completion_lifecycle_inspect.sql
```

Das Live-Skript setzt für den begrenzten Probelauf Refresh=30s und Full-Refresh=90s. Ein erfolgreicher CP1-Lauf muss mindestens einen `lifecycle unchanged` für Prepared und weiterhin echte vollständige Proofs bis Registry und Persistence belegen. Die Abschlussbilanz erfordert `dispatched=accepted+retryable+non_advancing+timeouts+withdrawn_in_flight`. `accepted` zählt Full- und Unchanged-Acknowledgements; nur Full-Records werden in den SQL-Verifikationsdatensatz aufgenommen. Ein fehlender Live-Nachweis pro Venue bleibt offen.

Optionaler begrenzter Byreal-Altdatensatz-Check:

```powershell
$env:COMPLETION_LIFECYCLE_RECHECK_PAUSED_ONCE="true"
.\scripts\completion_lifecycle_live.ps1 -Venue Byreal -DurationSeconds 600 -PoolLimit 16
Remove-Item Env:\COMPLETION_LIFECYCLE_RECHECK_PAUSED_ONCE
```

Der Code wurde über den GitHub-Connector committed. Ein `cargo build --locked` oder echter RPC-/SQL-Lauf ist ohne lokale Repo- und Providerumgebung nicht automatisch nachgewiesen. Keine Merge-Freigabe ohne Build und Live-Abnahme nach `AGENTS.md`.
