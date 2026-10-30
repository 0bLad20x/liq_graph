# CP8H: PumpSwap-Gegenpools und Prognoseabgleich

Basis ist der echte CP8G-Stundenlauf auf `3547f32`. Keine neue Signalformel,
kein Mengenoptimierer, keine neue RPC-Abfrage und keine Providerumstellung.

## Zulassung und Gegenpools

`POOL_MIN_TVL_USD` bleibt fuer DLMM bei 500 USD. PumpSwap ist von der eigenen
TVL-Bedingung ausgenommen; Identitaet, Mintqualifikation und Prepared-Gate bleiben
erforderlich. Das gilt auch fuer PumpSwap-Konverter. Fehlende oder geringe
PumpSwap-TVL entzieht keine Zulassung. Andere aktivierte Venues behalten die Regel.

Es gibt keine zusaetzliche Mint-Whitelist oder zweite Poolsuche. Der Graph verbindet
bekannte Pools ueber gemeinsame Mints und die bestehenden Konvertierungsrouten.
Discovery findet weiterhin neue Pools ueber die vorhandenen Quellen; unbekannte
Gegenpools werden nicht als vorhanden angenommen. Pool-/Accountbudgets bleiben aktiv.

Registry zaehlt beim Shutdown fuer Nicht-Referenz-Mints zugelassener DLMM-Pools:
bekannte, zugelassene und Prepared-PumpSwap-Gegenpools. MarketState liefert seinen
eigenen finalen Selected-/Priced-Bestand ueber einen kleinen Core-Vertrag an die
Orchestrierung; Registry ergaenzt damit die letzten beiden Stufen. Keine kopierten
Preise, kein weiterer Dauerkonsument. Die Zahlen betreffen gemeinsame Zielmints,
nicht ausschliesslich identische Quote-Mints. Daten stehen unter
`registry_tvl.dlmm_pumpswap_counterparts` und in `summary.json`.

## Was verglichen wird

Die marginale Netto-Differenz ist kein mengenunabhaengiges Renditeversprechen.
Die erste positive Mengenprobe ist ebenfalls nicht die vom Executor gewaehlte
Handelsgroesse. CP8H behaelt deshalb getrennt:

1. Original-Witness, tatsaechlichen Packing-Seed und marginale Signalbewertung.
2. Beobachtete DEX-CPI-Poolfolge und netto Wallet-Tokenfluesse je Aufruf.
3. Vorhandene erste Mengenprobe einschliesslich ihrer Menge/Poolfolge.
4. Modellquote derselben beobachteten Poolfolge mit derselben Eingangsmenge.
5. Simulierten Walletgewinn, Netzwerkgebuehr und Kontoeinlagen aus den bisherigen Salden.

Executor erkennt deklarierte Poolaccounts in DEX-CPIs. SPL Transfer/TransferChecked
wird ueber parsed JSON oder die Standard-Tokeninstruction gelesen, ohne DEX-
spezifische Swapdecoder. Wallet-ATAs werden aus den vorhandenen Mintproofs abgeleitet.
Rueckerstattungen werden je Mint innerhalb eines Aufrufs verrechnet. Fehlende
Stackhoehe, mehrdeutige Pools, unaufgeloeste Transfers und nicht unterstuetzte
Transfererweiterungen bleiben ausdruecklich unvollstaendig. Rueckgerollte
Transfers belegen einen Versuch, keinen realisierten Gewinn.

ExecutionCandidates berechnet nach Ende der Messphase mit seinen bereits
eingefrorenen vollstaendigen Modellen. Es verwendet die vorhandene `amounts::quote`
einmal je Leg fuer die beobachtete Menge und fuer die verkettete Routenquote.
Kein Groessensuchlauf oder neuer Optimierer. Ein vollstaendiger Routenvergleich
setzt einen zusammenhaengenden einzelnen Settlement-Zyklus mit 2..4 Legs voraus.
Andere Fluesse bleiben fuer den Legvergleich sichtbar und erhalten einen Grund,
warum keine vergleichbare Gesamtquote vorliegt.

`source_path_relation`/`seed_path_relation` unterscheiden gleiche Route, andere
Route, abgebrochenen Praefix, fehlenden DEX-Aufruf und unklare Poolidentitaet.
Legdaten nennen Modelloutput, beobachteten Output, Abweichung, Snapshot-Slot und
Alter bei Requestbau. Die Gesamtquote verwendet dieselbe Eingangsmenge; DEX-Gebuehren
sind enthalten, externe Executor-/Flashloan-/Netzwerkgebuehren und Rent nicht.
Ein erkannter Zwischenabzug ausserhalb der DEX-CPIs verhindert den Gesamtvergleich.

Auch bei gleicher Route/Menge bleiben Snapshot und Simulation zeitlich verschieden.
Abweichungen koennen durch Zustandsaenderungen, Rundung, protokollspezifische
Rueckverguetungen oder ein Modellproblem entstehen. CP8H erfindet keine Ursache
aus einem Errorcode. Bei Fehlern bleibt `simulated_dex_return_bps=null`; der
versuchte Transfervergleich wird separat als `attempted_dex_return_bps` gespeichert.

## Aufzeichnung und Nachweis

`execution.json` und Ereignisse verwenden Schema 3. Jeder Versuch erhaelt seinen
kompakten Trace und `calibration` im vorhandenen JSONL. Vollstaendige Detailkopien
bleiben begrenzt. Der Analyzer zaehlt alle Vergleiche, Pfadbeziehungen,
fehlende Modelle und Abweichungen je Venue; hoechstens acht grosse Routenabweichungen
erscheinen als Beispiele. Rohwerte unterschiedlicher Mints werden nicht summiert.
Negative wirtschaftliche Ergebnisse lassen den technischen Nachweis nicht scheitern.

Die zusaetzliche Tracezeit wird in `trace_us`, die Nachberechnung in
`calibration.processing_ns` gemessen. Der Signalpfad und seine bisherigen Zeiten
bleiben getrennt. Flashloan bleibt verpflichtend, `minimum_profit=0`, `trade_size=0`,
Netto-Threshold 0 BPS, keine Broadcasts. Keine Abschaltung unbekannter Mengenfaelle.

```powershell
.\scripts\runtime_live.ps1 `
    -ExpectedBranch cp8h-pumpswap-signal-calibration `
    -EnabledVenues 'meteora_dlmm,pumpswap' `
    -Stage Signals `
    -DurationSeconds 1800 `
    -PoolLimit 0 `
    -AccountBudget 25000 `
    -MinPoolTvlUsd 500 `
    -ExecutionDetailLimit 16 `
    -ProfileSignals `
    -RequirePreparedEvidence `
    -RequireCalibrationEvidence `
    -SimulateV10 `
    -SimulationLimit 0 `
    -WalletPath (Join-Path $env:USERPROFILE '.solana\meteora-arb-test.json')
```

Providerkapazitaeten aus der privaten ENV beibehalten. Voraussetzung: locked Release-
Build auf dem Feature-HEAD. Fachliche Abnahme erst mit echtem Lauf: vollstaendige
Uebergabe, ein Vergleich je Request, wenigstens eine quotierbare beobachtete Route,
keine Evidenzverletzungen und ausgewiesene Gegenpoolabdeckung. Altlaeufe haben
keinen Schema-3-Trace und belegen diese neuen Datenwege nicht.
