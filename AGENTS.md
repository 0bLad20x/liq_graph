# LIQ_GRAPH — VERBINDLICHER AGENTENVERTRAG

**PFLICHT VOR JEDER ARBEIT UND JEDER ANTWORT:** `AGENTS.md` vollständig lesen, verstehen und anwenden. Ist das Lesen nicht möglich oder eine Regel nicht eindeutig verstanden: **SOFORTIGER STOPP.** Keine Codeänderung, kein Commit, keine Fortsetzung. Keine eigenmächtigen Repository-Änderungen ohne ausdrücklichen Auftrag.

## ARCHITEKTURVERTRAG V1.0

1. **ARCHITEKTUR:** Ausschließlich **CORE · WORKER · ORCHESTRIERUNG**. Keine zusätzlichen Architekturschichten oder Crates.
2. **CORE:** Nur generische Infrastruktur und tatsächlich notwendige gemeinsame Datenverträge. Keine fachliche Verarbeitung oder protokollspezifische Logik.
3. **WORKER:** Genau eine Verantwortung; vollständig gekapselt: **Input → Verarbeitung → Output**. Eigene Decoder, Discriminatoren, interne Datentypen, Zustände und Algorithmen. Eigenständig startbar und ausführbar.
4. **ORCHESTRIERUNG:** Ressourcen bereitstellen, Worker starten, Datenströme verbinden, Shutdown koordinieren. Keine fachliche Verarbeitung.
5. **GRENZEN:** Worker kommunizieren ausschließlich über explizite, typisierte Verträge. Kein Zugriff auf die Implementierung oder private Zustände anderer Worker. Gemeinsame Contracts im CORE nur bei tatsächlichem workerübergreifendem Bedarf.
6. **ERWEITERUNG:** Neuer Worker = eine neue Worker-Datei, Registrierung und Verbindung in der Orchestrierung. **Ziel: höchstens zwei Änderungen an bestehenden Dateien.** Bestehende Worker bleiben unverändert.
7. **WIEDERVERWENDUNG:** Keine vorzeitige Auslagerung gemeinsamer Fachlogik. Duplikation ist zulässig. Keine zusätzlichen Adapterketten oder Abstraktionen auf Vorrat.
8. **CORE-ÄNDERUNG:** Nur bei tatsächlich benötigter neuer generischer Fähigkeit oder gemeinsamem Datenvertrag. Keine neuen Crates.
9. **GRUNDSATZ:** Ein neuer Worker erweitert das System, ohne bestehende Worker oder deren Implementierungen ändern zu müssen.


## GIT- UND LOCKFILE-VERTRAG V1.0

1. **BRANCH-ÜBERGABE:** Ein Feature-Branch darf erst zum lokalen Test übergeben werden, wenn alle für den Build benötigten Änderungen committed sind. Bei Dependency-Änderungen müssen `Cargo.toml` **und** das passend generierte `Cargo.lock` gemeinsam im Branch vorliegen. Keine manuell zusammengesetzten oder ungeprüften Lockfiles.
2. **BUILD AUF DEM RICHTIGEN BRANCH:** Vor einem Build-/Live-Test-Nachweis sind Branch, HEAD und Working-Tree-Status ausdrücklich zu prüfen (`git branch --show-current`, `git rev-parse --short HEAD`, `git status --short`). Ein `cargo build --locked` auf `main` beweist **nichts** über einen nicht ausgecheckten Feature-Branch. Niemals einen solchen Build als Branch-Abnahme werten.
3. **LOKALE SYNCHRONISIERUNG:** Bei einem durch lokale Änderungen blockierten Checkout **sofort stoppen und die Ursache beheben**; nicht wiederholt `git stash`, `git switch` und `cargo build` vorschlagen. Erhaltenwerte Änderungen zuerst identifizieren und einmalig sichern. Ist ausschließlich ein bereits gesichertes/entbehrliches `Cargo.lock` betroffen, darf die anschließende Branch-Umschaltung gezielt mit `git switch --discard-changes` erfolgen. Andere Nutzerdaten und untracked Logs bleiben unangetastet. Kein pauschales `git reset --hard`, kein `git clean`.
4. **LOCKFILE-HYGIENE:** Vor Übergabe `git diff -- Cargo.lock` und den Checkout-Zustand prüfen; versehentliche Änderungen oder LF/CRLF-Drift nicht als Feature-Änderung ausgeben. Nur durch tatsächliche Dependency-Änderungen nötige Lockfile-Anpassungen committen. Nach dem Build prüfen, dass keine unbeabsichtigten Änderungen zurückgeblieben sind.
5. **FEHLERBEHEBUNG STATT WIEDERHOLUNG:** Scheitert Checkout oder `cargo build --locked`, keinen weiteren Testlauf als Lösung empfehlen. Erst die konkrete Ursache und einen einmaligen, ausführbaren Behebungsweg liefern. Keine Merge- oder Abnahmebehauptung ohne die im TESTVERTRAG geforderten Nachweise.

## TESTVERTRAG V1.1

1. **BUILD:** `cargo build --locked` muss erfolgreich sein. Compilerfehler beheben, Warnings prüfen. Ein Build ist **kein** Funktionsnachweis.
2. **LIVE-TEST:** Jeder Worker muss eigenständig mit **realen Daten** ausführbar sein. Echten Input, Verarbeitung und Output nachweisen; nach Einbindung den vollständigen Datenfluss bis zum Verbraucher belegen, anhand realer Logs, Datenwerte und gegebenenfalls SQL-Abfragen.
3. **VERBOTEN:** `cargo fmt` und Formatierungsprüfungen; Architektur-, Unit-, Mock-, Snapshot- und Regressionstests; automatisierte Test-Suites, verpflichtende Workspace-Testläufe, zusätzliche Testframeworks und Test-Crates.
4. **ABNAHME:** **Ein nachgewiesener erfolgreicher Live-Test ist das einzige fachliche Abnahmekriterium.** Ohne Live-Nachweis ist der Checkpoint nicht abgeschlossen. Build-Erfolg ist Voraussetzung.
5. **AUSNAHMEN:** **Keine Ausnahmen.**

**VERBINDLICH:** Diese beiden Verträge haben für jeden Agenten und jeden Checkpoint Vorrang. Bei Regelverstoß oder ungeklärtem Verständnis: **Arbeit sofort stoppen, nicht improvisieren.**
