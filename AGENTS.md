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

## TESTVERTRAG V1.1

1. **BUILD:** `cargo build --locked` muss erfolgreich sein. Compilerfehler beheben, Warnings prüfen. Ein Build ist **kein** Funktionsnachweis.
2. **LIVE-TEST:** Jeder Worker muss eigenständig mit **realen Daten** ausführbar sein. Echten Input, Verarbeitung und Output nachweisen; nach Einbindung den vollständigen Datenfluss bis zum Verbraucher belegen, anhand realer Logs, Datenwerte und gegebenenfalls SQL-Abfragen.
3. **VERBOTEN:** `cargo fmt` und Formatierungsprüfungen; Architektur-, Unit-, Mock-, Snapshot- und Regressionstests; automatisierte Test-Suites, verpflichtende Workspace-Testläufe, zusätzliche Testframeworks und Test-Crates.
4. **ABNAHME:** **Ein nachgewiesener erfolgreicher Live-Test ist das einzige fachliche Abnahmekriterium.** Ohne Live-Nachweis ist der Checkpoint nicht abgeschlossen. Build-Erfolg ist Voraussetzung.
5. **AUSNAHMEN:** **Keine Ausnahmen.**

**VERBINDLICH:** Diese beiden Verträge haben für jeden Agenten und jeden Checkpoint Vorrang. Bei Regelverstoß oder ungeklärtem Verständnis: **Arbeit sofort stoppen, nicht improvisieren.**
