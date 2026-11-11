<!-- i18n-source: 8ecbcecc3224 -->
> 🇩🇪 Deutsche Übersetzung von [`docs/development.md`](../development.md). Maßgeblich ist die englische Fassung.

# Entwicklung

## Repo-Übersicht

```
docs/
  adr/                 ADRs und der Index: Anforderungen, Architektur, Contracts, Entscheidungen je Service
  results.md           Messaufbau, Lesehilfe für die Verdikte, generierter Diagrammblock
  metrics.md           generiert: die vollständige Metrik-Tabelle
  drills.md            generiert: Zusammenfassung und jeder Drill-Lauf mit seinem Zeitverlaufsdiagramm
  findings.md          was Drills, Lasttests und Reviews aufgedeckt haben (handgeschriebene Einträge, generierte Tabelle)
  guarantees.md        die Garantien verteilter Systeme und der Drill hinter jeder einzelnen
  known-limits.md      bekannte Grenzen und Lücken für den Produktivbetrieb
  runbook.md           Start, Stopp, alarmwürdige Signale, Rezepte für Ausfälle, bekannte Lücken
  load-output/         rohe k6-Zusammenfassungen (+ .env.json-Setup-Protokolle) und Tool-Ausgaben: die Quelle jeder gemessenen Zahl
  drill-output/        rohe Ausgabe der Ausfall-Drills: timeline.jsonl, events.log, result.json je Lauf
  img/                 architecture.svg (von Hand) und die generierten Diagramme
  de/                  deutsche Übersetzungen ausgewählter Seiten (geprüft von scripts/check-i18n.mjs)
services/
  station-gateway/     NestJS: OCPP-1.6J-WebSocket-Edge, Routing, Offline-Replay
  sessions/            NestJS: Katalog, Reservierungen, Session-Zustandsautomat, Allokationsrunden
  billing/             NestJS: Saga, Mock-PSP, Rechnungen
  driver-api/          NestJS: Fahrer-Authentifizierung und die öffentliche API
  smart-charging/      Go: Water-Filling-Allokator, Tool allocbench
  tariff/              Go: deterministische Preisberechnung, Tool quotebench
  telemetry-ingest/    Go: Kafka nach TimescaleDB, COPY in Batches
simulator/             Node: OCPP-Stationsflotte, Nachfrage und Physik, Control-API
packages/
  contracts/           TypeScript-Contracts, Event-Schemas, gemeinsame Flotten-Konvention
  contracts-go/        Go-Seite der Contracts
  platform/            gemeinsame NestJS-Bausteine: Consumer, Outbox, HTTP-Client, Metriken
  platform-go/         gemeinsame Go-Bausteine
load/                  k6-Skripte, run.sh (hält das Setup neben jeder Zusammenfassung fest) und targets.json
drills/                Ausfall-Drills: Broker-Verlust, Gateway-Verlust, Reconnect-Storm, langsames Postgres, Allokator-Ausfall, Offline-Replay einer Station
scripts/
  charts/              Generator für die Diagramme, die Metrik-Tabelle, die Drill-Seite und die generierten README-Blöcke (reines Node, keine Abhängigkeiten)
  backfill-telemetry/  Go: Historie direkt in TimescaleDB, Messungen zu Kompression und Abfragen
  check-topics.mjs     prüft, dass die Kafka-Topic-Listen im Code und in infra/kafka/topics.sh übereinstimmen
  check-i18n.mjs       prüft, dass jede Übersetzung unter docs/de/ eine englische Quelle hat und nicht veraltet ist
infra/                 Compose-Konfiguration: HAProxy, Kafka-Topics, Postgres-Init, Prometheus, Grafana
docker-compose.yml     der gesamte Stack
Makefile               make check, make up, make down
```


## Diagramme und generierte Tabellen neu erzeugen

```sh
node scripts/charts/build.mjs
node --test scripts/charts/     # Generator-Tests; läuft auch in make check
```

Dabei werden `docs/img/*.svg`, `docs/metrics.md`, `docs/drills.md` und die generierten Blöcke im README und in den Seiten unter `docs/` (Statistiken, Ziele auf einen Blick, Diagramme, Drill-Tabelle, Findings-Tabelle) ausschließlich aus den Rohdateien neu geschrieben. Siehe [`scripts/charts/README.md`](../../scripts/charts/README.md).

## Übersetzungen

Englisch ist die Quelle. Eine Übersetzung liegt unter `docs/de/` im selben relativen Pfad wie das Original und beginnt mit
`<!-- i18n-source: <hash> -->`, dem Hash der englischen Datei ohne deren generierte Blöcke. `node scripts/check-i18n.mjs`
(Teil von `make check`) schlägt fehl, wenn sich das Original geändert hat, und nennt den neuen Hash, der nach dem Aktualisieren der Übersetzung einzutragen ist.
Generierte Seiten (`drills.md`, `metrics.md`) werden nicht übersetzt. Zahlen, Code, Befehle und Metriknamen bleiben unverändert.

## Commit-Nachrichten

Commit-Betreffzeilen und PR-Titel folgen [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
(`feat(billing): add credit notes`); Typen, Scopes und Regeln stehen in [`CLAUDE.md`](../../CLAUDE.md). Der Workflow `commits`
prüft den PR-Titel und jeden Commit eines PRs; dieselbe Prüfung läuft lokal mit
`node scripts/check-commits.mjs origin/main HEAD`.
