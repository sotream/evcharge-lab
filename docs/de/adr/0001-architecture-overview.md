<!-- i18n-source: 74dfd7b93249 -->
> 🇩🇪 Deutsche Übersetzung von [`docs/adr/0001-architecture-overview.md`](../../adr/0001-architecture-overview.md). Maßgeblich ist die englische Fassung.

# 0001. Architekturübersicht

- Status: accepted
- Datum: 2026-11-08
- Getrieben von ADR 0000: das gesamte Lastmodell; die Instanzzahlen stammen aus dessen Tabelle „Lab-Ziele“.

## Kontext

evcharge-lab ist die Seite des Charge Point Operators (CPO) einer Ladeplattform für Elektrofahrzeuge. Stationen
verbinden sich über langlebige WebSockets mit OCPP 1.6J. Fahrer finden Stationen, reservieren einen Connector,
starten und beenden Sessions und erhalten eine Abrechnung. Jeder Standort hat ein hartes Leistungslimit, das eine Smart-Charging-Engine
auf die aktiven Sessions verteilt. Messwerte strömen ununterbrochen ein.

Das Projekt ist ein System-Design-Lab. Jeder Service muss eine konkrete Frage beantworten:

| Service | Sprache | System-Design-Frage | ADR |
|---|---|---|---|
| `station-gateway` | NestJS | Wie hält man Tausende langlebiger WebSockets über N Instanzen und routet ein Kommando an genau die Instanz, die eine Station besitzt? Wie übersteht man Reconnect-Storms und Offline-Replay? | [0003](../../adr/0003-ocpp-subset-station-auth-and-offline-replay.md), [0004](../../adr/0004-websocket-connection-routing.md), [0012](../../adr/0012-kafka-replication-and-producer-backpressure.md) |
| `sessions` | NestJS | Wie nimmt man Reservierungen bei konkurrierenden Zugriffen an, ohne einen Connector doppelt zu buchen? Wie betreibt man Session-Zustandsautomaten, die von unzuverlässigen Stationsnachrichten getrieben werden? | [0005](../../adr/0005-reservation-contention.md) |
| `smart-charging` | Go | Wie verteilt man ein hartes Standortlimit fair, schnell und sicher auf Sessions, und was passiert, wenn der Allokator ausgefallen ist? | [0006](../../adr/0006-smart-charging-allocation.md) |
| `tariff` | Go | Wie berechnet man den Preis einer Session deterministisch (ganzzahlige Geldbeträge, Tageszeitgrenzen, Rundung)? | [0011](../../adr/0011-tariff-model.md) |
| `billing` | NestJS | Wie bekommt man aus At-least-once-Zustellung eine Exactly-once-*Wirkung* (eine Rechnung, eine Abbuchung), und wie kompensiert man über Services hinweg? | [0007](../../adr/0007-billing-saga-and-exactly-once-effect.md) |
| `telemetry-ingest` | Go | Wie nimmt man eine hohe, stoßartige Schreibrate von Messwerten mit Deduplizierung und begrenztem Arbeitsspeicher auf? | [0008](../../adr/0008-telemetry-storage.md), [0012](../../adr/0012-kafka-replication-and-producer-backpressure.md) |
| `driver-api` | NestJS | Wie authentifiziert man Fahrer und setzt Aufrufe an nachgelagerte Services mit Timeouts, Retries und Idempotenz zusammen? | [0010](../../adr/0010-authentication.md) |
| `simulator` | Node | Wie erzeugt man realistische Last von Tausenden Stationen, einschließlich Nachrichtenfluten, Offline-Phasen und Replays? | [0003](../../adr/0003-ocpp-subset-station-auth-and-offline-replay.md), [0013](../../adr/0013-traffic-generators-simulator-and-backfill.md) |

## Entscheidung

### Topologie

```
 stations (simulator) ──WS/OCPP──▶ station-lb (HAProxy, leastconn)
                                     │
                       ┌─────────────┴─────────────┐
                 station-gateway-1           station-gateway-2      ◀── Redis: Registry station→instance,
                       │  sync HTTP (Start/Stop/Authorize/credential)       Command-Kanal je Instanz, Dedup
                       ▼
                    sessions ──HTTP──▶ smart-charging (Go, zustandsloser Allokator)
                       │ outbox
                       ▼
   Kafka (3 Broker, RF 3): station.events · telemetry.meter-values · station.commands · station.command-results
             session.events · billing.events · ocpp.raw-log (14 d)
                       │                         │
                    billing ──HTTP──▶ tariff (Go)   telemetry-ingest (Go) ──▶ postgres-telemetry (Timescale) + Redis(latest)
                       │ outbox
 drivers ──HTTPS/JWT──▶ driver-api ──HTTP──▶ sessions, billing, telemetry-ingest
```

### Datenhoheit

Es gibt eine Datenbank je Service. Die OLTP-Datenbanken (`sessions`, `billing`, `tariff`, `drivers`) teilen sich eine
Postgres-18-Instanz. `telemetry` hat eine eigene TimescaleDB-Instanz, weil ihr Schreibvolumen und ihre gelockerte Durability
OLTP nicht berühren dürfen (ADR 0008). Kein Service liest die Datenbank eines anderen. Daten
überqueren Servicegrenzen nur über die Contracts in `packages/contracts` (ADR 0002): Kafka-
Events und interne HTTP-Aufrufe. Eine gemeinsame OLTP-Instanz ist eine Vereinfachung des Labs. Die Grenze ist die
Datenbank, ein späteres Aufteilen der Instanzen ändert also nur Connection-Strings.

`sessions` besitzt den Katalog (Standorte, Stationen, Connectoren, Credentials je Station), Reservierungen,
Sessions und die Zuordnung idTag→Fahrer. `driver-api` besitzt die Fahrerkonten. `billing` besitzt Zahlungsmittel,
Autorisierungen, Rechnungen und die Saga. `tariff` besitzt die Tarife. `telemetry-ingest` besitzt die
Messwert-Zeitreihe.

### Kommunikationsregeln

1. Nachrichten Station → CSMS, die eine synchrone OCPP-Antwort brauchen (Authorize, StartTransaction,
   StopTransaction), sind synchrone HTTP-Aufrufe vom Gateway an `sessions`, mit einem Timeout von 1 s (ADR 0000: CALLRESULT p99 < 500 ms)
   und Idempotency-Keys. Schlagen sie fehl, bekommt die Station einen CALLERROR und versucht es erneut, was OCPP-
   Stationen bei Transaktionsnachrichten ohnehin tun.
2. Alles andere von Stationen (Status, Boot, Messwerte) veröffentlicht das Gateway in Kafka.
   Das Gateway beantwortet den CALL, nachdem der Broker den Record bestätigt hat.
3. Kommandos an Stationen sind Kafka-Records auf `station.commands.v1`. Sie erreichen die besitzende Gateway-
   Instanz über die Redis-Registry (ADR 0004).
4. Domain-Events (`session.events`, `billing.events`) werden über eine transaktionale Outbox geschrieben.
   Jeder Consumer ist über eine Inbox-Tabelle idempotent bezüglich `eventId`.
5. Jeder ausgehende Aufruf hat einen Timeout. Retries sind begrenzt, nutzen Backoff mit Jitter und sind idempotent.

### Ports (Host-Seite; am 2026-11-08 mit `lsof` als frei geprüft)

| Port | Was |
|---|---|
| 25432 | Postgres (OLTP) |
| 25433 | Postgres + TimescaleDB (Telemetrie) |
| 26379 | Redis |
| 29091 / 29092 / 29093 | Kafka-Broker 1–3 (externe Listener) |
| 28080 | station-lb (OCPP-WebSocket-Edge); 28099 HAProxy-Statistik |
| 28081 | sessions |
| 28082 | driver-api |
| 28083 | billing |
| 28084 | smart-charging |
| 28085 | tariff |
| 28086 | telemetry-ingest |
| 28087 / 28088 | station-gateway-1 / -2 (direkt, für Metriken und Drills) |
| 28089 | Control-API des Simulators |
| 29090 / 23000 / 26686 / 24318 | Prometheus / Grafana / Jaeger-UI / OTLP HTTP |

Im Compose-Netzwerk lauscht jeder Service auf `:8080`. Jeder Service stellt auf diesem Port
`/health/live`, `/health/ready` und `/metrics` bereit.

### Kapazitätsplan (aus ADR 0000)

| Service | Spitzenbedarf der Flotte | Lab-Ziel je Instanz | Produktivinstanzen (N+1) | Compose-Instanzen |
|---|---|---|---|---|
| station-gateway | 50.000 Verbindungen, 2.400 msg/s | ≥ 10.000 Verbindungen, ≥ 2.000 msg/s | 6 | 2 (hinter HAProxy) |
| sessions | 45 Starts/s + 50 Schreibzugriffe/s + ~115 Allokationen/s | ≥ 200 Kommandos/s | 2 | 1 |
| smart-charging | ~115 Allokationen/s | ≥ 1.000/s, p99 < 20 ms | 2 | 1 |
| tariff | ~100 Berechnungen/s | ≥ 2.000/s, p99 < 20 ms | 2 | 1 |
| telemetry-ingest | 1.333 Zeilen/s | ≥ 2.500 Zeilen/s | 2 | 1 |
| driver-api | 500 Lese-RPS + 50 Schreib-RPS | ≥ 1.000 Lese-RPS, p99 < 300 ms | 2 | 1 |
| billing | 15 Rechnungen/s Spitze | keines (nicht in ADR 0000); nicht gemessen, es gibt keinen Billing-Lastlauf | 2 | 1 |

Das Lab misst eine Instanz je Service. Die Flottenzahlen sind `[derived]` aus dem gemessenen
Wert je Instanz. Jeder Service ist außer seiner Datenbank zustandslos, N+1 ist also eine Replica-Anzahl,
kein Redesign.

**Durability.** `sessions` und `billing` committen mit `synchronous_commit=on` (RPO 0 auf einem einzelnen
Knoten). Für RPO 0 über Knotenverlust hinweg braucht der Produktivbetrieb einen synchronen Standby, eine im Runbook
dokumentierte Lücke. Telemetrie lockert auf `synchronous_commit=off` je Verbindung (ADR 0008, RPO ≤ 5 min).

**Lange Aufbewahrung.** Sessions (117 GB/Jahr) und Rechnungen (10 Jahre, append-only) liegen in nach Monat
partitionierten Tabellen (`PARTITION BY RANGE (started_at / issued_at)`), sodass alte Partitionen ohne Rewrite
in kalten Speicher ausgelagert werden können.

### Lieferreihenfolge

1. Contracts, Scaffold, Compose, `make check` (diese Phase; die Checks liefen nur auf der Entwicklermaschine, teilweise abgelöst durch ADR 0020, das sie in CI ausführt).
2. Welle 1, parallel in Worktrees: station-gateway + simulator; sessions + Reservierungen;
   smart-charging; tariff; billing + telemetry-ingest.
3. Welle 2: driver-api + Auth, sowie Reviewer der Welle 1 gegen die Contracts und ADRs.
4. Welle 3: Lasttests, Ausfall-Drills, Diagramme, Runbook, README.

## Verworfene Alternativen

- **Ein einzelner modularer Monolith.** Er ist leichter zu betreiben, würde aber die Fragen verbergen, um die es in diesem Lab
  geht: serviceübergreifende Konsistenz, Routing zu zustandsbehafteten Verbindungen und Fallback, wenn eine Abhängigkeit
  ausgefallen ist.
- **Kafka für StartTransaction.** Ein asynchrones Request/Reply über Kafka für einen Aufruf, auf den eine Station
  blockierend wartet, fügt zwei Broker-Hops und Korrelationszustand hinzu und braucht trotzdem einen Timeout. Synchrones
  HTTP mit Idempotency-Keys ist einfacher und hat dieselbe Fehlersemantik, weil die Station es in beiden Fällen
  erneut versucht.
- **gRPC zwischen Services.** Das wäre eine weitere Toolchain (protoc) über zwei Sprachen hinweg. Das interne
  Aufrufvolumen ist gering. JSON über HTTP, validiert durch zod und JSON-Schema-Contract-Tests, genügt.
- **Eine eigene Postgres-Instanz je Service.** Sie kostet Speicher in einer 8-GB-Docker-VM und lehrt
  nichts über das hinaus, was Datenbank-je-Service auf einer gemeinsamen Instanz bereits zeigt.

## Konsequenzen

- `sessions` liegt auf dem Hot Path von StartTransaction und StopTransaction, seine Verfügbarkeit begrenzt also
  direkt das Starten neuer Sessions. Telemetrie und Status hängen nicht davon ab.
- Das Gateway ist für jede Nicht-Transaktionsnachricht vom Broker abhängig. ADR 0004 und der Drill zum Broker-Verlust zeigen das Verhalten:
  Stationen puffern und spielen dann nach.
- Datenbank-je-Service heißt, dass Abfragen über Services hinweg (etwa eine Rechnung verbunden mit einer Session)
  durch das Zusammensetzen von APIs in `driver-api` beantwortet werden, nicht durch SQL-Joins.
