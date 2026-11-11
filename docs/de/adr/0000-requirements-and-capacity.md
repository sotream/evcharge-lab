<!-- i18n-source: d4c7dfa9562b -->
> 🇩🇪 Deutsche Übersetzung von [`docs/adr/0000-requirements-and-capacity.md`](../../adr/0000-requirements-and-capacity.md). Maßgeblich ist die englische Fassung.

# ADR 0000: Anforderungen, Lastmodell und SLOs

- Status: accepted
- Datum: 2026-11-08

## Kontext

Jedes spätere ADR wählt Technologien und Topologien. Diese Entscheidungen sind nur anhand expliziter Zahlen vertretbar. Dieses ADR legt das Lastmodell, die Datenvolumina und die SLOs für die gesamte Plattform fest. Jede Zahl trägt ein Label:

- `[assumption]`: eine für dieses Lab gewählte Eingabe, dimensioniert für eine große CPMS-Plattform, die mehrere deutsche CPOs bedient (50.000 Stationen sind mehr, als ein einzelner deutscher CPO betreibt; die nationale Gesamtzahl vor dem Zitieren prüfen), nicht von einem realen Unternehmen übernommen;
- `[derived]`: aus Annahmen berechnet, mit gezeigter Formel;
- `[measured]`: später aus `docs/load-output/` oder `docs/drill-output/` eingetragen; nie von Hand getippt.

## Flotte und Nutzung

| Eingabe | Wert | Label |
|---|---|---|
| Ladepunkte (Stationen), jeweils mit einer persistenten OCPP-WebSocket-Verbindung | 50.000 | [assumption] |
| Connectoren je Station | 2 → 100.000 Connectoren | [assumption] |
| Stationen je Standort | 10 im Mittel, 40 max → 5.000 Standorte | [assumption] |
| Leistungsmix | 80 % AC 11/22 kW, 20 % DC 50-150 kW | [assumption] |
| Connector-Auslastung | 10 % im Mittel, 40 % in der Spitze (abends) | [assumption] |
| Sessiondauer | 45 min im Mittel (2.700 s); konservativ: AC-Sessions dauern typischerweise 2–4 h, die Startrate wird also um etwa den Faktor 3 überschätzt; als Reserve beibehalten | [assumption] |
| Registrierte Fahrer / täglich aktiv | 2.000.000 / 200.000 | [assumption] |
| Offline-Fenster einer Station (puffert lokal, spielt beim Reconnect nach) | bis zu 24 h | [assumption] |

## Traffic

| Fluss | Mittel | Spitze | Formel | Label |
|---|---|---|---|---|
| Gleichzeitige Sessions | 10.000 | 40.000 | Connectoren × Auslastung | [derived] |
| Session-Starts (= Stopps) | 3,7/s | 15/s, Auslegung 45/s (×3 Burst) | gleichzeitig / 2.700 s | [derived] |
| Sessions pro Tag | ~320.000 | | 10.000 × 86.400 / 2.700 | [derived] |
| Heartbeat (alle 300 s) | 167/s | 167/s | 50.000 / 300 | [derived] |
| MeterValues (alle 30 s je aktivem Connector) | 333/s | 1.333/s | gleichzeitig / 30 | [derived] |
| StatusNotification (~5 je Session) | 19/s | 75/s | Starts × 5 | [derived] |
| CALLRESULT für ausgehende Kommandos (SetChargingProfile) | | ~900/s | = ausgehende SetChargingProfile | [derived] |
| **OCPP eingehend gesamt** | ~550/s | **~2.500/s, Auslegung 3.750/s (×1,5)** | Summe, einschließlich CALLRESULTs der Kommandos | [derived] |
| Reconnect-Storm nach einem Gateway-Deploy oder -Absturz (alle Stationen innerhalb von 60 s) | | 833 Connects/s, ~2.500 msg/s (Boot + 2 Status) | 50.000 / 60 | [derived] |
| Smart-Charging-Neuberechnungen | | ~115/s | Starts+Stopps 30/s + 5.000 Standorte / 60 s periodisch | [derived] |
| SetChargingProfile ausgehend | | ~900/s | 115 × ~8 aktive Connectoren je ausgelastetem Standort | [derived] |
| Driver-API-Lesezugriffe (Karte, Verfügbarkeit, Session-Status) | 100 RPS | 500 RPS | [assumption] | [assumption] |
| Driver-API-Schreibzugriffe (reservieren, starten, stoppen) | 10 RPS | 50 RPS | [assumption] | [assumption] |
| Tarif-Quotes + abschließende Preisberechnungen | | ~100/s | Quotes + 15 Stopps/s | [derived] |

Sessions pro Tag (~320.000) bei 200.000 täglich aktiven Fahrern ergeben 1,6 Sessions je aktivem Fahrer und Tag, mehr als üblich; beide Eingaben sind Annahmen, und die Sessionrate ist die konservative.

## Datenvolumen

| Daten | Volumen | Aufbewahrung | Label |
|---|---|---|---|
| Messwerte, ~200 B je Zeile | 28,8 Mio. Zeilen/Tag ≈ 5,8 GB/Tag | roh 90 Tage (~520 GB), 1-Minuten-Rollups 2 Jahre | [derived] |
| Sessions, ~1 KB je Zeile | ~320 MB/Tag ≈ 117 GB/Jahr | 10 Jahre [assumption; prüfen: seit BEG IV (2025) können für Buchungsbelege 8 Jahre gelten] | [derived] |
| Abrechnungsdatensätze und Rechnungen | ~320.000/Tag | 10 Jahre, append-only | [derived] |
| Rohes OCPP-Nachrichtenlog | ~550 msg/s × ~300 B ≈ 14 GB/Tag | 14 Tage | [derived] |

## SLOs

| Bereich | Ziel |
|---|---|
| Remote-Start | Fahrer-Tap → RemoteStartTransaction an der Station zugestellt: p99 < 2 s; bestätigtes Laden: p99 < 5 s |
| OCPP-Antworten | CALLRESULT für Authorize / StartTransaction / StopTransaction: p99 < 500 ms |
| Telemetrie | Messwert persistiert: p99 < 5 s, in der Driver-API sichtbar < 10 s; Verlust ≤ 0,01 % im Normalbetrieb |
| Driver-API | 99,9 % Verfügbarkeit im Monat (43 min Budget); Lese-p99 < 300 ms, Schreib-p99 < 800 ms |
| Station-Gateway | 99,95 %; nach Verlust einer Instanz sind ihre Stationen innerhalb von 60 s wieder verbunden und routbar |
| Reservierungen | keine Doppelbuchungen eines Connectors; Reservierungs-TTL 15 min |
| Abrechnung | Exactly-once-Wirkung: 0 verloren, 0 doppelt belastet; Rechnung p99 < 5 min nach Sessionende |
| Smart Charging | das Standort-Leistungslimit wird durch die Allokation nie überschritten; Neuverteilung nach Session-Start/-Stopp p99 < 5 s; eine neue Session startet mit einem sicheren Minimalprofil, bis ihre Allokation erfolgt ist |
| Degradation | ist der Allokationsservice ausgefallen, wendet das Gateway einen statischen sicheren Anteil an (Limit / Connectoren); das Laden stoppt nie, weil das Backend ausgefallen ist |
| Durability | RPO 0 für Sessions und Abrechnung (synchroner Commit); RPO ≤ 5 min für Telemetrie. **Geändert durch ADR 0018:** RPO 0 gilt auf einem Knoten (begründet, nicht neu gemessen) und über einen Knotenverlust hinweg nur mit einem synchronen Standby; mit dem asynchronen Replica des Labs wird es verfehlt (281 und 282 bestätigte Schreibvorgänge in zwei Läufen verloren, als das Replica 3 s lang unerreichbar war, bevor der Primary ausfiel). |

Je Instanz, wenn 5 von 6 überleben: 750 msg/s [derived: 3.750 / 5].

## Lab-Ziele (eine Instanz, MacBook Air M1, docker compose)

Die volle Flotte lässt sich nicht auf einem Laptop betreiben. Gemessen wird eine Instanz je Service; die Flottengröße wird daraus abgeleitet.

| Service | Ziel je Instanz | Bedarf der Flotte | Instanzen (N+1) | Label |
|---|---|---|---|---|
| Station-Gateway | ≥ 10.000 WebSocket-Verbindungen, ≥ 2.000 msg/s | 50.000 Verbindungen, 3.750 msg/s | 6 | Ziel; derived |
| Telemetry-Ingest | ≥ 2.500 Zeilen/s nachhaltig | 1.333 Zeilen/s Spitze | 2 | Ziel; derived |
| Sessions / Reservierungen | ≥ 200 Kommandos/s | 45 Starts/s + 50 Schreibzugriffe/s | 2 | Ziel; derived |
| Smart Charging (Go) | ≥ 1.000 Standort-Allokationen/s, p99 < 20 ms | 115/s | 2 | Ziel; derived |
| Tarife (Go) | ≥ 2.000 Berechnungen/s, p99 < 20 ms | 100/s | 2 | Ziel; derived |
| Driver-API | ≥ 1.000 RPS lesend bei p99 < 300 ms | 500 RPS | 2 | Ziel; derived |

Ratenregel (geändert 2026-11-09): k6 meldet `http_reqs.rate` als Anzahl der Requests geteilt durch die gesamte Testlaufzeit einschließlich Anlauf und Auslauf, sodass ein Lauf mit konstanter Ankunftsrate, der genau mit der Zielrate angeboten wird, eine Rate knapp darunter meldet (zum Beispiel 199,99/s gegen 200/s). Ein auf diese Weise gemessenes Durchsatzziel gilt als erfüllt, wenn der Lauf keine Iteration verworfen hat, kein Request fehlgeschlagen ist und die gemeldete Rate mindestens 99,9 % des Ziels beträgt. Jedes andere Ziel, einschließlich jedes Latenz-, Verlust- und Drill-Checks, wird ohne Toleranz verglichen. Ein Lauf, der Iterationen verworfen hat, verfehlt sein Durchsatzziel, unabhängig von der Rate.

Offenlegung zu dieser Regel (Review November 2026): Sie wurde am 2026-11-09 hinzugefügt, einen Tag nach den k6-Läufen, die sie beurteilt (2026-11-08). Sie hat das Verdikt von drei Ergebnissen von verfehlt auf erfüllt geändert (sessions 199,99/s gegen 200, smart-charging 999,97/s gegen 1.000, tariff 1.999,97/s gegen 2.000: in keinem davon eine verworfene Iteration oder ein fehlgeschlagener Request). Sie gilt nur für `http_reqs.rate` von k6. Der Telemetry-Ingest-Benchmark wird ohne sie beurteilt: sein Producer bot nur 2.498,7 Zeilen/s gegen das Ziel von 2.500 an, sodass die nachhaltigen 2.471,23 Zeilen/s (98,9 % des Ziels, außerhalb der Toleranz von 0,1 %) verfehlt bleiben, obwohl die Lücke teilweise von der angebotenen Rate kommt, was dieselbe Art von Nenner-Artefakt ist, die die Regel für k6 verzeiht. Die Regel wird dort nicht angewendet, weil der Benchmark verworfene oder fehlgeschlagene Arbeit nicht so meldet wie k6; diese Asymmetrie ist konservativ (verfehlt bleibt verfehlt) und wird hier benannt statt verborgen.

Die Diagramme im README zeigen das Ziel gegenüber `[measured]` je Service. Ein verfehltes Ziel bleibt als verfehlt in der Tabelle, mit benanntem Engpass.

## Nicht-Ziele

OCPI-Roaming, OCPP 2.0.1, ISO 15118 Plug & Charge, ein echter Zahlungsanbieter (ein Mock-PSP mit Latenz- und Fehlerinjektion), Multi-Region-Active-Active (nur als abgeleiteter Skalierungspfad beschrieben), Anbindung an Netzbetreiber oder Energiemärkte.

## Konsequenzen

- ADR 0001 und jedes Service-ADR zitieren die Zeilen oben, die die Entscheidung treiben (zum Beispiel Wahl des Telemetrie-Speichers ← 5,8 GB/Tag und 1.333 Zeilen/s; Gateway-Routing ← 50.000 Sticky-Verbindungen, 6 Instanzen).
- Lasttests in `load/` und Drills in `drills/` verwenden diese Ziele als Pass/Fail-Schwellen.
- Zeigt eine Messung, dass ein Ziel auf dieser Hardware unmöglich ist, wird das ADR mit der gemessenen Zahl geändert, statt das Ziel stillschweigend zu senken.
