/** Emits JSON Schema for every event + internal API body into schemas/ (consumed by Go contract tests). */
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { z } from 'zod';
import { ALL_EVENTS } from './events';
import { AllocateReq, AllocateRes, QuoteReq, QuoteRes } from './api';

const out = process.argv[2] ?? 'schemas';
mkdirSync(out, { recursive: true });
const all: Record<string, z.ZodType> = { ...ALL_EVENTS, AllocateReq, AllocateRes, QuoteReq, QuoteRes };
for (const [name, schema] of Object.entries(all)) {
  writeFileSync(join(out, `${name}.json`), JSON.stringify(z.toJSONSchema(schema, { io: 'input' }), null, 2) + '\n');
}
process.stdout.write(`wrote ${Object.keys(all).length} schemas to ${out}\n`);

// Fleet fixture for cross-language checks of the fleet convention (Go backfill must match it).
import { buildCatalog } from './fleet';
const fixture = [...buildCatalog({ sites: 3, stationsPerSite: 10, dcShare: 0.2, secretSeed: 'fixture-seed' })].map((s) => ({
  siteId: s.siteId,
  capW: s.capW,
  stations: s.stations.map((st) => ({ stationId: st.stationId, secretSha256: st.secretSha256, connectors: st.connectors.map((c) => ({ connectorId: c.connectorId, currentType: c.currentType, maxPowerW: c.maxPowerW, minPowerW: c.minPowerW, tariffId: c.tariffId })) })),
}));
writeFileSync(join(out, 'fleet-fixture.json'), JSON.stringify(fixture, null, 2) + '\n');
