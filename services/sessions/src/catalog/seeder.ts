/* oxlint-disable no-await-in-loop */ // batches are inserted sequentially inside one transaction
import { type Logger, type Pool, withTx } from '@evcharge/platform';
import { fleet } from '@evcharge/contracts';

const BATCH_SITES = 200;

/**
 * Seeds the deterministic lab catalog when `sites` is empty. One transaction under an advisory lock, so two
 * replicas starting together seed once. Periodic allocation rounds are spread over the first period.
 * Returns the number of sites inserted.
 */
export async function seedCatalogIfEmpty(pool: Pool, opts: fleet.SeedOptions & { allocationPeriodS: number }, logger?: Logger): Promise<number> {
  if (opts.sites === 0) return 0;
  return withTx(pool, async (c) => {
    await c.query('SELECT pg_advisory_xact_lock(72616003)');
    const { rows } = await c.query<{ any: boolean }>('SELECT EXISTS (SELECT 1 FROM sites) AS any');
    if (rows[0]?.any) return 0;
    let batch: fleet.SeedSite[] = [];
    let total = 0;
    const flush = async () => {
      if (batch.length === 0) return;
      await insertSites(c, batch, opts.allocationPeriodS);
      total += batch.length;
      batch = [];
    };
    for (const site of fleet.buildCatalog(opts)) {
      batch.push(site);
      if (batch.length >= BATCH_SITES) await flush();
    }
    await flush();
    logger?.info({ sites: total, stationsPerSite: opts.stationsPerSite, dcShare: opts.dcShare }, 'catalog seeded');
    return total;
  });
}

async function insertSites(db: { query: Pool['query'] }, sites: fleet.SeedSite[], periodS: number): Promise<void> {
  await db.query(
    `INSERT INTO sites (site_id, name, lat, lng, cap_w, alloc_due_at)
     SELECT s.id, s.name, s.lat, s.lng, s.cap, now() + random() * make_interval(secs => $6::double precision)
       FROM unnest($1::text[], $2::text[], $3::float8[], $4::float8[], $5::int[]) AS s(id, name, lat, lng, cap)`,
    [sites.map((s) => s.siteId), sites.map((s) => s.name), sites.map((s) => s.lat), sites.map((s) => s.lng), sites.map((s) => s.capW), periodS],
  );
  const stations = sites.flatMap((s) => s.stations);
  await db.query(
    `INSERT INTO stations (station_id, site_id, lat, lng, secret_sha256)
     SELECT * FROM unnest($1::text[], $2::text[], $3::float8[], $4::float8[], $5::text[])`,
    [stations.map((s) => s.stationId), stations.map((s) => s.siteId), stations.map((s) => s.lat), stations.map((s) => s.lng), stations.map((s) => s.secretSha256)],
  );
  const conns = stations.flatMap((s) => s.connectors.map((k) => ({ ...k, stationId: s.stationId, siteId: s.siteId })));
  await db.query(
    `INSERT INTO connectors (station_id, connector_id, site_id, current_type, phases, max_power_w, min_power_w, tariff_id)
     SELECT * FROM unnest($1::text[], $2::int[], $3::text[], $4::text[], $5::smallint[], $6::int[], $7::int[], $8::text[])`,
    [
      conns.map((x) => x.stationId),
      conns.map((x) => x.connectorId),
      conns.map((x) => x.siteId),
      conns.map((x) => x.currentType),
      conns.map((x) => x.phases),
      conns.map((x) => x.maxPowerW),
      conns.map((x) => x.minPowerW),
      conns.map((x) => x.tariffId),
    ],
  );
}
