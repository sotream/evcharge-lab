import type { api } from '@evcharge/contracts';
import type { Pool } from '@evcharge/platform';
import type { z } from 'zod';

export type ListSitesRes = z.infer<typeof api.SessionsPublic.listSitesRes>;

const KM_PER_DEG_LAT = 111.32;
export const MAX_RADIUS_KM = 100;
export const MAX_SITES = 200;
/** Nearest sites returned by default: a map view needs tens, not hundreds (146 sites were 429 KB per response). */
export const DEFAULT_SITES = 20;

/** Lat/lng bounding box around a point; prefilter on the (lat, lng) index before the exact distance. */
export function boundingBox(lat: number, lng: number, radiusKm: number): { minLat: number; maxLat: number; minLng: number; maxLng: number } {
  const dLat = radiusKm / KM_PER_DEG_LAT;
  const dLng = radiusKm / (KM_PER_DEG_LAT * Math.max(0.01, Math.cos((lat * Math.PI) / 180)));
  return { minLat: lat - dLat, maxLat: lat + dLat, minLng: lng - dLng, maxLng: lng + dLng };
}

export class CatalogService {
  constructor(private readonly pool: Pool) {}

  /** GET /v1/sites: sites within radiusKm (nearest first) with stations, connectors, status and online flag. */
  async listSites(lat: number, lng: number, radiusKm: number, limit = DEFAULT_SITES): Promise<ListSitesRes> {
    const b = boundingBox(lat, lng, radiusKm);
    const { rows: sites } = await this.pool.query<{ site_id: string; name: string; lat: number; lng: number; cap_w: number }>(
      `SELECT site_id, name, lat, lng, cap_w FROM (
         SELECT site_id, name, lat, lng, cap_w,
                6371.0088 * 2 * asin(sqrt(power(sin(radians(lat - $1) / 2), 2) + cos(radians($1)) * cos(radians(lat)) * power(sin(radians(lng - $2) / 2), 2))) AS km
           FROM sites
          WHERE lat BETWEEN $4 AND $5 AND lng BETWEEN $6 AND $7) s
        WHERE km <= $3 ORDER BY km LIMIT $8`,
      [lat, lng, radiusKm, b.minLat, b.maxLat, b.minLng, b.maxLng, Math.min(limit, MAX_SITES)],
    );
    if (sites.length === 0) return { sites: [] };
    const ids = sites.map((s) => s.site_id);
    const { rows: conns } = await this.pool.query<{ station_id: string; site_id: string; online: boolean; lat: number; lng: number; connector_id: number | null; status: string | null; max_power_w: number | null; tariff_id: string | null }>(
      `SELECT st.station_id, st.site_id, st.online, st.lat, st.lng, c.connector_id, c.status, c.max_power_w, c.tariff_id
         FROM stations st LEFT JOIN connectors c ON c.station_id = st.station_id
        WHERE st.site_id = ANY($1::text[]) AND st.enabled
        ORDER BY st.station_id, c.connector_id`,
      [ids],
    );
    type Station = ListSitesRes['sites'][number]['stations'][number];
    const bySite = new Map<string, Map<string, Station>>();
    for (const r of conns) {
      let stations = bySite.get(r.site_id);
      if (!stations) bySite.set(r.site_id, (stations = new Map()));
      let st = stations.get(r.station_id);
      if (!st) stations.set(r.station_id, (st = { stationId: r.station_id, siteId: r.site_id, online: r.online, lat: r.lat, lng: r.lng, connectors: [] }));
      if (r.connector_id !== null) st.connectors.push({ connectorId: r.connector_id, status: r.status ?? 'Unavailable', maxPowerW: r.max_power_w ?? 0, tariffId: r.tariff_id ?? 'std' });
    }
    return {
      sites: sites.map((s) => ({ siteId: s.site_id, name: s.name, lat: s.lat, lng: s.lng, capW: s.cap_w, stations: [...(bySite.get(s.site_id)?.values() ?? [])] })),
    };
  }
}
