import type { Queryable } from '@evcharge/platform';

/**
 * Marks a site due for reallocation inside the caller's transaction (ADR 0006: reallocate on every start/stop).
 * `alloc_requested_at` keeps the oldest pending trigger for the reallocation-latency metric.
 */
export async function markSiteDue(db: Queryable, siteId: string): Promise<void> {
  await db.query('UPDATE sites SET alloc_due_at = now(), alloc_requested_at = coalesce(alloc_requested_at, clock_timestamp()) WHERE site_id = $1', [siteId]);
}

/** In-process notification after commit, so the allocation loop can run the site immediately instead of on its next tick. */
export class SiteSignal {
  private listener: ((siteId: string) => void) | undefined;

  onChanged(fn: (siteId: string) => void): void {
    this.listener = fn;
  }

  changed(siteId: string): void {
    this.listener?.(siteId);
  }
}
