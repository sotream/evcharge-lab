import type { Pool } from '@evcharge/platform';

const FINAL = "('PAID', 'CAPTURE_FAILED', 'AUTH_FAILED')";

export async function markDriverDeleted(pool: Pool, driverId: string): Promise<void> {
  await pool.query('INSERT INTO deleted_drivers (driver_id) VALUES ($1) ON CONFLICT DO NOTHING', [driverId]);
}

export async function isDriverDeleted(pool: Pool, driverId: string): Promise<boolean> {
  return ((await pool.query('SELECT 1 FROM deleted_drivers WHERE driver_id = $1', [driverId])).rowCount ?? 0) > 0;
}

/**
 * Removes the stored card and the saga tokens of deleted drivers. A saga that is still unfinished (pricing, capturing)
 * needs its token to collect the money, so nothing is removed for a driver until every saga of theirs reached a final
 * state; the periodic call in the runtime then finishes the job. Invoices are never touched (retention). Idempotent.
 */
export async function scrubDeletedDrivers(pool: Pool): Promise<number> {
  const settled = `NOT EXISTS (SELECT 1 FROM billing_sagas u WHERE u.driver_id = dd.driver_id AND u.state NOT IN ${FINAL})`;
  const tokens = await pool.query(
    `UPDATE billing_sagas s SET auth_token = NULL, capture_token = NULL FROM deleted_drivers dd
      WHERE s.driver_id = dd.driver_id AND s.state IN ${FINAL} AND (s.auth_token IS NOT NULL OR s.capture_token IS NOT NULL)`,
  );
  const cards = await pool.query(`DELETE FROM payment_methods pm USING deleted_drivers dd WHERE pm.driver_id = dd.driver_id AND ${settled}`);
  return (tokens.rowCount ?? 0) + (cards.rowCount ?? 0);
}
