import { type CanActivate, type ExecutionContext, Injectable } from '@nestjs/common';
import { z } from 'zod';
import { ProblemError } from './problem';

export const DRIVER_ID_HEADER = 'x-driver-id';
const uuid = z.uuid();

/**
 * ADR 0010: `x-driver-id` is forwarded only by driver-api after it verified the JWT. Apply AFTER
 * InternalTokenGuard (`@UseGuards(InternalTokenGuard, DriverGuard)`), so a driver id without a valid internal
 * token is rejected with 401 before this guard runs. This guard then requires a well-formed driver id.
 */
@Injectable()
export class DriverGuard implements CanActivate {
  canActivate(ctx: ExecutionContext): boolean {
    const req = ctx.switchToHttp().getRequest<{ headers: Record<string, string | string[] | undefined>; driverId?: string }>();
    const raw = req.headers[DRIVER_ID_HEADER];
    const id = Array.isArray(raw) ? raw[0] : raw;
    if (!id) throw new ProblemError(401, 'driver_required', 'Driver identity required', `${DRIVER_ID_HEADER} header is required`);
    if (!uuid.safeParse(id).success) throw new ProblemError(400, 'bad_driver_id', 'Bad driver id', `${DRIVER_ID_HEADER} must be a uuid`);
    req.driverId = id.toLowerCase();
    return true;
  }
}

export function driverIdOf(req: { driverId?: string }): string {
  if (!req.driverId) throw new ProblemError(401, 'driver_required', 'Driver identity required');
  return req.driverId;
}
