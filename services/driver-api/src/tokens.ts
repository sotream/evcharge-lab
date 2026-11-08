/** Nest injection tokens for values built in `createDriverApi` (plain objects, not Nest-managed classes). */
export const LOGGER = Symbol('LOGGER');
export const APP_CONFIG = Symbol('APP_CONFIG');
export const PG_POOL = Symbol('PG_POOL');
export const RATE_LIMITER = Symbol('RATE_LIMITER');
export const JWT = Symbol('JWT');
export const DOWNSTREAM = Symbol('DOWNSTREAM');
