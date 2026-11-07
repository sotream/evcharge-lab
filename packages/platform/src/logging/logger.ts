import { isSpanContextValid, trace } from '@opentelemetry/api';
import type { LoggerService, LogLevel as NestLogLevel } from '@nestjs/common';
import pino from 'pino';

export type Logger = pino.Logger;

const SECRET_KEYS = ['authorization', 'password', 'token', 'accessToken', 'refreshToken', 'internalToken', 'secret', 'apiKey', '"x-internal-token"', 'cookie'];

/** pino redact paths: each secret key at the top level and up to three levels deep. */
export const REDACT_PATHS: string[] = SECRET_KEYS.flatMap((k) => {
  const seg = k.startsWith('"') ? `[${k}]` : `.${k}`;
  const top = k.startsWith('"') ? `[${k}]` : k;
  return [top, `*${seg}`, `*.*${seg}`, `*.*.*${seg}`];
});

export interface LoggerOptions {
  service: string;
  level?: string;
  /** Defaults to stdout (fd 1). Tests pass a stream to capture lines. */
  destination?: pino.DestinationStream;
}

/**
 * JSON logger: every line carries `service`, and `traceId`/`spanId` when an OTel span is active.
 * Secret-looking fields (authorization, password, token, ...) are replaced by "[REDACTED]".
 */
export function createLogger(opts: LoggerOptions): Logger {
  return pino(
    {
      level: opts.level ?? 'info',
      base: { service: opts.service },
      timestamp: pino.stdTimeFunctions.isoTime,
      messageKey: 'msg',
      formatters: { level: (label) => ({ level: label }) },
      redact: { paths: REDACT_PATHS, censor: '[REDACTED]' },
      mixin() {
        const span = trace.getActiveSpan();
        if (!span) return {};
        const sc = span.spanContext();
        return isSpanContextValid(sc) ? { traceId: sc.traceId, spanId: sc.spanId } : {};
      },
    },
    opts.destination ?? pino.destination({ fd: 1, sync: false }),
  );
}

const NEST_TO_PINO: Record<NestLogLevel, pino.Level> = {
  log: 'info',
  error: 'error',
  warn: 'warn',
  debug: 'debug',
  verbose: 'trace',
  fatal: 'fatal',
};

/** Routes Nest's internal logging (and `new Logger(ctx)` calls) through pino. */
export class NestLoggerAdapter implements LoggerService {
  constructor(private readonly logger: Logger) {}

  log(message: unknown, ...params: unknown[]) {
    this.write('log', message, params);
  }
  error(message: unknown, ...params: unknown[]) {
    this.write('error', message, params);
  }
  warn(message: unknown, ...params: unknown[]) {
    this.write('warn', message, params);
  }
  debug(message: unknown, ...params: unknown[]) {
    this.write('debug', message, params);
  }
  verbose(message: unknown, ...params: unknown[]) {
    this.write('verbose', message, params);
  }
  fatal(message: unknown, ...params: unknown[]) {
    this.write('fatal', message, params);
  }
  setLogLevels(levels: NestLogLevel[]) {
    const order: pino.Level[] = ['trace', 'debug', 'info', 'warn', 'error', 'fatal'];
    const lowest = order.find((l) => levels.some((n) => NEST_TO_PINO[n] === l));
    this.logger.level = lowest ?? 'silent';
  }

  private write(level: NestLogLevel, message: unknown, params: unknown[]) {
    const rest = [...params];
    const obj: Record<string, unknown> = {};
    // Nest passes the context name as the last string argument.
    if (rest.length > 0 && typeof rest[rest.length - 1] === 'string') obj['context'] = rest.pop();
    // Nest's error(message, stack, context) signature.
    if (level === 'error' && typeof rest[0] === 'string' && rest[0].includes('\n')) obj['stack'] = rest.shift();
    if (rest.length > 0) obj['params'] = rest;
    let msg: string;
    if (message instanceof Error) {
      obj['err'] = message;
      msg = message.message;
    } else if (message !== null && typeof message === 'object') {
      Object.assign(obj, message);
      msg = typeof (message as { msg?: unknown }).msg === 'string' ? (message as { msg: string }).msg : '';
    } else {
      msg = String(message);
    }
    this.logger[NEST_TO_PINO[level]](obj, msg);
  }
}
