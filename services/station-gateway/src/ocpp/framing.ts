/**
 * OCPP-J framing and validation (ADR 0003; packages/contracts ocpp.ts is normative).
 *   CALL [2, id, action, payload]  CALLRESULT [3, id, payload]  CALLERROR [4, id, code, description, details]
 */
import { ocpp } from '@evcharge/contracts';
import type { z } from 'zod';

export type OcppErrorCode = ocpp.OcppErrorCode;
export type StationAction = ocpp.StationAction;

export type InboundFrame =
  | { kind: 'call'; uniqueId: string; action: string; payload: Record<string, unknown> }
  | { kind: 'result'; uniqueId: string; payload: Record<string, unknown> }
  | { kind: 'error'; uniqueId: string; code: string; description: string; details: Record<string, unknown> }
  /** Unusable frame: answer with CALLERROR `code` (uniqueId "-1" when none could be read). */
  | { kind: 'invalid'; uniqueId: string; code: OcppErrorCode; description: string };

/** Parses one text frame. Never throws. */
export function parseFrame(text: string): InboundFrame {
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    return { kind: 'invalid', uniqueId: '-1', code: 'FormationViolation', description: 'frame is not valid JSON' };
  }
  if (!Array.isArray(raw)) return { kind: 'invalid', uniqueId: '-1', code: 'FormationViolation', description: 'frame is not a JSON array' };
  const uniqueId = typeof raw[1] === 'string' && raw[1].length > 0 && raw[1].length <= 36 ? raw[1] : '-1';
  switch (raw[0]) {
    case ocpp.MessageTypeId.Call: {
      const r = ocpp.CallFrame.safeParse(raw);
      if (!r.success) return { kind: 'invalid', uniqueId, code: 'FormationViolation', description: 'malformed CALL' };
      return { kind: 'call', uniqueId: r.data[1], action: r.data[2], payload: r.data[3] };
    }
    case ocpp.MessageTypeId.CallResult: {
      const r = ocpp.CallResultFrame.safeParse(raw);
      if (!r.success) return { kind: 'invalid', uniqueId, code: 'FormationViolation', description: 'malformed CALLRESULT' };
      return { kind: 'result', uniqueId: r.data[1], payload: r.data[2] };
    }
    case ocpp.MessageTypeId.CallError: {
      const r = ocpp.CallErrorFrame.safeParse(raw);
      if (!r.success) return { kind: 'invalid', uniqueId, code: 'FormationViolation', description: 'malformed CALLERROR' };
      return { kind: 'error', uniqueId: r.data[1], code: r.data[2], description: r.data[3], details: r.data[4] };
    }
    default:
      return { kind: 'invalid', uniqueId, code: 'FormationViolation', description: 'unknown MessageTypeId' };
  }
}

export function isStationAction(action: string): action is StationAction {
  return Object.hasOwn(ocpp.StationToCsms, action);
}

export type ValidationResult<T> = { ok: true; value: T } | { ok: false; code: OcppErrorCode; description: string };

/**
 * Maps zod failures to OCPP codes: a missing required field is `ProtocolError` ("payload is
 * incomplete"); a present field of the wrong type or value is `TypeConstraintViolation`.
 */
export function validatePayload<S extends z.ZodType>(schema: S, payload: unknown): ValidationResult<z.infer<S>> {
  const r = schema.safeParse(payload);
  if (r.success) return { ok: true, value: r.data };
  const first = r.error.issues[0];
  const missing = r.error.issues.some((i) => i.code === 'invalid_type' && /received undefined/.test(i.message));
  const where = first?.path.length ? first.path.join('.') : '(payload)';
  return { ok: false, code: missing ? 'ProtocolError' : 'TypeConstraintViolation', description: `${where}: ${first?.message ?? 'invalid'}`.slice(0, 255) };
}

/** Validates a station -> CSMS CALL payload; unknown actions are `NotImplemented`. */
export function validateStationCall(action: string, payload: unknown): ValidationResult<unknown> & { action?: StationAction } {
  if (!isStationAction(action)) return { ok: false, code: 'NotImplemented', description: `action ${action} is not implemented` };
  return { ...validatePayload(ocpp.StationToCsms[action].req, payload), action };
}

export const encodeCall = (uniqueId: string, action: string, payload: unknown): string => JSON.stringify([ocpp.MessageTypeId.Call, uniqueId, action, payload]);
export const encodeResult = (uniqueId: string, payload: unknown): string => JSON.stringify([ocpp.MessageTypeId.CallResult, uniqueId, payload]);
export const encodeError = (uniqueId: string, code: OcppErrorCode, description: string, details: Record<string, unknown> = {}): string =>
  JSON.stringify([ocpp.MessageTypeId.CallError, uniqueId, code, description.slice(0, 255), details]);
