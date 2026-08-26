import {createUuid, type UuidCrypto} from './uuid.ts';

export type HumanEvent = {
  id: string;
  event_group_id: string;
  event_type: string;
  team?: string;
  device_wall_ts_ms: number;
  device_perf_ts_ms?: number;
  pointerdown_perf_ts_ms?: number;
  server_receive_ts_ns?: number;
  target_event_id?: string;
  detail?: Record<string, unknown>;
};

export type PendingEventState = {
  primary: HumanEvent;
  assessment?: string;
  varChecked: boolean;
  resolution?: string;
  voided: boolean;
  pending: boolean;
};

export type HumanEventPayload = {
  event_id: string;
  event_group_id: string;
  event_type: string;
  team?: string;
  device_wall_ts_ms: number;
  device_perf_ts_ms?: number;
  pointerdown_perf_ts_ms?: number;
  calibration_id: string | null;
  score_at_click?: unknown;
  kalshi_match_clock_at_click?: unknown;
  target_event_id?: unknown;
  detail: Record<string, unknown>;
};

type EventDraftInput = {
  eventType: string;
  team?: string;
  group?: string;
  detail?: Record<string, unknown>;
  pointerdownPerfTsMs?: number;
  calibrationId?: string | null;
  scoreAtClick?: unknown;
  matchClockAtClick?: unknown;
};

type EventDraftRuntime = {
  now?: () => number;
  performanceNow?: () => number | undefined;
  crypto?: UuidCrypto | null;
};

function safePerformanceNow(): number | undefined {
  try {
    return globalThis.performance?.now();
  } catch {
    return undefined;
  }
}

export function createHumanEventDraft(input: EventDraftInput, runtime: EventDraftRuntime = {}) {
  // Capture click clocks before UUID generation and other non-essential work.
  const deviceWallTsMs = runtime.now?.() ?? Date.now();
  const devicePerfTsMs = runtime.performanceNow?.() ?? safePerformanceNow();
  const eventId = 'crypto' in runtime ? createUuid(runtime.crypto ?? null) : createUuid();
  const eventGroupId = input.group ?? ('crypto' in runtime ? createUuid(runtime.crypto ?? null) : createUuid());
  const pointerdownPerfTsMs = input.pointerdownPerfTsMs != null && devicePerfTsMs != null
    && input.pointerdownPerfTsMs <= devicePerfTsMs && devicePerfTsMs - input.pointerdownPerfTsMs < 2000
    ? input.pointerdownPerfTsMs
    : undefined;
  const detail = input.detail ?? {};
  const local: HumanEvent = {
    id: eventId,
    event_group_id: eventGroupId,
    event_type: input.eventType,
    team: input.team,
    device_wall_ts_ms: deviceWallTsMs,
    device_perf_ts_ms: devicePerfTsMs,
    pointerdown_perf_ts_ms: pointerdownPerfTsMs,
    target_event_id: detail.target_event_id as string | undefined,
    detail,
  };
  return {
    eventId,
    eventGroupId,
    local,
    payload: {
      event_id: eventId,
      event_group_id: eventGroupId,
      event_type: input.eventType,
      team: input.team,
      device_wall_ts_ms: deviceWallTsMs,
      device_perf_ts_ms: devicePerfTsMs,
      pointerdown_perf_ts_ms: pointerdownPerfTsMs,
      calibration_id: input.calibrationId ?? null,
      score_at_click: input.scoreAtClick,
      kalshi_match_clock_at_click: input.matchClockAtClick,
      target_event_id: detail.target_event_id,
      detail,
    } satisfies HumanEventPayload,
  };
}

export function humanEventFromPayload(payload: HumanEventPayload): HumanEvent {
  return {
    id: payload.event_id,
    event_group_id: payload.event_group_id,
    event_type: payload.event_type,
    team: payload.team,
    device_wall_ts_ms: payload.device_wall_ts_ms,
    device_perf_ts_ms: payload.device_perf_ts_ms,
    pointerdown_perf_ts_ms: payload.pointerdown_perf_ts_ms,
    target_event_id: payload.target_event_id as string | undefined,
    detail: payload.detail,
  };
}

const primaryTypes = new Set(['BALL_IN_NET', 'PENALTY_EVENT', 'RED_CARD_EVENT']);
const assessmentTypes: Record<string, string> = {BALL_IN_NET: 'GOAL_ASSESSMENT', PENALTY_EVENT: 'PENALTY_ASSESSMENT', RED_CARD_EVENT: 'RED_CARD_ASSESSMENT'};
const resolutionTypes: Record<string, Set<string>> = {
  BALL_IN_NET: new Set(['GOAL_CONFIRMED', 'GOAL_CANCELLED']),
  PENALTY_EVENT: new Set(['PENALTY_CONFIRMED', 'PENALTY_CANCELLED']),
  RED_CARD_EVENT: new Set(['RED_CARD_CONFIRMED', 'RED_CARD_CANCELLED']),
};

export function buildPendingEventState(events: HumanEvent[]): PendingEventState[] {
  const groups = new Map<string, HumanEvent[]>();
  for (const event of events) {
    const list = groups.get(event.event_group_id) ?? [];
    list.push(event);
    groups.set(event.event_group_id, list);
  }
  const states: PendingEventState[] = [];
  for (const groupEvents of groups.values()) {
    groupEvents.sort((a, b) => (a.server_receive_ts_ns ?? a.device_wall_ts_ms * 1e6) - (b.server_receive_ts_ns ?? b.device_wall_ts_ms * 1e6));
    const primary = groupEvents.find(event => primaryTypes.has(event.event_type));
    if (!primary) continue;
    const assessment = [...groupEvents].reverse().find(event => event.event_type === assessmentTypes[primary.event_type])?.detail?.assessment as string | undefined;
    const resolution = groupEvents.find(event => resolutionTypes[primary.event_type].has(event.event_type))?.event_type;
    const voided = groupEvents.some(event => event.event_type === 'EVENT_VOIDED' && event.target_event_id === primary.id);
    const varChecked = groupEvents.some(event => event.event_type === 'VAR_CHECK' && event.detail?.parent_event_id === primary.id);
    states.push({primary, assessment, varChecked, resolution, voided, pending: !resolution && !voided});
  }
  return states.sort((a, b) => (b.primary.server_receive_ts_ns ?? b.primary.device_wall_ts_ms * 1e6) - (a.primary.server_receive_ts_ns ?? a.primary.device_wall_ts_ms * 1e6));
}
