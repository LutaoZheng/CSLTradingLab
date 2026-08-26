import assert from 'node:assert/strict';
import test from 'node:test';
import {buildPendingEventState, createHumanEventDraft} from '../lib/human-events.ts';
import {createUuid} from '../lib/uuid.ts';

const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

test('native randomUUID is used when available', () => {
  const native = '11111111-1111-4111-8111-111111111111';
  assert.equal(createUuid({randomUUID: () => native}), native);
});

test('HTTP/incompatible browser fallback produces unique valid event and calibration IDs', () => {
  const ids = new Set(Array.from({length: 100}, () => createUuid(null)));
  assert.equal(ids.size, 100);
  for (const id of ids) assert.match(id, uuidPattern);
  assert.match(createUuid({}), uuidPattern);
});

test('Human Event payload and GOAL pending state survive unavailable randomUUID', async () => {
  const draft = createHumanEventDraft({
    eventType: 'BALL_IN_NET',
    team: 'HOME',
    pointerdownPerfTsMs: 95,
    calibrationId: 'calibration-1',
    scoreAtClick: {home: 0, away: 0},
  }, {
    now: () => 1_700_000_000_000,
    performanceNow: () => 100,
    crypto: null,
  });

  assert.match(draft.eventId, uuidPattern);
  assert.match(draft.eventGroupId, uuidPattern);
  assert.equal(draft.payload.pointerdown_perf_ts_ms, 95);
  assert.equal(draft.payload.calibration_id, 'calibration-1');

  let postedPayload: unknown;
  await (async payload => { postedPayload = payload; return {ok: true}; })(draft.payload);
  assert.equal(postedPayload, draft.payload);

  const pending = buildPendingEventState([draft.local]);
  assert.equal(pending.length, 1);
  assert.equal(pending[0].primary.event_type, 'BALL_IN_NET');
  assert.equal(pending[0].pending, true);
});
