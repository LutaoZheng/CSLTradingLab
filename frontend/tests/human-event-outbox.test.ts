import assert from 'node:assert/strict';
import test from 'node:test';
import {HumanEventOutbox, HUMAN_EVENT_OUTBOX_KEY} from '../lib/human-event-outbox.ts';
import {createHumanEventDraft, type HumanEventPayload} from '../lib/human-events.ts';

class MemoryStorage {
  value: string | null = null;
  getItem() { return this.value; }
  setItem(_key: string, value: string) { this.value = value; }
}

const settle = () => new Promise(resolve => setImmediate(resolve));
const noSchedule = () => 1;
const noCancel = () => undefined;

function draft(eventType = 'SHOT', group?: string, detail: Record<string, unknown> = {}) {
  return createHumanEventDraft({eventType, team: 'HOME', group, detail, pointerdownPerfTsMs: 95, calibrationId: 'cal-1'}, {
    now: () => 1_700_000_000_000,
    performanceNow: () => 100,
    crypto: null,
  });
}

test('failed POST preserves the exact immutable event and retry identity/timestamps', async () => {
  const storage = new MemoryStorage();
  const sent: HumanEventPayload[] = [];
  const manager = new HumanEventOutbox({storage, send: async (_sid, event) => { sent.push(event); throw new Error('offline'); }, schedule: noSchedule, cancel: noCancel});
  manager.start();
  const original = draft().payload;
  manager.enqueue('A', original);
  await manager.retry(original.event_id);
  await manager.retry(original.event_id);

  assert.equal(sent.length, 2);
  for (const retry of sent) {
    assert.equal(retry.event_id, original.event_id);
    assert.equal(retry.device_wall_ts_ms, original.device_wall_ts_ms);
    assert.equal(retry.device_perf_ts_ms, original.device_perf_ts_ms);
    assert.equal(retry.pointerdown_perf_ts_ms, original.pointerdown_perf_ts_ms);
    assert.equal(retry.calibration_id, 'cal-1');
  }
  const saved = JSON.parse(storage.value ?? '[]');
  assert.equal(saved[0].event.event_id, original.event_id);
  assert.equal(saved[0].event.pointerdown_perf_ts_ms, 95);
  assert.equal(Object.isFrozen(manager.snapshot().items[0].event), true);
  assert.equal(Object.isFrozen(manager.snapshot().items[0].event.detail), true);
  manager.stop();
});

test('page reload restores localStorage event and delivers it', async () => {
  const storage = new MemoryStorage();
  const original = draft('DANGER').payload;
  const first = new HumanEventOutbox({storage, send: async () => { throw new Error('offline'); }, schedule: noSchedule, cancel: noCancel});
  first.start(); first.enqueue('SESSION-A', original); await first.retry(original.event_id); first.stop();

  const delivered: Array<{sid: string; event: HumanEventPayload}> = [];
  const reloaded = new HumanEventOutbox({storage, send: async (sid, event) => { delivered.push({sid, event}); return {ok: true}; }, schedule: noSchedule, cancel: noCancel});
  reloaded.start(); await settle();
  assert.equal(delivered.length, 1);
  assert.equal(delivered[0].sid, 'SESSION-A');
  assert.equal(delivered[0].event.event_id, original.event_id);
  assert.equal(reloaded.snapshot().items.length, 0);
  reloaded.stop();
});

test('network recovery retryAll delivers the same queued event', async () => {
  const storage = new MemoryStorage();
  const original = draft('SHOT').payload;
  let online = false;
  const sent: HumanEventPayload[] = [];
  const manager = new HumanEventOutbox({storage, send: async (_sid, event) => { sent.push(event); if (!online) throw new Error('offline'); return {ok: true}; }, schedule: noSchedule, cancel: noCancel});
  manager.start(); manager.enqueue('A', original); await manager.retry(original.event_id);
  online = true; manager.retryAll(); await settle();
  assert.equal(sent.length, 2);
  assert.equal(sent[0].event_id, sent[1].event_id);
  assert.equal(sent[0].device_wall_ts_ms, sent[1].device_wall_ts_ms);
  assert.equal(manager.snapshot().items.length, 0);
  manager.stop();
});

test('duplicate acknowledgement removes outbox without creating a second logical event', async () => {
  const storage = new MemoryStorage();
  const original = draft('BALL_IN_NET').payload;
  let attempts = 0;
  const manager = new HumanEventOutbox({storage, send: async () => { attempts += 1; if (attempts === 1) throw new Error('response lost'); return {ok: true, duplicate: true}; }, schedule: noSchedule, cancel: noCancel});
  manager.start(); manager.enqueue('A', original);
  await manager.retry(original.event_id);
  assert.equal(manager.snapshot().items.length, 1);
  await manager.retry(original.event_id);
  assert.equal(attempts, 2);
  assert.equal(manager.snapshot().items.length, 0);
  assert.equal(JSON.parse(storage.value ?? '[]').length, 0);
  manager.stop();
});

test('session isolation always posts to the original Session', async () => {
  const storage = new MemoryStorage();
  const calls: string[] = [];
  const original = draft().payload;
  const manager = new HumanEventOutbox({storage, send: async sid => { calls.push(sid); return {ok: true}; }, schedule: noSchedule, cancel: noCancel});
  manager.start(); manager.enqueue('SESSION-A', original); await manager.retry(original.event_id);
  assert.deepEqual(calls, ['SESSION-A']);
  manager.stop();
});

test('GOAL primary and follow-ups queue independently with group/parent relationships intact', () => {
  const storage = new MemoryStorage();
  const manager = new HumanEventOutbox({storage, send: async () => { throw new Error('offline'); }, schedule: noSchedule, cancel: noCancel});
  manager.start();
  const primary = draft('BALL_IN_NET');
  const assessment = draft('GOAL_ASSESSMENT', primary.eventGroupId, {assessment: 'LIKELY_VALID', parent_event_id: primary.eventId});
  const review = draft('VAR_CHECK', primary.eventGroupId, {parent_event_id: primary.eventId});
  const resolution = draft('GOAL_CONFIRMED', primary.eventGroupId, {parent_event_id: primary.eventId});
  for (const item of [primary, assessment, review, resolution]) manager.enqueue('A', item.payload);
  const events = manager.snapshot().items.map(item => item.event);
  assert.deepEqual(events.map(event => event.event_type), ['BALL_IN_NET', 'GOAL_ASSESSMENT', 'VAR_CHECK', 'GOAL_CONFIRMED']);
  assert.ok(events.every(event => event.event_group_id === primary.eventGroupId));
  assert.ok(events.slice(1).every(event => event.detail.parent_event_id === primary.eventId));
  manager.stop();
});

test('storage failure degrades durability but does not block HTTP delivery', async () => {
  const storage = {getItem: () => null, setItem: () => { throw new Error('quota denied'); }};
  let sent = false;
  const manager = new HumanEventOutbox({storage, send: async () => { sent = true; return {ok: true}; }, schedule: noSchedule, cancel: noCancel});
  manager.start();
  const original = draft().payload;
  const queued = manager.enqueue('A', original);
  assert.equal(queued.durable, false);
  assert.equal(manager.snapshot().durabilityDegraded, true);
  await manager.retry(original.event_id);
  assert.equal(sent, true);
  manager.stop();
});

test('outbox uses one stable storage key', () => {
  assert.equal(HUMAN_EVENT_OUTBOX_KEY, 'csl-human-event-outbox-v1');
});
