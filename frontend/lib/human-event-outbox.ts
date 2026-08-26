import type {HumanEventPayload} from './human-events.ts';

export const HUMAN_EVENT_OUTBOX_KEY = 'csl-human-event-outbox-v1';
const RETRY_DELAYS_MS = [1000, 2000, 4000, 8000, 15000, 30000];

export type OutboxItem = {
  sessionId: string;
  event: HumanEventPayload;
  createdAt: number;
  attemptCount: number;
  lastAttemptAt?: number;
  lastError?: string;
};

export type OutboxSnapshot = {
  items: OutboxItem[];
  durabilityDegraded: boolean;
  storageError?: string;
};

type StorageLike = Pick<Storage, 'getItem' | 'setItem'>;
type Send = (sessionId: string, event: HumanEventPayload) => Promise<unknown>;
type Schedule = (callback: () => void, delayMs: number) => unknown;
type Cancel = (handle: unknown) => void;

type Options = {
  storage: StorageLike;
  send: Send;
  onChange?: (snapshot: OutboxSnapshot) => void;
  onDelivered?: (item: OutboxItem) => void;
  schedule?: Schedule;
  cancel?: Cancel;
  now?: () => number;
};

function deepFreeze<T>(value: T): T {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    for (const child of Object.values(value as Record<string, unknown>)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
}

function immutableEvent(event: HumanEventPayload): HumanEventPayload {
  return deepFreeze(JSON.parse(JSON.stringify(event)) as HumanEventPayload);
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export class HumanEventOutbox {
  private readonly storage: StorageLike;
  private readonly send: Send;
  private readonly onChange?: Options['onChange'];
  private readonly onDelivered?: Options['onDelivered'];
  private readonly schedule: Schedule;
  private readonly cancel: Cancel;
  private readonly now: () => number;
  private readonly inFlight = new Set<string>();
  private readonly timers = new Map<string, unknown>();
  private items = new Map<string, OutboxItem>();
  private durabilityDegraded = false;
  private storageError?: string;
  private stopped = false;

  constructor(options: Options) {
    this.storage = options.storage;
    this.send = options.send;
    this.onChange = options.onChange;
    this.onDelivered = options.onDelivered;
    this.schedule = options.schedule ?? ((callback, delay) => window.setTimeout(callback, delay));
    this.cancel = options.cancel ?? (handle => window.clearTimeout(handle as number));
    this.now = options.now ?? Date.now;
  }

  start(): void {
    this.stopped = false;
    this.load();
    this.emit();
    this.retryAll();
  }

  stop(): void {
    this.stopped = true;
    for (const handle of this.timers.values()) this.cancel(handle);
    this.timers.clear();
  }

  enqueue(sessionId: string, event: HumanEventPayload): {item: OutboxItem; durable: boolean} {
    const existing = this.items.get(event.event_id);
    if (existing) return {item: existing, durable: !this.durabilityDegraded};
    const item: OutboxItem = {
      sessionId,
      event: immutableEvent(event),
      createdAt: this.now(),
      attemptCount: 0,
    };
    this.items.set(event.event_id, item);
    const durable = this.persist();
    this.emit();
    return {item, durable};
  }

  retryAll(): void {
    for (const eventId of this.items.keys()) void this.retry(eventId);
  }

  async retry(eventId: string): Promise<void> {
    const item = this.items.get(eventId);
    if (!item || this.stopped || this.inFlight.has(eventId)) return;
    const timer = this.timers.get(eventId);
    if (timer !== undefined) {
      this.cancel(timer);
      this.timers.delete(eventId);
    }
    this.inFlight.add(eventId);
    item.attemptCount += 1;
    item.lastAttemptAt = this.now();
    delete item.lastError;
    this.persist();
    this.emit();
    try {
      // Always use the immutable original sessionId and event payload.
      await this.send(item.sessionId, item.event);
      this.items.delete(eventId);
      this.persist();
      this.emit();
      if (!this.stopped) this.onDelivered?.(item);
    } catch (error) {
      item.lastError = errorText(error);
      this.persist();
      this.emit();
      if (!this.stopped) {
        const delay = RETRY_DELAYS_MS[Math.min(item.attemptCount - 1, RETRY_DELAYS_MS.length - 1)];
        this.timers.set(eventId, this.schedule(() => {
          this.timers.delete(eventId);
          void this.retry(eventId);
        }, delay));
      }
    } finally {
      this.inFlight.delete(eventId);
    }
  }

  snapshot(): OutboxSnapshot {
    return {
      items: [...this.items.values()].map(item => ({...item, event: item.event})),
      durabilityDegraded: this.durabilityDegraded,
      storageError: this.storageError,
    };
  }

  private load(): void {
    try {
      const raw = this.storage.getItem(HUMAN_EVENT_OUTBOX_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw) as unknown;
      if (!Array.isArray(parsed)) throw new Error('Invalid Human Event outbox');
      for (const value of parsed) {
        const item = value as OutboxItem;
        if (!item?.sessionId || !item.event?.event_id) continue;
        this.items.set(item.event.event_id, {...item, event: immutableEvent(item.event)});
      }
    } catch (error) {
      this.durabilityDegraded = true;
      this.storageError = errorText(error);
    }
  }

  private persist(): boolean {
    try {
      this.storage.setItem(HUMAN_EVENT_OUTBOX_KEY, JSON.stringify([...this.items.values()]));
      this.durabilityDegraded = false;
      this.storageError = undefined;
      return true;
    } catch (error) {
      this.durabilityDegraded = true;
      this.storageError = errorText(error);
      return false;
    }
  }

  private emit(): void {
    if (!this.stopped) this.onChange?.(this.snapshot());
  }
}

export function getBrowserOutboxStorage(): StorageLike {
  try {
    return window.localStorage;
  } catch (error) {
    const message = errorText(error);
    return {
      getItem: () => { throw new Error(message); },
      setItem: () => { throw new Error(message); },
    };
  }
}
