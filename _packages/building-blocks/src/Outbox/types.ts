import { EventMap, EventName } from "../Messaging/index.js";

export type OutboxStatus = "pending" | "published" | "failed";

/** Can be used to create the Outbox Model */
export type OutboxRecordFor<K extends EventName> = {
  id: string;
  eventType: K;
  attempts: number;
  createdAt: Date;
  occurredAt: Date;
  aggregateId: string;
  payload: EventMap[K];
  status: OutboxStatus;
  persistent?: boolean;
  aggregateType: string;
  correlationId: string;
  lastError: string | null;
  publishedAt: Date | null;
  headers?: Record<string, unknown>;
};

/** The union TypeScript actually checks against. Adding a key to EventMap extends this automatically  */
export type OutboxRecord = { [K in EventName]: OutboxRecordFor<K> }[EventName];

/** What is passed in to create one. */
export type NewOutboxRecord<K extends EventName = EventName> = {
  eventType: K;
  aggregateId: string;
  payload: EventMap[K];
  aggregateType: string;
  persistent?: boolean;
  correlationId: string;
  headers?: Record<string, unknown>;
};
