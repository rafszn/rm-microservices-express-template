import { randomUUID } from "node:crypto";
import { EventName } from "../Messaging/index.js";
import { NewOutboxRecord, OutboxRecordFor } from "./types.js";

/** The only sanctioned way to build an outbox row. strictness is enforced */
export function createOutboxRecord<K extends EventName>(
  input: NewOutboxRecord<K>,
): OutboxRecordFor<K> {
  return {
    id: randomUUID(),
    attempts: 0,
    lastError: null,
    publishedAt: null,
    status: "pending",
    createdAt: new Date(),
    occurredAt: new Date(),
    ...input,
  };
}
