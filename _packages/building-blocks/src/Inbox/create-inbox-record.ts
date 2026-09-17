import { EventName } from "../Messaging/index.js";
import { InboxRecordFor, NewInboxRecord } from "./types.js";

export function createInboxRecord<K extends EventName>(
  input: NewInboxRecord<K>,
): InboxRecordFor<K> {
  return {
    lastError: null,
    processedAt: null,
    status: "received",
    receivedAt: new Date(),
    ...input,
  };
}
