import { EventMap, EventName } from "../Messaging/index.js";

export type InboxStatus = "received" | "processed" | "failed";

/** Can be used to create the Inbox Model */
export type InboxRecordFor<K extends EventName> = {
  messageId: string; // dedupe key
  eventType: K;
  receivedAt: Date;
  status: InboxStatus;
  payload: EventMap[K];
  processedAt: Date | null;
  lastError: string | null;
};

export type InboxRecord = { [K in EventName]: InboxRecordFor<K> }[EventName];

export type NewInboxRecord<K extends EventName = EventName> = {
  eventType: K;
  messageId: string; // from producer, never auto generate
  payload: EventMap[K];
};
