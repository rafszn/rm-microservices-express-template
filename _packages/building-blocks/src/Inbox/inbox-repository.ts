import { InboxRecord } from "./types.js";

export interface IInboxRepository {
  /**
   Atomically claims a message for processing. Returns false if this message id was already claimed (by this call or a prior delivery)  
  */
  tryClaim(record: InboxRecord): Promise<boolean>;

  markProcessed(messageId: string): Promise<void>;
  markFailed(messageId: string, error: string): Promise<void>;

  /** For a reaper/retry job over messages stuck in "failed". */
  fetchFailed(limit: number): Promise<InboxRecord[]>;
}
