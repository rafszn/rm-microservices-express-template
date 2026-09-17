import { OutboxRecord } from "./types.js";

export interface IOutboxRepository {
  /** Called inside the SAME transaction as the business write. */
  save(record: OutboxRecord): Promise<void>;
  markPublished(id: string): Promise<void>;
  fetchPending(limit: number): Promise<OutboxRecord[]>;
  markFailed(id: string, error: string): Promise<void>;
}