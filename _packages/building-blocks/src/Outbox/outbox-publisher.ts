import { OutboxRecord } from "./types.js";
import { IPublisher } from "../Messaging/index.js";
import { IOutboxRepository } from "./outbox-repository.js";

export class OutboxPublisher {
  constructor(
    private readonly repo: IOutboxRepository,
    private readonly publisher: IPublisher,
  ) {}

  async pollAndPublish(batchSize = 50): Promise<void> {
    const records = await this.repo.fetchPending(batchSize);

    for (const record of records) {
      try {
        await this.publish(record);
        await this.repo.markPublished(record.id);
      } catch (err) {
        await this.repo.markFailed(record.id, String(err));
      }
    }
  }

  private async publish(record: OutboxRecord): Promise<void> {
    // record.eventType and record.payload are correlated, no cast needed.
    await this.publisher.publish(record.eventType, record.payload, {
      correlationId: record.correlationId,
      headers: record.headers,
      persistent: record.persistent,
    });
  }
}