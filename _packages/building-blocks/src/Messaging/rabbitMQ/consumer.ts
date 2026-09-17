import { logger } from "../../Logger/winstonLogger.js";
import type { Channel, ConsumeMessage, Options } from "amqplib";
import type { EventMap, EventName } from "../contracts/event-map.js";
import { QueueName } from "../contracts/queues.js";

export interface MessageMetadata {
  type?: string;
  exchange: string;
  timestamp?: number;
  routingKey: string;
  messageId: string;
  deliveryTag: number;
  redelivered: boolean;
  correlationId: string;
  headers?: Record<string, unknown>;
}

export interface RetryOptions {
  factor?: number;
  maxDelayMs?: number;
  maxRetries?: number;
  initialDelayMs?: number;
}

export type ResolvedRetryOptions = Required<RetryOptions>;

export const DEFAULT_RETRY_OPTIONS: ResolvedRetryOptions = {
  factor: 2,
  maxRetries: 3,
  maxDelayMs: 30_000,
  initialDelayMs: 1000,
};

export interface SubscribeOptions {
  prefetch?: number;
  requeueOnError?: boolean;
  queueOptions?: Options.AssertQueue;
  /**
   * Enables the delayed retry + DLQ pipeline for this queue.
   * Omit to keep the legacy immediate-nack behavior controlled by `requeueOnError`.
   */
  retry?: RetryOptions;
}

export interface IConsumer {
  subscribe<K extends EventName>(
    queue: QueueName,
    event: K,
    handler: (payload: EventMap[K], metadata: MessageMetadata) => Promise<void>,
    options?: SubscribeOptions,
  ): Promise<void>;

  requeueDlqMessages(
    queue: QueueName,
    options?: { limit?: number; resetRetryCount?: boolean },
  ): Promise<{ requeued: number; failed: number }>;

  close(): Promise<void>;
}

type EventHandler = (
  payload: unknown,
  metadata: MessageMetadata,
) => Promise<void>;

interface QueueState {
  prefetch?: number;
  requeueOnError: boolean;
  consumerTag: string | null;
  retry?: ResolvedRetryOptions;
  queueOptions?: Options.AssertQueue;
  handlers: Map<EventName, EventHandler>;
  initializationPromise: Promise<void> | null;
}

export class Consumer implements IConsumer {
  private closed = false;
  private readonly queues = new Map<string, QueueState>();

  constructor(private channel: Channel) {}

  /**
    Called after a reconnect, with a fresh channel from the new connection. Replays every queue's setup (assertQueue, retry topology, prefetch, consume) and every event's exchange binding on the new channel.
   
    Per-queue failures are isolated: one queue failing to resubscribe does not prevent the others from succeeding.
   */
  public async handleChannelReplaced(channel: Channel): Promise<void> {
    this.channel = channel;

    for (const [queue, state] of this.queues) {
      state.consumerTag = null;
      state.initializationPromise = null;

      try {
        await this.ensureQueueInitialized(queue, state);

        for (const event of state.handlers.keys()) {
          await this.channel.assertExchange(event, "fanout", { durable: true });
          await this.channel.bindQueue(queue, event, "");
        }

        logger.info(
          `RabbitMQ consumer resubscribed for queue "${queue}" after reconnect.`,
        );
      } catch (error) {
        logger.error(
          `[RabbitMQ] Failed to resubscribe queue "${queue}" after reconnect.`,
          error,
        );
      }
    }
  }

  public async subscribe<K extends EventName>(
    queue: string,
    event: K,
    handler: (payload: EventMap[K], metadata: MessageMetadata) => Promise<void>,
    options: SubscribeOptions = {},
  ): Promise<void> {
    this.ensureOpen();

    const queueState = this.getOrCreateQueueState(queue, options);

    if (queueState.handlers.has(event)) {
      throw new Error(
        `A handler for event "${String(event)}" is already registered on queue "${queue}".`,
      );
    }

    queueState.handlers.set(event, handler as EventHandler);

    try {
      await this.ensureQueueInitialized(queue, queueState);

      await this.channel.assertExchange(event, "fanout", {
        durable: true,
      });

      await this.channel.bindQueue(queue, event, "");

      logger.info(
        `RabbitMQ subscription registered: exchange "${String(event)}" -> queue "${queue}".`,
      );
    } catch (error) {
      queueState.handlers.delete(event);

      throw error;
    }
  }

  public async close(): Promise<void> {
    if (this.closed) {
      return;
    }

    this.closed = true;

    const cancellations = Array.from(this.queues.entries())
      .filter(([, state]) => state.consumerTag !== null)
      .map(async ([queue, state]) => {
        if (!state.consumerTag) {
          return;
        }

        try {
          await this.channel.cancel(state.consumerTag);

          logger.info(`RabbitMQ consumer cancelled for queue "${queue}".`);
        } catch (error) {
          logger.error(
            `[RabbitMQ] Failed to cancel consumer for queue "${queue}".`,
            error,
          );
        }
      });

    await Promise.all(cancellations);

    this.queues.clear();
  }

  public async requeueDlqMessages(
    queue: string,
    options: { limit?: number; resetRetryCount?: boolean } = {},
  ): Promise<{ requeued: number; failed: number }> {
    const state = this.queues.get(queue);

    if (!state) {
      throw new Error(
        `Cannot requeue DLQ messages: "${queue}" is not a queue this Consumer has subscribed to.`,
      );
    }

    if (!state.retry) {
      throw new Error(
        `Cannot requeue DLQ messages: "${queue}" has no retry/DLQ configured, so it has no DLQ to drain.`,
      );
    }

    const dlqName = this.getDlqName(queue);
    const resetRetryCount = options.resetRetryCount ?? true;
    let requeued = 0;
    let failed = 0;

    while (options.limit === undefined || requeued + failed < options.limit) {
      const message = await this.channel.get(dlqName, { noAck: false });

      if (message === false) {
        break; // DLQ is empty -- caught up
      }

      try {
        const properties: Options.Publish = {
          messageId: message.properties.messageId,
          type: message.properties.type,
          timestamp: message.properties.timestamp,
          contentType: message.properties.contentType,
          contentEncoding: message.properties.contentEncoding,
          correlationId: message.properties.correlationId,
          persistent: true,
          headers: {
            ...message.properties.headers,
            ...(resetRetryCount ? { "x-retry-count": 0 } : {}),
          },
        };

        // Publish directly to the queue (default exchange)
        const buffered = this.channel.publish(
          "",
          queue,
          message.content,
          properties,
        );

        if (!buffered) {
          logger.warn(
            `[RabbitMQ] Publish buffer full while requeueing a DLQ message for "${queue}".`,
          );
        }

        this.channel.ack(message);
        requeued++;
      } catch (error) {
        logger.error(
          `[RabbitMQ] Failed to requeue a DLQ message for "${queue}". Leaving it in the DLQ.`,
          error,
        );

        this.channel.nack(message, false, true); // put it back
        failed++;
      }
    }

    logger.info(
      `[RabbitMQ] Requeued ${requeued} message(s) from "${dlqName}" back to "${queue}" (${failed} failed).`,
    );

    return { requeued, failed };
  }

  private getOrCreateQueueState(
    queue: string,
    options: SubscribeOptions,
  ): QueueState {
    const existing = this.queues.get(queue);

    if (existing) {
      this.warnOnRetryConfigDrift(queue, existing, options);
      return existing;
    }

    const state: QueueState = {
      handlers: new Map<EventName, EventHandler>(),
      consumerTag: null,
      initializationPromise: null,
      prefetch: options.prefetch,
      requeueOnError: options.requeueOnError ?? false,
      retry: this.resolveRetryOptions(options.retry),
      queueOptions: options.queueOptions,
    };

    this.queues.set(queue, state);

    return state;
  }

  private resolveRetryOptions(
    retry?: RetryOptions,
  ): ResolvedRetryOptions | undefined {
    if (!retry) {
      return undefined;
    }

    const resolved: ResolvedRetryOptions = {
      ...DEFAULT_RETRY_OPTIONS,
      ...retry,
    };

    if (resolved.maxRetries < 1) {
      throw new Error("RetryOptions.maxRetries must be at least 1.");
    }

    if (resolved.initialDelayMs < 1) {
      throw new Error("RetryOptions.initialDelayMs must be a positive number.");
    }

    if (resolved.factor < 1) {
      throw new Error("RetryOptions.factor must be at least 1.");
    }

    if (resolved.maxDelayMs < resolved.initialDelayMs) {
      throw new Error(
        "RetryOptions.maxDelayMs must be greater than or equal to initialDelayMs.",
      );
    }

    return resolved;
  }

  private warnOnRetryConfigDrift(
    queue: string,
    existing: QueueState,
    options: SubscribeOptions,
  ): void {
    const incoming = this.resolveRetryOptions(options.retry);

    if (JSON.stringify(incoming) !== JSON.stringify(existing.retry)) {
      logger.warn(
        `[RabbitMQ] Queue "${queue}" was already initialized with different retry options. ` +
          `Retry config is set once per queue; this later subscribe() call's retry options are ignored.`,
      );
    }
  }

  private async ensureQueueInitialized(
    queue: string,
    state: QueueState,
  ): Promise<void> {
    if (state.consumerTag) {
      return;
    }

    if (state.initializationPromise) {
      await state.initializationPromise;
      return;
    }

    state.initializationPromise = this.initializeQueue(queue, state);

    try {
      await state.initializationPromise;
    } catch (error) {
      state.initializationPromise = null;
      this.queues.delete(queue);

      throw error;
    }
  }

  private async initializeQueue(
    queue: string,
    state: QueueState,
  ): Promise<void> {
    await this.channel.assertQueue(queue, {
      durable: true,
      ...state.queueOptions,
    });

    if (state.retry) {
      await this.setupRetryTopology(queue, state.retry);
    }

    /*
     Always set this explicitly (defaulting to 0 = unlimited) rather than skipping the call when unset — this channel is shared across queues,  and skipping the call lets this queue's consumer silently inherit whatever prefetch value a previous queue's initialization last set.
     */
    await this.channel.prefetch(state.prefetch ?? 0);

    const result = await this.channel.consume(
      queue,
      async (message) => {
        await this.handleMessage(queue, state, message);
      },
      {
        noAck: false,
      },
    );

    state.consumerTag = result.consumerTag;

    logger.info(`RabbitMQ consumer started for queue "${queue}".`);
  }

  private async setupRetryTopology(
    queue: string,
    retry: ResolvedRetryOptions,
  ): Promise<void> {
    const retryExchange = this.getRetryExchangeName(queue);

    await this.channel.assertExchange(retryExchange, "direct", {
      durable: true,
    });

    for (let attempt = 1; attempt <= retry.maxRetries; attempt++) {
      const retryQueue = this.getRetryQueueName(queue, attempt);
      const ttl = this.computeBackoffMs(attempt, retry);

      await this.channel.assertQueue(retryQueue, {
        durable: true,
        arguments: {
          "x-message-ttl": ttl,
          "x-dead-letter-exchange": "",
          "x-dead-letter-routing-key": queue,
        },
      });

      await this.channel.bindQueue(retryQueue, retryExchange, String(attempt));
    }

    await this.channel.assertQueue(this.getDlqName(queue), { durable: true });

    logger.info(
      `RabbitMQ retry topology initialized for queue "${queue}" (${retry.maxRetries} stage(s)).`,
    );
  }

  private computeBackoffMs(
    attempt: number,
    retry: ResolvedRetryOptions,
  ): number {
    const delay = retry.initialDelayMs * Math.pow(retry.factor, attempt - 1);
    return Math.min(Math.round(delay), retry.maxDelayMs);
  }

  private getRetryExchangeName(queue: string): string {
    return `${queue}.retry`;
  }

  private getRetryQueueName(queue: string, attempt: number): string {
    return `${queue}.retry.${attempt}`;
  }

  private getDlqName(queue: string): string {
    return `${queue}.dlq`;
  }

  private async handleMessage(
    queue: string,
    state: QueueState,
    message: ConsumeMessage | null,
  ): Promise<void> {
    if (message === null) {
      logger.warn(
        `RabbitMQ consumer for queue "${queue}" was cancelled by the broker.`,
      );

      return;
    }

    const event = message.properties.type as EventName;

    const handler = state.handlers.get(event);

    if (!handler) {
      logger.error(
        `[RabbitMQ] No handler registered for event "${String(event)}" on queue "${queue}".`,
      );

      this.channel.nack(message, false, false);

      return;
    }

    const metadata: MessageMetadata = {
      type: message.properties.type,
      exchange: message.fields.exchange,
      headers: message.properties.headers,
      routingKey: message.fields.routingKey,
      redelivered: message.fields.redelivered,
      messageId: message.properties.messageId,
      deliveryTag: message.fields.deliveryTag,
      timestamp: message.properties.timestamp,
      correlationId: message.properties.correlationId,
    };

    try {
      const payload = this.parsePayload(message);

      await handler(payload, metadata);

      this.channel.ack(message);
    } catch (error) {
      logger.error(
        `[RabbitMQ] Failed to process event "${String(event)}" from queue "${queue}".`,
        error,
      );

      if (state.retry) {
        await this.handleRetry(queue, state.retry, message);
        return;
      }

      this.channel.nack(message, false, state.requeueOnError);
    }
  }

  private async handleRetry(
    queue: string,
    retry: ResolvedRetryOptions,
    message: ConsumeMessage,
  ): Promise<void> {
    const nextRetryCount = this.getRetryCount(message) + 1;

    const properties: Options.Publish = {
      messageId: message.properties.messageId,
      type: message.properties.type,
      timestamp: message.properties.timestamp,
      contentType: message.properties.contentType,
      contentEncoding: message.properties.contentEncoding,
      correlationId: message.properties.correlationId,
      persistent: true,
      headers: {
        ...message.properties.headers,
        "x-retry-count": nextRetryCount,
      },
    };

    try {
      if (nextRetryCount > retry.maxRetries) {
        this.publishToDlq(queue, message.content, properties);

        logger.warn(
          `[RabbitMQ] Retries exhausted for a message on queue "${queue}". Routed to DLQ.`,
        );
      } else {
        this.publishToRetryStage(
          queue,
          nextRetryCount,
          message.content,
          properties,
        );
      }

      this.channel.ack(message);
    } catch (error) {
      /*
       * If we can't even route the message into the retry/DLQ pipeline,
       * fall back to requeueing it on the main queue rather than losing it.
       * This is bounded by the same "no reconnect strategy" gap flagged
       * earlier — a broken channel here will fail this too.
       */
      logger.error(
        `[RabbitMQ] Failed to route message into retry/DLQ pipeline for queue "${queue}". Requeueing instead.`,
        error,
      );

      this.channel.nack(message, false, true);
    }
  }

  private getRetryCount(message: ConsumeMessage): number {
    const headerValue = message.properties.headers?.["x-retry-count"];
    return typeof headerValue === "number" ? headerValue : 0;
  }

  private publishToRetryStage(
    queue: string,
    attempt: number,
    content: Buffer,
    properties: Options.Publish,
  ): void {
    const retryExchange = this.getRetryExchangeName(queue);

    const buffered = this.channel.publish(
      retryExchange,
      String(attempt),
      content,
      properties,
    );

    if (!buffered) {
      logger.warn(
        `[RabbitMQ] Publish buffer full while routing retry (attempt ${attempt}) for queue "${queue}".`,
      );
    }
  }

  private publishToDlq(
    queue: string,
    content: Buffer,
    properties: Options.Publish,
  ): void {
    const buffered = this.channel.publish(
      "",
      this.getDlqName(queue),
      content,
      properties,
    );

    if (!buffered) {
      logger.warn(
        `[RabbitMQ] Publish buffer full while routing to DLQ for queue "${queue}".`,
      );
    }
  }

  private parsePayload(message: ConsumeMessage): unknown {
    const content = message.content.toString("utf-8");

    return JSON.parse(content);
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new Error("RabbitMQ consumer has already been closed.");
    }
  }
}
