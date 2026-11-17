import { createRequire } from 'node:module';
import type { N8nItem } from './n8n-helper';

interface ConfirmChannel {
  on(event: 'error', handler: (error: Error) => void): void;
  checkExchange(exchange: string): Promise<unknown>;
  publish(
    exchange: string,
    routingKey: string,
    body: Buffer,
    options: Record<string, never>,
    confirm: (error: Error | null) => void,
  ): boolean;
}

interface AmqpConnection {
  on(event: 'error', handler: (error: Error) => void): void;
  createConfirmChannel(): Promise<ConfirmChannel>;
  close(): Promise<void>;
}

export async function publishRabbitMq(
  exchange: string,
  routingKey: string,
  messages: string[],
): Promise<N8nItem[]> {
  const url = process.env.RABBITMQ_URL;
  if (!url) throw new Error('RABBITMQ_URL is required to publish messages');

  // Only generated services with RabbitMQ nodes install this package.
  const { connect } = createRequire(import.meta.url)('amqplib') as {
    connect(url: string): Promise<AmqpConnection>;
  };
  const connection = await connect(url);
  // amqplib rejects pending operations on these errors and also emits events.
  connection.on('error', () => {});
  try {
    const channel = await connection.createConfirmChannel();
    channel.on('error', () => {});
    await channel.checkExchange(exchange);

    const results: N8nItem[] = [];
    for (const message of messages) {
      const body = Buffer.from(message);
      const success = await new Promise<boolean>((resolve, reject) => {
        const writable = channel.publish(exchange, routingKey, body, {}, (error) => {
          if (error) reject(error);
          else resolve(writable);
        });
      });
      results.push({ json: { success } });
    }
    return results;
  } finally {
    await connection.close().catch(() => {});
  }
}
