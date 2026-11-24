'use strict';

/**
 * Enterprise RabbitMQ Worker Consumer with Exponential Retry & DLQ Routing
 */

const amqp = require('amqplib');
const {
  TOPOLOGY,
  getRetryCount,
  getNextRetryRoute,
  setupResilientTopology
} = require('./retryStrategy');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost:5672';
const PREFETCH_COUNT = parseInt(process.env.PREFETCH_COUNT || '10', 10);

let connection = null;
let channel = null;
let isShuttingDown = false;

async function connectWithRetry() {
  let attempt = 0;
  while (!isShuttingDown) {
    try {
      attempt++;
      console.log(`[Worker] Connecting to RabbitMQ at ${RABBITMQ_URL} (attempt ${attempt})...`);
      connection = await amqp.connect(RABBITMQ_URL);

      connection.on('error', (err) => {
        console.error('[Worker] RabbitMQ connection error:', err.message);
      });

      connection.on('close', () => {
        if (!isShuttingDown) {
          console.warn('[Worker] Connection closed unexpectedly. Reconnecting in 5s...');
          setTimeout(connectWithRetry, 5000);
        }
      });

      channel = await connection.createChannel();
      await channel.prefetch(PREFETCH_COUNT);
      await setupResilientTopology(channel);

      console.log(`[Worker] Listening for messages on '${TOPOLOGY.QUEUES.PRIMARY}' (Prefetch=${PREFETCH_COUNT})...`);
      await consumeMessages();
      break;
    } catch (err) {
      console.error(`[Worker] Connection failed: ${err.message}. Retrying in 5s...`);
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

async function processBusinessTask(taskData) {
  // Simulate task processing; if task has simulate_failure: true, raise error
  if (taskData.simulate_failure) {
    throw new Error(`Simulated transient processing error for task ${taskData.taskId}`);
  }
  console.log(`  -> Successfully processed task: ${taskData.taskId} [${taskData.type}]`);
  return true;
}

async function consumeMessages() {
  await channel.consume(
    TOPOLOGY.QUEUES.PRIMARY,
    async (msg) => {
      if (!msg) return;

      const rawContent = msg.content.toString();
      const currentRetries = getRetryCount(msg);

      try {
        const taskData = JSON.parse(rawContent);
        console.log(`\n[Worker] Received message (deliveryTag=${msg.fields.deliveryTag}, retryCount=${currentRetries}):`, taskData.taskId || 'unnamed');

        // Execute task
        await processBusinessTask(taskData);

        // Acknowledge successful processing
        channel.ack(msg);
      } catch (err) {
        console.error(`[Worker] Error processing message: ${err.message}`);

        const route = getNextRetryRoute(currentRetries);
        const nextRetryCount = currentRetries + 1;

        if (route.action === 'RETRY') {
          console.warn(`[Worker] Routing message to Delay Queue '${route.delayQueue}' for retry #${nextRetryCount}...`);

          // Republish to retry exchange with incremented retry count
          channel.publish(
            TOPOLOGY.EXCHANGES.RETRY,
            route.routingKey,
            msg.content,
            {
              persistent: true,
              headers: {
                ...msg.properties.headers,
                'x-retry-count': nextRetryCount,
                'x-last-error': err.message,
                'x-original-timestamp': msg.properties.timestamp || Date.now()
              }
            }
          );
          // Ack the original message so it leaves primary queue
          channel.ack(msg);
        } else {
          console.error(`[Worker] Max retries (${TOPOLOGY.MAX_RETRIES}) exceeded! Quarantining to Final DLQ '${TOPOLOGY.QUEUES.DEAD_LETTER}'...`);

          channel.publish(
            TOPOLOGY.EXCHANGES.DEAD_LETTER,
            '',
            msg.content,
            {
              persistent: true,
              headers: {
                ...msg.properties.headers,
                'x-exhausted-retries': nextRetryCount,
                'x-fatal-error': err.message,
                'x-quarantine-time': Date.now()
              }
            }
          );
          channel.ack(msg);
        }
      }
    },
    { noAck: false }
  );
}

// Graceful shutdown
async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log(`\n[Worker] Received ${signal}. Initiating graceful shutdown...`);

  try {
    if (channel) {
      await channel.close();
      console.log('[Worker] AMQP Channel closed.');
    }
    if (connection) {
      await connection.close();
      console.log('[Worker] AMQP Connection closed.');
    }
  } catch (err) {
    console.error('[Worker] Error during shutdown:', err.message);
  } finally {
    process.exit(0);
  }
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

if (require.main === module) {
  connectWithRetry();
}

module.exports = {
  connectWithRetry,
  consumeMessages,
  gracefulShutdown
};
