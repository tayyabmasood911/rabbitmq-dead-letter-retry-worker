'use strict';

/**
 * RabbitMQ Task Producer Utility
 * Dispatches test tasks with persistent delivery mode and custom headers.
 */

const amqp = require('amqplib');
const { TOPOLOGY, setupResilientTopology } = require('./retryStrategy');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost:5672';

async function publishSampleTasks() {
  console.log(`[Publisher] Connecting to RabbitMQ at ${RABBITMQ_URL}...`);
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();

  await setupResilientTopology(channel);

  const tasks = [
    {
      taskId: 'task_001_success',
      type: 'payment.settle',
      amount: 150.00,
      simulate_failure: false
    },
    {
      taskId: 'task_002_fail_and_retry',
      type: 'webhook.dispatch',
      url: 'https://api.external.com/hook',
      simulate_failure: true
    }
  ];

  for (const task of tasks) {
    const payload = Buffer.from(JSON.stringify(task));
    channel.publish(
      TOPOLOGY.EXCHANGES.PRIMARY,
      TOPOLOGY.ROUTING_KEYS.EXECUTE,
      payload,
      {
        persistent: true,
        contentType: 'application/json',
        timestamp: Date.now(),
        headers: {
          'x-retry-count': 0,
          'x-source': 'task-publisher-service'
        }
      }
    );
    console.log(`[Publisher] Dispatched task ${task.taskId} to ${TOPOLOGY.EXCHANGES.PRIMARY}`);
  }

  setTimeout(async () => {
    await channel.close();
    await connection.close();
    console.log('[Publisher] Done.');
  }, 500);
}

if (require.main === module) {
  publishSampleTasks().catch(err => {
    console.error('[Publisher] Error:', err);
    process.exit(1);
  });
}

module.exports = { publishSampleTasks };
