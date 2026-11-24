'use strict';

/**
 * RabbitMQ Resilient Dead-Letter & Exponential Backoff Delay Topology Engine
 *
 * Implements the Enterprise Dead-Letter-Exchange (DLX) & Delayed Retry Pattern:
 * - Direct primary exchange routing to high-throughput worker queues
 * - Dedicated retry exchange with tiered message TTL delay queues (10s, 60s, 300s)
 * - Automatic republish back to primary queue when TTL expires via DLX
 * - Strict retry threshold enforcement with terminal routing to quarantine DLQ
 * - Full jitter exponential backoff to prevent thundering herd on downstream recoveries
 */

const TOPOLOGY = {
  EXCHANGES: {
    PRIMARY: 'tasks.direct',
    RETRY: 'tasks.retry.exchange',
    DEAD_LETTER: 'tasks.dlx'
  },
  QUEUES: {
    PRIMARY: 'tasks.primary',
    DEAD_LETTER: 'tasks.dlq',
    RETRY_10S: 'tasks.retry.10s',
    RETRY_60S: 'tasks.retry.60s',
    RETRY_300S: 'tasks.retry.300s'
  },
  ROUTING_KEYS: {
    EXECUTE: 'task.execute',
    RETRY_10S: 'task.retry.10s',
    RETRY_60S: 'task.retry.60s',
    RETRY_300S: 'task.retry.300s',
    DEAD_LETTER: 'task.dlq'
  },
  MAX_RETRIES: 3
};

/**
 * Calculates exponential backoff with full jitter
 * @param {number} retryCount Current retry attempt index
 * @param {number} baseDelayMs Base delay in milliseconds (default: 1000ms)
 * @param {number} maxDelayMs Maximum ceiling delay (default: 60000ms)
 * @returns {number} Delay in milliseconds
 */
function calculateBackoff(retryCount, baseDelayMs = 1000, maxDelayMs = 60000) {
  const exponential = Math.min(baseDelayMs * Math.pow(2, retryCount), maxDelayMs);
  // Full jitter: random between 0 and exponential delay
  return Math.floor(Math.random() * exponential);
}

/**
 * Extracts the current retry count from message properties or x-death headers
 * @param {Object} msg AMQP message
 * @returns {number} Current retry count
 */
function getRetryCount(msg) {
  if (!msg || !msg.properties) return 0;
  
  // Custom header priority
  const headers = msg.properties.headers || {};
  if (typeof headers['x-retry-count'] === 'number') {
    return headers['x-retry-count'];
  }

  // RabbitMQ native x-death header inspection
  if (Array.isArray(headers['x-death'])) {
    const primaryDeath = headers['x-death'].find(
      entry => entry.queue === TOPOLOGY.QUEUES.PRIMARY
    );
    if (primaryDeath && typeof primaryDeath.count === 'number') {
      return primaryDeath.count;
    }
  }

  return 0;
}

/**
 * Determines the next routing action based on retry threshold
 * @param {number} currentRetries
 * @returns {{ action: 'RETRY'|'DEAD_LETTER', routingKey: string, delayQueue: string }}
 */
function getNextRetryRoute(currentRetries) {
  if (currentRetries >= TOPOLOGY.MAX_RETRIES) {
    return {
      action: 'DEAD_LETTER',
      routingKey: TOPOLOGY.ROUTING_KEYS.DEAD_LETTER,
      delayQueue: TOPOLOGY.QUEUES.DEAD_LETTER
    };
  }

  if (currentRetries === 0) {
    return {
      action: 'RETRY',
      routingKey: TOPOLOGY.ROUTING_KEYS.RETRY_10S,
      delayQueue: TOPOLOGY.QUEUES.RETRY_10S
    };
  } else if (currentRetries === 1) {
    return {
      action: 'RETRY',
      routingKey: TOPOLOGY.ROUTING_KEYS.RETRY_60S,
      delayQueue: TOPOLOGY.QUEUES.RETRY_60S
    };
  } else {
    return {
      action: 'RETRY',
      routingKey: TOPOLOGY.ROUTING_KEYS.RETRY_300S,
      delayQueue: TOPOLOGY.QUEUES.RETRY_300S
    };
  }
}

/**
 * Declares all exchanges, queues, DLQs, and dead-letter routing bindings
 * @param {Object} channel amqplib channel instance
 */
async function setupResilientTopology(channel) {
  console.log('[Topology] Initializing durable enterprise RabbitMQ topology...');

  // 1. Declare Primary and Dead Letter Exchanges
  await channel.assertExchange(TOPOLOGY.EXCHANGES.PRIMARY, 'direct', { durable: true });
  await channel.assertExchange(TOPOLOGY.EXCHANGES.RETRY, 'direct', { durable: true });
  await channel.assertExchange(TOPOLOGY.EXCHANGES.DEAD_LETTER, 'fanout', { durable: true });

  // 2. Declare Quarantine DLQ and bind to DLX
  await channel.assertQueue(TOPOLOGY.QUEUES.DEAD_LETTER, { durable: true });
  await channel.bindQueue(TOPOLOGY.QUEUES.DEAD_LETTER, TOPOLOGY.EXCHANGES.DEAD_LETTER, '');

  // 3. Declare Primary Work Queue with Dead Letter routing to DLX
  await channel.assertQueue(TOPOLOGY.QUEUES.PRIMARY, {
    durable: true,
    deadLetterExchange: TOPOLOGY.EXCHANGES.DEAD_LETTER
  });
  await channel.bindQueue(TOPOLOGY.QUEUES.PRIMARY, TOPOLOGY.EXCHANGES.PRIMARY, TOPOLOGY.ROUTING_KEYS.EXECUTE);

  // 4. Declare Tiered Delay Queues (TTL + Dead-Letter back to Primary Exchange)
  const declareDelayQueue = async (queueName, routingKey, ttlMs) => {
    await channel.assertQueue(queueName, {
      durable: true,
      messageTtl: ttlMs,
      deadLetterExchange: TOPOLOGY.EXCHANGES.PRIMARY,
      deadLetterRoutingKey: TOPOLOGY.ROUTING_KEYS.EXECUTE
    });
    await channel.bindQueue(queueName, TOPOLOGY.EXCHANGES.RETRY, routingKey);
  };

  await declareDelayQueue(TOPOLOGY.QUEUES.RETRY_10S, TOPOLOGY.ROUTING_KEYS.RETRY_10S, 10000);   // 10s delay
  await declareDelayQueue(TOPOLOGY.QUEUES.RETRY_60S, TOPOLOGY.ROUTING_KEYS.RETRY_60S, 60000);   // 60s delay
  await declareDelayQueue(TOPOLOGY.QUEUES.RETRY_300S, TOPOLOGY.ROUTING_KEYS.RETRY_300S, 300000); // 5m delay

  console.log('[Topology] Resilient RabbitMQ DLX and retry pipeline successfully initialized.');
}

module.exports = {
  TOPOLOGY,
  calculateBackoff,
  getRetryCount,
  getNextRetryRoute,
  setupResilientTopology
};
