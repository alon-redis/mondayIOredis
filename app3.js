const Redis = require('ioredis');
const Redlock = require('redlock');
const genericPool = require('generic-pool');
const crypto = require('crypto');

class RedisApp {
  constructor(host, port, poolSize, keySize) {
    this.host = host;
    this.port = port;
    this.poolSize = poolSize;
    this.keySize = keySize;

    this.redisOptions = {
      host: this.host,
      port: this.port,
      maxRetriesPerRequest: null,
      enableOfflineQueue: false,
      reconnectOnError: (err) => {
        console.error('Redis connection error:', err.message);
        return true;
      },
    };

    this.pool = this.createPool();
    this.redlock = this.createRedlock();
  }

  createPool() {
    const factory = {
      create: async () => {
        const client = new Redis(this.redisOptions);
        client.on('error', (err) => console.error('Redis client error:', err.message));
        client.on('end', () => console.log('Redis connection closed'));
        await client.ping(); // Verify connection
        return client;
      },
      destroy: async (client) => {
        await client.quit();
      },
    };

    return genericPool.createPool(factory, {
      max: this.poolSize,
      min: 1,
    });
  }

  createRedlock() {
    const client = new Redis(this.redisOptions);
    return new Redlock(
      [client],
      {
        driftFactor: 0.01,
        retryCount: 50,
        retryDelay: 500,
        retryJitter: 300,
        automaticExtensionThreshold: 1000,
      }
    );
  }

  async acquireLock(resource, ttl) {
    return this.redlock.acquire([resource], ttl);
  }

  async setValue(key, value) {
    const client = await this.pool.acquire();
    try {
      await client.set(key, value);
    } finally {
      await this.pool.release(client);
    }
  }

  async getValue(key) {
    const client = await this.pool.acquire();
    try {
      return await client.get(key);
    } finally {
      await this.pool.release(client);
    }
  }

  async performLockedOperation(resource, ttl, operation) {
    const lock = await this.acquireLock(resource, ttl);
    const renewInterval = setInterval(() => {
      lock.extend(ttl).catch((err) => console.error('Failed to renew lock:', err.message));
    }, ttl / 2);

    try {
      return await operation();
    } finally {
      clearInterval(renewInterval);
      await this.redlock.release(lock);
    }
  }

  async shutdown() {
    await this.pool.drain();
    await this.pool.clear();
    await this.redlock.quit();
  }
}

function generateRandomString(length) {
  return crypto.randomBytes(Math.ceil(length / 2))
    .toString('hex')
    .slice(0, length);
}

async function runTest(keyValueSize, numConnections, numRequests) {
  const app = new RedisApp('redis-10000.aws-alon-5160.env0.qa.redislabs.com', 10000, numConnections, keyValueSize);

  console.log(`Starting test with ${numRequests} requests, ${numConnections} connections, and key-value size of ${keyValueSize} bytes`);

  const startTime = Date.now();

  try {
    const promises = [];
    for (let i = 0; i < numRequests; i++) {
      const key = `key_${i}`;
      const value = generateRandomString(keyValueSize);
      promises.push(app.performLockedOperation(key, 15000, async () => {
        await app.setValue(key, value);
        const retrievedValue = await app.getValue(key);
        if (retrievedValue !== value) {
          console.error(`Mismatch for key ${key}`);
        }
      }));
    }

    await Promise.all(promises);

    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;
    console.log(`Test completed in ${duration} seconds`);
    console.log(`Throughput: ${numRequests / duration} operations per second`);
  } catch (error) {
    console.error('Error during test:', error);
  } finally {
    await app.shutdown();
  }
}

async function main() {
  const keyValueSize = 1024; // Key-value size in bytes
  const numConnections = 10; // Number of connections
  const numRequests = 100; // Number of requests

  await runTest(keyValueSize, numConnections, numRequests);
}

main().catch(console.error);
