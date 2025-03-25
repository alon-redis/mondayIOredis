const Redis = require('ioredis');
const Redlock = require('redlock');
const genericPool = require('generic-pool');

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
    };

    this.pool = this.createPool();
    this.redlock = this.createRedlock();
  }

  createPool() {
    const factory = {
      create: async () => {
        return new Redis(this.redisOptions);
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
        retryCount: 10,
        retryDelay: 200,
        retryJitter: 200,
        automaticExtensionThreshold: 500,
      }
    );
  }

  async acquireLock(resource, ttl) {
    return this.redlock.acquire([resource], ttl);
  }

  async releaseLock(lock) {
    return lock.release();
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
    try {
      return await operation();
    } finally {
      await this.redlock.release(lock);
    }
  }

  async shutdown() {
    await this.pool.drain();
    await this.pool.clear();
    await this.redlock.quit();
  }
}

// Usage example
async function main() {
  const app = new RedisApp('redis-10000.aws-alon-5160.env0.qa.redislabs.com', 10000, 2000, 1024);

  try {
    await app.performLockedOperation('myResource', 5000, async () => {
      await app.setValue('myKey', 'myValue');
      const value = await app.getValue('myKey');
      console.log('Value:', value);
    });
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await app.shutdown();
  }
}

main().catch(console.error);
