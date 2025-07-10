#!/usr/bin/env node

// Polyfill for Array.prototype.flat (for older Node versions)
if (!Array.prototype.flat) {
  Array.prototype.flat = function(depth = 1) {
    return this.reduce((acc, val) => acc.concat(
      (depth > 1 && Array.isArray(val)) ? val.flat(depth - 1) : val
    ), []);
  };
}

// redis-benchmark.js
// A Redis benchmark tool using ioredis and generic-pool for connection pooling.
// Supports HSET + configurable number of HGET commands per iteration,
// with optional forced disconnect/reconnect after a given number of commands.

const Redis = require('ioredis');
const genericPool = require('generic-pool');
const { program } = require('commander');

program
  .requiredOption('-h, --host <host>', 'Redis host')
  .requiredOption('-p, --port <port>', 'Redis port', parseInt)
  .requiredOption('-c, --connections <number>', 'Max pool size (parallel connections)', parseInt)
  .requiredOption('-r, --requests <number>', 'Number of iterations per worker', parseInt)
  .requiredOption('-l, --pipeline-length <number>', 'Number of HGET commands per pipeline iteration', parseInt)
  .requiredOption('-d, --disconnect-interval <number>', 'Number of commands after which to force disconnect', parseInt)
  .option('--key-prefix <prefix>', 'Key prefix for HSET/HGET operations', 'benchmark')
  .parse(process.argv);

const { host, port, connections, requests, pipelineLength, disconnectInterval, keyPrefix } = program.opts();

// Create a Redis connection pool
const factory = {
  create: () => new Redis({ host, port }),
  destroy: client => client.quit(),
};
const pool = genericPool.createPool(factory, {
  max: connections,
  min: 0,
  idleTimeoutMillis: 30000,
});

async function runWorker(workerId) {
  let commandsSinceReconnect = 0;

  for (let i = 0; i < requests; i++) {
    const client = await pool.acquire();
    const key = `${keyPrefix}:${workerId}:${i}`;
    const value = `value-${workerId}-${i}`;

    try {
      // Pipeline 1 HSET + pipelineLength HGETs
      const pipeline = client.pipeline();
      pipeline.hset(key, 'field', value);
      for (let k = 0; k < pipelineLength; k++) {
        pipeline.hget(key, 'field');
      }
      await pipeline.exec();

      // Track commands executed
      commandsSinceReconnect += 1 + pipelineLength;
    } catch (err) {
      console.error(`Worker ${workerId} iteration ${i} error:`, err);
    }

    // Decide whether to destroy or release the connection
    if (commandsSinceReconnect >= disconnectInterval) {
      await pool.destroy(client);
      console.log(`Worker ${workerId} disconnected after ${commandsSinceReconnect} commands`);
      commandsSinceReconnect = 0;
    } else {
      await pool.release(client);
    }
  }
}

(async () => {
  console.log(`Starting benchmark: host=${host}, port=${port}, poolMax=${connections}, requests=${requests}, pipelineLength=${pipelineLength}, disconnectInterval=${disconnectInterval}`);
  const start = Date.now();

  // Launch all workers in parallel
  await Promise.all(
    Array.from({ length: connections }, (_, id) => runWorker(id))
  );

  const duration = Date.now() - start;
  const totalCommands = connections * requests * (1 + pipelineLength);
  console.log(`Completed approx. ${totalCommands} commands (1 HSET + ${pipelineLength} HGET) in ${duration}ms`);
  console.log(`Avg round-trip per command: ${(duration / totalCommands).toFixed(2)}ms`);

  // Drain and clear the pool
  await pool.drain();
  await pool.clear();
  console.log('Benchmark complete, pool drained and cleared.');
})();
