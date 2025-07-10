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
// optional forced disconnect/reconnect after a given number of commands,
// and bursty connection prewarming.

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
  .option('-b, --burst-size <number>', 'Number of connections to open at once when prewarming', parseInt, 0)
  .option('--key-prefix <prefix>', 'Key prefix for HSET/HGET operations', 'benchmark')
  .parse(process.argv);

const { host, port, connections, requests, pipelineLength, disconnectInterval, burstSize, keyPrefix } = program.opts();

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

// Worker function: performs requests, pipelines commands, and handles disconnects
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
      commandsSinceReconnect += 1 + pipelineLength;
    } catch (err) {
      console.error(`Worker ${workerId} iteration ${i} error:`, err);
    }
    // Disconnect or release based on interval
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
  console.log(`Config: poolMax=${connections}, requests=${requests}, pipelineLength=${pipelineLength}, disconnectInterval=${disconnectInterval}, burstSize=${burstSize}`);

  // Optional prewarm the pool in bursts
  if (burstSize > 0) {
    console.log(`Prewarming connection pool in bursts of ${burstSize}...`);
    let opened = 0;
    while (opened < connections) {
      const toOpen = Math.min(burstSize, connections - opened);
      const clients = await Promise.all(
        Array.from({ length: toOpen }, () => pool.acquire())
      );
      console.log(`  Opened burst of ${toOpen} connections (total ${opened + toOpen}/${connections})`);
      await Promise.all(clients.map(client => pool.release(client)));
      opened += toOpen;
    }
    console.log('Pool prewarm complete.');
  }

  const start = Date.now();

  // Launch workers in bursts
  const allWorkers = [];
  let started = 0;
  const burstCount = burstSize > 0 ? burstSize : connections;
  console.log(`Starting workers in bursts of ${burstCount}...`);
  while (started < connections) {
    const toLaunch = Math.min(burstCount, connections - started);
    console.log(`  Launching burst of ${toLaunch} workers (started ${started + toLaunch}/${connections})`);
    for (let i = 0; i < toLaunch; i++) {
      allWorkers.push(runWorker(started + i));
    }
    started += toLaunch;
    // Optional delay: await new Promise(res => setTimeout(res, 10));
  }

  // Wait for all workers to finish
  await Promise.all(allWorkers);

  const duration = Date.now() - start;
  const totalCommands = connections * requests * (1 + pipelineLength);
  console.log(`Completed approx. ${totalCommands} commands (1 HSET + ${pipelineLength} HGET) in ${duration}ms`);
  console.log(`Avg round-trip per command: ${(duration / totalCommands).toFixed(2)}ms`);

  // Drain and clear the pool
  await pool.drain();
  await pool.clear();
  console.log('Benchmark complete, pool drained and cleared.');
})();
