#!/usr/bin/env node
// Redis High Performance Benchmark App (CLI) — Optimized for sustained high performance
// Improvements: Removed redlock bottleneck, better error handling, memory management
// Uses ioredis@5.3.1, generic-pool
// Author: Alon Shmuely (optimized for sustained performance)

const Redis = require('ioredis');
const genericPool = require('generic-pool');
const { randomBytes } = require('crypto');
const { Worker, isMainThread, parentPort, workerData, threadId } = require('worker_threads');
const { performance } = require('perf_hooks');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// ----- CLI Parameters ----
const argv = yargs(hideBin(process.argv))
    .usage('Usage: $0 [options]')
    .option('host', { describe: 'Redis host', type: 'string', default: '127.0.0.1' })
    .option('port', { describe: 'Redis port', type: 'number', default: 6379 })
    .option('connections', { describe: 'Number of parallel connections', type: 'number', default: 10 })
    .option('requests', { describe: 'Number of requests per connection', type: 'number', default: 10000 })
    .option('keyprefix', { describe: 'Key prefix (will generate key as <prefix>:<worker>:<batch>)', type: 'string', default: 'testkey' })
    .option('valmin', { describe: 'Min value size', type: 'number', default: 64 })
    .option('valmax', { describe: 'Max value size', type: 'number', default: 1024 })
    .option('pipemin', { describe: 'Min pipeline batch size', type: 'number', default: 5 })
    .option('pipemax', { describe: 'Max pipeline batch size', type: 'number', default: 50 })
    .option('duration', { describe: 'Test duration in seconds', type: 'number', default: 60 })
    .option('logint', { describe: 'Log interval (ms)', type: 'number', default: 2000 })
    .option('keyrange', { describe: 'Number of unique keys to cycle through', type: 'number', default: 1000 })
    .option('setpercent', { describe: 'Percentage of operations that are SETs (rest are GETs)', type: 'number', default: 20 })
    .help('h').alias('h', 'help')
    .argv;

const config = {
    redisHost: argv.host,
    redisPort: argv.port,
    numConnections: argv.connections,
    requestsPerConnection: argv.requests,
    keyPrefix: argv.keyprefix,
    valueSizeRange: [argv.valmin, argv.valmax],
    pipelineRange: [argv.pipemin, argv.pipemax],
    durationSeconds: argv.duration,
    logInterval: argv.logint,
    keyRange: argv.keyrange,
    setPercentage: argv.setpercent,
};

// Helper to randomize integer in [min, max]
function randInt([min, max]) {
    return min + Math.floor(Math.random() * (max - min + 1));
}

// Helper to generate random string of given length
function randString(len) {
    return randomBytes(Math.ceil(len / 2)).toString('hex').slice(0, len);
}

// Circular buffer for latency tracking to prevent memory leaks
class CircularBuffer {
    constructor(size = 1000) {
        this.buffer = new Array(size);
        this.size = size;
        this.index = 0;
        this.count = 0;
    }
    
    push(value) {
        this.buffer[this.index] = value;
        this.index = (this.index + 1) % this.size;
        if (this.count < this.size) this.count++;
    }
    
    getAverage() {
        if (this.count === 0) return 0;
        const sum = this.buffer.slice(0, this.count).reduce((a, b) => a + b, 0);
        return sum / this.count;
    }
    
    getPercentile(p) {
        if (this.count === 0) return 0;
        const sorted = this.buffer.slice(0, this.count).sort((a, b) => a - b);
        const index = Math.ceil((p / 100) * sorted.length) - 1;
        return sorted[Math.max(0, index)];
    }
    
    clear() {
        this.index = 0;
        this.count = 0;
    }
}

// ----- Main Worker Functionality ----
async function runWorker(workerCfg) {
    const workerId = threadId || Math.floor(Math.random() * 100000);
    
    // Create larger, more efficient connection pool
    const factory = {
        create: () => {
            return new Redis({
                host: workerCfg.redisHost,
                port: workerCfg.redisPort,
                retryStrategy: times => Math.min(times * 50, 2000),
                lazyConnect: true,
                maxRetriesPerRequest: 3,
                connectTimeout: 60000,
                commandTimeout: 5000,
                // Optimize for high throughput
                enableReadyCheck: false,
                maxLoadingTimeout: 0,
            });
        },
        destroy: client => client.quit(),
    };
    
    const pool = genericPool.createPool(factory, {
        max: 25, // Increased pool size
        min: 10,
        acquireTimeoutMillis: 30000,
        createTimeoutMillis: 30000,
        destroyTimeoutMillis: 5000,
        idleTimeoutMillis: 300000,
    });

    // Statistics tracking
    let totalRequests = 0;
    let totalErrors = 0;
    let totalSet = 0, totalGet = 0;
    let consecutiveErrors = 0;
    const latencies = new CircularBuffer(1000);
    let finished = false;
    let batchId = 0;
    const startTime = performance.now();
    
    // Pre-populate some keys to ensure GETs have data
    const keyPool = [];
    for (let i = 0; i < workerCfg.keyRange; i++) {
        keyPool.push(`${workerCfg.keyPrefix}:${workerId}:${i}`);
    }

    setTimeout(() => { finished = true; }, workerCfg.durationSeconds * 1000);

    // Initial key population
    async function populateKeys() {
        console.log(`Worker ${workerId}: Populating initial keys...`);
        const client = await pool.acquire();
        try {
            const pipeline = client.pipeline();
            for (let i = 0; i < Math.min(100, workerCfg.keyRange); i++) {
                const key = keyPool[i];
                const value = randString(randInt(workerCfg.valueSizeRange));
                pipeline.set(key, value, 'EX', 3600); // 1 hour TTL
            }
            await pipeline.exec();
        } catch (err) {
            console.error(`Worker ${workerId}: Error populating keys:`, err.message);
        } finally {
            pool.release(client);
        }
    }

    async function doBatch() {
        if (consecutiveErrors > 10) {
            await new Promise(resolve => setTimeout(resolve, 100)); // Brief pause on errors
            consecutiveErrors = 0;
        }
        
        const client = await pool.acquire();
        try {
            const pipeline = client.pipeline();
            const pipelineSize = randInt(workerCfg.pipelineRange);
            const operations = [];
            
            // Mix of SET and GET operations based on percentage
            for (let i = 0; i < pipelineSize; i++) {
                const key = keyPool[Math.floor(Math.random() * keyPool.length)];
                const isSet = Math.random() * 100 < workerCfg.setPercentage;
                
                if (isSet) {
                    const value = randString(randInt(workerCfg.valueSizeRange));
                    pipeline.set(key, value, 'EX', 3600);
                    operations.push('SET');
                    totalSet++;
                } else {
                    pipeline.get(key);
                    operations.push('GET');
                    totalGet++;
                }
            }
            
            const t0 = performance.now();
            const results = await pipeline.exec();
            const t1 = performance.now();
            
            // Check for errors in pipeline results
            let hasError = false;
            if (results) {
                for (const [err] of results) {
                    if (err) {
                        hasError = true;
                        break;
                    }
                }
            }
            
            if (hasError) {
                totalErrors++;
                consecutiveErrors++;
            } else {
                consecutiveErrors = 0;
                latencies.push(t1 - t0);
                totalRequests += pipelineSize;
            }
            
            batchId++;
        } catch (err) {
            totalErrors++;
            consecutiveErrors++;
        } finally {
            pool.release(client);
        }
    }

    // Run multiple batch loops in parallel with staggered starts
    async function mainLoop() {
        await populateKeys();
        
        const concurrency = 15; // Increased concurrency
        const workers = [];
        
        for (let i = 0; i < concurrency; i++) {
            workers.push((async () => {
                // Stagger worker starts to reduce initial contention
                await new Promise(resolve => setTimeout(resolve, i * 10));
                
                while (!finished) {
                    try {
                        await doBatch();
                        
                        // Small random delay to prevent thundering herd
                        if (Math.random() < 0.1) {
                            await new Promise(resolve => setTimeout(resolve, Math.random() * 5));
                        }
                    } catch (err) {
                        console.error(`Worker ${workerId}: Batch error:`, err.message);
                        totalErrors++;
                        consecutiveErrors++;
                        
                        // Exponential backoff on errors
                        const backoffMs = Math.min(1000, 50 * Math.pow(2, Math.min(consecutiveErrors, 5)));
                        await new Promise(resolve => setTimeout(resolve, backoffMs));
                    }
                }
            })());
        }
        
        await Promise.all(workers);
        
        // Cleanup
        await pool.drain();
        await pool.clear();
        
        parentPort.postMessage({ 
            totalRequests, 
            totalErrors, 
            totalSet, 
            totalGet, 
            avgLatency: latencies.getAverage(),
            p95Latency: latencies.getPercentile(95),
            p99Latency: latencies.getPercentile(99)
        });
    }
    
    mainLoop().catch(err => {
        console.error(`Worker ${workerId}: Fatal error:`, err);
        process.exit(1);
    });

    // Logging with memory management
    setInterval(() => {
        const elapsed = (performance.now() - startTime) / 1000;
        const avgLatency = latencies.getAverage().toFixed(2);
        const p95Latency = latencies.getPercentile(95).toFixed(2);
        const rps = (totalRequests / elapsed).toFixed(0);
        
        parentPort.postMessage({
            type: 'log',
            stats: { 
                elapsed, 
                totalRequests, 
                totalSet, 
                totalGet, 
                totalErrors, 
                avgLatency, 
                p95Latency,
                rps,
                consecutiveErrors
            },
        });
        
        // Periodically clear latency buffer to prevent memory growth
        if (Math.random() < 0.1) {
            latencies.clear();
        }
    }, workerCfg.logInterval);
}

if (isMainThread) {
    const stats = [];
    let doneWorkers = 0;
    
    console.log(`Starting sustained benchmark with ${config.numConnections} workers for ${config.durationSeconds}s...`);
    console.log(`SET/GET ratio: ${config.setPercentage}%/${100-config.setPercentage}%`);
    console.log(`Key range: ${config.keyRange} unique keys`);
    console.log(`Pipeline size: ${config.pipelineRange[0]}-${config.pipelineRange[1]} operations`);
    
    for (let i = 0; i < config.numConnections; ++i) {
        const worker = new Worker(__filename, { workerData: config });
        
        worker.on('message', msg => {
            if (msg.type === 'log') {
                const s = msg.stats;
                console.log(`Worker ${i}: ${s.elapsed.toFixed(1)}s | RPS: ${s.rps} | Reqs: ${s.totalRequests} | SET: ${s.totalSet} | GET: ${s.totalGet} | Errors: ${s.totalErrors} | Avg: ${s.avgLatency}ms | P95: ${s.p95Latency}ms`);
            } else {
                stats.push(msg);
                doneWorkers++;
                if (doneWorkers === config.numConnections) {
                    const totalReq = stats.reduce((a, b) => a + b.totalRequests, 0);
                    const totalErr = stats.reduce((a, b) => a + b.totalErrors, 0);
                    const totalSet = stats.reduce((a, b) => a + b.totalSet, 0);
                    const totalGet = stats.reduce((a, b) => a + b.totalGet, 0);
                    const avgLat = (stats.reduce((a, b) => a + b.avgLatency, 0) / stats.length).toFixed(2);
                    const avgP95 = (stats.reduce((a, b) => a + b.p95Latency, 0) / stats.length).toFixed(2);
                    const avgP99 = (stats.reduce((a, b) => a + b.p99Latency, 0) / stats.length).toFixed(2);
                    const totalRPS = (totalReq / config.durationSeconds).toFixed(0);
                    
                    console.log(`\n===== Sustained Benchmark Complete =====`);
                    console.log(`Total Requests: ${totalReq.toLocaleString()}`);
                    console.log(`Total RPS: ${totalRPS}`);
                    console.log(`SET: ${totalSet.toLocaleString()}  GET: ${totalGet.toLocaleString()}`);
                    console.log(`Errors: ${totalErr.toLocaleString()} (${((totalErr/totalReq)*100).toFixed(2)}%)`);
                    console.log(`Avg Latency: ${avgLat}ms`);
                    console.log(`P95 Latency: ${avgP95}ms`);
                    console.log(`P99 Latency: ${avgP99}ms`);
                    process.exit(0);
                }
            }
        });
        
        worker.on('error', err => {
            console.error(`Worker ${i} error:`, err);
        });
        
        worker.on('exit', code => {
            if (code !== 0) console.error(`Worker ${i} stopped with exit code ${code}`);
        });
    }
} else {
    runWorker(workerData);
}
