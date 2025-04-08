const Redis = require('ioredis');

/**
 * Redis application demonstrating connection management with cyclic command patterns
 * Implements a deterministic command multiplexing strategy based on connection indices:
 * - SET commands execute on every 5th connection (mod 5 === 0)
 * - SCRIPT EXISTS commands execute on every 7th connection (mod 7 === 0)
 * - Default GET commands execute on all other connections
 */
async function redisApplication() {
  const connections = [];
  const redisConfig = {
    host: 'REDIS-HOST',
    port: PORT,
    password: 'PASSWORD',
    retryStrategy: (times) => {
      // Exponential backoff with max delay of 5000ms
      return Math.min(times * 100, 5000);
    }
  };
  
  // Example SHA for SCRIPT EXISTS command
  const exampleSHA = '0123456789abcdef0123456789abcdef01234567';

  while (true) {
    console.log('Opening 20 Redis connections...');
    
    // Open 20 connections
    for (let i = 0; i < 21; i++) {
      const client = new Redis(redisConfig);
      connections.push(client);
    }
    
    // Execute commands based on connection index patterns
    for (let i = 0; i < connections.length; i++) {
      try {
        let result;
        const connectionIndex = i + 1; // 1-indexed for logging clarity
        
        // Command multiplexing logic
        if (connectionIndex % 5 === 0) {
          // Every 5th connection executes SET
          result = await connections[i].set('cyclic-key', `value-from-conn-${connectionIndex}`);
          console.log(`Connection ${connectionIndex}: SET command executed - Result: ${result}`);
        } else if (connectionIndex % 7 === 0) {
          // Every 7th connection executes SCRIPT EXISTS
          result = await connections[i].script('exists', exampleSHA);
          console.log(`Connection ${connectionIndex}: SCRIPT EXISTS command executed - Response: ${result}`);
        } else {
          // All other connections execute GET
          result = await connections[i].get('cyclic-key');
          console.log(`Connection ${connectionIndex}: GET command executed - Value retrieved: ${result}`);
        }
      } catch (error) {
        console.error(`Error on connection ${i + 1}:`, error.message);
      }
    }
    
    console.log('Idling for 7 seconds...');
    await new Promise((resolve) => setTimeout(resolve, 7000)); // Idle for 7 seconds
    
    console.log('Closing all Redis connections...');
    // Properly close connections with Promise.all for parallel execution
    await Promise.all(
      connections.map(async (client, index) => {
        try {
          await client.quit(); // Graceful connection closure
          console.log(`Connection ${index + 1} closed successfully`);
        } catch (err) {
          console.error(`Failed to close connection ${index + 1}:`, err.message);
        }
      })
    );
    
    // Clear the array for the next iteration
    connections.length = 0;
  }
}

redisApplication().catch(err => {
  console.error('Fatal application error:', err);
  process.exit(1);
});
