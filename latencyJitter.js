const Redis = require('ioredis');

async function redisApplication() {
  const connections = [];
  const redisConfig = {
    host: 'redis-17311.c163331.us-east-1-mz.ec2.qa-cloud.rlrcp.com',
    port: 17311,
    password: 'VTaVXlnrYk8',
  };

  while (true) {
    console.log('Opening 10 Redis connections...');
    
    // Open 10 connections
    for (let i = 0; i < 250; i++) {
      const client = new Redis(redisConfig);
      connections.push(client);
    }

    // Perform GET command on each connection
    for (let i = 0; i < connections.length; i++) {
      try {
        const value = await connections[i].get('key'); // Replace 'key' with your desired key
        console.log(`Connection ${i + 1}: Value retrieved - ${value}`);
      } catch (error) {
        console.error(`Error on connection ${i + 1}:`, error);
      }
    }

    console.log('Idling for 30 seconds...');
    await new Promise((resolve) => setTimeout(resolve, 1000)); // Idle for 30 seconds

    console.log('Closing all Redis connections...');
    for (const client of connections) {
      await client.quit(); // Close connection
    }

    // Clear the array for the next iteration
    connections.length = 0;
  }
}

redisApplication();


// memtier_benchmark -h redis-15451.c37735.us-east-1-mz.ec2.cloud.rlrcp.com -p 15451 -a KzVyscZQ65tB5Zbbp8xLvJvTcTX6uMFW --test-time=999999999 --rate-limiting=25 -x 1000
