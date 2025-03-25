install the following:
```
ulimit -n 1000000
curl -fsSL https://deb.nodesource.com/setup_18.x | bash -
apt-get install -y nodejs
npm install ioredis@5.3.2 generic-pool minimist @supercharge/promise-pool yargs redlock@4.2.0
```
