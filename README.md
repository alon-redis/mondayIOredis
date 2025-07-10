install the following:
```
ulimit -n 1000000
curl -fsSL https://deb.nodesource.com/setup_18.x | bash -
apt-get install -y nodejs
npm install ioredis@5.3.2 generic-pool minimist @supercharge/promise-pool yargs redlock@4.2.0

```

Monday outage 100% DMC:
```
npm install ioredis commander@6
npm install generic-pool ioredis-conn-pool
npm install --save redlock
sudo sysctl -w net.ipv4.tcp_fin_timeout=10
sudo sysctl -w net.ipv4.tcp_tw_reuse=1
```
