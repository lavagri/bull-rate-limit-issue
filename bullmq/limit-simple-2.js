// Q: Rate limit bunch of task with static|random delay before limiter consume
const http = require('http');
const { RateLimiterRedis } = require('rate-limiter-flexible');
const { createClient } = require('redis');
const { Queue, Worker, QueueScheduler } = require('bullmq');
const { shuffle, delay } = require('../helpers');

const ID = Math.floor(Math.random() * 100000000000);

const handleJob = async (job) => {
    console.log('Process job', job.id, job.name);

    // Delay will simulate different latency to network.
    // await delay(500); // ok
    const delayTime = Math.round(Math.random() * 1000);
    await delay(delayTime); // not ok, sometimes fails
    try {
        await limiter.consume(ID);
    } catch (e) {
        console.error('Error', job.id, delayTime, e);
        throw e;
    }
    console.log('Process job ended', job.id, delayTime, job.name);
    return job.id;
};

const queue = new Queue('test-queue-2');
const worker = new Worker('test-queue-2', async (job) => handleJob(job), {
    concurrency: 1, // = limiter.max
    limiter: {
        max: 1,
        duration: 1000
    }
});
const scheduler = new QueueScheduler('test-queue-2');
const limiter = new RateLimiterRedis({
    points: 1,
    duration: 1,
    keyPrefix: 'rate-limit:test-2',
    storeClient: createClient({ host: 'localhost', port: 6379 })
});

require('http')
    .createServer()
    .listen(8125)
    .on('listening', async () => {
        const genArray = (n, mapFn) => new Array(n).fill(null).map(mapFn);
        await Promise.all(
            shuffle([
                ...genArray(20, () => ({ name: 'task-one', data: { f: 1 } }))
                // ...genArray(5, () => ({ name: 'task-two', data: { f: 2 } }))
            ]).map((e) => queue.add(e.name, e.data))
        );
    });
