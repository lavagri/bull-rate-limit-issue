const http = require('http');
const { RateLimiterRedis } = require('rate-limiter-flexible');
const { createClient } = require('redis');
const { Queue, Worker, QueueScheduler } = require('bullmq');
const { delay } = require('../helpers');

const ID = Math.floor(Math.random() * 100000000000);

const handleJob = async (job) => {
    console.log('Process job', job.id, job.name);

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

const queue = new Queue('test-queue-3');
const worker = new Worker('test-queue-3', async (job) => handleJob(job), {
    concurrency: 4, // = limiter.max
    limiter: {
        max: 4,
        duration: 2000
    }
});
const scheduler = new QueueScheduler('test-queue-3');
const limiter = new RateLimiterRedis({
    points: 4,
    duration: 2,
    keyPrefix: 'rate-limit:test-3',
    storeClient: createClient({ host: 'localhost', port: 6379 })
});

http.createServer()
    .listen(8125)
    .on('listening', async () => {
        await Promise.all(
            new Array(20)
                .fill(null)
                .map(() => ({ name: 'task-one', data: { f: 1 } }))
                .map((e) => queue.add(e.name, e.data))
        );
    });
