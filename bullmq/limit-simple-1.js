const http = require('http');
const { RateLimiterRedis } = require('rate-limiter-flexible');
const { createClient } = require('redis');
const { Queue, Worker, QueueScheduler } = require('bullmq');
const { shuffle } = require('../helpers');

const ID = Math.floor(Math.random() * 100000000000);

const handleJob = async (job) => {
    console.log('Process job', job.id, job.name);

    try {
        await limiter.consume(ID);
    } catch (e) {
        console.error('Error', job.id, e);
        throw e;
    }
    console.log('Process job ended', job.id, job.name);
    return job.id;
};

const queue = new Queue('test-queue-1');
const worker = new Worker('test-queue-1', async (job) => handleJob(job), {
    concurrency: 3, // = limiter.max
    limiter: {
        max: 3,
        duration: 5000
    }
});
const scheduler = new QueueScheduler('test-queue-1');
const limiter = new RateLimiterRedis({
    points: 3,
    duration: 5,
    keyPrefix: 'rate-limit:test-1',
    storeClient: createClient({ host: 'localhost', port: 6379 })
});

http.createServer()
    .listen(8125)
    .on('listening', async () => {
        const genArray = (n, mapFn) => new Array(n).fill(null).map(mapFn);
        await Promise.all(
            shuffle([
                ...genArray(8, () => ({ name: 'task-one', data: { f: 1 } })),
                ...genArray(4, () => ({ name: 'task-two', data: { f: 2 } }))
            ]).map((e) => queue.add(e.name, e.data))
        );
    });
