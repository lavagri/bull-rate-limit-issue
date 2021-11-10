const http = require('http');
const { RateLimiterRedis } = require('rate-limiter-flexible');
const { createClient } = require('redis');
const Queue = require('bull');
const { delay } = require('../helpers');
const { shuffle } = require('../helpers');

const ID = Math.floor(Math.random() * 100000000000);

const handleJob = async (job) => {
    console.log('Process job', job.id, job.name);

    // await delay(500); // ok
    const delayTime = Math.round(Math.random() * 1000);
    await delay(delayTime); // not ok, sometimes fails
    try {
        await limiter.consume(ID);
    } catch (e) {
        console.error('Error', job.id, e);
        throw e;
    }
    console.log('Process job ended', job.id, job.name);
    return job.id;
};

const queue = new Queue('test-queue-bull-1', {
    limiter: {
        max: 3,
        duration: 3000
    }
});
queue.process(3, async (job) => handleJob(job));

const limiter = new RateLimiterRedis({
    points: 3,
    duration: 3,
    keyPrefix: 'rate-limit:test-bull-1',
    storeClient: createClient({ host: 'localhost', port: 6379 })
});

http.createServer()
    .listen(8125)
    .on('listening', async () => {
        const genArray = (n, mapFn) => new Array(n).fill(null).map(mapFn);
        await Promise.all(
            shuffle([
                ...genArray(12, () => ({ name: 'task-one', data: { f: 1 } })),
                ...genArray(5, () => ({ name: 'task-two', data: { f: 2 } }))
            ]).map((e) => queue.add(e.data))
        );
    });
