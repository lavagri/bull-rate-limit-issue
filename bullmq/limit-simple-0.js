// Q: Rate limit window refresh when worker grab task or when worker complete task
const { Queue, Worker, QueueScheduler } = require('bullmq');
const { delay } = require('../helpers');

const handleJob = async (job) => {
    console.timeLog('TEST_LBL', 'Process job', job.id, job.name);
    await delay(5000);
    console.timeLog('TEST_LBL', 'Process job ended', job.id, job.name);
    return job.id;
};

const queue = new Queue('test-queue-2');
const worker = new Worker('test-queue-2', async (job) => handleJob(job), {
    concurrency: 1, // = limiter.max
    limiter: {
        max: 1,
        duration: 10000
    }
});
const scheduler = new QueueScheduler('test-queue-2');

require('http')
    .createServer()
    .listen(8125)
    .on('listening', async () => {
        await Promise.all([queue.add('task-one', { f: 1 }), queue.add('task-two', { f: 2 }), queue.add('task-three', { f: 3 })]);
    });
