import { expect } from 'chai';
import { default as IORedis } from 'ioredis';
import { after as afterAll, beforeEach, describe, it } from 'mocha';
import { v4 } from 'uuid';
import { Queue, QueueEvents, Worker } from '../src/classes';
import { removeAllQueueData } from '../src/utils';

describe('Stalled jobs with missing hash (issue #3929)', function () {
  const redisHost = process.env.REDIS_HOST || 'localhost';
  const prefix = process.env.BULLMQ_TEST_PREFIX || 'bull';
  let queue: Queue;
  let queueName: string;
  let connection: IORedis;

  beforeEach(async function () {
    queueName = `test-${v4()}`;
    connection = new IORedis(redisHost, { maxRetriesPerRequest: null });
    queue = new Queue(queueName, { connection, prefix });
  });

  afterAll(async function () {
    await connection.quit();
  });

  it('should handle stalled jobs when job hash is deleted', async function () {
    this.timeout(10000);

    const queueEvents = new QueueEvents(queueName, { connection, prefix });
    await queueEvents.waitUntilReady();

    let workerClosed = false;
    const worker = new Worker(
      queueName,
      async () => {
        // Simulate long-running job
        await new Promise(resolve => setTimeout(resolve, 10000));
      },
      {
        connection,
        prefix,
        autorun: false,
        stalledInterval: 100,
        maxStalledCount: 0,
      }
    );

    const job = await queue.add('test', { foo: 'bar' });

    // Start processing
    worker.run();

    // Wait for job to become active
    await new Promise(resolve => setTimeout(resolve, 200));

    const isActive = await job.isActive();
    expect(isActive).to.be.true;

    // Force close worker (simulating crash)
    await worker.close(true);
    workerClosed = true;

    // Manually delete job hash (simulating aggressive retention cleanup)
    const jobKey = queue.toKey(job.id!);
    await connection.del(jobKey);

    // Verify job hash is gone
    const jobExists = await connection.exists(jobKey);
    expect(jobExists).to.equal(0);

    // Create new worker to trigger stall detection
    const worker2 = new Worker(
      queueName,
      async () => ({ success: true }),
      {
        connection,
        prefix,
        stalledInterval: 100,
        maxStalledCount: 0,
      }
    );

    // Wait for stall check to run
    const stalledPromise = new Promise<void>(resolve => {
      queueEvents.on('stalled', ({ jobId, missing }) => {
        if (jobId === job.id) {
          // Should emit stalled event with missing flag
          expect(missing).to.equal('1');
          resolve();
        }
      });
    });

    await stalledPromise;

    // Verify job is removed from active list even though hash was missing
    const activeJobs = await connection.lrange(queue.toKey('active'), 0, -1);
    expect(activeJobs).to.not.include(job.id!);

    await worker2.close();
    await queueEvents.close();
    await removeAllQueueData(connection, queueName);
  });

  it('should not break normal stalled job processing', async function () {
    this.timeout(10000);

    const queueEvents = new QueueEvents(queueName, { connection, prefix });
    await queueEvents.waitUntilReady();

    const worker = new Worker(
      queueName,
      async () => {
        // Simulate work
        await new Promise(resolve => setTimeout(resolve, 10000));
      },
      {
        connection,
        prefix,
        autorun: false,
        stalledInterval: 100,
        maxStalledCount: 1,
      }
    );

    const job = await queue.add('test', { foo: 'bar' });

    worker.run();
    await new Promise(resolve => setTimeout(resolve, 200));

    // Force close without deleting job hash
    await worker.close(true);

    const worker2 = new Worker(
      queueName,
      async () => ({ success: true }),
      {
        connection,
        prefix,
        stalledInterval: 100,
        maxStalledCount: 1,
      }
    );

    const stalledPromise = new Promise<void>(resolve => {
      queueEvents.on('stalled', ({ jobId, missing }) => {
        if (jobId === job.id) {
          // Normal stalled event should not have missing flag
          expect(missing).to.be.undefined;
          resolve();
        }
      });
    });

    await stalledPromise;

    // Job should be moved back to wait
    const jobState = await job.getState();
    expect(jobState).to.be.oneOf(['waiting', 'active', 'completed']);

    await worker2.close();
    await queueEvents.close();
    await removeAllQueueData(connection, queueName);
  });
});
