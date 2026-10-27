import Fastify from 'fastify';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { Logger } from 'pino';

import { isJobInput } from '../jobs/handlers.ts';
import { enqueueJob, findJob, getJobCounts, QueueCapacityExceededError, type EnqueueOptions } from '../jobs/store.ts';
import { recordObservation } from '../observability/best-effort.ts';
import { createLogger } from '../observability/logger.ts';
import { createApiMetrics, createMetricsRegistry } from '../observability/metrics.ts';

export function createApp(
    orm: MikroORM, logger: boolean | Logger = true, registry = createMetricsRegistry(),
    enqueueOptions?: EnqueueOptions,
) {
    const app = Fastify(logger === false
        ? { logger: false }
        : { loggerInstance: logger === true ? createLogger('api') : logger });
    const metrics = createApiMetrics(registry);

    app.addHook('onClose', async () => {
        await orm.close();
    });

    app.get('/health', async () => {
        return {
            status: 'ok',
        };
    });

    app.get('/metrics', async (_request, reply) => {
        // Scrape마다 한 SQL로 조회하며 실패 시 이전 값이나 0을 응답하지 않는다.
        const counts = await getJobCounts(orm);
        metrics.queueDepth.set(counts.queueDepth);
        metrics.readyQueueDepth.set(counts.readyQueueDepth);
        metrics.processingJobs.set(counts.processingJobs);
        return reply.type(registry.contentType).send(await registry.metrics());
    });

    app.post('/jobs', async (request, reply) => {
        if (!isJobInput(request.body)) {
            return reply.code(400).send({ error: '지원하지 않는 Job type 또는 잘못된 payload입니다. type과 payload만 입력해야 합니다' });
        }

        const { type, payload } = request.body;
        let job;
        try {
            job = await enqueueJob(orm, type, payload, enqueueOptions);
        } catch (error) {
            if (!(error instanceof QueueCapacityExceededError)) throw error;
            recordObservation(() => metrics.enqueueRejected.inc());
            recordObservation(() => request.log.warn({
                event: 'job.enqueue_rejected', reason: 'QUEUE_CAPACITY_EXCEEDED',
                outstandingJobs: error.outstandingJobs, maxOutstandingJobs: error.maxOutstandingJobs,
            }, error.message));
            return reply.code(503).send({ error: error.message, code: 'QUEUE_CAPACITY_EXCEEDED' });
        }
        recordObservation(() => metrics.enqueued.inc());
        recordObservation(() => request.log.info({
            event: 'job.enqueued', jobId: job.id, jobType: type, status: job.status,
        }, 'Job 등록 완료'));
        return reply.code(201).send(job);
    });

    app.get<{ Params: { id: string } }>('/jobs/:id', async (request, reply) => {
        if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(request.params.id)) {
            return reply.code(400).send({ error: 'Job ID는 UUID 형식이어야 합니다' });
        }

        const job = await findJob(orm, request.params.id);
        if (!job) {
            return reply.code(404).send({ error: 'Job을 찾을 수 없습니다' });
        }

        return job;
    });

    return app;
}
