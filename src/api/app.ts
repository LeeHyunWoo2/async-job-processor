import Fastify from 'fastify';
import type { MikroORM } from '@mikro-orm/postgresql';

import { isJobInput } from '../jobs/handlers.ts';
import { enqueueJob, findJob } from '../jobs/store.ts';

export function createApp(orm: MikroORM, logger = true) {
    const app = Fastify({
        logger,
    });

    app.addHook('onClose', async () => {
        await orm.close();
    });

    app.get('/health', async () => {
        return {
            status: 'ok',
        };
    });

    app.post('/jobs', async (request, reply) => {
        if (!isJobInput(request.body)) {
            return reply.code(400).send({ error: '지원하지 않는 Job type 또는 잘못된 payload입니다. type과 payload만 입력해야 합니다' });
        }

        const job = await enqueueJob(orm, request.body.type, request.body.payload);
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
