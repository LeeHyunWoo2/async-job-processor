import { randomUUID } from 'node:crypto';
import { once } from 'node:events';

import { executeHandler } from '../jobs/handlers.ts';
import { recordObservation } from '../observability/best-effort.ts';
import { createLogger } from '../observability/logger.ts';
import { createEventLoopMetrics } from '../observability/event-loop.ts';
import { createMetricsRegistry, createWorkerMetrics } from '../observability/metrics.ts';
import { createWorkerMetricsServer } from './metrics-server.ts';
import { runWorker } from './run.ts';

const workerId = randomUUID();
const logger = createLogger('worker').child({ workerId });

async function main() {
    const controller = new AbortController();
    const shutdown = (signal: NodeJS.Signals) => {
        if (!controller.signal.aborted) {
            recordObservation(() => logger.info({ event: 'worker.stopping', signal }, 'Worker 종료 요청: 현재 Job 완료 대기'));
            controller.abort();
        }
    };
    const onSigint = () => shutdown('SIGINT');
    const onSigterm = () => shutdown('SIGTERM');
    process.on('SIGINT', onSigint);
    process.on('SIGTERM', onSigterm);

    try {
        const { env } = await import('../config/env.ts');
        const { createOrm } = await import('../db/orm.ts');
        const orm = await createOrm();
        const registry = createMetricsRegistry();
        const metrics = createWorkerMetrics(registry);
        const metricsServer = createWorkerMetricsServer(registry, logger);
        const eventLoop = createEventLoopMetrics(registry);
        try {
            metricsServer.listen({ host: env.worker.metricsHost, port: env.worker.metricsPort });
            await once(metricsServer, 'listening');
            const address = metricsServer.address();
            recordObservation(() => logger.info({ event: 'worker.started', metricsPort: typeof address === 'object' ? address?.port : undefined }, 'Worker 시작'));
            await runWorker(orm, workerId, executeHandler, controller.signal, { logger, metrics });
        } finally {
            eventLoop.disable();
            try {
                if (metricsServer.listening) {
                    await new Promise<void>((resolve, reject) => {
                        metricsServer.close((error) => error ? reject(error) : resolve());
                    });
                }
            } finally {
                await orm.close();
            }
        }
    } finally {
        process.off('SIGINT', onSigint);
        process.off('SIGTERM', onSigterm);
    }
}

main().catch((error) => {
    recordObservation(() => logger.error({ event: 'worker.error', err: error }, 'Worker 실행 실패: DB 또는 결과 저장 오류로 중단합니다'));
    process.exitCode = 1;
});
