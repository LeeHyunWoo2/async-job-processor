import { randomUUID } from 'node:crypto';
import { once } from 'node:events';

import { createApp } from '../../src/api/app.ts';
import { executeHandler } from '../../src/jobs/handlers.ts';
import { recordObservation } from '../../src/observability/best-effort.ts';
import { createEventLoopMetrics } from '../../src/observability/event-loop.ts';
import { createLogger } from '../../src/observability/logger.ts';
import { createMetricsRegistry, createWorkerMetrics } from '../../src/observability/metrics.ts';
import { createWorkerMetricsServer } from '../../src/worker/metrics-server.ts';
import { runWorker } from '../../src/worker/run.ts';

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
        const { env } = await import('../../src/config/env.ts');
        const { createOrm } = await import('../../src/db/orm.ts');
        const apiOrm = await createOrm();
        const app = createApp(apiOrm, createLogger('api'), createMetricsRegistry(), env.api.enqueue);
        try {
            // app.close()의 API ORM 정리가 Worker 연결에 영향을 주지 않도록 별도 연결을 소유한다.
            const workerOrm = await createOrm();
            try {
                const registry = createMetricsRegistry();
                const metrics = createWorkerMetrics(registry);
                const metricsServer = createWorkerMetricsServer(registry, logger);
                // 공유 Event Loop는 한 번만 계측하며 Worker /metrics에서 조회한다.
                const eventLoop = createEventLoopMetrics(registry);
                try {
                    await app.listen({ host: env.api.host, port: env.api.port });
                    metricsServer.listen({ host: env.worker.metricsHost, port: env.worker.metricsPort });
                    await once(metricsServer, 'listening');
                    const address = metricsServer.address();
                    recordObservation(() => logger.info({ event: 'worker.started', metricsPort: typeof address === 'object' ? address?.port : undefined }, 'Worker 시작'));
                    await runWorker(workerOrm, workerId, executeHandler, controller.signal, { logger, metrics });
                } finally {
                    eventLoop.disable();
                    if (metricsServer.listening) {
                        await new Promise<void>((resolve, reject) => {
                            metricsServer.close((error) => error ? reject(error) : resolve());
                        });
                    }
                }
            } finally {
                await workerOrm.close();
            }
        } finally {
            // 현재 Job의 결과 저장과 Worker 중단 이후에 Listener와 API ORM을 정리한다.
            await app.close();
        }
    } finally {
        process.off('SIGINT', onSigint);
        process.off('SIGTERM', onSigterm);
    }
}

main().catch((error) => {
    recordObservation(() => logger.error({ event: 'worker.error', err: error }, '통합 실험 프로세스 실행 실패'));
    process.exitCode = 1;
});
