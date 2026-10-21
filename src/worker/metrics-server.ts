import { createServer } from 'node:http';

import type { Registry } from '@prometheus-io/client';
import type { Logger } from 'pino';

import { recordObservation } from '../observability/best-effort.ts';

export function createWorkerMetricsServer(registry: Registry, logger: Logger) {
    return createServer(async (request, response) => {
        if (request.method !== 'GET' || request.url?.split('?')[0] !== '/metrics') {
            response.writeHead(404).end();
            return;
        }

        try {
            const metrics = await registry.metrics();
            response.writeHead(200, { 'Content-Type': registry.contentType }).end(metrics);
        } catch (error) {
            recordObservation(() => logger.error({ err: error }, 'Worker Metrics 조회 실패'));
            response.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' }).end('Metrics 조회 실패');
        }
    });
}
