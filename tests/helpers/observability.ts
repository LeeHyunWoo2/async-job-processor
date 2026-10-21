import assert from 'node:assert/strict';

import type { Registry } from '@prometheus-io/client';

import { createLogger } from '../../src/observability/logger.ts';
import { createMetricsRegistry, createWorkerMetrics } from '../../src/observability/metrics.ts';

export interface LogEntry {
    event?: string;
    level: number;
    component: string;
    jobId?: string;
    workerId?: string;
    attempt?: number;
    jobType?: string;
    status?: string;
    reqId?: string;
    err?: { type?: string; name?: string; message: string; stack?: string };
}

export function captureWorkerObservability(workerId: string, onLog?: (log: LogEntry) => void) {
    const logs: LogEntry[] = [];
    const logger = createLogger('worker', {
        write(chunk: string) {
            const log = JSON.parse(chunk) as LogEntry;
            logs.push(log);
            onLog?.(log);
        },
    }).child({ workerId });
    const registry = createMetricsRegistry();
    const metrics = createWorkerMetrics(registry);
    return { logger, registry, metrics, logs };
}

export async function metricValue(registry: Registry, name: string): Promise<number> {
    const text = await registry.metrics();
    const sample = text.split('\n').find((line) => line.startsWith(`${name} `));
    assert.ok(sample, `${name} 지표가 없습니다`);
    return Number(sample.slice(name.length + 1));
}
