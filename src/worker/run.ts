import { setTimeout } from 'node:timers/promises';
import { performance } from 'node:perf_hooks';

import type { MikroORM } from '@mikro-orm/postgresql';
import type { Logger } from 'pino';

import type { JsonValue } from '../db/entities/job.ts';
import { RetryableJobError } from '../jobs/retry.ts';
import { claimJob, recoverStaleJobs, saveJobOutcome, scheduleRetry, type ClaimedJob, type JobOutcome } from '../jobs/store.ts';
import { recordObservation } from '../observability/best-effort.ts';
import type { createWorkerMetrics } from '../observability/metrics.ts';

export type JobHandler = (job: ClaimedJob) => Promise<JsonValue>;

export interface WorkerObservability {
    logger?: Logger;
    metrics?: ReturnType<typeof createWorkerMetrics>;
}

export async function processJob(orm: MikroORM, job: ClaimedJob, handler: JobHandler, observability: WorkerObservability = {}): Promise<void> {
    const { logger, metrics } = observability;
    let outcome: JobOutcome | { status: 'queued'; lastError: JsonValue };
    let handlerError: unknown;

    try {
        const startedAt = performance.now();
        let result: JsonValue;
        try {
            result = await handler(job);
        } finally {
            const duration = (performance.now() - startedAt) / 1_000;
            recordObservation(() => metrics?.handlerDuration.observe(duration));
        }
        outcome = { status: 'succeeded', result };
    } catch (error) {
        handlerError = error;
        outcome = {
            status: error instanceof RetryableJobError && job.attempt < job.maxAttempts ? 'queued' : 'failed',
            lastError: {
                name: error instanceof Error ? error.name : 'Error',
                message: error instanceof Error ? error.message : String(error),
            },
        };
    }

    const fields = { jobId: job.id, workerId: job.workerId, attempt: job.attempt, jobType: job.type };
    if (outcome.status === 'queued') {
        if (!await scheduleRetry(orm, job, outcome.lastError)) {
            throw new Error(`Job 재시도를 예약할 수 없습니다: 실행 상태가 변경되었습니다 (${job.id})`);
        }
        recordObservation(() => metrics?.retryScheduled.inc());
        recordObservation(() => logger?.warn({
            ...fields, event: 'job.retry_scheduled', status: 'queued',
            err: handlerError instanceof Error ? handlerError : new Error(String(handlerError)),
        }, 'Job 재시도 예약 완료'));
        return;
    }

    // DB 오류를 Handler 실패로 기록하지 않는다. 저장 실패나 실행 소유권 상실 시 Worker를 중단한다.
    if (!await saveJobOutcome(orm, job, outcome)) {
        throw new Error(`Job 결과를 저장할 수 없습니다: 실행 상태가 변경되었습니다 (${job.id})`);
    }
    if (outcome.status === 'succeeded') {
        recordObservation(() => metrics?.succeeded.inc());
        recordObservation(() => logger?.info({ ...fields, event: 'job.succeeded', status: 'succeeded' }, 'Job 성공 결과 저장 완료'));
    } else {
        recordObservation(() => metrics?.failed.inc());
        recordObservation(() => logger?.warn({
            ...fields, event: 'job.failed', status: 'failed',
            err: handlerError instanceof Error ? handlerError : new Error(String(handlerError)),
        }, 'Job 최종 실패 결과 저장 완료'));
    }
}

export async function runWorker(orm: MikroORM, workerId: string, handler: JobHandler, signal: AbortSignal, observability: WorkerObservability = {}): Promise<void> {
    const { logger, metrics } = observability;
    let nextRecoveryAt = 0;

    while (!signal.aborted) {
        if (Date.now() >= nextRecoveryAt) {
            const recovered = await recoverStaleJobs(orm);
            for (const job of recovered) {
                recordObservation(() => metrics?.recovered.inc());
                if (job.status === 'failed') recordObservation(() => metrics?.failed.inc());
                recordObservation(() => logger?.warn({
                    event: 'job.recovered', jobId: job.jobId, workerId: job.workerId,
                    attempt: job.attempt, jobType: job.jobType, status: job.status, err: job.lastError,
                }, 'Stale Job 복구 완료'));
            }
            nextRecoveryAt = Date.now() + 10_000;
        }
        if (signal.aborted) break;

        const job = await claimJob(orm, workerId);

        if (job) {
            recordObservation(() => metrics?.claimed.inc());
            recordObservation(() => logger?.info({
                event: 'job.claimed', jobId: job.id, workerId: job.workerId,
                attempt: job.attempt, jobType: job.type, status: 'processing',
            }, 'Job Claim 완료'));
            // Claim 중 종료 요청을 받아도 이미 가져온 Job은 완료한다.
            await processJob(orm, job, handler, observability);
        } else if (!signal.aborted) {
            try {
                await setTimeout(1_000, undefined, { signal });
            } catch (error) {
                if (!(error instanceof Error && error.name === 'AbortError' && signal.aborted)) {
                    throw error;
                }
            }
        }
    }
}
