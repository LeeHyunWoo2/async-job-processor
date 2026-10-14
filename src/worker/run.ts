import { setTimeout } from 'node:timers/promises';

import type { MikroORM } from '@mikro-orm/postgresql';

import type { JsonValue } from '../db/entities/job.ts';
import { RetryableJobError } from '../jobs/retry.ts';
import { claimJob, saveJobOutcome, scheduleRetry, type ClaimedJob, type JobOutcome } from '../jobs/store.ts';

export type JobHandler = (job: ClaimedJob) => Promise<JsonValue>;

export async function processJob(orm: MikroORM, job: ClaimedJob, handler: JobHandler): Promise<void> {
    let outcome: JobOutcome | { status: 'queued'; lastError: JsonValue };

    try {
        outcome = { status: 'succeeded', result: await handler(job) };
    } catch (error) {
        outcome = {
            status: error instanceof RetryableJobError && job.attempt < job.maxAttempts ? 'queued' : 'failed',
            lastError: {
                name: error instanceof Error ? error.name : 'Error',
                message: error instanceof Error ? error.message : String(error),
            },
        };
    }

    if (outcome.status === 'queued') {
        if (!await scheduleRetry(orm, job, outcome.lastError)) {
            throw new Error(`Job 재시도를 예약할 수 없습니다: 실행 상태가 변경되었습니다 (${job.id})`);
        }
        return;
    }

    // DB 오류를 Handler 실패로 기록하지 않는다. 저장 실패나 실행 소유권 상실 시 Worker를 중단한다.
    if (!await saveJobOutcome(orm, job, outcome)) {
        throw new Error(`Job 결과를 저장할 수 없습니다: 실행 상태가 변경되었습니다 (${job.id})`);
    }
}

export async function runWorker(orm: MikroORM, workerId: string, handler: JobHandler, signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
        const job = await claimJob(orm, workerId);

        if (job) {
            // Claim 중 종료 요청을 받아도 이미 가져온 Job은 완료한다.
            await processJob(orm, job, handler);
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
