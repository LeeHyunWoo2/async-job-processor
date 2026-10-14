import type { MikroORM } from '@mikro-orm/postgresql';

import { Job, JobStatus, type JobType, type JsonValue } from '../db/entities/job.ts';
import { getRetryDelayMs } from './retry.ts';

export async function enqueueJob(orm: MikroORM, type: JobType, payload: JsonValue): Promise<{ id: string; status: JobStatus }> {
    // 최초 실행을 포함한 최대 Claim 횟수만 지정하고 초기 상태와 시각은 DB 기본값을 사용한다.
    const [job] = await orm.em.fork().execute<{ id: string; status: JobStatus }[]>(`
        insert into jobs (type, payload, max_attempts)
        values (?, ?::jsonb, 3)
        returning id, status
    `, [type, JSON.stringify(payload)]);

    if (!job) {
        throw new Error('Job 등록 결과가 없습니다');
    }

    return job;
}

export function findJob(orm: MikroORM, id: string): Promise<Job | null> {
    return orm.em.fork().findOne(Job, id);
}

export interface ClaimedJob {
    id: string;
    type: string;
    payload: JsonValue;
    attempt: number;
    maxAttempts: number;
    workerId: string;
    processingStartedAt: Date;
}

export type JobOutcome =
    | { status: 'succeeded'; result: JsonValue }
    | { status: 'failed'; lastError: JsonValue };

export async function claimJob(orm: MikroORM, workerId: string): Promise<ClaimedJob | null> {
    // 기본 READ COMMITTED 트랜잭션을 Claim 반환 전에 종료한다.
    return orm.em.fork().transactional(async (em) => {
        const [job] = await em.execute<(Omit<ClaimedJob, 'processingStartedAt'> & { processingStartedAt: string })[]>(`
            with candidate as (
                select id from jobs
                where status = 'queued'
                  and available_at <= now()
                  and attempt < max_attempts
                order by available_at, created_at, id
                for update skip locked
                limit 1
            )
            update jobs as job
            set status = 'processing',
                attempt = job.attempt + 1,
                worker_id = ?,
                processing_started_at = now(),
                updated_at = now()
            from candidate
            where job.id = candidate.id
            returning job.id, job.type, job.payload, job.attempt,
                      job.max_attempts as "maxAttempts",
                      job.worker_id as "workerId",
                      job.processing_started_at as "processingStartedAt"
        `, [workerId]);

        // raw SQL의 timestamptz는 문자열이며 Entity의 Date 타입으로 변환한다.
        return job ? { ...job, processingStartedAt: new Date(job.processingStartedAt) } : null;
    });
}

export async function scheduleRetry(orm: MikroORM, job: ClaimedJob, lastError: JsonValue): Promise<boolean> {
    const rows = await orm.em.fork().execute<{ id: string }[]>(`
        update jobs
        set status = 'queued',
            available_at = now() + (? * interval '1 millisecond'),
            last_error = ?::jsonb,
            result = null,
            updated_at = now()
        where id = ? and status = 'processing' and worker_id = ? and attempt = ?
        returning id
    `, [getRetryDelayMs(job.attempt), JSON.stringify(lastError), job.id, job.workerId, job.attempt]);

    return rows.length === 1;
}

export async function saveJobOutcome(orm: MikroORM, job: ClaimedJob, outcome: JobOutcome): Promise<boolean> {
    const rows = await orm.em.fork().execute<{ id: string }[]>(`
        update jobs
        set status = ?, result = ?::jsonb, last_error = ?::jsonb, updated_at = now()
        where id = ? and status = ? and worker_id = ? and attempt = ?
        returning id
    `, [
        outcome.status,
        outcome.status === JobStatus.succeeded ? JSON.stringify(outcome.result) : null,
        outcome.status === JobStatus.failed ? JSON.stringify(outcome.lastError) : null,
        job.id,
        JobStatus.processing,
        job.workerId,
        job.attempt,
    ]);

    return rows.length === 1;
}
