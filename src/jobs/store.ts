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

export interface RecoveredJob {
    jobId: string;
    status: 'queued' | 'failed';
    workerId: string | null;
    attempt: number;
    jobType: string;
    lastError: { name: string; message: string };
}

export async function recoverStaleJobs(orm: MikroORM): Promise<RecoveredJob[]> {
    // Timeout은 실행 결과를 알 수 없다는 뜻이며, Handler 실패나 Worker 사망을 확정하지 않는다.
    // 경합한 UPDATE는 Row Lock을 기다린 뒤 현재 status와 시작 시각 조건을 다시 평가한다.
    return orm.em.fork().execute<RecoveredJob[]>(`
        update jobs
        set status = case when attempt < max_attempts
                         then 'queued'::public.job_status else 'failed'::public.job_status end,
            available_at = case when attempt < max_attempts then now() else available_at end,
            last_error = ?::jsonb,
            updated_at = now()
        where status = 'processing'
          and processing_started_at < now() - interval '30 seconds'
        returning id as "jobId", status, worker_id as "workerId", attempt,
                  type as "jobType", last_error as "lastError"
    `, [JSON.stringify({
        name: 'StaleJobError',
        message: '마지막 Claim 이후 30초가 경과하여 복구했습니다. 이전 실행의 완료 여부와 결과를 확인할 수 없습니다',
    })]);
}

export async function getJobCounts(orm: MikroORM): Promise<{ queueDepth: number; readyQueueDepth: number; processingJobs: number }> {
    const [counts] = await orm.em.fork().execute<{ queueDepth: number; readyQueueDepth: number; processingJobs: number }[]>(`
        select count(*) filter (where status = 'queued')::integer as "queueDepth",
               count(*) filter (where status = 'queued' and available_at <= now() and attempt < max_attempts)::integer as "readyQueueDepth",
               count(*) filter (where status = 'processing')::integer as "processingJobs"
        from jobs
    `);
    if (!counts) throw new Error('Job 집계 결과가 없습니다');
    return counts;
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
