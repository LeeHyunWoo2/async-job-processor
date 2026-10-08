import { randomUUID } from 'node:crypto';

import { defineEntity, type InferEntity, p } from '@mikro-orm/core';

export const jobTypes = [
    'cpu-heavy',
    'io-bound',
    'slow',
    'always-fail',
    'fail-then-succeed',
] as const;

export type JobType = typeof jobTypes[number];

export const JobStatus = {
    queued: 'queued',
    processing: 'processing',
    succeeded: 'succeeded',
    failed: 'failed',
} as const;

export type JobStatus = typeof JobStatus[keyof typeof JobStatus];

export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

export const Job = defineEntity({
    name: 'Job',
    tableName: 'jobs',
    properties: {
        id: p.uuid().primary().onCreate(() => randomUUID()).defaultRaw('gen_random_uuid()'),
        type: p.text().$type<JobType>(),
        payload: p.json<JsonValue>().columnType('jsonb'),
        status: p.enum(JobStatus).nativeEnumName('job_status').default(JobStatus.queued),
        attempt: p.integer().default(0),
        maxAttempts: p.integer().check('max_attempts >= 1'),
        availableAt: p.datetime().columnType('timestamptz').onCreate(() => new Date()).defaultRaw('now()'),
        processingStartedAt: p.datetime().columnType('timestamptz').nullable(),
        workerId: p.text().nullable(),
        result: p.json<JsonValue>().columnType('jsonb').nullable(),
        lastError: p.json<JsonValue>().columnType('jsonb').nullable(),
        createdAt: p.datetime().columnType('timestamptz').onCreate(() => new Date()).defaultRaw('now()'),
        updatedAt: p.datetime().columnType('timestamptz').onCreate(() => new Date()).onUpdate(() => new Date()).defaultRaw('now()'),
    },
});

export type Job = InferEntity<typeof Job>;
