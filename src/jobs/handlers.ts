import { setTimeout } from 'node:timers/promises';
import { performance } from 'node:perf_hooks';

import { jobTypes, type JobType, type JsonValue } from '../db/entities/job.ts';
import { RetryableJobError } from './retry.ts';

interface JobInput {
    type: JobType;
    payload: JsonValue;
}

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isDurationPayload(payload: unknown): boolean {
    return isObject(payload)
        && Object.keys(payload).length === 1
        && typeof payload.durationMs === 'number'
        && Number.isInteger(payload.durationMs)
        && payload.durationMs >= 1
        && payload.durationMs <= 10_000;
}

const handlers: Record<JobType, {
    validate: (payload: unknown) => boolean;
    execute: (payload: JsonValue, attempt: number) => Promise<JsonValue>;
}> = {
    slow: {
        validate: isDurationPayload,
        execute: async (payload) => {
            const { durationMs } = payload as { durationMs: number };
            await setTimeout(durationMs);
            return { durationMs };
        },
    },
    cpu: {
        validate: isDurationPayload,
        execute: async (payload) => {
            const { durationMs } = payload as { durationMs: number };
            const startedAt = performance.now();
            let checksum = 1;
            let iterations = 0;
            do {
                // 현재 JavaScript 스레드에서 계산하고 결과에 사용한다. 중간에 Event Loop로 양보하지 않는다.
                for (let i = 0; i < 1_000; i++) {
                    checksum = (Math.imul(checksum, 1_664_525) + 1_013_904_223) >>> 0;
                }
                iterations += 1_000;
            } while (performance.now() - startedAt < durationMs);
            return { durationMs, iterations, checksum };
        },
    },
    fail: {
        validate: (payload) => isObject(payload) && Object.keys(payload).length === 0,
        execute: async () => {
            throw new Error('의도적인 Job 실패');
        },
    },
    flaky: {
        validate: (payload) => isObject(payload) && Object.keys(payload).length === 0,
        execute: async (_payload, attempt) => {
            if (attempt < 3) {
                throw new RetryableJobError('일시적인 Job 실패');
            }
            return { attempt };
        },
    },
};

export function isJobInput(value: unknown): value is JobInput {
    if (!isObject(value) || Object.keys(value).length !== 2
        || !Object.hasOwn(value, 'type') || !Object.hasOwn(value, 'payload')
        || typeof value.type !== 'string' || !(jobTypes as readonly string[]).includes(value.type)) {
        return false;
    }

    return handlers[value.type as JobType].validate(value.payload);
}

export async function executeHandler(job: { type: string; payload: JsonValue; attempt: number }): Promise<JsonValue> {
    const input = { type: job.type, payload: job.payload };
    if (!isJobInput(input)) {
        throw new Error('지원하지 않는 Job type 또는 잘못된 payload입니다');
    }

    return handlers[input.type].execute(input.payload, job.attempt);
}
