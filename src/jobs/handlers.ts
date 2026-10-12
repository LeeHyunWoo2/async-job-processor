import { setTimeout } from 'node:timers/promises';

import { jobTypes, type JobType, type JsonValue } from '../db/entities/job.ts';

interface JobInput {
    type: JobType;
    payload: JsonValue;
}

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

const handlers: Record<JobType, {
    validate: (payload: unknown) => boolean;
    execute: (payload: JsonValue) => Promise<JsonValue>;
}> = {
    slow: {
        validate: (payload) => isObject(payload)
            && Object.keys(payload).length === 1
            && typeof payload.durationMs === 'number'
            && Number.isInteger(payload.durationMs)
            && payload.durationMs >= 1
            && payload.durationMs <= 10_000,
        execute: async (payload) => {
            const { durationMs } = payload as { durationMs: number };
            await setTimeout(durationMs);
            return { durationMs };
        },
    },
    fail: {
        validate: (payload) => isObject(payload) && Object.keys(payload).length === 0,
        execute: async () => {
            throw new Error('의도적인 Job 실패');
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

export async function executeHandler(job: { type: string; payload: JsonValue }): Promise<JsonValue> {
    const input = { type: job.type, payload: job.payload };
    if (!isJobInput(input)) {
        throw new Error('지원하지 않는 Job type 또는 잘못된 payload입니다');
    }

    return handlers[input.type].execute(input.payload);
}
