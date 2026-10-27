function requiredEnv(name: string): string {
    const value = process.env[name];

    if (!value) {
        throw new Error(`필수 환경 변수가 없습니다: ${name}`);
    }

    return value;
}

function booleanEnv(name: string, fallback: boolean): boolean {
    const value = process.env[name];
    if (value === undefined) return fallback;
    if (value === 'true') return true;
    if (value === 'false') return false;
    throw new Error(`환경 변수는 true 또는 false여야 합니다: ${name}`);
}

function positiveIntegerEnv(name: string, fallback: number): number {
    const value = process.env[name];
    if (value === undefined) return fallback;
    const parsed = Number(value);
    if (!/^[0-9]+$/.test(value) || !Number.isSafeInteger(parsed) || parsed <= 0) {
        throw new Error(`환경 변수는 양의 정수여야 합니다: ${name}`);
    }
    return parsed;
}

export const env = {
    api: {
        host: process.env.API_HOST ?? '127.0.0.1',
        port: Number(process.env.API_PORT ?? 3000),
        enqueue: {
            enableBackpressure: booleanEnv('ENABLE_BACKPRESSURE', true),
            maxOutstandingJobs: positiveIntegerEnv('MAX_OUTSTANDING_JOBS', 100),
        },
    },

    worker: {
        metricsHost: process.env.WORKER_METRICS_HOST ?? '127.0.0.1',
        metricsPort: Number(process.env.WORKER_METRICS_PORT ?? 9464),
    },

    db: {
        host: requiredEnv('DB_HOST'),
        port: Number(process.env.DB_PORT ?? 5432),
        name: requiredEnv('DB_NAME'),
        user: requiredEnv('DB_USER'),
        password: requiredEnv('DB_PASSWORD'),
    },
};
