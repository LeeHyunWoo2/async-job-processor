import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

import { env } from '../../src/config/env.ts';
import { resolvedJobsSchemaSql } from './job-test-database.ts';

export async function waitFor<T>(
    read: () => Promise<T | undefined>,
    signal: AbortSignal,
    timeoutMs = 12_000,
): Promise<T> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        signal.throwIfAborted();
        const result = await read();
        if (result !== undefined) return result;
        await delay(40, undefined, { signal });
    }
    throw new Error('검증할 상태를 제한 시간 안에 확인하지 못했습니다');
}

type JobProcessEntry = 'api/server.ts' | 'worker/main.ts' | 'crash-worker' | 'co-located';
type CrashPhase = 'after-claim' | 'in-handler';

function startJobProcess(entry: JobProcessEntry, schema: string, signal: AbortSignal, crashPhase?: CrashPhase) {
    signal.throwIfAborted();
    const entryUrl = entry === 'crash-worker'
        ? new URL('./crash-worker.ts', import.meta.url)
        : entry === 'co-located'
            ? new URL('../../experiments/cpu-event-loop/co-located.ts', import.meta.url)
            : new URL(`../../src/${entry}`, import.meta.url);
    const bootstrap = `
        const { createOrm } = await import(${JSON.stringify(new URL('../../src/db/orm.ts', import.meta.url).href)});
        const orm = await createOrm();
        try {
            const [resolved] = await orm.em.getConnection().execute(${JSON.stringify(resolvedJobsSchemaSql)});
            if (resolved?.schema !== ${JSON.stringify(schema)}) throw new Error('프로세스 테스트 DB 격리 실패');
        } finally { await orm.close(); }
        await import(${JSON.stringify(entryUrl.href)});
    `;
    const child = spawn(process.execPath, ['--input-type=module', '--eval', bootstrap], {
        cwd: fileURLToPath(new URL('../../', import.meta.url)),
        env: {
            ...process.env, DB_HOST: env.db.host, DB_PORT: String(env.db.port), DB_NAME: env.db.name,
            DB_USER: env.db.user, DB_PASSWORD: env.db.password,
            API_HOST: '127.0.0.1', API_PORT: '0',
            WORKER_METRICS_HOST: '127.0.0.1', WORKER_METRICS_PORT: '0',
            PGOPTIONS: `-c search_path=${schema} -c statement_timeout=5000`,
            ...(crashPhase ? { TEST_CRASH_PHASE: crashPhase } : {}),
        },
        stdio: ['ignore', 'pipe', 'pipe'],
    });
    let output = '';
    child.stdout.on('data', (chunk) => { output += chunk; });
    child.stderr.on('data', (chunk) => { output += chunk; });
    let spawnError: Error | undefined;
    child.once('error', (error) => { spawnError = error; });
    const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
        child.once('close', (code, exitSignal) => resolve({ code, signal: exitSignal }));
    });
    const terminate = () => { child.kill('SIGKILL'); };
    signal.addEventListener('abort', terminate, { once: true });
    return {
        child, exited, output: () => output,
        ready: (pattern: RegExp) => waitFor(async () => {
            if (spawnError) throw spawnError;
            assert.equal(child.exitCode, null, output);
            assert.equal(child.signalCode, null, output);
            return pattern.exec(output) ?? undefined;
        }, signal),
        close: async () => {
            signal.removeEventListener('abort', terminate);
            if (child.exitCode === null && child.signalCode === null) terminate();
            await exited;
        },
    };
}

export async function withJobProcesses(
    schema: string,
    signal: AbortSignal,
    run: (start: (entry: JobProcessEntry, crashPhase?: CrashPhase) => ReturnType<typeof startJobProcess>) => Promise<void>,
): Promise<void> {
    const processes: ReturnType<typeof startJobProcess>[] = [];
    try {
        await run((entry, crashPhase) => {
            const started = startJobProcess(entry, schema, signal, crashPhase);
            processes.push(started);
            return started;
        });
    } finally {
        await Promise.all(processes.map((started) => started.close()));
    }
}
