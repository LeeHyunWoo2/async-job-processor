import assert from 'node:assert/strict';
import { execFile, spawn, type ChildProcess } from 'node:child_process';
import { closeSync, mkdtempSync, openSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';

import { MikroORM } from '@mikro-orm/postgresql';

import config from '../../src/db/mikro-orm.config.ts';
import { waitFor } from './job-process.ts';
import type { LogEntry } from './observability.ts';

const execute = promisify(execFile);
const project = 'async-job-worker-safety';
const composeArgs = ['compose', '-p', project, '-f', 'compose.yml', '-f', 'tests/compose/worker-safety.yml'];

export interface Worker {
    containerId: string;
    workerId: string;
    instance: string;
    logPath: string;
}

export interface JobRow {
    id: string;
    status: string;
    attempt: number;
    workerId: string | null;
    processingStartedAt: string | null;
    updatedAt: string;
}

interface Target {
    labels: { job: string; instance: string };
    health: string;
    lastScrape: string;
}

export async function withComposeSafety(
    signal: AbortSignal,
    run: (environment: Awaited<ReturnType<typeof createEnvironment>>) => Promise<void>,
) {
    const environment = await createEnvironment(signal);
    console.log(`검증 자료: ${environment.directory}`);
    // 이미 실행 중인 같은 검증 프로젝트는 중단하거나 변경하지 않는다.
    assert.equal((await environment.compose('ps', '--all', '--quiet')).trim(), '',
        `${project} 컨테이너가 이미 있습니다. 기존 실행 상태를 먼저 확인하세요`);
    try {
        await environment.start();
        await run(environment);
    } catch (error) {
        environment.record('검증 실패', { error: String(error) });
        throw error;
    } finally {
        await environment.close();
    }
}

async function createEnvironment(signal: AbortSignal) {
    const directory = mkdtempSync(join(tmpdir(), 'worker-safety-'));
    const jobs: string[] = [];
    const evidence: { label: string; data: unknown }[] = [];
    const workers = new Map<string, Worker>();
    const followers: ChildProcess[] = [];
    let orm: MikroORM | undefined;
    let apiUrl = '';
    let prometheusUrl = '';

    async function docker(...args: string[]) {
        const { stdout } = await execute('docker', args, { timeout: 60_000, maxBuffer: 10 * 1024 * 1024 });
        return stdout;
    }
    const compose = (...args: string[]) => docker(...composeArgs, ...args);
    function record(label: string, data: unknown) {
        evidence.push({ label, data });
        writeFileSync(join(directory, 'evidence.json'), JSON.stringify({ project, jobs, evidence }, null, 2));
        console.log(label);
    }
    async function until<T>(label: string, read: () => Promise<T | undefined>, timeoutMs = 30_000) {
        try {
            return await waitFor(async () => {
                const value = await read();
                if (value === undefined) await delay(300, undefined, { signal });
                return value;
            }, signal, timeoutMs);
        } catch (error) {
            throw new Error(`${label}: ${String(error)}`, { cause: error });
        }
    }
    async function json(url: string, init?: RequestInit) {
        const response = await fetch(url, { ...init, signal: AbortSignal.any([signal, AbortSignal.timeout(5_000)]) });
        assert.ok(response.ok, `${url}: HTTP ${response.status}`);
        return response.json();
    }
    async function port(service: string, internal: string) {
        return Number((await compose('port', service, internal)).trim().split(':').at(-1));
    }
    function logs(worker: Worker): LogEntry[] {
        return readFileSync(worker.logPath, 'utf8').split('\n').flatMap((line) => {
            try { return [JSON.parse(line) as LogEntry]; } catch { return []; }
        });
    }
    async function activeWorkers(count: number) {
        const ids = (await compose('ps', '--quiet', 'worker')).trim().split('\n').filter(Boolean);
        assert.equal(ids.length, count, '실행 중인 Worker 수가 다릅니다');
        const active: Worker[] = [];
        for (const containerId of ids) {
            const [inspection] = JSON.parse(await docker('inspect', containerId));
            // Compose가 같은 컨테이너를 재시작해도 프로세스 UUID와 Counter는 새 실행이다.
            const key = `${containerId}:${inspection.State.StartedAt}`;
            let worker = workers.get(key);
            if (!worker) {
                const logPath = join(directory, `${containerId.slice(0, 12)}-${workers.size}.jsonl`);
                const fd = openSync(logPath, 'w');
                const follower = spawn('docker', ['logs', '--follow', '--since', inspection.State.StartedAt, containerId], { stdio: ['ignore', fd, fd] });
                closeSync(fd);
                follower.on('error', (error) => record('로그 수집 오류', { containerId, error: String(error) }));
                followers.push(follower);
                const started = await until('Worker 시작 로그', async () => {
                    const entry = logs({ logPath } as Worker).find((log) => log.event === 'worker.started');
                    return entry?.workerId;
                });
                const network = Object.values(inspection.NetworkSettings.Networks)[0] as { IPAddress: string };
                worker = { containerId, workerId: started, instance: `${network.IPAddress}:9464`, logPath };
                workers.set(key, worker);
            }
            active.push(worker);
        }
        return active;
    }
    async function rows(ids: string[]): Promise<JobRow[]> {
        assert.ok(orm);
        if (!ids.length) return [];
        const found = await orm.em.getConnection().execute<JobRow[]>(`
            select id, status, attempt, worker_id as "workerId",
                   processing_started_at as "processingStartedAt", updated_at as "updatedAt"
            from jobs where id in (${ids.map(() => '?').join(',')}) order by id
        `, ids);
        assert.equal(found.length, ids.length, '등록한 실험 Job이 누락되었습니다');
        return found;
    }
    async function enqueue(count: number, durationMs: number) {
        const ids: string[] = [];
        for (let i = 0; i < count; i++) {
            const job = await json(`${apiUrl}/jobs`, {
                method: 'POST', headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ type: 'slow', payload: { durationMs } }),
            }) as { id: string };
            jobs.push(job.id);
            ids.push(job.id);
        }
        record('실험 Job 등록', { ids, durationMs });
        return ids;
    }
    async function succeeded(ids: string[], timeoutMs = 30_000) {
        return until('실험 Job 전체 성공', async () => {
            const found = await rows(ids);
            assert.ok(found.every((row) => row.status !== 'failed'), '실험 Job이 실패했습니다');
            return found.every((row) => row.status === 'succeeded') ? found : undefined;
        }, timeoutMs);
    }
    async function targets(active: Worker[], label: string) {
        const snapshot = await until('Prometheus 개별 Target과 성공 Counter 수집', async () => {
            const { data } = await json(`${prometheusUrl}/api/v1/targets?state=active`) as {
                data: { activeTargets: Target[] };
            };
            const workerTargets = data.activeTargets.filter((target) => target.labels.job === 'worker');
            if (workerTargets.length !== active.length || workerTargets.some((target) => target.health !== 'up')
                || !active.every((worker) => workerTargets.some((target) => target.labels.instance === worker.instance))
                || !data.activeTargets.some((target) => target.labels.job === 'api' && target.health === 'up')) return;
            const { data: counters } = await json(`${prometheusUrl}/api/v1/query?query=${encodeURIComponent('async_job_succeeded_total{job="worker"}')}`) as {
                data: { result: { metric: { instance: string }; value: [number, string] }[] };
            };
            const completed = (await rows(jobs)).filter((row) => row.status === 'succeeded');
            if (!active.every((worker) => {
                const owned = completed.filter((row) => row.workerId === worker.workerId);
                const expected = owned.length;
                const sample = counters.result.find((entry) => entry.metric.instance === worker.instance);
                const target = workerTargets.find((entry) => entry.labels.instance === worker.instance)!;
                // 같은 IP를 재사용한 프로세스의 과거 Sample로 통과하지 않도록 완료 이후 Scrape를 기다린다.
                return owned.every((row) => new Date(target.lastScrape).getTime() >= new Date(row.updatedAt).getTime())
                    && sample && Number(sample.value[1]) >= expected;
            })) return;
            return { targets: data.activeTargets, counters: counters.result, workers: active };
        }, 90_000);
        record(label, snapshot);
    }
    return {
        directory, compose, docker, record, until, logs, activeWorkers, rows, enqueue, succeeded, targets,
        async scale(count: number) {
            await compose('up', '-d', '--no-build', '--no-deps', '--scale', `worker=${count}`, 'worker');
            return activeWorkers(count);
        },
        async start() {
            await compose('up', '-d', '--no-build', '--scale', 'worker=1');
            apiUrl = `http://127.0.0.1:${await port('api', '3000')}`;
            prometheusUrl = `http://127.0.0.1:${await port('prometheus', '9090')}`;
            orm = await MikroORM.init({
                ...config, host: '127.0.0.1', port: await port('postgres', '5432'), contextName: project,
                driverOptions: { connectionTimeoutMillis: 5_000, options: '-c statement_timeout=5000' },
            });
            const [existing] = await orm.em.getConnection().execute<{ count: number }[]>('select count(*)::int as count from jobs');
            assert.equal(existing?.count, 0, '검증 전용 DB에 기존 Job이 있어 실행을 중단합니다. evidence.json의 Job ID로 이전 실행을 확인하세요');
            await until('API 준비', async () => {
                try { return (await json(`${apiUrl}/health`)).status === 'ok' ? true : undefined; }
                catch { signal.throwIfAborted(); return undefined; }
            });
            await until('Prometheus 준비', async () => {
                try {
                    const response = await fetch(`${prometheusUrl}/-/ready`, {
                        signal: AbortSignal.any([signal, AbortSignal.timeout(5_000)]),
                    });
                    await response.text();
                    return response.ok ? true : undefined;
                } catch { signal.throwIfAborted(); return undefined; }
            });
            record('격리 환경 준비', { apiUrl, prometheusUrl });
        },
        async close() {
            // 실패한 실행도 Worker를 먼저 멈춰서 정리 중인 Job을 재Claim하지 못하게 한다.
            try {
                await compose('stop', 'worker');
                writeFileSync(join(directory, 'services.log'), await compose('logs', '--no-color', '--no-log-prefix'));
                if (orm) {
                    record('정리 직전 DB 상태', await rows(jobs));
                    if (jobs.length) await orm.em.getConnection().execute(`delete from jobs where id in (${jobs.map(() => '?').join(',')})`, jobs);
                }
            } finally {
                try { if (orm) await orm.close(); }
                finally {
                    try { await compose('down'); }
                    finally {
                        for (const follower of followers) follower.kill('SIGTERM');
                    }
                }
            }
            record('정리 완료', { deletedJobIds: jobs, volumes: '검증 프로젝트의 Named Volume은 보존했습니다' });
        },
    };
}
