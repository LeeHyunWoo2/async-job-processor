import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { appendFile, mkdir, readFile, writeFile } from 'node:fs/promises';
import { cpus, platform, release, totalmem } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { parseArgs, promisify } from 'node:util';

import { MikroORM } from '@mikro-orm/postgresql';

import config from '../../src/db/mikro-orm.config.ts';
import { batchTiming, spread, writeSummary, type JobRow, type RunResult } from './results.ts';

const { values } = parseArgs({ options: {
    workers: { type: 'string', default: '1,2,4,8' },
    repeats: { type: 'string', default: '2' },
    count: { type: 'string', default: '480' },
    'duration-ms': { type: 'string', default: '1000' },
    'interval-ms': { type: 'string', default: '5000' },
    out: { type: 'string', default: `artifacts/worker-load/${new Date().toISOString().replaceAll(':', '-')}` },
} });
function integer(value: string, name: string, min = 1, max = Number.MAX_SAFE_INTEGER) {
    const parsed = Number(value);
    assert.ok(Number.isSafeInteger(parsed) && parsed >= min && parsed <= max, `${name} 값이 범위를 벗어났습니다`);
    return parsed;
}
const counts = values.workers.split(',').map((value) => integer(value, 'workers'));
assert.equal(new Set(counts).size, counts.length, 'Worker 조건이 중복되었습니다');
assert.ok(counts.includes(1), 'Speedup 계산을 위해 1 Worker 조건이 필요합니다');
const repeats = integer(values.repeats, 'repeats', 1, 3);
const count = integer(values.count, 'count', 1, 50_000);
const durationMs = integer(values['duration-ms'], 'duration-ms', 1, 10_000);
const intervalMs = integer(values['interval-ms'], 'interval-ms', 1_000);
const directory = resolve(values.out);
const project = 'async-job-worker-load';
const composeArgs = ['compose', '-p', project, '-f', 'compose.yml', '-f', 'experiments/worker-load/compose.yml'];
const execute = promisify(execFile);
const controller = new AbortController();
const signal = controller.signal;
const onSignal = () => controller.abort(new Error('종료 요청으로 실험을 중단했습니다'));
process.on('SIGINT', onSignal);
process.on('SIGTERM', onSignal);

async function docker(...args: string[]) {
    return (await execute('docker', args, { timeout: 60_000, maxBuffer: 20 * 1024 ** 2 })).stdout;
}
const compose = (...args: string[]) => docker(...composeArgs, ...args);
const save = (path: string, data: unknown) => writeFile(path, JSON.stringify(data, null, 2) + '\n');
async function json(url: string, init?: RequestInit) {
    const response = await fetch(url, { ...init, signal: AbortSignal.any([signal, AbortSignal.timeout(5_000)]) });
    assert.ok(response.ok, `${url}: HTTP ${response.status}`);
    return response.json();
}
async function until<T>(label: string, read: () => Promise<T | undefined>, timeoutMs = 90_000) {
    const deadline = Date.now() + timeoutMs;
    while (true) {
        signal.throwIfAborted();
        const value = await read();
        if (value !== undefined) return value;
        assert.ok(Date.now() < deadline, `${label}: 대기 시간을 초과했습니다`);
        await delay(1_000, undefined, { signal });
    }
}
async function port(service: string, internal: string) {
    return (await compose('port', service, internal)).trim().split(':').at(-1)!;
}
interface Worker {
    id: string;
    workerId: string;
    instance: string;
    startedAt: string;
    imageId: string;
    limits: Record<string, unknown>;
}
interface Target {
    labels: { job: string; instance: string };
    health: string;
    lastScrape: string;
}
interface Sample {
    startedAt: string;
    finishedAt: string;
    containers?: { id: string; service: string; cpuPercent: number; memoryBytes: number; raw: unknown }[];
    connections?: { state: string; count: number; sampledAt: string }[];
    targets?: Target[];
    error?: string;
}
function memoryBytes(value: string) {
    const match = value.match(/^([\d.]+)\s*(B|kB|KB|KiB|MB|MiB|GB|GiB|TB|TiB)$/);
    assert.ok(match, `메모리 단위를 해석할 수 없습니다: ${value}`);
    const unit = match[2]!;
    const scale: Record<string, number> = { B: 1, kB: 1e3, KB: 1e3, KiB: 1024, MB: 1e6, MiB: 1024 ** 2, GB: 1e9, GiB: 1024 ** 3, TB: 1e12, TiB: 1024 ** 4 };
    return Number(match[1]) * scale[unit]!;
}

let orm: MikroORM | undefined;
let ownsEnvironment = false;
const results: RunResult[] = [];
let apiUrl = '';
let prometheusUrl = '';

async function rows(ids: string[]): Promise<JobRow[]> {
    assert.ok(orm);
    return orm.em.getConnection().execute<JobRow[]>(`
        select id, type, payload, status, attempt, worker_id as "workerId",
               processing_started_at as "processingStartedAt", updated_at as "updatedAt",
               (status = 'queued' and available_at <= now() and attempt < max_attempts) as ready,
               last_error as "lastError", result
        from jobs where id in (${ids.map(() => '?').join(',')}) order by id
    `, ids);
}
async function targets(): Promise<Target[]> {
    return (await json(`${prometheusUrl}/api/v1/targets?state=active`)).data.activeTargets;
}
async function activeWorkers(expected: number): Promise<Worker[]> {
    const ids = (await compose('ps', '--quiet', 'worker')).trim().split('\n').filter(Boolean);
    assert.equal(ids.length, expected, '실행 중인 Worker 수가 다릅니다');
    const workers: Worker[] = [];
    for (const id of ids) {
        const [inspection] = JSON.parse(await docker('inspect', id));
        const workerId = await until('Worker 시작 로그', async () => {
            const logs = (await docker('logs', '--since', inspection.State.StartedAt, id)).split('\n');
            for (const line of logs) {
                try {
                    const event = JSON.parse(line);
                    if (event.event === 'worker.started') return event.workerId as string;
                } catch { /* JSON 이외의 Docker 출력은 시작 근거로 사용하지 않는다. */ }
            }
        });
        const [network] = Object.values(inspection.NetworkSettings.Networks) as { IPAddress: string }[];
        workers.push({ id, workerId, instance: `${network!.IPAddress}:9464`, startedAt: inspection.State.StartedAt,
            imageId: inspection.Image, limits: Object.fromEntries(['NanoCpus', 'CpuQuota', 'CpuPeriod', 'CpusetCpus', 'Memory', 'MemorySwap']
                .map((key) => [key, inspection.HostConfig[key]])) });
    }
    return workers;
}
async function readyTargets(workers: Worker[], after = 0) {
    return until('Worker 수와 Prometheus Target 일치·Scrape', async () => {
        const found = await targets();
        const workerTargets = found.filter((target) => target.labels.job === 'worker');
        if (workerTargets.length !== workers.length || !workers.every((worker) => workerTargets.some((target) =>
            target.labels.instance === worker.instance && target.health === 'up'
            && Date.parse(target.lastScrape) >= Math.max(after, Date.parse(worker.startedAt))))
            || !found.some((target) => target.labels.job === 'api' && target.health === 'up' && Date.parse(target.lastScrape) >= after)) return;
        return found;
    });
}

const queries = {
    succeeded: 'async_job_succeeded_total{job="worker"}',
    succeededRate: 'rate(async_job_succeeded_total{job="worker"}[2m])',
    totalSucceededRate: 'sum(rate(async_job_succeeded_total{job="worker"}[2m]))',
    claimed: 'async_job_claimed_total{job="worker"}',
    failed: 'async_job_failed_total{job="worker"}',
    retried: 'async_job_retry_scheduled_total{job="worker"}',
    recovered: 'async_job_recovered_total{job="worker"}',
    handlerCount: 'async_job_handler_duration_seconds_count{job="worker"}',
    handlerSum: 'async_job_handler_duration_seconds_sum{job="worker"}',
    handlerBuckets: 'async_job_handler_duration_seconds_bucket{job="worker"}',
    handlerMean: 'sum(rate(async_job_handler_duration_seconds_sum{job="worker"}[2m])) / sum(rate(async_job_handler_duration_seconds_count{job="worker"}[2m]))',
    queue: 'async_job_queue_depth{job="api"}',
    readyQueue: 'async_job_ready_queue_depth{job="api"}',
    processing: 'async_job_processing_jobs{job="api"}',
    up: 'up{job=~"api|worker"}',
};

async function runBatch(workersCount: number, repeat: number) {
    assert.ok(orm);
    const name = `r${repeat}-w${workersCount}`;
    const runDirectory = join(directory, name);
    await mkdir(runDirectory);
    const ids: string[] = [];
    const result: RunResult = { name, workers: workersCount, repeat, count, durationMs, valid: false };
    results.push(result);
    const samples: Sample[] = [];
    const manifest = { name, workers: workersCount, repeat, count, durationMs, ids, createdAt: new Date().toISOString() };
    await save(join(runDirectory, 'batch.json'), manifest);
    let workers: Worker[] = [];
    try {
        assert.equal((await compose('ps', '--quiet', 'worker')).trim(), '', '사전 등록 중 Worker가 실행 중입니다');
        for (let index = 0; index < count; index++) {
            signal.throwIfAborted();
            const job = await json(`${apiUrl}/jobs`, { method: 'POST', headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ type: 'slow', payload: { durationMs } }) });
            assert.ok(typeof job.id === 'string', 'API 응답에 Job ID가 없습니다');
            ids.push(job.id);
            // 등록 도중 중단해도 이미 응답받은 ID는 보존한다.
            await save(join(runDirectory, 'batch.json'), manifest);
        }
        const before = await rows(ids);
        assert.equal(before.length, count);
        assert.equal(new Set(ids).size, count);
        assert.ok(before.every((row) => row.ready && row.attempt === 0 && row.processingStartedAt === null
            && row.workerId === null && row.lastError === null && row.result === null), 'Worker 시작 전 처리되었거나 Ready가 아닌 Job이 있습니다');
        await save(join(runDirectory, 'before.json'), before);

        // 사전 등록한 행만 잠가 Worker·Target 준비 중 SKIP LOCKED가 해당 Batch를 건너뛰게 한다.
        const gateStartedAt = new Date().toISOString();
        let gateReadyAt = '';
        await orm.em.fork().transactional(async (em) => {
            const locked = await em.execute<{ id: string }[]>(`select id from jobs where id in (${ids.map(() => '?').join(',')}) for update`, ids);
            assert.equal(locked.length, count);
            await compose('up', '-d', '--no-build', '--no-deps', '--scale', `worker=${workersCount}`, 'worker');
            workers = await activeWorkers(workersCount);
            result.workerContainers = workers;
            const ready = await readyTargets(workers, Date.parse(gateStartedAt));
            const gatedRows = await rows(ids);
            assert.ok(gatedRows.every((row) => row.ready && row.attempt === 0), 'Target 준비 전에 Batch가 처리되었습니다');
            const queue = await json(`${prometheusUrl}/api/v1/query?query=${encodeURIComponent(queries.readyQueue)}`);
            assert.equal(Number(queue.data.result[0]?.value[1]), count, 'Prometheus Ready Queue가 Batch 수와 다릅니다');
            const counters = await json(`${prometheusUrl}/api/v1/query?query=${encodeURIComponent(queries.claimed)}`);
            assert.ok(workers.every((worker) => counters.data.result.some((entry: { metric: { instance: string }; value: [number, string] }) =>
                entry.metric.instance === worker.instance && Number(entry.value[1]) === 0)), '새 Worker의 Claim Counter가 0이 아닙니다');
            gateReadyAt = new Date().toISOString();
            await save(join(runDirectory, 'gate.json'), { gateStartedAt, readyAt: gateReadyAt, workers, targets: ready, queue, counters, rows: gatedRows });
        });
        result.gateReleasedAt = new Date().toISOString();
        result.targetWaitSeconds = (Date.parse(result.gateReleasedAt) - Date.parse(gateStartedAt)) / 1_000;
        console.log(`${name}: ${count}개 준비·Target ${workersCount}개 확인, 측정 시작`);

        const containerIds = [...workers.map((worker) => worker.id), (await compose('ps', '--quiet', 'postgres')).trim()];
        const deadline = Date.now() + Math.max(180_000, count * durationMs / workersCount * 3 + 90_000);
        let finalRows: JobRow[] = [];
        let nextProgress = Date.now() + 30_000;
        while (true) {
            signal.throwIfAborted();
            const startedAt = new Date().toISOString();
            const [found, observation] = await Promise.all([rows(ids), (async (): Promise<Sample> => {
                try {
                    const [stats, connections, currentTargets] = await Promise.all([
                        docker('stats', '--no-stream', '--format', '{{json .}}', ...containerIds),
                        orm!.em.getConnection().execute<{ state: string; count: number; sampledAt: string }[]>(`
                            select coalesce(state, 'unknown') as state, count(*)::int as count, statement_timestamp() as "sampledAt"
                            from pg_stat_activity where datname = current_database() and backend_type = 'client backend' group by state
                        `), targets(),
                    ]);
                    const containers = stats.trim().split('\n').filter(Boolean).map((line) => {
                        const raw = JSON.parse(line);
                        return { id: raw.ID as string, service: workers.some((worker) => worker.id.startsWith(raw.ID)) ? 'worker' : 'postgres',
                            cpuPercent: Number(raw.CPUPerc.replace('%', '')), memoryBytes: memoryBytes(raw.MemUsage.split('/')[0].trim()), raw };
                    });
                    assert.equal(containers.length, workersCount + 1, '자원 샘플의 컨테이너가 누락되었습니다');
                    return { startedAt, finishedAt: new Date().toISOString(), containers, connections, targets: currentTargets };
                } catch (error) {
                    return { startedAt, finishedAt: new Date().toISOString(), error: String(error) };
                }
            })()]);
            samples.push(observation);
            await appendFile(join(runDirectory, 'samples.jsonl'), JSON.stringify(observation) + '\n');
            await save(join(runDirectory, 'latest.json'), found);
            assert.equal(found.length, count, '등록한 Batch Job이 누락되었습니다');
            assert.ok(found.every((row) => row.attempt <= 1 && row.status !== 'failed' && row.lastError === null), 'Retry·Recovery·최종 실패가 섞여 정상 비교에서 제외합니다');
            if (found.every((row) => row.status === 'succeeded')) { finalRows = found; break; }
            assert.ok(Date.now() < deadline, 'Batch 완료 대기 시간을 초과했습니다');
            if (Date.now() >= nextProgress) {
                console.log(`${name}: ${found.filter((row) => row.status === 'succeeded').length}/${count}개 성공`);
                nextProgress = Date.now() + 30_000;
            }
            await delay(Math.max(0, intervalMs - (Date.now() - Date.parse(startedAt))), undefined, { signal });
        }
        const timing = batchTiming(finalRows, ids, durationMs);
        assert.ok(Date.parse(timing.firstClaim) >= Date.parse(gateReadyAt), '준비 확인 전 Claim이 있습니다');
        assert.deepEqual(new Set(finalRows.map((row) => row.workerId)), new Set(workers.map((worker) => worker.workerId)), '모든 지정 Worker가 이번 Batch에 참여하지 않았습니다');
        await save(join(runDirectory, 'jobs.json'), finalRows);
        const collectedTargets = await readyTargets(workers, Date.parse(timing.lastSuccess));
        await save(join(runDirectory, 'targets-final.json'), collectedTargets);
        const prometheus: Record<string, unknown> = {};
        const rangeStart = Date.parse(gateStartedAt) / 1_000;
        const rangeEnd = Date.now() / 1_000;
        for (const [key, query] of Object.entries(queries)) {
            prometheus[key] = { query, response: await json(`${prometheusUrl}/api/v1/query_range?` + new URLSearchParams({
                query, start: String(rangeStart), end: String(rangeEnd), step: '5s',
            })) };
        }
        await save(join(runDirectory, 'prometheus.json'), { start: rangeStart, end: rangeEnd, step: '5s', queries: prometheus });
        const finalMetrics: Record<string, { metric: { instance: string }; value: [number, string] }[]> = {};
        for (const key of ['succeeded', 'handlerCount', 'handlerSum'] as const) {
            finalMetrics[key] = (await json(`${prometheusUrl}/api/v1/query?query=${encodeURIComponent(queries[key])}`)).data.result;
        }
        await save(join(runDirectory, 'metrics-final.json'), finalMetrics);
        const metric = (key: string, instance: string) => {
            const entry = finalMetrics[key]!.find((entry) => entry.metric.instance === instance);
            assert.ok(entry, `마지막 Metric이 누락되었습니다: ${key}, ${instance}`);
            return Number(entry.value[1]);
        };
        const handlerWorkers = workers.map((worker) => {
            const count = metric('handlerCount', worker.instance);
            const sumSeconds = metric('handlerSum', worker.instance);
            const owned = finalRows.filter((row) => row.workerId === worker.workerId).length;
            assert.equal(metric('succeeded', worker.instance), owned, '이번 프로세스의 성공 Counter와 DB 완료 수가 다릅니다');
            assert.equal(count, owned, 'Handler 관측 수와 DB 완료 수가 다릅니다');
            return { instance: worker.instance, count, sumSeconds, meanSeconds: sumSeconds / count };
        });
        const handlerCount = handlerWorkers.reduce((sum, worker) => sum + worker.count, 0);
        const handlerSum = handlerWorkers.reduce((sum, worker) => sum + worker.sumSeconds, 0);
        for (const worker of workers) {
            const [inspection] = JSON.parse(await docker('inspect', worker.id));
            assert.ok(inspection.State.Running && inspection.State.StartedAt === worker.startedAt && inspection.RestartCount === 0,
                'Worker 중단·재시작이 관측되어 정상 비교에서 제외합니다');
            const logs = await docker('logs', '--since', worker.startedAt, worker.id);
            await writeFile(join(runDirectory, `${worker.id.slice(0, 12)}.jsonl`), logs);
            const events = logs.trim().split('\n').filter(Boolean).map((line) => JSON.parse(line));
            assert.equal(events.some((entry) => ['job.retry_scheduled', 'job.recovered', 'job.failed', 'worker.error'].includes(entry.event)), false);
            const owned = finalRows.filter((row) => row.workerId === worker.workerId);
            const lifecycle = events.filter((entry) => ['job.claimed', 'job.succeeded'].includes(entry.event));
            assert.equal(lifecycle.length, owned.length * 2, 'Batch 외 처리 또는 중복 Claim·성공 로그가 있습니다');
            for (const row of owned) {
                for (const event of ['job.claimed', 'job.succeeded']) {
                    assert.equal(lifecycle.filter((entry) => entry.event === event && entry.jobId === row.id
                        && entry.workerId === row.workerId && entry.attempt === 1).length, 1, 'Job별 단일 Claim·성공 근거가 다릅니다');
                }
            }
        }
        const resourceValues: Record<string, number[]> = {};
        function add(key: string, value: number) { (resourceValues[key] ??= []).push(value); }
        for (const sample of samples) {
            if (!sample.containers || Date.parse(sample.startedAt) < Date.parse(timing.firstClaim)
                || Date.parse(sample.finishedAt) > Date.parse(timing.lastSuccess)) continue;
            const workerStats = sample.containers.filter((container) => container.service === 'worker');
            const postgres = sample.containers.find((container) => container.service === 'postgres')!;
            add('workerCpuPercent', workerStats.reduce((sum, container) => sum + container.cpuPercent, 0));
            add('workerMemoryBytes', workerStats.reduce((sum, container) => sum + container.memoryBytes, 0));
            add('postgresCpuPercent', postgres.cpuPercent);
            add('postgresMemoryBytes', postgres.memoryBytes);
            add('databaseConnections', sample.connections!.reduce((sum, state) => sum + state.count, 0));
            for (const worker of workerStats) {
                add(`${worker.id}.cpuPercent`, worker.cpuPercent);
                add(`${worker.id}.memoryBytes`, worker.memoryBytes);
            }
        }
        result.timing = timing;
        result.resources = Object.fromEntries(Object.entries(resourceValues).map(([key, value]) => [key, spread(value)]));
        result.handler = { count: handlerCount, sumSeconds: handlerSum, meanSeconds: handlerSum / handlerCount, workers: handlerWorkers };
        result.sampling = { total: samples.length, errors: samples.filter((sample) => sample.error).length, insideWindow: resourceValues.workerCpuPercent?.length ?? 0 };
        result.valid = true;
        console.log(`${name}: ${timing.elapsedSeconds.toFixed(3)}초, ${timing.throughput.toFixed(4)} Job/s, 구간 내 자원 샘플 ${resourceValues.workerCpuPercent?.length ?? 0}개`);
    } catch (error) {
        result.error = String(error);
        if (ids.length) await save(join(runDirectory, 'incomplete-jobs.json'), await rows(ids));
        throw error;
    } finally {
        await save(join(runDirectory, 'result.json'), result);
        await compose('stop', 'worker');
        for (const worker of workers) {
            await writeFile(join(runDirectory, `${worker.id.slice(0, 12)}.jsonl`), await docker('logs', '--since', worker.startedAt, worker.id));
        }
        await compose('rm', '-f', 'worker');
        await writeSummary(directory, results);
    }
}

try {
    // 이미 존재하는 프로젝트는 성공·실패와 관계없이 중단하거나 변경하지 않는다.
    assert.equal((await compose('ps', '--all', '--quiet')).trim(), '', `${project} 컨테이너가 이미 있습니다. 기존 실행을 먼저 확인하세요`);
    await mkdir(directory, { recursive: true });
    // 기존 결과 경로 재사용을 거부한다.
    await writeFile(join(directory, 'environment.json'), '{}\n', { flag: 'wx' });
    console.log(`실험 자료: ${directory}`);
    ownsEnvironment = true;
    await compose('up', '-d', '--no-build', 'postgres', 'migrate');
    const migration = (await compose('ps', '--all', '--quiet', 'migrate')).trim();
    assert.equal((await docker('wait', migration)).trim(), '0', 'Migration이 실패했습니다');
    orm = await MikroORM.init({ ...config, host: '127.0.0.1', port: Number(await port('postgres', '5432')),
        contextName: project, pool: { min: 1, max: 2 },
        driverOptions: { connectionTimeoutMillis: 5_000, application_name: 'worker-load-observer', options: '-c statement_timeout=10000' },
    });
    const [existing] = await orm.em.getConnection().execute<{ total: number; unfinished: number }[]>(
        "select count(*)::int as total, count(*) filter (where status in ('queued', 'processing'))::int as unfinished from jobs");
    assert.equal(existing?.unfinished, 0, '실험 전용 DB에 미완료 Job이 있습니다. Worker를 시작하지 않고 중단합니다. 이전 Batch ID로 개별 확인하세요');
    await compose('up', '-d', '--no-build', '--no-deps', 'api', 'prometheus');
    apiUrl = `http://127.0.0.1:${await port('api', '3000')}`;
    prometheusUrl = `http://127.0.0.1:${await port('prometheus', '9090')}`;
    for (const [label, url] of [['API', `${apiUrl}/health`], ['Prometheus', `${prometheusUrl}/-/ready`]]) {
        await until(`${label} 준비`, async () => {
            try {
                const response = await fetch(url!, { signal: AbortSignal.any([signal, AbortSignal.timeout(5_000)]) });
                await response.text();
                return response.ok ? true : undefined;
            } catch { signal.throwIfAborted(); return undefined; }
        }, 30_000);
    }
    const info = JSON.parse(await docker('info', '--format', '{{json .}}'));
    const containers = JSON.parse(await docker('inspect', ...(await compose('ps', '--quiet')).trim().split('\n')));
    const settings = await orm.em.getConnection().execute("select name, setting, unit from pg_settings where name in ('max_connections','shared_buffers','work_mem','max_worker_processes','max_parallel_workers') order by name");
    const revision = (await execute('git', ['rev-parse', 'HEAD'])).stdout.trim();
    const files = ['experiments/worker-load/run.ts', 'experiments/worker-load/results.ts', 'experiments/worker-load/compose.yml', 'package-lock.json', 'compose.yml', 'prometheus.yml',
        'src/jobs/store.ts', 'src/jobs/handlers.ts', 'src/worker/run.ts', 'src/db/mikro-orm.config.ts'];
    const hashes = Object.fromEntries(await Promise.all(files.map(async (file) => [file, createHash('sha256').update(await readFile(file)).digest('hex')])));
    await save(join(directory, 'environment.json'), {
        startedAt: new Date().toISOString(), project, revision, branch: (await execute('git', ['branch', '--show-current'])).stdout.trim(),
        workingTree: (await execute('git', ['status', '--short'])).stdout.trim(), hashes,
        host: { platform: platform(), release: release(), cpuModel: cpus()[0]?.model, logicalCpus: cpus().length, memoryBytes: totalmem() },
        docker: { version: info.ServerVersion, operatingSystem: info.OperatingSystem, kernel: info.KernelVersion, logicalCpus: info.NCPU, memoryBytes: info.MemTotal },
        node: process.version, compose: (await docker('compose', 'version', '--short')).trim(), apiUrl, prometheusUrl,
        settings, existingRows: existing, worker: { jobsAtOnce: 1, idlePollMs: 1000, recoveryCheckMs: 10000, staleTimeoutMs: 30000, pool: 'MikroORM 기본값' },
        containers: containers.map((container: { Name: string; Image: string; HostConfig: Record<string, unknown>; Config: { Image: string } }) => ({
            name: container.Name, image: container.Config.Image, imageId: container.Image,
            limits: Object.fromEntries(['NanoCpus', 'CpuQuota', 'CpuPeriod', 'CpusetCpus', 'Memory', 'MemorySwap'].map((key) => [key, container.HostConfig[key]])),
        })),
        parameters: { workers: counts, repeats, count, durationMs, intervalMs, scrapeIntervalMs: 15000, dnsRefreshMs: 15000,
            prometheusRangeStepMs: 5000, observerPoolMax: 2, maxTargetWaitMs: 90000 },
    });
    for (let repeat = 1; repeat <= repeats; repeat++) {
        for (const workers of counts) await runBatch(workers, repeat);
    }
    console.log(`실험 완료: ${join(directory, 'report.md')}`);
} catch (error) {
    console.error(String(error));
    if (ownsEnvironment) await save(join(directory, 'failure.json'), { error: String(error), at: new Date().toISOString(), results });
    process.exitCode = 1;
} finally {
    process.off('SIGINT', onSignal);
    process.off('SIGTERM', onSignal);
    if (ownsEnvironment) {
        try {
            await compose('stop', 'worker');
            await writeFile(join(directory, 'services.log'), await compose('logs', '--no-color', '--no-log-prefix'));
            if (orm) await orm.close();
        } finally {
            // 실측 데이터와 전용 Named Volume을 남긴다. 전체 DELETE나 down -v는 사용하지 않는다.
            await compose('down');
        }
    }
}
