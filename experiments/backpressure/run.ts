import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { appendFile, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { cpus, platform, release, totalmem } from 'node:os';
import { join, resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import { MikroORM } from '@mikro-orm/postgresql';

import config from '../../src/db/mikro-orm.config.ts';
import { options, produce, requestSummary, sampleSummary, type Counts, type Sample } from './experiment.ts';

const o = options(process.argv.slice(2));
const directory = resolve(o.out);
const project = 'async-job-backpressure';
const lock = resolve('artifacts/backpressure/.lock');
const composeArgs = ['compose', '-p', project, '-f', 'compose.yml', '-f', 'experiments/backpressure/compose.yml'];
const execute = promisify(execFile);
const controller = new AbortController();
const signal = controller.signal;
const onSignal = () => controller.abort(new Error('종료 요청으로 실험을 중단했습니다'));
process.on('SIGINT', onSignal);
process.on('SIGTERM', onSignal);
let enabled = false;
let ownsLock = false;
let ownsEnvironment = false;
let orm: MikroORM | undefined;
let apiUrl = '';
let prometheusUrl = '';
let imageId = '';
const results: Record<string, unknown>[] = [];
const save = (path: string, data: unknown) => writeFile(path, JSON.stringify(data, null, 2) + '\n');
async function docker(...args: string[]) {
    return (await execute('docker', args, { timeout: 90_000, maxBuffer: 30 * 1024 ** 2,
        env: { ...process.env, DB_NAME: 'async_job_backpressure', ENABLE_BACKPRESSURE: String(enabled), MAX_OUTSTANDING_JOBS: String(o.limit) },
    })).stdout;
}
const compose = (...args: string[]) => docker(...composeArgs, ...args);
async function port(service: string, internal: string) {
    return (await compose('port', service, internal)).trim().split(':').at(-1)!;
}
async function json(path: string) {
    const response = await fetch(`${prometheusUrl}${path}`, { signal: AbortSignal.any([signal, AbortSignal.timeout(5000)]) });
    assert.ok(response.ok, `Prometheus HTTP ${response.status}`);
    const body = await response.json();
    assert.equal(body.status, 'success');
    return body.data;
}
async function until<T>(label: string, read: () => Promise<T | undefined>, timeoutMs = 90_000) {
    const deadline = performance.now() + timeoutMs;
    while (true) {
        signal.throwIfAborted();
        const value = await read();
        if (value !== undefined) return value;
        assert.ok(performance.now() < deadline, `${label}: 대기 시간을 초과했습니다`);
        await delay(500, undefined, { signal });
    }
}
async function counts(): Promise<Counts> {
    const [row] = await orm!.em.getConnection().execute<Counts[]>(`select count(*)::int as total,
        count(*) filter (where status = 'queued')::int as queued,
        count(*) filter (where status = 'processing')::int as processing,
        count(*) filter (where status in ('queued', 'processing'))::int as outstanding,
        count(*) filter (where status = 'succeeded')::int as succeeded,
        count(*) filter (where status = 'failed')::int as failed from jobs`);
    return row!;
}
interface JobRow { id: string; status: string; attempt: number; last_error: unknown; result: unknown; type: string; payload: unknown }
const allRows = () => orm!.em.getConnection().execute<JobRow[]>('select * from jobs order by id');
interface Container {
    Id: string; Image: string; RestartCount: number;
    Config: { Env: string[]; Labels: Record<string, string> };
    State: { Running: boolean; StartedAt: string };
    HostConfig: Record<string, unknown>;
    NetworkSettings: { Networks: Record<string, { IPAddress: string }> };
}
async function inspect(): Promise<Container[]> {
    const ids = (await compose('ps', '--all', '--quiet')).trim().split('\n').filter(Boolean);
    return ids.length ? JSON.parse(await docker('inspect', ...ids)) : [];
}
// 비밀번호 등 전체 컨테이너 환경변수는 자료에 쓰지 않는다.
function metadata(containers: Container[]) {
    return containers.map((c) => ({ id: c.Id, service: c.Config.Labels['com.docker.compose.service'], imageId: c.Image,
        startedAt: c.State.StartedAt, running: c.State.Running, restartCount: c.RestartCount,
        settings: c.Config.Env.filter((v) => /^(ENABLE_BACKPRESSURE|MAX_OUTSTANDING_JOBS|LOG_LEVEL|DB_NAME)=/.test(v)),
        limits: Object.fromEntries(['NanoCpus', 'CpuQuota', 'CpuPeriod', 'CpusetCpus', 'Memory', 'MemorySwap'].map((key) => [key, c.HostConfig[key]])),
    }));
}
interface Target { labels: { job: string; instance: string }; health: string; lastScrape: string }
async function readyTargets(worker: Container, api: Container, after: number) {
    const ip = Object.values(worker.NetworkSettings.Networks)[0]!.IPAddress;
    return until('API·현재 Worker Target의 실제 Scrape', async () => {
        const targets: Target[] = (await json('/api/v1/targets?state=active')).activeTargets;
        const workers = targets.filter((t) => t.labels.job === 'worker');
        if (workers.length !== 1 || workers[0]!.labels.instance !== `${ip}:9464`
            || workers[0]!.health !== 'up' || Date.parse(workers[0]!.lastScrape) < Math.max(after, Date.parse(worker.State.StartedAt))) return;
        if (!targets.some((t) => t.labels.job === 'api' && t.health === 'up'
            && Date.parse(t.lastScrape) >= Math.max(after, Date.parse(api.State.StartedAt)))) return;
        return targets;
    });
}
const metricNames = ['async_job_enqueued_total', 'async_job_enqueue_rejected_total', 'async_job_queue_depth',
    'async_job_ready_queue_depth', 'async_job_processing_jobs', 'async_job_claimed_total', 'async_job_succeeded_total',
    'async_job_failed_total', 'async_job_retry_scheduled_total', 'async_job_recovered_total'];
const selector = `{__name__=~"${metricNames.join('|')}",job=~"api|worker"}`;
interface Metric { metric: { __name__: string; instance: string }; value: [number, string] }
async function metrics(targets: Target[]) {
    const data = await json(`/api/v1/query?query=${encodeURIComponent(selector)}`);
    // 과거 컨테이너 시계열을 현재 인스턴스로 취급하지 않는다.
    const instances = new Set(targets.map((t) => t.labels.instance));
    const current: Metric[] = data.result.filter((r: Metric) => instances.has(r.metric.instance));
    for (const name of metricNames) assert.equal(current.filter((r) => r.metric.__name__ === name).length, 1, `Metric 누락·중복: ${name}`);
    return { raw: data, current, values: Object.fromEntries(current.map((r) => [r.metric.__name__, Number(r.value[1])])) };
}

async function run(mode: string, repeat: number) {
    signal.throwIfAborted();
    enabled = mode === 'on';
    const name = `r${repeat}-${mode}`;
    const path = join(directory, name);
    await mkdir(path);
    const result: Record<string, unknown> = { name, mode, repeat, valid: false, parameters: o };
    const cleanup: { succeeded: boolean | null; errors: string[] } = { succeeded: null, errors: [] };
    result.cleanup = cleanup;
    results.push(result);
    let stopSampling = false;
    let sampler: Promise<void> | undefined;
    let samplerError: unknown;
    const samples: Sample[] = [];
    let origin = performance.now();
    const sample = async (phase: string) => {
        const started = performance.now();
        const row: Sample = { ...await counts(), elapsedMs: started - origin, queryMs: performance.now() - started, phase };
        samples.push(row);
        await appendFile(join(path, 'samples.jsonl'), JSON.stringify(row) + '\n');
        return row;
    };
    try {
        const baseline = await allRows();
        const baselineIds = new Set(baseline.map((row) => row.id));
        const initial = await counts();
        result.initial = initial;
        assert.equal(initial.outstanding, 0, '다른 실행의 미완료 Job이 있어 Worker를 시작하지 않습니다');
        await save(join(path, 'before.json'), { counts: initial, ids: [...baselineIds] });
        await compose('up', '-d', '--no-build', '--no-deps', '--scale', 'worker=1', 'api', 'worker');
        apiUrl = `http://127.0.0.1:${await port('api', '3000')}`;
        await until('API 준비', async () => {
            try {
                const response = await fetch(`${apiUrl}/health`, { signal: AbortSignal.any([signal, AbortSignal.timeout(3000)]) });
                await response.text();
                return response.ok ? true : undefined;
            } catch { signal.throwIfAborted(); return undefined; }
        });
        const containers = await inspect();
        const workers = containers.filter((c) => c.Config.Labels['com.docker.compose.service'] === 'worker');
        assert.equal(workers.length, 1);
        const worker = workers[0]!;
        const api = containers.find((c) => c.Config.Labels['com.docker.compose.service'] === 'api')!;
        for (const container of [worker, api]) assert.equal(container.Image, imageId, 'ON/OFF의 애플리케이션 이미지가 다릅니다');
        assert.ok(api.Config.Env.includes(`ENABLE_BACKPRESSURE=${enabled}`));
        assert.ok(api.Config.Env.includes(`MAX_OUTSTANDING_JOBS=${o.limit}`));
        const targets = await readyTargets(worker, api, Date.now());
        const beforeMetrics = await metrics(targets);
        for (const value of Object.values(beforeMetrics.values)) assert.equal(value, 0, '새 프로세스 Counter·Queue 초기값이 0이 아닙니다');
        await save(join(path, 'ready.json'), { at: new Date().toISOString(), initial, containers: metadata(containers), targets, metrics: beforeMetrics });
        console.log(`${name}: 준비 완료, ${o.rate} jobs/s × ${o.seconds}초 지속 유입 시작`);
        const startedAt = new Date().toISOString();
        origin = performance.now();
        await sample('start');
        sampler = (async () => {
            while (!stopSampling && !signal.aborted) {
                const next = performance.now() + o.intervalMs;
                await delay(Math.max(0, next - performance.now()));
                if (!stopSampling && !signal.aborted) await sample('periodic');
            }
        })().catch(async (error) => {
            samplerError = error;
            await save(join(path, 'sampling-error.json'), { error: String(error), elapsedMs: performance.now() - origin });
        });
        const produced = await produce(o, apiUrl, origin, signal, (request) => appendFile(join(path, 'requests.jsonl'), JSON.stringify(request) + '\n'));
        const http = requestSummary(produced.requests, o);
        await save(join(path, 'accepted-ids.json'), http.ids);
        const { ids, ...httpCounts } = http;
        result.http = { ...httpCounts, peakInflight: produced.peakInflight, settledMs: produced.settledMs };
        result.startedAt = startedAt;
        result.loadEnd = await sample('load-end');
        const loadEnd = result.loadEnd as Sample;
        result.loadBalance = { elapsedMs: loadEnd.elapsedMs, accepted: http.accepted,
            completed: loadEnd.succeeded - initial.succeeded,
            acceptedPerSecond: http.accepted * 1000 / loadEnd.elapsedMs,
            completedPerSecond: (loadEnd.succeeded - initial.succeeded) * 1000 / loadEnd.elapsedMs,
            deltaOutstanding: loadEnd.outstanding - initial.outstanding };
        await save(join(path, 'load-end-jobs.json'), (await allRows()).filter((row) => !baselineIds.has(row.id)));
        console.log(`${name}: HTTP 수락 ${http.accepted}, 정책 거부 ${http.rejected}, 전역 미완료 ${(result.loadEnd as Sample).outstanding}; 완료 대기`);
        signal.throwIfAborted();
        assert.equal(http.sent, http.planned, '예약 요청을 모두 발송하지 못했습니다');
        assert.equal(http.otherHttp + http.networkOrBodyErrors, 0, '예상 외 HTTP 응답·네트워크/본문 오류가 있습니다');
        assert.ok(http.maxScheduleLagMs < 1000, '발송 지연이 커서 정상 지속 유입 비교에서 제외합니다');
        const deadline = performance.now() + o.drainSeconds * 1000;
        let nextProgress = performance.now() + 30_000;
        while ((await counts()).outstanding !== 0) {
            signal.throwIfAborted();
            if (samplerError) throw samplerError;
            assert.ok(performance.now() < deadline, '전체 DB 미완료 Job의 완료 대기 시간을 초과했습니다');
            if (performance.now() >= nextProgress) {
                console.log(`${name}: 전역 미완료 ${(await counts()).outstanding}개`);
                nextProgress = performance.now() + 30_000;
            }
            await delay(o.intervalMs, undefined, { signal });
        }
        result.completed = await sample('completed');
        stopSampling = true;
        await sampler;
        if (samplerError) throw samplerError;
        const finalRows = await allRows();
        const added = finalRows.filter((row) => !baselineIds.has(row.id));
        await save(join(path, 'jobs.json'), added);
        assert.deepEqual(finalRows.filter((row) => baselineIds.has(row.id)), baseline, '이전 완료 행이 변경되었습니다');
        assert.equal(ids.length, new Set(ids).size, '수락 응답의 Job ID가 중복되었습니다');
        assert.deepEqual(new Set(added.map((row) => row.id)), new Set(ids), '응답 ID와 실제 신규 행이 다릅니다. 응답 유실·외부 등록 가능성을 확인하세요');
        assert.ok(added.every((row) => row.status === 'succeeded' && row.attempt === 1 && row.last_error === null
            && row.type === 'slow' && JSON.stringify(row.payload) === JSON.stringify({ durationMs: o.durationMs })
            && JSON.stringify(row.result) === JSON.stringify({ durationMs: o.durationMs })), '실패·Retry·Recovery 또는 잘못된 최종 상태가 있어 정상 결과에서 제외합니다');
        result.finalJobs = { total: added.length, succeeded: added.length, failed: 0, unfinished: 0, attempts: [1], unknownResponseIds: 0 };
        result.sampling = sampleSummary(samples);
        if (enabled) assert.ok(samples.every((s) => s.outstanding <= o.limit), '샘플에서 Admission 상한 초과를 관측했습니다');
        else assert.equal(http.rejected, 0, 'OFF에서 용량 정책 거부가 발생했습니다');
        // 기본 과부하 조건의 질적 관찰도 통과 조건에 포함한다. 작은 Smoke에는 강제하지 않는다.
        if (o.rate === 10 && o.seconds === 30 && o.durationMs === 1000 && o.limit === 100) {
            assert.ok((result.loadEnd as Sample).outstanding > initial.outstanding, '유입 구간의 누적을 관측하지 못했습니다');
            if (enabled) assert.ok(http.rejected > 0, '기본 ON 조건에서 거부를 관측하지 못했습니다');
            else assert.ok(sampleSummary(samples).observedMaxOutstanding > o.limit, '기본 OFF 조건에서 100개 초과 누적을 관측하지 못했습니다');
        }
        const finalTargets = await readyTargets(worker, api, Date.now());
        const finalMetrics = await metrics(finalTargets);
        await save(join(path, 'metrics-final.json'), { targets: finalTargets, ...finalMetrics });
        for (const [metric, expected] of Object.entries({ async_job_enqueued_total: http.accepted, async_job_enqueue_rejected_total: http.rejected,
            async_job_claimed_total: added.length, async_job_succeeded_total: added.length, async_job_failed_total: 0,
            async_job_retry_scheduled_total: 0, async_job_recovered_total: 0, async_job_queue_depth: 0,
            async_job_ready_queue_depth: 0, async_job_processing_jobs: 0 })) assert.equal(finalMetrics.values[metric], expected, `HTTP/DB와 최종 Metric이 일치하지 않습니다: ${metric}`);
        await save(join(path, 'prometheus.json'), await json(`/api/v1/query_range?query=${encodeURIComponent(selector)}&start=${Date.parse(startedAt) / 1000}&end=${Date.now() / 1000}&step=15`));
        const after = await inspect();
        await save(join(path, 'containers-final.json'), metadata(after));
        for (const original of [api, worker]) {
            const current = after.find((c) => c.Id === original.Id);
            assert.ok(current?.State.Running && current.RestartCount === 0 && current.State.StartedAt === original.State.StartedAt, 'API·Worker 중단 또는 재시작이 있습니다');
        }
        const logs = await compose('logs', '--no-color', '--no-log-prefix', 'worker');
        await writeFile(join(path, 'worker.jsonl'), logs);
        const events = logs.trim().split('\n').filter(Boolean).map((line) => JSON.parse(line));
        assert.ok(!events.some((event) => ['job.retry_scheduled', 'job.recovered', 'job.failed', 'worker.error', 'job.ownership_lost'].includes(event.event)), 'Worker 이상 Event가 있습니다');
        for (const event of ['job.claimed', 'job.succeeded']) {
            const found = events.filter((entry) => entry.event === event);
            assert.equal(found.length, ids.length, 'Job별 단일 Claim·성공 로그 수가 다릅니다');
            assert.deepEqual(new Set(found.map((entry) => entry.jobId)), new Set(ids));
            assert.ok(found.every((entry) => entry.attempt === 1));
        }
        result.metricsVerified = true;
        result.valid = true;
        console.log(`${name}: 수락 ${added.length}개 모두 첫 시도 성공, 관측 최대 ${sampleSummary(samples).observedMaxOutstanding}`);
    } catch (error) {
        result.error = String(error);
        throw error;
    } finally {
        stopSampling = true;
        if (sampler) await sampler;
        result.sampling = sampleSummary(samples);
        await save(join(path, 'result.json'), result);
        await save(join(directory, 'summary.json'), results);
        for (const action of [
            () => compose('stop', 'api', 'worker'),
            async () => { await writeFile(join(path, 'services.log'), await compose('logs', '--no-color', '--no-log-prefix', 'api', 'worker')); },
            async () => { await save(join(path, 'stopped-global-counts.json'), await counts()); },
            async () => { await save(join(path, 'stopped-global-jobs.json'), await allRows()); },
            () => compose('rm', '-f', 'api', 'worker'),
        ]) {
            try { await action(); } catch (error) { cleanup.errors.push(String(error)); }
        }
        cleanup.succeeded = cleanup.errors.length === 0;
        await save(join(path, 'result.json'), result);
        await save(join(directory, 'summary.json'), results);
        if (!cleanup.succeeded) throw new Error(`${name}: 종료 처리에 실패했습니다: ${cleanup.errors.join('; ')}`);
    }
}

try {
    await mkdir(resolve('artifacts/backpressure'), { recursive: true });
    await mkdir(lock);
    ownsLock = true;
    assert.equal((await compose('ps', '--all', '--quiet')).trim(), '', `${project} 컨테이너가 이미 있어 변경 없이 중단합니다`);
    await mkdir(directory, { recursive: true });
    await writeFile(join(directory, 'environment.json'), '{}\n', { flag: 'wx' });
    console.log(`실험 자료: ${directory}`);
    ownsEnvironment = true;
    await compose('up', '-d', '--no-build', 'postgres', 'migrate');
    const migration = (await compose('ps', '--all', '--quiet', 'migrate')).trim();
    assert.equal((await docker('wait', migration)).trim(), '0', 'Migration이 실패했습니다');
    const migrationContainer = (await inspect()).find((c) => c.Id === migration)!;
    imageId = migrationContainer.Image;
    orm = await MikroORM.init({ ...config, host: '127.0.0.1', port: Number(await port('postgres', '5432')),
        dbName: 'async_job_backpressure', contextName: project, pool: { min: 1, max: 2 },
        driverOptions: { connectionTimeoutMillis: 5000, application_name: 'backpressure-observer', options: '-c statement_timeout=10000' },
    });
    const initial = await counts();
    await save(join(directory, 'preflight.json'), initial);
    assert.equal(initial.outstanding, 0, '실험 전용 DB에 미완료 Job이 있습니다. API·Worker 시작 전에 중단합니다');
    await compose('up', '-d', '--no-build', '--no-deps', 'prometheus');
    prometheusUrl = `http://127.0.0.1:${await port('prometheus', '9090')}`;
    await until('Prometheus 준비', async () => {
        try {
            const response = await fetch(`${prometheusUrl}/-/ready`, { signal: AbortSignal.any([signal, AbortSignal.timeout(3000)]) });
            await response.text();
            return response.ok ? true : undefined;
        } catch { signal.throwIfAborted(); return undefined; }
    });
    const files = ['experiments/backpressure/run.ts', 'experiments/backpressure/experiment.ts', 'experiments/backpressure/compose.yml',
        'compose.yml', 'prometheus.yml', 'Dockerfile', 'package-lock.json', 'src/jobs/store.ts', 'src/jobs/handlers.ts', 'src/api/app.ts'];
    const hashes = Object.fromEntries(await Promise.all(files.map(async (file) => [file, createHash('sha256').update(await readFile(file)).digest('hex')])));
    const order = Array.from({ length: o.repeats }, (_, i) => ({ repeat: i + 1, modes: i % 2 ? [...o.modes].reverse() : o.modes }));
    const info = JSON.parse(await docker('info', '--format', '{{json .}}'));
    await save(join(directory, 'environment.json'), {
        at: new Date().toISOString(), project, dbName: 'async_job_backpressure', revision: (await execute('git', ['rev-parse', 'HEAD'])).stdout.trim(),
        branch: (await execute('git', ['branch', '--show-current'])).stdout.trim(), workingTree: (await execute('git', ['status', '--short'])).stdout,
        hashes, imageId, parameters: o, order, initial, apiUrl, prometheusUrl,
        host: { platform: platform(), release: release(), cpuModel: cpus()[0]?.model, logicalCpus: cpus().length, memoryBytes: totalmem() },
        docker: { version: info.ServerVersion, operatingSystem: info.OperatingSystem, kernel: info.KernelVersion, logicalCpus: info.NCPU, memoryBytes: info.MemTotal },
        node: process.version, compose: (await docker('compose', 'version', '--short')).trim(),
        observation: { sampleIntervalMs: o.intervalMs, observerPoolMax: 2, scrapeIntervalMs: 15000, rangeStepSeconds: 15, clock: 'performance.now()' },
        settings: await orm.em.getConnection().execute("select name, setting, unit from pg_settings where name in ('max_connections','shared_buffers','work_mem') order by name"),
        containers: metadata(await inspect()),
    });
    for (const item of order) for (const mode of item.modes) await run(mode, item.repeat);
    console.log(`실험 완료: ${join(directory, 'summary.json')}`);
} catch (error) {
    console.error(String(error));
    if (ownsEnvironment) await save(join(directory, 'failure.json'), { error: String(error), at: new Date().toISOString(), results });
    process.exitCode = 1;
} finally {
    // 종료 자료 수집을 모두 시도하고, 실패한 단계도 별도로 남긴다. 볼륨·행은 지우지 않는다.
    if (ownsEnvironment) {
        const errors: string[] = [];
        for (const action of [
            () => compose('stop', 'api', 'worker'),
            async () => { await save(join(directory, 'containers-stopped.json'), metadata(await inspect())); },
            async () => { if (orm) { await save(join(directory, 'final-global-counts.json'), await counts()); await save(join(directory, 'final-global-jobs.json'), await allRows()); } },
            async () => { await writeFile(join(directory, 'services.log'), await compose('logs', '--no-color', '--no-log-prefix')); },
            async () => { if (orm) await orm.close(); },
            () => compose('down'),
        ]) {
            try { await action(); } catch (error) { errors.push(String(error)); }
        }
        if (errors.length) { await save(join(directory, 'cleanup-errors.json'), errors); process.exitCode = 1; }
        await save(join(directory, 'cleanup.json'), { succeeded: errors.length === 0, errors });
    }
    if (ownsLock) await rm(lock, { recursive: true });
    process.off('SIGINT', onSignal);
    process.off('SIGTERM', onSignal);
}
