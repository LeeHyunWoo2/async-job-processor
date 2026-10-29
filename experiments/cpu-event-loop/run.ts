import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { appendFile, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { cpus, platform, release, totalmem } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import { MikroORM } from '@mikro-orm/postgresql';

import config from '../../src/db/mikro-orm.config.ts';
import { clockDiagnostic, histogram, metric, options, probeSummary, probeValidity, runOrder, startProbes,
    type Condition, type Mode } from './experiment.ts';

const o = options(process.argv.slice(2));
const directory = resolve(o.out);
const project = 'async-job-cpu-event-loop';
const dbName = 'async_job_cpu_event_loop';
const lock = resolve('artifacts/cpu-event-loop/.lock');
const composeArgs = ['compose', '-p', project, '-f', 'compose.yml', '-f', 'experiments/cpu-event-loop/compose.yml'];
const execute = promisify(execFile);
const controller = new AbortController();
const signal = controller.signal;
const onSignal = () => controller.abort(new Error('종료 요청으로 실험을 중단했습니다'));
process.on('SIGINT', onSignal);
process.on('SIGTERM', onSignal);
let ownsLock = false;
let ownsEnvironment = false;
let orm: MikroORM | undefined;
let imageId = '';
let imageHashes: Record<string, string> = {};
const results: Record<string, unknown>[] = [];
const save = (path: string, data: unknown) => writeFile(path, JSON.stringify(data, null, 2) + '\n');
const boundary = () => ({ monoMs: performance.now(), wallMs: Date.now() });
async function docker(...args: string[]) {
    return (await execute('docker', args, { timeout: 60_000, maxBuffer: 30 * 1024 ** 2 })).stdout;
}
const compose = (...args: string[]) => docker(...composeArgs, ...args);
async function port(service: string, internal: string) {
    return (await compose('port', service, internal)).trim().split(':').at(-1)!;
}
async function until<T>(label: string, read: () => Promise<T | undefined>, timeoutMs = 30_000) {
    const deadline = performance.now() + timeoutMs;
    while (true) {
        signal.throwIfAborted();
        const value = await read();
        if (value !== undefined) return value;
        assert.ok(performance.now() < deadline, `${label}: 대기 시간 초과`);
        await delay(50, undefined, { signal });
    }
}
async function http(url: string, init?: RequestInit) {
    const response = await fetch(url, { ...init, signal: AbortSignal.any([signal, AbortSignal.timeout(o.timeoutMs)]) });
    const text = await response.text();
    assert.ok(response.ok, `${url}: HTTP ${response.status}: ${text}`);
    return { status: response.status, text };
}
interface JobRow {
    id: string; status: string; attempt: number; type: string; payload: { durationMs: number };
    result: { durationMs?: number; iterations?: number; checksum?: number } | null;
    worker_id: string | null; last_error: unknown;
}
const allRows = () => orm!.em.getConnection().execute<JobRow[]>('select * from jobs order by id');
async function preflight() {
    const rows = await allRows();
    assert.ok(rows.every((r) => r.status !== 'queued' && r.status !== 'processing'),
        '전용 DB에 미완료 Job이 있어 API·Worker 시작 전에 거부합니다');
    return rows;
}
interface Container {
    Id: string; Image: string; RestartCount: number; Path: string; Args: string[];
    Config: { Labels: Record<string, string>; Env: string[]; Cmd: string[] };
    State: { Running: boolean; StartedAt: string; ExitCode: number };
    HostConfig: Record<string, unknown>;
}
const service = (c: Container) => c.Config.Labels['com.docker.compose.service'];
async function inspect(): Promise<Container[]> {
    const ids = (await compose('ps', '--all', '--quiet')).trim().split('\n').filter(Boolean);
    return ids.length ? JSON.parse(await docker('inspect', ...ids)) : [];
}
function metadata(containers: Container[]) {
    return containers.map((c) => ({ id: c.Id, service: service(c), imageId: c.Image, command: [c.Path, ...c.Args],
        startedAt: c.State.StartedAt, running: c.State.Running, exitCode: c.State.ExitCode, restartCount: c.RestartCount,
        settings: c.Config.Env.filter((v) => /^(DB_NAME|LOG_LEVEL|ENABLE_BACKPRESSURE|MAX_OUTSTANDING_JOBS)=/.test(v)),
        limits: Object.fromEntries(['NanoCpus', 'CpuQuota', 'CpuPeriod', 'CpusetCpus', 'CpuShares', 'Memory', 'MemorySwap'].map((key) => [key, c.HostConfig[key]])),
    }));
}
interface Event { event?: string; jobId?: string; workerId?: string; attempt?: number; time: number; handlerElapsedMs?: number }
function events(logs: string): Event[] {
    return logs.split('\n').flatMap((line) => { try { return [JSON.parse(line) as Event]; } catch { return []; } });
}
function resourceSampler(ids: string[], path: string, origin: number) {
    let stopped = false;
    let samples = 0;
    const perContainer: Record<string, number> = {};
    const errors: string[] = [];
    // Streaming Stats는 TTY 제어 문자를 출력하므로 완결된 JSON 표본을 순차 수집한다.
    const task = (async () => {
        do {
            const requestedMs = performance.now() - origin;
            try {
                const output = await docker('stats', '--no-stream', '--no-trunc', '--format', '{{json .}}', ...ids);
                for (const line of output.trim().split('\n').filter(Boolean)) {
                    const row = { requestedMs, elapsedMs: performance.now() - origin, wallMs: Date.now(), ...JSON.parse(line) };
                    await appendFile(join(path, 'resources.jsonl'), JSON.stringify(row) + '\n');
                    samples++;
                    const id = ids.find((id) => row.ID === id || row.Container === id);
                    if (id) perContainer[id] = (perContainer[id] ?? 0) + 1;
                }
            } catch (error) { errors.push(String(error)); break; }
        } while (!stopped);
    })();
    return { async stop() { stopped = true; await task; return { samples, perContainer, errors }; } };
}

async function run(item: { mode: Mode; condition: Condition; repeat: number }) {
    const { mode, condition, repeat } = item;
    const name = `r${repeat}-${mode}-${condition}`;
    const path = join(directory, name);
    await mkdir(path);
    const result: Record<string, unknown> = { name, ...item, valid: false, reasons: [], cleanup: null };
    results.push(result);
    const cleanupErrors: string[] = [];
    const start = boundary();
    const phases: Record<string, unknown>[] = [];
    const mark = async (phase: string) => {
        const now = boundary();
        const value = { phase, elapsedMs: now.monoMs - origin, wallMs: now.wallMs,
            ...clockDiagnostic(observationStart, now) };
        phases.push(value);
        await appendFile(join(path, 'phases.jsonl'), JSON.stringify(value) + '\n');
    };
    let probes: ReturnType<typeof startProbes> | undefined;
    let resources: ReturnType<typeof resourceSampler> | undefined;
    let probeData: Awaited<ReturnType<ReturnType<typeof startProbes>['stop']>> | undefined;
    let stats: Awaited<ReturnType<ReturnType<typeof resourceSampler>['stop']>> | undefined;
    let origin = start.monoMs;
    let observationStart = start;
    const snapshots: Record<string, Record<string, unknown>> = {};
    let apiUrl = '';
    let workerUrl = '';
    const snapshot = async (phase: string) => {
        const values: Record<string, unknown> = {};
        for (const [target, url] of [['api', `${apiUrl}/metrics`], ['worker', workerUrl]] as const) {
            const before = boundary();
            const response = await http(url);
            const after = boundary();
            const loop = mode === 'co-located' && target === 'api' ? null : histogram(response.text);
            if (mode === 'co-located' && target === 'api') assert.ok(!response.text.includes('nodejs_event_loop_delay_'));
            values[target] = { histogram: loop, startedMs: before.monoMs - origin, finishedMs: after.monoMs - origin,
                wallMs: after.wallMs, process: mode === 'co-located' ? 'shared' : target };
            await writeFile(join(path, `${phase}-${target}.prom`), response.text);
            values[`${target}Counters`] = Object.fromEntries((target === 'api'
                ? ['enqueued', 'enqueue_rejected'] : ['claimed', 'succeeded', 'failed', 'retry_scheduled', 'recovered'])
                .map((suffix) => [`${suffix}`, metric(response.text, `async_job_${suffix}_total`)]));
        }
        snapshots[phase] = values;
        await save(join(path, 'snapshots.json'), snapshots);
        return values;
    };
    try {
        signal.throwIfAborted();
        const baseline = await preflight();
        await save(join(path, 'before.json'), baseline);
        const services = mode === 'co-located' ? ['co-located'] : ['api', 'worker'];
        await compose('up', '-d', '--no-build', '--no-deps', ...services);
        const apiService = mode === 'co-located' ? 'co-located' : 'api';
        const workerService = mode === 'co-located' ? 'co-located' : 'worker';
        apiUrl = `http://127.0.0.1:${await port(apiService, '3000')}`;
        workerUrl = `http://127.0.0.1:${await port(workerService, '9464')}/metrics`;
        await until('API·Worker HTTP·Event Loop 표본 준비', async () => {
            try {
                assert.equal(JSON.parse((await http(`${apiUrl}/health`)).text).status, 'ok');
                const w = histogram((await http(workerUrl)).text);
                if (w.count <= 0) return;
                if (mode === 'separated' && histogram((await http(`${apiUrl}/metrics`)).text).count <= 0) return;
                return true;
            } catch { signal.throwIfAborted(); return undefined; }
        });
        const containers = await inspect();
        const apps = containers.filter((c) => ['api', 'worker', 'co-located'].includes(service(c)!));
        assert.deepEqual(apps.map(service).sort(), [...services].sort(), '실제 실행 구조·컨테이너 수 불일치');
        const processes = [];
        for (const c of apps) {
            assert.ok(c.State.Running && c.RestartCount === 0);
            assert.equal(c.Image, imageId, '비교 이미지가 다릅니다');
            const expected = service(c) === 'co-located' ? 'experiments/cpu-event-loop/co-located.ts'
                : service(c) === 'api' ? 'src/api/server.ts' : 'src/worker/main.ts';
            assert.deepEqual(c.Config.Cmd, ['node', expected]);
            const top = await docker('top', c.Id, '-eo', 'pid,ppid,args');
            assert.equal(top.trim().split('\n').slice(1).filter((line) => /\bnode\s/.test(line)).length, 1, '컨테이너별 Node.js 프로세스 수 불일치');
            assert.ok(top.includes(expected), '실제 Node.js 진입점 불일치');
            const runtime = JSON.parse(await docker('exec', c.Id, 'node', '--input-type=module', '-e', `
                import { readFileSync, existsSync } from 'node:fs';
                import { availableParallelism } from 'node:os';
                const paths = ['/sys/fs/cgroup/cpu.max', '/sys/fs/cgroup/cpuset.cpus.effective',
                    '/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/cpu/cpu.cfs_quota_us',
                    '/sys/fs/cgroup/cpu/cpu.cfs_period_us', '/sys/fs/cgroup/cpuset/cpuset.cpus'];
                console.log(JSON.stringify({ node: process.version, availableParallelism: availableParallelism(),
                    cgroup: Object.fromEntries(paths.filter(existsSync).map(p => [p, readFileSync(p, 'utf8').trim()])) }));
            `));
            processes.push({ service: service(c), top, runtime });
            const actualHashes = await docker('exec', c.Id, 'sha256sum', ...Object.keys(imageHashes));
            for (const line of actualHashes.trim().split('\n')) {
                const [hash, file] = line.split(/\s+/);
                assert.equal(hash, imageHashes[file!], `이미지와 현재 소스가 다릅니다: ${file}`);
            }
        }
        const startup = events(await compose('logs', '--no-color', '--no-log-prefix', workerService)).filter((e) => e.event === 'worker.started');
        assert.equal(startup.length, 1, 'Worker가 정확히 하나여야 합니다');
        result.topology = { nodeProcesses: apps.length, workers: 1, containers: metadata(apps), processes, workerId: startup[0]!.workerId };
        await save(join(path, 'ready.json'), { apiUrl, workerUrl, topology: result.topology });
        observationStart = boundary();
        origin = observationStart.monoMs;
        probes = startProbes(o, { api: `${apiUrl}/health`, worker: workerUrl }, name, origin, signal,
            (row) => appendFile(join(path, 'requests.jsonl'), JSON.stringify(row) + '\n'));
        resources = resourceSampler(apps.map((c) => c.Id), path, origin);
        await mark('baseline-start');
        await snapshot('baseline');
        await delay(o.windowMs, undefined, { signal });
        const before = await snapshot('before-load');
        await mark('load-start');
        let jobId: string | null = null;
        let apiJob: Record<string, unknown> | null = null;
        if (condition === 'idle') await delay(o.durationMs, undefined, { signal });
        else {
            const response = await http(`${apiUrl}/jobs`, { method: 'POST', headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ type: condition, payload: { durationMs: o.durationMs } }) });
            await save(join(path, 'enqueue.json'), response);
            assert.equal(response.status, 201);
            const job = JSON.parse(response.text);
            jobId = job.id;
            result.jobId = jobId;
            await until('DB Job 실제 완료', async () => {
                const rows = await orm!.em.getConnection().execute<JobRow[]>('select * from jobs where id = ?', [jobId]);
                await appendFile(join(path, 'db-samples.jsonl'), JSON.stringify({ elapsedMs: performance.now() - origin, wallMs: Date.now(), rows }) + '\n');
                assert.equal(rows.length, 1);
                assert.notEqual(rows[0]!.status, 'failed', 'Job 최종 실패');
                return rows[0]!.status === 'succeeded' ? rows[0] : undefined;
            });
            const queried = await http(`${apiUrl}/jobs/${jobId}`);
            await save(join(path, 'job-api.json'), JSON.parse(queried.text));
            const found = JSON.parse(queried.text);
            apiJob = found;
            assert.equal(found.status, 'succeeded');
            assert.equal(found.attempt, 1);
        }
        await mark('completion-observed');
        await snapshot('completion');
        await delay(o.windowMs, undefined, { signal });
        const final = await snapshot('final');
        // Timer 재개 이후 표본 증가를 확인한다. Max의 기대 크기는 성공 조건으로 삼지 않는다.
        for (const target of mode === 'co-located' ? ['worker'] : ['api', 'worker']) {
            const count = (v: Record<string, unknown>) => (v[target] as { histogram: { count: number } }).histogram.count;
            assert.ok(count(before) > 0 && count(final) > count(snapshots.completion!), '완료 이후 Event Loop 표본 증가 없음');
        }
        await mark('observation-end');
        probeData = await probes.stop();
        stats = await resources.stop();
        const end = boundary();
        result.clock = { ...clockDiagnostic(observationStart, end),
            maxAbsBoundaryDifferenceMs: Math.max(...phases.map((p) => Math.abs(Number(p.differenceMs))),
                Math.abs(clockDiagnostic(observationStart, end).differenceMs)) };
        result.phases = phases;
        result.http = Object.fromEntries(['api', 'worker'].map((target) => [target, probeSummary(probeData!.rows.filter((r) => r.target === target))]));
        result.peakInflight = probeData.peakInflight;
        result.dispatch = { stopRequestedMs: probeData.stopRequestedMs, expectedPerTarget: probeData.expectedPerTarget };
        result.histograms = Object.fromEntries(Object.entries(snapshots).map(([phase, data]) => [phase,
            Object.fromEntries((mode === 'co-located' ? ['worker'] : ['api', 'worker']).map((target) => [mode === 'co-located' ? 'shared' : target,
                (data[target] as { histogram: unknown }).histogram]))]));
        const after = await allRows();
        await save(join(path, 'jobs.json'), after);
        const ids = new Set(baseline.map((r) => r.id));
        assert.deepEqual(after.filter((r) => ids.has(r.id)), baseline, '보존 행 변경');
        const added = after.filter((r) => !ids.has(r.id));
        assert.deepEqual(added.map((r) => r.id), jobId ? [jobId] : []);
        for (const row of added) {
            assert.equal(row.status, 'succeeded'); assert.equal(row.attempt, 1); assert.equal(row.last_error, null);
            assert.equal(apiJob?.id, row.id);
            assert.deepEqual(apiJob?.result, row.result);
            assert.equal(row.type, condition); assert.deepEqual(row.payload, { durationMs: o.durationMs });
            assert.equal(row.worker_id, startup[0]!.workerId);
            assert.equal(row.result?.durationMs, o.durationMs);
            if (condition === 'cpu') {
                assert.ok(Number.isSafeInteger(row.result?.iterations) && row.result!.iterations! > 0);
                assert.ok(Number.isSafeInteger(row.result?.checksum));
            } else assert.deepEqual(row.result, { durationMs: o.durationMs });
        }
        result.jobs = { added: added.length, succeeded: added.length, attempt: jobId ? 1 : null };
        for (const [target, expected] of [['api', { enqueued: added.length, enqueue_rejected: 0 }],
            ['worker', { claimed: added.length, succeeded: added.length, failed: 0, retry_scheduled: 0, recovered: 0 }]] as const) {
            assert.deepEqual(final[`${target}Counters`], expected, '최종 Counter와 DB 불일치');
        }
        const logs = await compose('logs', '--no-color', '--no-log-prefix', ...services);
        await writeFile(join(path, 'events.jsonl'), logs);
        const entries = events(logs);
        assert.ok(!entries.some((e) => ['job.failed', 'job.retry_scheduled', 'job.recovered', 'worker.error'].includes(e.event!)), '이상 Worker 사건');
        for (const event of ['job.claimed', 'job.handler_started', 'job.handler_finished', 'job.succeeded']) {
            const found = entries.filter((e) => e.event === event);
            assert.equal(found.length, added.length, `${event} 사건 수 불일치`);
            assert.ok(found.every((e) => e.jobId === jobId && e.attempt === 1));
        }
        const reasons = probeValidity(probeData.rows, o.intervalMs, (result.clock as { maxAbsBoundaryDifferenceMs: number }).maxAbsBoundaryDifferenceMs);
        for (const target of ['api', 'worker']) {
            if (probeData.rows.filter((r) => r.target === target).length !== probeData.expectedPerTarget) reasons.push(`${target}: 예약 구간의 요청 기록 누락`);
        }
        if (probeData.writeError) reasons.push(`Probe 저장 실패: ${probeData.writeError}`);
        if (apps.some((c) => !stats!.perContainer[c.Id]) || stats.errors.length) reasons.push('Docker 자원 표본 결측·수집 오류');
        result.resources = stats;
        if (jobId) {
            const started = entries.find((e) => e.event === 'job.handler_started')!;
            const finished = entries.find((e) => e.event === 'job.handler_finished')!;
            assert.ok(finished.time >= started.time && Number.isFinite(finished.handlerElapsedMs));
            result.handler = { startedWallMs: started.time, finishedWallMs: finished.time, elapsedMs: finished.handlerElapsedMs,
                alignment: 'Wall Clock 사건과 클라이언트 발송 시각 비교; 프로세스 간 offset은 측정하지 않음' };
            result.overlap = Object.fromEntries(['api', 'worker'].map((target) => {
                const matching = probeData!.rows.filter((r) => r.target === target);
                const sent = matching.filter((r) => r.sentWallMs !== null && r.sentWallMs > started.time && r.sentWallMs < finished.time);
                const scheduled = matching.filter((r) => observationStart.wallMs + r.scheduledMs > started.time && observationStart.wallMs + r.scheduledMs < finished.time);
                if (!sent.length) reasons.push(`${target}: Handler 구간 내 실제 발송 근거 없음`);
                return [target, { sent: sent.length, scheduled: scheduled.length, indices: sent.map((r) => r.index) }];
            }));
        }
        const current = await inspect();
        for (const c of apps) {
            const found = current.find((v) => v.Id === c.Id);
            assert.ok(found?.State.Running && found.RestartCount === 0 && found.State.StartedAt === c.State.StartedAt, '프로세스 중단·재시작');
        }
        result.reasons = reasons;
        result.valid = reasons.length === 0;
        console.log(`${name}: valid=${result.valid}, Job=${added.length}, Probe=${probeData.rows.length}`);
    } catch (error) {
        result.error = String(error);
        console.error(`${name}: ${String(error)}`);
    } finally {
        // 실패·중단·저장 오류가 나도 나머지 수집과 전용 컨테이너 정리를 계속한다.
        const actions = [
            async () => { probeData ??= await probes?.stop(); if (probeData) await save(join(path, 'requests.json'), probeData); },
            async () => { stats ??= await resources?.stop(); if (stats) await save(join(path, 'resource-status.json'), stats); },
            () => compose('stop', 'api', 'worker', 'co-located'),
            async () => { await writeFile(join(path, 'services.log'), await compose('logs', '--no-color', '--no-log-prefix', 'api', 'worker', 'co-located')); },
            async () => { await save(join(path, 'stopped-containers.json'), metadata(await inspect())); },
            async () => { await save(join(path, 'stopped-jobs.json'), await allRows()); },
            () => compose('rm', '-f', 'api', 'worker', 'co-located'),
        ];
        for (const action of actions) { try { await action(); } catch (error) { cleanupErrors.push(String(error)); } }
        const cleanup = { succeeded: cleanupErrors.length === 0, errors: cleanupErrors };
        result.cleanup = cleanup;
        for (const [file, data] of [[join(path, 'result.json'), result], [join(directory, 'summary.json'), results]] as const) {
            try { await save(file, data); }
            catch (error) {
                cleanup.succeeded = false;
                cleanupErrors.push(String(error));
                process.exitCode = 1;
                console.error(`자료 저장 실패: ${file}: ${String(error)}`);
            }
        }
        // 요약 파일 자체의 저장 실패도 별도 경로에 남기고, 저장 가능한 반복 결과는 갱신한다.
        if (cleanupErrors.length) {
            for (const file of ['cleanup.json', 'result.json']) {
                try { await save(join(path, file), file === 'cleanup.json' ? cleanup : result); }
                catch (error) { console.error(`종료 자료 재저장 실패: ${file}: ${String(error)}`); }
            }
        }
    }
}

try {
    await mkdir(dirname(lock), { recursive: true });
    await mkdir(lock);
    ownsLock = true;
    assert.equal((await compose('ps', '--all', '--quiet')).trim(), '', `${project} 컨테이너가 이미 있어 변경 없이 거부합니다`);
    await mkdir(directory, { recursive: true });
    await writeFile(join(directory, 'environment.json'), '{}\n', { flag: 'wx' });
    ownsEnvironment = true;
    console.log(`실험 자료: ${directory}`);
    const listed = (await execute('git', ['ls-files', '-co', '--exclude-standard', 'src', 'experiments/cpu-event-loop', 'compose.yml', 'Dockerfile', '.dockerignore', 'package.json', 'package-lock.json'])).stdout;
    const files = [...new Set(listed.trim().split('\n'))].filter(Boolean);
    const hashes: Record<string, string> = {};
    for (const file of files) {
        const data = await readFile(file);
        hashes[file] = createHash('sha256').update(data).digest('hex');
        const copy = join(directory, 'source', file);
        await mkdir(dirname(copy), { recursive: true });
        await writeFile(copy, data);
    }
    imageHashes = Object.fromEntries(Object.entries(hashes).filter(([file]) => file.startsWith('src/')
        || ['package.json', 'package-lock.json', 'experiments/cpu-event-loop/co-located.ts'].includes(file)));
    const info = JSON.parse(await docker('info', '--format', '{{json .}}'));
    const environment: Record<string, unknown> = {
        at: new Date().toISOString(), project, dbName, revision: (await execute('git', ['rev-parse', 'HEAD'])).stdout.trim(),
        branch: (await execute('git', ['branch', '--show-current'])).stdout.trim(), workingTree: (await execute('git', ['status', '--short'])).stdout,
        hashes, parameters: o, order: runOrder(o), node: process.version,
        host: { platform: platform(), release: release(), logicalCpus: cpus().length, cpuModel: cpus()[0]?.model, memoryBytes: totalmem() },
        docker: { version: info.ServerVersion, operatingSystem: info.OperatingSystem, kernel: info.KernelVersion, logicalCpus: info.NCPU, memoryBytes: info.MemTotal },
        compose: (await docker('compose', 'version', '--short')).trim(),
        observation: { clock: 'performance.now()', histogram: '프로세스 계측 활성화 이후 누적',
            reset: '조건마다 새 프로세스; 준비 구간 포함', resourceClock: 'Docker Stats 수신 시각; 원자적 표본 아님',
            prometheus: '기동하지 않음; 독립 Probe와 직접 Metrics Snapshot 사용' },
    };
    await save(join(directory, 'environment.json'), environment);
    await writeFile(join(directory, 'source.diff'), (await execute('git', ['diff', '--binary', 'HEAD'])).stdout);
    await compose('up', '-d', '--no-build', 'postgres', 'migrate');
    const migration = (await compose('ps', '--all', '--quiet', 'migrate')).trim();
    assert.equal((await docker('wait', migration)).trim(), '0', 'Migration 실패');
    imageId = (await inspect()).find((c) => c.Id === migration)!.Image;
    environment.imageId = imageId;
    environment.containers = metadata(await inspect());
    await save(join(directory, 'environment.json'), environment);
    orm = await MikroORM.init({ ...config, host: '127.0.0.1', port: Number(await port('postgres', '5432')), dbName,
        contextName: project, pool: { min: 1, max: 2 },
        driverOptions: { connectionTimeoutMillis: 5000, application_name: 'cpu-event-loop-observer', options: '-c statement_timeout=5000' },
    });
    await save(join(directory, 'preflight-jobs.json'), await allRows());
    await preflight();
    for (const item of runOrder(o)) { signal.throwIfAborted(); await run(item); }
    if (results.some((r) => !r.valid || !(r.cleanup as { succeeded: boolean }).succeeded)) process.exitCode = 1;
    console.log(`실험 종료: ${join(directory, 'summary.json')}`);
} catch (error) {
    console.error(String(error));
    process.exitCode = 1;
    if (ownsEnvironment) {
        try { await save(join(directory, 'failure.json'), { error: String(error), results }); }
        catch (saveError) { console.error(`실패 자료 저장 오류: ${String(saveError)}`); }
    }
} finally {
    if (ownsEnvironment) {
        const errors: string[] = [];
        for (const action of [
            () => compose('stop', 'api', 'worker', 'co-located'),
            async () => { if (orm) await save(join(directory, 'final-jobs.json'), await allRows()); },
            async () => { await writeFile(join(directory, 'services.log'), await compose('logs', '--no-color', '--no-log-prefix')); },
            async () => { if (orm) await orm.close(); },
            () => compose('down'),
        ]) { try { await action(); } catch (error) { errors.push(String(error)); } }
        if (errors.length) process.exitCode = 1;
        try { await save(join(directory, 'cleanup.json'), { succeeded: errors.length === 0, errors }); }
        catch (error) { process.exitCode = 1; console.error(`정리 결과 저장 오류: ${String(error)}`); }
    }
    if (ownsLock) await rm(lock, { recursive: true });
    process.off('SIGINT', onSignal);
    process.off('SIGTERM', onSignal);
}
