import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appendFile, mkdir, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { cpus, platform, release, totalmem } from 'node:os';
import { join, resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { MikroORM } from '@mikro-orm/postgresql';

import config from '../../src/db/mikro-orm.config.ts';
import { category, evaluate, options, postKillRequest, runOrder, startProducer, survivorSuccessRequest,
    type Evidence, type JobRow, type Mode, type Observation, type Sample } from './experiment.ts';
import { boundary, compose, dbName, docker, execute, followWorker, inspect, metadata, port, project,
    resources, service, type Container } from './runtime.ts';

const o = options(process.argv.slice(2));
const directory = resolve(o.out);
const lock = resolve('artifacts/failure-load/.lock');
const controller = new AbortController();
const signal = controller.signal;
const onSignal = () => controller.abort(new Error('종료 요청으로 실험을 중단했습니다'));
process.on('SIGINT', onSignal);
process.on('SIGTERM', onSignal);
const save = (path: string, value: unknown) => writeFile(path, JSON.stringify(value, null, 2) + '\n');
const line = (path: string, value: unknown) => appendFile(path, JSON.stringify(value) + '\n');
let ownsLock = false, ownsEnvironment = false;
let orm: MikroORM | undefined;
let imageId = '', prometheusUrl = '';
const results: Record<string, unknown>[] = [];
const sourceFiles = ['Dockerfile', 'package.json', 'package-lock.json', 'compose.yml', 'prometheus.yml',
    ...(await readdir('src', { recursive: true })).filter((f) => f.endsWith('.ts')).map((f) => join('src', f))];
let hashes: Record<string, string> = {};
const allRows = async () => (await orm!.em.getConnection().execute<{ row: JobRow }[]>(
    'select to_jsonb(j) as row from jobs j order by id')).map((r) => r.row);
const jobRow = async (id: string) => (await orm!.em.getConnection().execute<{ row: JobRow; ageMs: number }[]>(`
    select to_jsonb(j) as row, (extract(epoch from (clock_timestamp() - processing_started_at)) * 1000)::double precision as "ageMs"
    from jobs j where id = ?`, [id]))[0];
async function preflight() {
    const rows = await allRows();
    assert.ok(rows.every((r) => r.status !== 'queued' && r.status !== 'processing'),
        '실험 전용 DB에 미완료 Job이 있습니다. API·Worker 시작 전에 중단합니다');
    return rows;
}
async function until<T>(name: string, read: () => Promise<T | undefined>, timeoutMs = 30_000) {
    const deadline = performance.now() + timeoutMs;
    while (true) {
        signal.throwIfAborted();
        const value = await read();
        if (value !== undefined) return value;
        assert.ok(performance.now() < deadline, `${name}: 대기 시간 초과`);
        await delay(100, undefined, { signal });
    }
}
async function ready(url: string) {
    await until(`${url} 준비`, async () => {
        try {
            const response = await fetch(url, { signal: AbortSignal.any([signal, AbortSignal.timeout(3000)]) });
            await response.text();
            return response.ok ? true : undefined;
        } catch { signal.throwIfAborted(); return undefined; }
    });
}
interface Target { labels: { job: string; instance: string }; health: string; lastScrape: string }
async function prometheus(path: string) {
    const response = await fetch(`${prometheusUrl}${path}`, { signal: AbortSignal.any([signal, AbortSignal.timeout(5000)]) });
    assert.ok(response.ok, `Prometheus HTTP ${response.status}`);
    const body = await response.json();
    assert.equal(body.status, 'success');
    return body.data;
}
async function targetsFor(workers: Container[]) {
    const expected = workers.map((w) => `${Object.values(w.NetworkSettings.Networks)[0]!.IPAddress}:9464`).sort();
    return until('현재 Worker Target·Scrape 확인', async () => {
        const targets: Target[] = (await prometheus('/api/v1/targets?state=active')).activeTargets;
        const found = targets.filter((t) => t.labels.job === 'worker');
        if (JSON.stringify(found.map((t) => t.labels.instance).sort()) !== JSON.stringify(expected)) return;
        if (!found.every((t) => t.health === 'up' && workers.some((w) => Date.parse(t.lastScrape) >= Date.parse(w.State.StartedAt)))) return;
        if (!targets.some((t) => t.labels.job === 'api' && t.health === 'up')) return;
        return targets;
    }, 90_000);
}
const selector = '{__name__=~"async_job_.*|nodejs_event_loop_.*",job=~"api|worker"}';
async function prometheusSnapshot(path: string, workers: Container[], startWallMs: number) {
    const targets = await targetsFor(workers);
    const raw = await prometheus(`/api/v1/query?query=${encodeURIComponent(selector)}`);
    const active = new Set(targets.map((t) => t.labels.instance));
    await save(path, { targets, raw, current: raw.result.filter((r: { metric: { instance: string } }) => active.has(r.metric.instance)),
        range: await prometheus(`/api/v1/query_range?query=${encodeURIComponent(selector)}&start=${startWallMs / 1000}&end=${Date.now() / 1000}&step=15`),
        authoritative: false, note: '사망 Worker의 마지막 Counter·Scrape는 누락될 수 있습니다. 수락/완료 정산은 HTTP·DB·사건 로그로 판정합니다.' });
}
async function attempt(label: string, action: () => Promise<unknown>, errors: string[]) {
    try { await action(); }
    catch (error) { const message = `${label}: ${String(error)}`; errors.push(message); console.error(message); }
}

async function run(mode: Mode, repeat: number) {
    signal.throwIfAborted();
    const name = `r${repeat}-${mode}`;
    const path = join(directory, name);
    await mkdir(path);
    const data: Evidence = { requests: [], samples: [], events: [], before: [], after: [], workers: [], observations: {},
        collectionErrors: [], topologyVerified: false, finalTopologyVerified: false };
    const result: Record<string, unknown> = { name, mode, repeat, parameters: o };
    results.push(result);
    const cleanup = { succeeded: false, errors: [] as string[] };
    result.cleanup = cleanup;
    let origin = boundary();
    const elapsed = () => performance.now() - origin.monoMs;
    let apps: Container[] = [], workers: Container[] = [], victim: Container | undefined;
    let producer: ReturnType<typeof startProducer> | undefined;
    let injection: Promise<void> | undefined;
    let stats: ReturnType<typeof resources> | undefined;
    const followers: ReturnType<typeof followWorker>[] = [];
    const tasks: Promise<void>[] = [];
    let stop = false, targetId = '';
    let requiresUserDecision = false;
    let firstProcessing: JobRow | undefined;
    const collectError = (error: unknown) => {
        data.collectionErrors.push(String(error));
        controller.abort(error);
    };
    const mark = async (name: string, detail: Record<string, unknown> = {}, at = elapsed()) => {
        if (data.observations[name]) return;
        const observation: Observation = { ...detail, elapsedMs: at, confirmedMs: elapsed() };
        data.observations[name] = observation;
        await line(join(path, 'observations.jsonl'), { name, ...observation });
    };
    const sample = async (phase: string) => {
        const started = boundary();
        const [counts] = await orm!.em.getConnection().execute<Omit<Sample,
            'elapsedMs' | 'queryMs' | 'actualIntervalMs' | 'wallMs' | 'clockDifferenceMs' | 'phase' | 'dbClockOffsetMs'>[]>(`
            select count(*)::int as total,
                count(*) filter (where status = 'queued')::int as queued,
                count(*) filter (where status = 'processing')::int as processing,
                count(*) filter (where status in ('queued','processing'))::int as outstanding,
                count(*) filter (where status = 'succeeded')::int as succeeded,
                count(*) filter (where status = 'failed')::int as failed,
                (extract(epoch from clock_timestamp()) * 1000)::double precision as "dbWallMs",
                (select count(*)::int from pg_stat_activity where datname = current_database()) as connections,
                (select count(*)::int from pg_stat_activity where datname = current_database() and state = 'active') as "activeConnections"
            from jobs`);
        assert.ok(counts);
        const finished = boundary();
        const elapsedMs = started.monoMs - origin.monoMs;
        const previous = data.samples.at(-1);
        const row: Sample = { ...counts, elapsedMs, queryMs: finished.monoMs - started.monoMs,
            actualIntervalMs: previous ? elapsedMs - previous.elapsedMs : null, wallMs: started.wallMs,
            clockDifferenceMs: started.wallMs - origin.wallMs - elapsedMs,
            dbClockOffsetMs: counts.dbWallMs - (started.wallMs + finished.wallMs) / 2, phase };
        data.samples.push(row);
        await line(join(path, 'samples.jsonl'), row);
        return row;
    };
    const monitorTarget = async () => {
        let previous = '';
        while (!stop && !signal.aborted) {
            if (targetId) {
                const startedMs = elapsed();
                const found = await jobRow(targetId);
                if (found) {
                    const key = JSON.stringify(found.row);
                    if (key !== previous) {
                        await line(join(path, 'target-states.jsonl'), { readStartedMs: startedMs, observedMs: elapsed(),
                            queryMs: elapsed() - startedMs, previousObservationMs: data.observations['target-db-observed']?.elapsedMs ?? null, ...found });
                        previous = key;
                    }
                    data.observations['target-db-observed'] = { elapsedMs: elapsed() };
                    const events = data.events.filter((e) => e.jobId === targetId);
                    for (const [event, attemptNumber, label] of [
                        ['job.recovered', 1, 'recovered'], ['job.claimed', 2, 'second-claim'],
                        ['job.handler_started', 2, 'second-handler'], ['job.succeeded', mode === 'failure' ? 2 : 1, 'target-succeeded'],
                    ] as const) {
                        const entry = events.find((e) => e.event === event && e.attempt === attemptNumber);
                        if (entry) {
                            const age = firstProcessing?.processing_started_at ? await orm!.em.getConnection().execute<{ ageMs: number }[]>(
                                'select (extract(epoch from (clock_timestamp() - ?::timestamptz)) * 1000)::double precision as "ageMs"',
                                [firstProcessing.processing_started_at]) : [];
                            await mark(label, { event: entry, dbAtConfirmation: found.row, firstClaimAgeMsAtConfirmation: age[0]?.ageMs,
                                observationDelayMs: null, note: '로그 Flush·전송 지연은 미상이며 관측 시각은 사건 발생 시각이 아닙니다' }, entry.observedMs);
                        }
                    }
                    if (found.row.status === 'processing' && found.row.attempt === 2) await mark('second-processing', {
                        row: found.row, readStartedMs: startedMs, observedMs: elapsed(), queryMs: elapsed() - startedMs });
                    const killed = data.observations['kill-confirmed'];
                    if (killed && !data.observations['survivor-before-recovery'] && !data.observations['recovered']) {
                        const other = data.events.find((e) => e.event === 'job.succeeded' && e.jobId !== targetId
                            && postKillRequest(data.requests, e, killed.elapsedMs)
                            && e.workerId !== firstProcessing?.worker_id);
                        if (other && found.row.status === 'processing' && found.row.attempt === 1 && found.row.worker_id === firstProcessing?.worker_id) {
                            const completed = await jobRow(other.jobId!);
                            // 성공 행을 확인한 뒤 대상 잔류를 다시 읽어 완료→대상 첫 processing의 순서를 확보한다.
                            const retained = completed ? await jobRow(targetId) : undefined;
                            const request = completed && retained ? survivorSuccessRequest(data, other, retained.row, completed.row) : undefined;
                            if (request) await mark('survivor-before-recovery', { request, event: other, completed: completed!.row, retainedTarget: retained!.row,
                                targetAgeMs: retained!.ageMs, readStartedMs: startedMs });
                        }
                    }
                }
            }
            await delay(200);
        }
    };
    const inject = async () => {
        const request = await producer!.target;
        assert.equal(category(request), 'accepted', '대상 Job이 201로 수락되지 않아 조건이 성립하지 않습니다');
        targetId = request.id!;
        await mark('target-accepted', { request });
        const confirmed = await until('대상 attempt=1·Handler 시작·소유 컨테이너 실행', async () => {
            const found = await jobRow(targetId);
            assert.ok(found && found.row.status !== 'succeeded' && found.row.status !== 'failed', '대상 Job이 확인 전에 이미 종료되었습니다');
            const handler = data.events.find((e) => e.event === 'job.handler_started' && e.jobId === targetId && e.attempt === 1);
            if (found.row.status !== 'processing' || found.row.attempt !== 1 || !handler) return;
            const owner = data.workers.find((w) => w.workerId === found.row.worker_id && w.containerId === handler.containerId);
            assert.ok(owner && handler.workerId === owner.workerId, '대상 DB·Handler 로그·Worker 소유권 불일치');
            const current = (await inspect()).find((c) => c.Id === owner.containerId);
            assert.ok(current?.State.Running && current.RestartCount === 0, '대상 컨테이너가 실행 중이 아닙니다');
            return { found, handler, current };
        }, 20_000);
        firstProcessing = confirmed.found.row;
        await mark('handler-started', { row: firstProcessing, event: confirmed.handler,
            dbAgeMsAtConfirmation: confirmed.found.ageMs, observationDelayMs: null }, confirmed.handler.observedMs);
        if (mode === 'normal') { await mark('control-confirmed', { row: firstProcessing }); return; }
        victim = confirmed.current;
        const checkedAt = elapsed();
        const fresh = await jobRow(targetId);
        assert.deepEqual(fresh?.row, firstProcessing, 'SIGKILL 직전 대상 상태·소유권이 변경되었습니다');
        assert.ok(fresh && fresh.ageMs + elapsed() - checkedAt < 2000, '대상 Claim 이후 2초를 초과해 장애 주입이 늦었습니다');
        await mark('kill-requested', { containerId: victim.Id, workerId: firstProcessing.worker_id, row: fresh.row, ageMs: fresh.ageMs });
        assert.ok(fresh.ageMs + elapsed() - checkedAt < 2000, '근거 저장 중 주입이 지연되어 SIGKILL을 취소합니다');
        assert.ok(!data.events.some((e) => e.jobId === targetId && e.attempt === 1 && e.event === 'job.handler_finished'),
            '대상 Handler가 이미 종료되어 SIGKILL을 취소합니다');
        await docker(['kill', '--signal', 'SIGKILL', victim.Id]);
        const current = await inspect();
        const dead = current.find((c) => c.Id === victim!.Id)!;
        assert.ok(!dead.State.Running && dead.State.ExitCode === 137 && !dead.State.OOMKilled && dead.RestartCount === 0,
            '실제 SIGKILL 종료 상태가 아닙니다');
        const survivors = current.filter((c) => service(c) === 'worker' && c.State.Running);
        assert.equal(survivors.length, 2, 'SIGKILL 뒤 생존 Worker가 2개가 아닙니다');
        for (const c of survivors) assert.equal(c.State.StartedAt, workers.find((w) => w.Id === c.Id)?.State.StartedAt, '생존 Worker가 재시작되었습니다');
        await mark('kill-confirmed', { victim: metadata([dead])[0], survivors: metadata(survivors) });
        const retained = await jobRow(targetId);
        assert.deepEqual(retained?.row, firstProcessing, 'SIGKILL 직후 대상 Job 잔류 상태가 변경되었습니다');
        await mark('retained-after-kill', { row: retained!.row, ageMs: retained!.ageMs });
        console.log(`${name}: 대상 Handler 실행 중 SIGKILL, ExitCode 137·생존 Worker 2개 확인`);
    };
    try {
        data.before = await preflight();
        await save(join(path, 'before.json'), data.before);
        await compose(['up', '-d', '--no-build', '--no-deps', '--scale', `worker=${o.workers}`, 'api', 'worker'], o.limit);
        const apiUrl = `http://127.0.0.1:${await port('api', '3000')}`;
        await ready(`${apiUrl}/health`);
        const containers = await inspect();
        workers = containers.filter((c) => service(c) === 'worker');
        apps = containers.filter((c) => ['api', 'worker'].includes(service(c)!));
        assert.equal(workers.length, o.workers);
        assert.equal(apps.length, o.workers + 1);
        const processes = [];
        for (const c of apps) {
            assert.ok(c.State.Running && c.RestartCount === 0);
            assert.equal(c.Image, imageId, '동일 이미지 조건이 아닙니다');
            assert.ok(c.Config.Env.includes('ENABLE_BACKPRESSURE=true') && c.Config.Env.includes(`MAX_OUTSTANDING_JOBS=${o.limit}`));
            const entrypoint = service(c) === 'worker' ? 'src/worker/main.ts' : 'src/api/server.ts';
            assert.deepEqual(c.Config.Cmd, ['node', entrypoint]);
            const top = await docker(['top', c.Id, '-eo', 'pid,ppid,args']);
            assert.equal(top.trim().split('\n').slice(1).filter((l) => /\bnode\s/.test(l)).length, 1);
            processes.push({ containerId: c.Id, top });
            const actual = await docker(['exec', c.Id, 'sha256sum', ...sourceFiles.filter((f) => f.startsWith('src/') || f.startsWith('package'))]);
            for (const row of actual.trim().split('\n')) {
                const [hash, file] = row.trim().split(/\s+/);
                assert.equal(hash, hashes[file!], `이미지와 현재 소스 불일치: ${file}`);
            }
        }
        for (const c of workers) {
            const workerId = await until('Worker UUID 확인', async () => {
                const logs = (await docker(['logs', c.Id])).split('\n').filter(Boolean).map((l) => JSON.parse(l));
                return logs.find((l) => l.event === 'worker.started')?.workerId as string | undefined;
            });
            data.workers.push({ containerId: c.Id, workerId });
        }
        assert.equal(new Set(data.workers.map((w) => w.workerId)).size, o.workers);
        const initialTargets = await targetsFor(workers);
        await save(join(path, 'ready.json'), { containers: metadata(containers), workers: data.workers, processes, targets: initialTargets, apiUrl });
        data.topologyVerified = true;
        origin = boundary();
        await save(join(path, 'clock.json'), { origin, observer: 'performance.now()', dbPollMs: 200, sampleIntervalMs: o.intervalMs,
            logObservation: '최초 Stream 수신 시각. Flush·전송 지연 상한은 미상. Wall Clock 차감으로 정밀 Recovery 시간을 산출하지 않음' });
        for (const c of workers) followers.push(followWorker(c, path, elapsed, data.events, collectError));
        await sample('start');
        stats = resources(containers.filter((c) => service(c) !== 'migrate'), path, elapsed, collectError);
        tasks.push((async () => {
            while (!stop && !signal.aborted) {
                await delay(o.intervalMs);
                if (!stop && !signal.aborted) await sample('periodic');
            }
        })().catch(collectError));
        tasks.push(monitorTarget().catch(collectError));
        console.log(`${name}: Worker ${o.workers}개, 일반 부하 ${o.rate} jobs/s × ${o.seconds}초, 별도 대상 1개 시작`);
        producer = startProducer(o, apiUrl, origin.monoMs, signal, (r) => line(join(path, 'requests.jsonl'), r), collectError);
        data.requests = producer.requests;
        injection = inject().catch((error) => { data.executionError = String(error); controller.abort(error); });
        result.dispatch = await producer.done;
        await mark('load-end');
        await sample('load-end');
        await save(join(path, 'load-end-jobs.json'), await allRows());
        await injection;
        signal.throwIfAborted();
        const deadline = performance.now() + o.drainSeconds * 1000;
        await until('수락 Job 최종 정산', async () => {
            const rows = await allRows();
            return rows.every((r) => !['queued', 'processing'].includes(r.status)) ? true : undefined;
        }, Math.max(1, deadline - performance.now()));
        // DB Commit과 Best-effort 로그 도착을 구분한다. 누락이면 평가에서 근거 부족으로 남긴다.
        const logDeadline = performance.now() + 3000;
        while (!data.observations['target-succeeded'] && performance.now() < logDeadline && !signal.aborted) await delay(100);
        await mark('drain-end');
        data.after = await allRows();
        const current = await inspect();
        await save(join(path, 'containers-final.json'), metadata(current));
        assert.equal(current.filter((c) => service(c) === 'worker').length, o.workers, '예상 외 Worker 컨테이너 추가·제거');
        assert.equal(current.filter((c) => service(c) === 'worker' && c.State.Running).length, mode === 'failure' ? 2 : o.workers);
        for (const original of apps) {
            const found = current.find((c) => c.Id === original.Id);
            assert.ok(found && found.State.StartedAt === original.State.StartedAt && found.RestartCount === 0, '프로세스 교체·재시작 관측');
            assert.equal(found.State.Running, original.Id !== victim?.Id, '예상 외 컨테이너 실행 상태');
            if (original.Id === victim?.Id) assert.equal(found.State.ExitCode, 137);
        }
        data.finalTopologyVerified = true;
        await prometheusSnapshot(join(path, 'prometheus.json'), current.filter((c) => service(c) === 'worker' && c.State.Running), origin.wallMs);
        await sample('completed');
    } catch (error) {
        data.executionError ??= String(error);
        controller.abort(error);
        console.error(`${name}: ${String(error)}`);
    } finally {
        // 모든 단계는 독립적으로 시도한다. 저장 실패도 stop/down을 건너뛰지 않는다.
        stop = true;
        if (producer) await attempt('발송 요청 정산', async () => { result.dispatch = await producer!.done; }, data.collectionErrors);
        if (injection) await attempt('장애 주입 정산', () => injection!, data.collectionErrors);
        // 중단 시 자원/로그 수집 종료를 기다리는 동안 새 Job을 계속 Claim하지 않게 먼저 종료 요청한다.
        await attempt('API·Worker 종료', () => compose(['stop', 'api', 'worker']), cleanup.errors);
        await Promise.all(tasks);
        if (stats) await attempt('자원 수집 종료', async () => {
            const summary = await stats!.stop();
            result.resources = summary;
            for (const c of apps) if (!summary.perContainer[c.Id]) data.collectionErrors.push(`${c.Id}: Docker Stats 표본 누락`);
        }, data.collectionErrors);
        for (const follower of followers) await attempt('로그 수집 종료', () => follower.stop(), data.collectionErrors);
        await attempt('최종 DB 조회', async () => { data.after = await allRows(); }, data.collectionErrors);
        const report = evaluate(o, mode, data);
        requiresUserDecision = report.scenario.suitability.requiresUserDecision;
        Object.assign(result, report, { observations: data.observations });
        await attempt('요청 복구 저장', () => save(join(path, 'requests.json'), data.requests), cleanup.errors);
        await attempt('사건 복구 저장', () => save(join(path, 'events.json'), data.events), cleanup.errors);
        await attempt('최종 행 저장', () => save(join(path, 'jobs.json'), data.after), cleanup.errors);
        await attempt('실행 근거 저장', () => save(join(path, 'evidence.json'), data), cleanup.errors);
        await attempt('서비스 로그 저장', async () => { await writeFile(join(path, 'services.log'), await compose(['logs', '--no-color', '--no-log-prefix'])); }, cleanup.errors);
        await attempt('종료 컨테이너 저장', async () => { await save(join(path, 'containers-stopped.json'), metadata(await inspect())); }, cleanup.errors);
        await attempt('종료 DB 저장', async () => { await save(join(path, 'stopped-jobs.json'), await allRows()); }, cleanup.errors);
        await attempt('반복 컨테이너 제거', () => compose(['rm', '-f', 'api', 'worker']), cleanup.errors);
        cleanup.succeeded = cleanup.errors.length === 0;
        await attempt('반복 결과 저장', () => save(join(path, 'result.json'), result), cleanup.errors);
        cleanup.succeeded = cleanup.errors.length === 0;
        await attempt('반복 정리 결과 저장', () => save(join(path, 'cleanup.json'), cleanup), cleanup.errors);
        cleanup.succeeded = cleanup.errors.length === 0;
        await attempt('전체 결과 저장', () => save(join(directory, 'summary.json'), results), cleanup.errors);
        cleanup.succeeded = cleanup.errors.length === 0;
        if (!report.scenario.valid || requiresUserDecision || report.functional.status !== 'passed' || !report.measurement.valid || !cleanup.succeeded) process.exitCode = 1;
        console.log(`${name}: 시나리오=${report.scenario.valid}, 기능=${report.functional.status}, 측정=${report.measurement.valid}, 정리=${cleanup.succeeded}`);
    }
    return requiresUserDecision;
}

try {
    await mkdir(resolve('artifacts/failure-load'), { recursive: true });
    await mkdir(lock);
    ownsLock = true;
    assert.equal((await compose(['ps', '--all', '--quiet'])).trim(), '', `${project} 컨테이너가 이미 있어 변경 없이 중단합니다`);
    await mkdir(directory, { recursive: true });
    await writeFile(join(directory, 'environment.json'), JSON.stringify({ at: new Date().toISOString(), project, dbName,
        parameters: o, order: runOrder(o), node: process.version,
        revision: (await execute('git', ['rev-parse', 'HEAD'])).stdout.trim(),
        branch: (await execute('git', ['branch', '--show-current'])).stdout.trim() }, null, 2) + '\n', { flag: 'wx' });
    ownsEnvironment = true;
    console.log(`실험 자료: ${directory}`);
    await compose(['up', '-d', '--no-build', 'postgres', 'migrate'], o.limit);
    const migration = (await compose(['ps', '--all', '--quiet', 'migrate'])).trim();
    assert.equal((await docker(['wait', migration])).trim(), '0', 'Migration 실패');
    imageId = (await inspect()).find((c) => c.Id === migration)!.Image;
    orm = await MikroORM.init({ ...config, host: '127.0.0.1', port: await port('postgres', '5432'), dbName,
        contextName: project, pool: { min: 1, max: 4 },
        driverOptions: { connectionTimeoutMillis: 5000, application_name: 'failure-load-observer', options: '-c statement_timeout=10000' } });
    await save(join(directory, 'preflight.json'), await allRows());
    const initial = await preflight();
    await compose(['up', '-d', '--no-build', '--no-deps', 'prometheus']);
    prometheusUrl = `http://127.0.0.1:${await port('prometheus', '9090')}`;
    await ready(`${prometheusUrl}/-/ready`);
    const toolFiles = (await readdir('experiments/failure-load')).filter((f) => /\.(ts|yml)$/.test(f)).map((f) => join('experiments/failure-load', f));
    hashes = Object.fromEntries(await Promise.all([...sourceFiles, ...toolFiles].map(async (f) =>
        [f, createHash('sha256').update(await readFile(f)).digest('hex')])));
    const info = JSON.parse(await docker(['info', '--format', '{{json .}}']));
    await save(join(directory, 'environment.json'), { at: new Date().toISOString(), project, dbName, parameters: o, order: runOrder(o),
        revision: (await execute('git', ['rev-parse', 'HEAD'])).stdout.trim(), branch: (await execute('git', ['branch', '--show-current'])).stdout.trim(),
        workingTree: (await execute('git', ['status', '--short'])).stdout, hashes, imageId, node: process.version,
        host: { platform: platform(), release: release(), cpu: cpus()[0]?.model, logicalCpus: cpus().length, memoryBytes: totalmem() },
        docker: { version: info.ServerVersion, os: info.OperatingSystem, kernel: info.KernelVersion, logicalCpus: info.NCPU, memoryBytes: info.MemTotal },
        compose: (await docker(['compose', 'version', '--short'])).trim(), initialRows: initial.length,
        observation: { observerPoolMax: 4, dbPollMs: 200, intervalMs: o.intervalMs, scrapeIntervalMs: 15_000,
            killMaxClaimAgeMs: 2000, statementTimeoutMs: 10_000 },
        settings: await orm.em.getConnection().execute("select name, setting, unit from pg_settings where name in ('max_connections','shared_buffers','work_mem') order by name"),
        containers: metadata(await inspect()) });
    for (const item of runOrder(o)) {
        signal.throwIfAborted();
        if (await run(item.mode, item.repeat)) throw new Error('정상 3개 Worker의 부하 조건이 부적합하거나 판단 보류입니다. 사용자 판단 전에는 다음 반복·본 실측을 진행하지 않습니다');
    }
} catch (error) {
    console.error(String(error));
    process.exitCode = 1;
    if (ownsEnvironment) await attempt('실패 근거 저장', () => save(join(directory, 'failure.json'), { error: String(error), results }), []);
} finally {
    const errors: string[] = [];
    if (ownsEnvironment) {
        await attempt('최종 API·Worker 종료', () => compose(['stop', 'api', 'worker']), errors);
        await attempt('최종 컨테이너 근거', async () => { await save(join(directory, 'containers-final.json'), metadata(await inspect())); }, errors);
        await attempt('최종 DB 근거', async () => { if (orm) await save(join(directory, 'final-jobs.json'), await allRows()); }, errors);
        await attempt('최종 로그 근거', async () => { await writeFile(join(directory, 'services.log'), await compose(['logs', '--no-color', '--no-log-prefix'])); }, errors);
        await attempt('DB 연결 종료', async () => { if (orm) await orm.close(); }, errors);
        await attempt('전용 컨테이너·네트워크 종료', () => compose(['down']), errors);
        await attempt('전체 결과 저장', () => save(join(directory, 'summary.json'), results), errors);
    }
    if (ownsLock) await attempt('실행 잠금 해제', () => rm(lock, { recursive: true }), errors);
    if (ownsEnvironment) await attempt('최종 정리 결과 저장', () => save(join(directory, 'cleanup.json'), { succeeded: errors.length === 0, errors }), errors);
    if (errors.length) process.exitCode = 1;
    process.off('SIGINT', onSignal);
    process.off('SIGTERM', onSignal);
}
