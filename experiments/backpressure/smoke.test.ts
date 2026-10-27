import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { promisify } from 'node:util';
import { MikroORM } from '@mikro-orm/postgresql';
import config from '../../src/db/mikro-orm.config.ts';

const execute = promisify(execFile);
const project = 'async-job-backpressure';
const args = ['compose', '-p', project, '-f', 'compose.yml', '-f', 'experiments/backpressure/compose.yml'];
const dockerEnv = { ...process.env, DB_NAME: 'async_job_backpressure' };
const docker = async (...command: string[]) => (await execute('docker', command, { env: dockerEnv, timeout: 90_000 })).stdout;
const compose = (...command: string[]) => docker(...args, ...command);
const runArgs = (out: string) => ['--env-file=.env', 'experiments/backpressure/run.ts', '--repeats=1', '--seconds=2',
    '--rate=10', '--duration-ms=100', '--limit=2', '--interval-ms=100', `--out=${out}`];
const read = async (path: string) => JSON.parse(await readFile(path, 'utf8'));
async function connect() {
    return MikroORM.init({ ...config, dbName: 'async_job_backpressure', host: '127.0.0.1',
        port: Number((await compose('port', 'postgres', '5432')).trim().split(':').at(-1)), contextName: randomUUID() });
}

test('전용 Compose에서 정상 ON/OFF·기존 프로젝트 거부·미완료 사전 검사·중단 자료 보존을 확인한다', { timeout: 360_000 }, async () => {
    assert.equal((await compose('ps', '--all', '--quiet')).trim(), '', '같은 실험 프로젝트가 이미 있습니다');
    const root = resolve(`artifacts/backpressure/smoke-${new Date().toISOString().replaceAll(':', '-')}`);
    await mkdir(root, { recursive: true });
    let orm: MikroORM | undefined;
    const fixtureIds = [randomUUID(), randomUUID()];
    const interruptedIds: string[] = [];
    try {
        const normal = join(root, 'normal');
        await execute(process.execPath, runArgs(normal), { timeout: 180_000 });
        const summary = await read(join(normal, 'summary.json'));
        assert.equal(summary.length, 2);
        assert.ok(summary.every((r: { valid: boolean }) => r.valid));
        assert.ok(summary.every((r: { cleanup: { succeeded: boolean; errors: string[] } }) => r.cleanup.succeeded && r.cleanup.errors.length === 0));
        assert.deepEqual(await read(join(normal, 'cleanup.json')), { succeeded: true, errors: [] });
        assert.equal(summary[0].http.sent, 20);
        assert.equal(summary[0].http.accepted, 20);
        assert.ok(summary[1].http.rejected > 0);
        assert.ok(summary[1].sampling.observedMaxOutstanding <= 2);
        const samples = (await readFile(join(normal, 'r1-on', 'samples.jsonl'), 'utf8')).trim().split('\n').map((line) => JSON.parse(line));
        assert.ok(samples.some((s) => s.phase === 'load-end') && samples.some((s) => s.phase === 'completed'));
        assert.equal((await compose('ps', '--all', '--quiet')).trim(), '');

        await compose('up', '-d', '--no-build', '--wait', 'postgres');
        const existing = (await compose('ps', '--quiet', 'postgres')).trim();
        await assert.rejects(execute(process.execPath, runArgs(join(root, 'existing')), { timeout: 30_000 }),
            (error: unknown) => error instanceof Error && 'stderr' in error && String(error.stderr).includes('컨테이너가 이미 있어'));
        assert.equal((await compose('ps', '--quiet', 'postgres')).trim(), existing, '거부한 도구가 기존 컨테이너에 간섭하면 안 됩니다');
        orm = await connect();
        await orm.em.getConnection().execute(`insert into jobs (id, type, payload, status, attempt, max_attempts, worker_id, processing_started_at)
            values (?, 'slow', '{"durationMs":1000}', 'queued', 0, 3, null, null),
                   (?, 'slow', '{"durationMs":1000}', 'processing', 1, 3, 'preserved-worker', now() - interval '31 seconds')`, fixtureIds);
        const snapshot = () => orm!.em.getConnection().execute('select to_jsonb(j) as row from jobs j where id in (?, ?) order by id', fixtureIds);
        const before = await snapshot();
        await orm.close(); orm = undefined;
        await compose('down');
        const since = new Date().toISOString();
        const preflight = join(root, 'preflight');
        await assert.rejects(execute(process.execPath, runArgs(preflight), { timeout: 90_000 }),
            (error: unknown) => error instanceof Error && 'stderr' in error && String(error.stderr).includes('실험 전용 DB에 미완료 Job'));
        assert.deepEqual((await read(join(preflight, 'failure.json'))).results, []);
        const events = await docker('events', '--since', since, '--until', new Date().toISOString(), '--filter', 'event=create',
            '--filter', `label=com.docker.compose.project=${project}`, '--filter', 'label=com.docker.compose.service=worker', '--format', '{{json .}}');
        assert.equal(events.trim(), '', '사전 검사 실패 시 Worker를 생성하면 안 됩니다');
        await compose('up', '-d', '--no-build', '--wait', 'postgres');
        orm = await connect();
        assert.deepEqual(await snapshot(), before, 'Ready·Stale 행의 모든 필드가 보존되어야 합니다');
        await orm.em.getConnection().execute('delete from jobs where id in (?, ?)', fixtureIds);
        const baseline = new Set((await orm.em.getConnection().execute<{ id: string }[]>('select id from jobs')).map((r) => r.id));
        await orm.close(); orm = undefined;
        await compose('down');

        const interrupted = join(root, 'interrupted');
        const child = spawn(process.execPath, [...runArgs(interrupted), '--modes=off', '--seconds=10', '--duration-ms=1000'], { stdio: ['ignore', 'pipe', 'pipe'] });
        let output = '';
        child.stdout.on('data', (chunk) => { output += String(chunk); });
        child.stderr.on('data', (chunk) => { output += String(chunk); });
        let exited = false;
        const exit = new Promise<number | null>((resolve) => child.on('exit', (code) => { exited = true; resolve(code); }));
        try {
            const deadline = performance.now() + 90_000;
            while (true) {
                assert.ok(!exited, output);
                try {
                    const requests = (await readFile(join(interrupted, 'r1-off', 'requests.jsonl'), 'utf8')).trim().split('\n');
                    if (requests.length >= 5) break;
                } catch { /* 준비·첫 송신을 기다린다. */ }
                assert.ok(performance.now() < deadline, `중단 Smoke 준비 시간 초과: ${output}`);
                await delay(100);
            }
            child.kill('SIGTERM');
            assert.equal(await exit, 1, output);
        } finally { if (!exited) { child.kill('SIGTERM'); await exit; } }
        const failure = await read(join(interrupted, 'failure.json'));
        assert.equal(failure.results[0].valid, false);
        assert.deepEqual(failure.results[0].cleanup, { succeeded: true, errors: [] }, '측정 중단과 정상 종료 처리를 구분해야 합니다');
        const requests = (await readFile(join(interrupted, 'r1-off', 'requests.jsonl'), 'utf8')).trim().split('\n').map((line) => JSON.parse(line));
        assert.ok(requests.every((r) => r.finishedMs >= r.sentMs));
        const global = await read(join(interrupted, 'final-global-counts.json'));
        assert.ok(global.outstanding > 0, '중단 시 미완료 상태를 보존해야 합니다');
        assert.ok((await readFile(join(interrupted, 'services.log'), 'utf8')).length > 0);
        interruptedIds.push(...(await read(join(interrupted, 'final-global-jobs.json'))).filter((r: { id: string }) => !baseline.has(r.id)).map((r: { id: string }) => r.id));
        assert.equal((await compose('ps', '--all', '--quiet')).trim(), '');
        await writeFile(join(root, 'verification.json'), JSON.stringify({ valid: true, normal, preflight, interrupted,
            preservedFixtureIds: fixtureIds, interruptedIds, assertions: ['정상 ON/OFF', '기존 프로젝트 무간섭', 'Ready·Stale 전체 행 보존', 'Worker 생성 전 거부', 'SIGTERM 후 요청 정산·자료·미완료 보존', '볼륨 보존·컨테이너 정리'] }, null, 2));
        console.log(`Smoke 근거: ${root}`);
    } finally {
        // 이 테스트가 만든 Ready·Stale·중단 Job ID만 정리한다. 정상 실행의 완료 행은 남긴다.
        await compose('up', '-d', '--no-build', '--wait', 'postgres');
        if (!orm) orm = await connect();
        const ids = [...fixtureIds, ...interruptedIds];
        try { await orm.em.getConnection().execute(`delete from jobs where id in (${ids.map(() => '?').join(',')})`, ids); }
        finally { await orm.close(); await compose('down'); }
    }
});

test('측정 성공 뒤 실제 로그 파일 저장이 실패하면 정리 실패를 별도로 기록하고 나머지 정리를 계속한다', { timeout: 120_000 }, async () => {
    assert.equal((await compose('ps', '--all', '--quiet')).trim(), '', '같은 실험 프로젝트가 이미 있습니다');
    const root = resolve(`artifacts/backpressure/cleanup-failure-${new Date().toISOString().replaceAll(':', '-')}`);
    const path = join(root, 'r1-off');
    const child = spawn(process.execPath, [...runArgs(root), '--modes=off', '--seconds=1', '--rate=1', '--duration-ms=1'], { stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '';
    child.stdout.on('data', (chunk) => { output += String(chunk); });
    child.stderr.on('data', (chunk) => { output += String(chunk); });
    let exited = false;
    const exit = new Promise<number | null>((resolve) => child.on('exit', (code) => { exited = true; resolve(code); }));
    try {
        const deadline = performance.now() + 90_000;
        while (true) {
            assert.ok(!exited, output);
            try { await readFile(join(path, 'before.json')); break; } catch { /* 반복 디렉터리 생성 이후를 기다린다. */ }
            assert.ok(performance.now() < deadline, `종료 처리 실패 Smoke 준비 시간 초과: ${output}`);
            await delay(100);
        }
        // 동일 경로에 디렉터리를 두어 실제 writeFile이 EISDIR로 실패하게 한다.
        await mkdir(join(path, 'services.log'));
        assert.equal(await exit, 1, output);
        const result = await read(join(path, 'result.json'));
        assert.equal(result.valid, true, '측정 유효성은 정리 실패와 구분해야 합니다');
        assert.equal(result.http.accepted, 1);
        assert.equal(result.cleanup.succeeded, false);
        assert.equal(result.cleanup.errors.length, 1);
        assert.match(result.cleanup.errors[0], /EISDIR/);
        assert.deepEqual((await read(join(root, 'summary.json')))[0], result);
        assert.deepEqual((await read(join(root, 'failure.json'))).results[0], result);
        assert.equal((await read(join(path, 'stopped-global-counts.json'))).outstanding, 0, '로그 저장 실패 뒤에도 DB 상태를 수집해야 합니다');
        assert.deepEqual(await read(join(root, 'cleanup.json')), { succeeded: true, errors: [] }, '반복 정리 실패와 최종 환경 정리 성공도 구분해야 합니다');
        assert.equal((await compose('ps', '--all', '--quiet')).trim(), '');
        console.log(`종료 처리 실패 근거: ${root}`);
    } finally {
        if (!exited) { child.kill('SIGTERM'); await exit; }
    }
});
