import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdir, readFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { MikroORM } from '@mikro-orm/postgresql';

import config from '../../src/db/mikro-orm.config.ts';
import { compose, dbName, docker, port, project } from './runtime.ts';

const read = async (path: string) => JSON.parse(await readFile(path, 'utf8'));
const root = resolve(`artifacts/failure-load/smoke-${Date.now()}`);
function start(out: string, args: string[] = []) {
    const child = spawn(process.execPath, ['--env-file=.env', 'experiments/failure-load/run.ts', `--out=${out}`,
        '--purpose=smoke', '--repeats=1', '--seconds=12', '--rate=6', '--target-at-seconds=1', '--limit=8', '--interval-ms=500', '--drain-seconds=90', ...args],
    { stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '', exited = false;
    child.stdout.on('data', (chunk) => { output += String(chunk); });
    child.stderr.on('data', (chunk) => { output += String(chunk); });
    const exit = new Promise<number | null>((resolve, reject) => {
        child.on('error', reject);
        child.on('exit', (code) => { exited = true; resolve(code); });
    });
    return { child, exit, output: () => output, exited: () => exited };
}
async function waitFor(label: string, read: () => Promise<boolean>, p: ReturnType<typeof start>) {
    const deadline = performance.now() + 120_000;
    while (true) {
        assert.ok(!p.exited(), p.output());
        if (await read()) return;
        assert.ok(performance.now() < deadline, `${label}: ${p.output()}`);
        await delay(100);
    }
}
const exists = async (file: string) => { try { await readFile(file); return true; } catch { return false; } };
async function connect() {
    return MikroORM.init({ ...config, host: '127.0.0.1', port: await port('postgres', '5432'), dbName,
        contextName: `failure-load-smoke-${randomUUID()}`, pool: { min: 1, max: 2 } });
}
const containers = async () => (await docker(['ps', '-a', '--format', '{{.ID}} {{.Names}} {{.State}} {{.Image}}'])).trim().split('\n').sort();

test('실제 정상·SIGKILL Smoke에서 지속 유입·생존 처리·30초 Stale Recovery·재Claim·정산과 환경 무간섭을 확인한다', { timeout: 240_000 }, async () => {
    assert.equal((await compose(['ps', '--all', '--quiet'])).trim(), '', '같은 실험 프로젝트가 이미 있습니다');
    const before = await containers();
    const out = join(root, 'paths');
    const p = start(out);
    try {
        assert.equal(await p.exit, 0, p.output());
        const results = await read(join(out, 'summary.json'));
        assert.deepEqual(results.map((r: { mode: string }) => r.mode), ['normal', 'failure']);
        for (const r of results) {
            assert.equal(r.scenario.valid, true, JSON.stringify(r));
            assert.equal(r.functional.status, 'passed', JSON.stringify(r));
            assert.equal(r.measurement.valid, true, JSON.stringify(r));
            assert.equal(r.scenario.suitability.status, 'observation-only');
            assert.equal(r.completion.matches, true, '최종 전체 DB 성공 수 변화와 로그 관측 성공 수가 같아야 합니다');
            assert.equal(r.cleanup.succeeded, true);
            assert.equal(r.http.regular.planned, 72);
            assert.equal(r.http.target.planned, 1);
            assert.equal(r.http.target.accepted, 1);
            assert.equal(r.sampling.observedMaxOutstanding, 8);
            assert.equal(r.reconciliation.unknownResponseRows.length, 0);
            assert.ok(r.resources.perContainer && Object.keys(r.resources.perContainer).length >= 5);
            const path = join(out, r.name);
            const evidence = await read(join(path, 'evidence.json'));
            assert.ok(evidence.samples.every((s: { queryMs: number; connections: number }) => s.queryMs >= 0 && s.connections > 0));
            assert.equal(evidence.workers.length, 3);
            const id = r.http.target.ids[0];
            const target = evidence.after.find((j: { id: string }) => j.id === id);
            assert.equal(target.status, 'succeeded');
            assert.deepEqual(target.result, { durationMs: 10_000 });
            assert.equal(target.attempt, r.mode === 'normal' ? 1 : 2);
            if (r.mode === 'failure') {
                const observations = r.observations;
                assert.equal(observations['kill-confirmed'].victim.exitCode, 137);
                assert.equal(observations['kill-confirmed'].survivors.length, 2);
                assert.equal(observations['retained-after-kill'].row.attempt, 1);
                assert.equal(observations['survivor-before-recovery'].retainedTarget.status, 'processing');
                const proof = observations['survivor-before-recovery'];
                assert.equal(proof.request.kind, 'regular');
                assert.equal(proof.request.status, 201);
                assert.ok(proof.request.sentMs > observations['kill-confirmed'].elapsedMs, '장애 전에 존재할 수 없는 신규 요청이어야 합니다');
                assert.equal(proof.completed.id, proof.request.id);
                assert.ok(observations['kill-confirmed'].survivors.some((s: { id: string }) => s.id === proof.event.containerId));
                assert.equal(proof.completed.worker_id, proof.event.workerId);
                assert.ok(observations['survivor-before-recovery'].elapsedMs < observations.recovered.elapsedMs);
                assert.ok(observations.recovered.firstClaimAgeMsAtConfirmation >= 30_000, 'DB의 원래 processing_started_at 이후 실제 30초를 기다려야 합니다');
                assert.equal(observations['second-processing'].row.attempt, 2);
                assert.ok(observations['target-succeeded'].elapsedMs > observations['second-claim'].elapsedMs);
                assert.ok(r.http.regular.rejected > 0);
                const states = (await readFile(join(path, 'target-states.jsonl'), 'utf8')).trim().split('\n').map((l) => JSON.parse(l));
                assert.ok(states.some((s) => s.row.status === 'processing' && s.row.attempt === 2));
                const final = await read(join(path, 'containers-final.json'));
                assert.equal(final.filter((c: { service: string; running: boolean }) => c.service === 'worker' && c.running).length, 2);
                assert.equal(final.find((c: { id: string }) => c.id === observations['kill-confirmed'].victim.id).restartCount, 0);
            }
        }
        assert.deepEqual(await read(join(out, 'cleanup.json')), { succeeded: true, errors: [] });
        assert.deepEqual(await containers(), before, '기존 Compose 프로젝트·컨테이너 상태가 변경되면 안 됩니다');
        console.log(`정상·장애 Smoke 근거: ${out}`);
        // 본 실험 기준을 만족시킬 관측 구간이 없으면 사용자 판단 전에 장애 반복으로 넘어가지 않는다.
        const gateOut = join(root, 'main-gate');
        const gate = start(gateOut, ['--purpose=main']);
        try {
            assert.equal(await gate.exit, 1, gate.output());
            const [normal, ...rest] = await read(join(gateOut, 'summary.json'));
            assert.equal(normal.scenario.valid, false);
            assert.equal(normal.scenario.suitability.status, 'inconclusive');
            assert.equal(normal.scenario.suitability.requiresUserDecision, true);
            assert.equal(normal.functional.status, 'passed');
            assert.equal(normal.measurement.valid, true);
            assert.equal(rest.length, 0, '사용자 판단 전에 다음 장애 조건을 실행하면 안 됩니다');
            assert.match((await read(join(gateOut, 'failure.json'))).error, /사용자 판단/);
            assert.equal((await compose(['ps', '--all', '--quiet'])).trim(), '');
            assert.deepEqual(await containers(), before);
        } finally { if (!gate.exited()) { gate.child.kill('SIGTERM'); await gate.exit; } }
    } finally { if (!p.exited()) { p.child.kill('SIGTERM'); await p.exit; } }
});

test('기존 실행 거부·Ready/Stale 전체 행 보존과 Worker 생성 전 사전 거부를 확인한다', { timeout: 120_000 }, async () => {
    assert.equal((await compose(['ps', '--all', '--quiet'])).trim(), '');
    let orm: MikroORM | undefined;
    const ids = [randomUUID(), randomUUID()];
    try {
        await compose(['up', '-d', '--no-build', '--wait', 'postgres']);
        const existing = (await compose(['ps', '--quiet', 'postgres'])).trim();
        const p = start(join(root, 'existing'));
        assert.equal(await p.exit, 1, p.output());
        assert.match(p.output(), /컨테이너가 이미 있어/);
        assert.equal((await compose(['ps', '--quiet', 'postgres'])).trim(), existing);
        orm = await connect();
        await orm.em.getConnection().execute(`insert into jobs (id, type, payload, status, attempt, max_attempts, worker_id, processing_started_at)
            values (?, 'slow', '{"durationMs":1000}', 'queued', 0, 3, null, null),
                   (?, 'slow', '{"durationMs":1000}', 'processing', 1, 3, 'preserved-worker', now() - interval '31 seconds')`, ids);
        const snapshot = () => orm!.em.getConnection().execute('select to_jsonb(j) as row from jobs j order by id');
        const before = await snapshot();
        await orm.close(); orm = undefined;
        await compose(['down']);
        const since = new Date().toISOString();
        const out = join(root, 'preflight');
        const denied = start(out);
        assert.equal(await denied.exit, 1, denied.output());
        assert.match(denied.output(), /미완료 Job/);
        assert.deepEqual(await read(join(out, 'summary.json')), []);
        const environment = await read(join(out, 'environment.json'));
        assert.equal(environment.parameters.rate, 6, '사전 거부 때도 재현 옵션을 남겨야 합니다');
        assert.equal(environment.project, project);
        const events = await docker(['events', '--since', since, '--until', new Date().toISOString(), '--filter', 'event=create',
            '--filter', `label=com.docker.compose.project=${project}`, '--filter', 'label=com.docker.compose.service=worker', '--format', '{{json .}}']);
        assert.equal(events.trim(), '', '미완료 Job 거부 전에 Worker를 생성하면 안 됩니다');
        await compose(['up', '-d', '--no-build', '--wait', 'postgres']);
        orm = await connect();
        assert.deepEqual(await snapshot(), before, '기존 완료 행·Ready·Stale 행의 모든 필드가 보존되어야 합니다');
        console.log(`사전 거부 근거: ${out}`);
    } finally {
        if (!orm) { await compose(['up', '-d', '--no-build', '--wait', 'postgres']); orm = await connect(); }
        try { await orm.em.getConnection().execute('delete from jobs where id in (?, ?)', ids); }
        finally { await orm.close(); await compose(['down']); }
    }
});

test('실제 결과·로그 저장 실패에도 후속 정리를 수행하고 SIGTERM은 요청·잔여 행·근거를 보존한다', { timeout: 240_000 }, async () => {
    for (const condition of ['save-failure', 'interrupt']) {
        const out = join(root, condition);
        const path = join(out, 'r1-normal');
        const p = start(out, ['--modes=normal', ...(condition === 'save-failure' ? ['--seconds=4'] : [])]);
        try {
            await waitFor('준비 완료', () => exists(join(path, 'ready.json')), p);
            if (condition === 'save-failure') {
                await mkdir(join(path, 'result.json'));
                await mkdir(join(path, 'services.log'));
            } else {
                await waitFor('일반 부하 6개 발송', async () => {
                    try { return (await readFile(join(path, 'requests.jsonl'), 'utf8')).trim().split('\n').length >= 6; }
                    catch { return false; }
                }, p);
                p.child.kill('SIGTERM');
            }
            assert.equal(await p.exit, 1, p.output());
            const [r] = await read(join(out, 'summary.json'));
            const evidence = await read(join(path, 'evidence.json'));
            if (condition === 'save-failure') {
                assert.equal(r.scenario.valid, true, JSON.stringify(r));
                assert.equal(r.functional.status, 'passed');
                assert.equal(r.measurement.valid, true);
                assert.equal(r.cleanup.succeeded, false);
                assert.match(r.cleanup.errors.join(' '), /EISDIR/);
                assert.ok(await exists(join(path, 'stopped-jobs.json')), '저장 실패 뒤에도 DB 근거를 남겨야 합니다');
            } else {
                assert.equal(r.measurement.valid, false);
                assert.equal(r.cleanup.succeeded, true);
                assert.ok(evidence.requests.some((r: { unsent?: string }) => r.unsent === '중단'));
                assert.ok(evidence.requests.filter((r: { sentMs: number | null }) => r.sentMs !== null)
                    .every((r: { finishedMs: number | null }) => r.finishedMs !== null));
                const final = await read(join(out, 'final-jobs.json'));
                assert.ok(final.some((j: { status: string }) => j.status === 'queued'), '중단 시 잔여 Job을 보존해야 합니다');
                const baseline = new Set(evidence.before.map((j: { id: string }) => j.id));
                const ownIds = final.filter((j: { id: string }) => !baseline.has(j.id)).map((j: { id: string }) => j.id);
                // 도구는 행을 삭제하지 않는다. Smoke가 만든 중단 fixture만 ID로 한정해 후속 실행을 준비한다.
                await compose(['up', '-d', '--no-build', '--wait', 'postgres']);
                const orm = await connect();
                try { if (ownIds.length) await orm.em.getConnection().execute(`delete from jobs where id in (${ownIds.map(() => '?').join(',')})`, ownIds); }
                finally { await orm.close(); await compose(['down']); }
            }
            assert.deepEqual(await read(join(out, 'cleanup.json')), { succeeded: true, errors: [] });
            assert.equal((await compose(['ps', '--all', '--quiet'])).trim(), '');
            console.log(`${condition} 근거: ${out}`);
        } finally { if (!p.exited()) { p.child.kill('SIGTERM'); await p.exit; } }
    }
});
