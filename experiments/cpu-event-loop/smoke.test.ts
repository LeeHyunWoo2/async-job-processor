import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { mkdir, readFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { test } from 'node:test';
import { promisify } from 'node:util';

const execute = promisify(execFile);
const composeArgs = ['compose', '-p', 'async-job-cpu-event-loop', '-f', 'compose.yml', '-f', 'experiments/cpu-event-loop/compose.yml'];
const read = async (file: string) => JSON.parse(await readFile(file, 'utf8'));
function start(out: string, args: string[]) {
    const child = spawn(process.execPath, ['--env-file=.env', 'experiments/cpu-event-loop/run.ts',
        `--out=${out}`, '--repeats=1', '--window-ms=1000', '--duration-ms=400', '--interval-ms=50', ...args], { stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '';
    child.stdout.on('data', (chunk) => { output += String(chunk); });
    child.stderr.on('data', (chunk) => { output += String(chunk); });
    let exited = false;
    const exit = new Promise<number | null>((resolve) => child.on('exit', (code) => { exited = true; resolve(code); }));
    return { child, exit, output: () => output, exited: () => exited };
}
async function waitForFile(file: string, process: ReturnType<typeof start>) {
    const deadline = performance.now() + 60_000;
    while (true) {
        assert.ok(!process.exited(), process.output());
        try { await readFile(file); return; } catch { /* 준비 사건을 확인한다. */ }
        assert.ok(performance.now() < deadline, `준비 시간 초과: ${process.output()}`);
        await delay(50);
    }
}
async function emptyProject() {
    assert.equal((await execute('docker', [...composeArgs, 'ps', '--all', '--quiet'])).stdout.trim(), '');
}

test('실제 Compose의 두 실행 구조에서 독립 Probe·cpu 결과·표본·정상 종료와 기존 환경 보존을 확인한다', { timeout: 180_000 }, async () => {
    const root = resolve(`artifacts/cpu-event-loop/smoke-${Date.now()}`);
    const otherBefore = (await execute('docker', ['ps', '-a', '--format', '{{.ID}} {{.Names}} {{.State}}'])).stdout;
    const process = start(root, ['--conditions=cpu']);
    try {
        assert.equal(await process.exit, 0, process.output());
        const results = await read(join(root, 'summary.json'));
        assert.equal(results.length, 2);
        for (const result of results) {
            assert.equal(result.valid, true);
            assert.equal(result.cleanup.succeeded, true);
            assert.equal(result.topology.workers, 1);
            assert.equal(result.topology.nodeProcesses, result.mode === 'co-located' ? 1 : 2);
            const path = join(root, result.name);
            const job = (await read(join(path, 'jobs.json'))).find((r: { id: string }) => r.id === result.jobId);
            assert.equal(job.status, 'succeeded'); assert.equal(job.attempt, 1);
            assert.ok(job.result.iterations > 0);
            assert.deepEqual((await read(join(path, 'job-api.json'))).result, job.result);
            assert.ok(result.overlap.api.sent > 0 && result.overlap.worker.sent > 0);
            assert.ok(result.http.api.sent > 2 && result.http.worker.sent > 2);
            assert.equal(result.http.api.sent, result.dispatch.expectedPerTarget);
            assert.equal(result.http.api.timeouts + result.http.worker.timeouts, 0);
            const stopped = (await read(join(path, 'stopped-containers.json'))).filter((c: { service: string }) => ['api', 'worker', 'co-located'].includes(c.service));
            assert.ok(stopped.every((c: { running: boolean; exitCode: number }) => !c.running && c.exitCode === 0));
        }
        assert.equal((await read(join(root, 'cleanup.json'))).succeeded, true);
        await emptyProject();
        assert.equal((await execute('docker', ['ps', '-a', '--format', '{{.ID}} {{.Names}} {{.State}}'])).stdout, otherBefore);
        console.log(`Smoke 원자료: ${root}`);
    } finally {
        if (!process.exited()) { process.child.kill('SIGTERM'); await process.exit; }
    }
});

test('실제 로그 저장 실패와 SIGTERM에서 측정·정리를 분리하고 보존 자료를 남긴다', { timeout: 180_000 }, async () => {
    for (const scenario of ['save-failure', 'result-save-failure', 'interrupt']) {
        const root = resolve(`artifacts/cpu-event-loop/${scenario}-${Date.now()}`);
        const path = join(root, 'r1-co-located-idle');
        const process = start(root, ['--modes=co-located', '--conditions=idle']);
        try {
            await waitForFile(join(path, 'ready.json'), process);
            if (scenario !== 'interrupt') await mkdir(join(path, scenario === 'save-failure' ? 'services.log' : 'result.json'));
            else {
                await waitForFile(join(path, 'baseline-api.prom'), process);
                process.child.kill('SIGTERM');
            }
            assert.equal(await process.exit, 1, process.output());
            const result = scenario === 'result-save-failure' ? (await read(join(root, 'summary.json')))[0] : await read(join(path, 'result.json'));
            if (scenario !== 'interrupt') {
                assert.equal(result.valid, true);
                assert.equal(result.cleanup.succeeded, false);
                assert.match(result.cleanup.errors.join(' '), /EISDIR/);
                assert.equal((await read(join(path, 'cleanup.json'))).succeeded, false);
            } else {
                assert.equal(result.valid, false);
                assert.ok(result.error);
                assert.equal(result.cleanup.succeeded, true);
                assert.ok((await read(join(path, 'requests.json'))).rows.length > 0);
            }
            assert.deepEqual(await read(join(path, 'stopped-jobs.json')), await read(join(path, 'before.json')));
            assert.equal((await read(join(root, 'cleanup.json'))).succeeded, true);
            await emptyProject();
            console.log(`${scenario} 원자료: ${root}`);
        } finally {
            if (!process.exited()) { process.child.kill('SIGTERM'); await process.exit; }
        }
    }
});
