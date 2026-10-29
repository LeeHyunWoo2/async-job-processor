import assert from 'node:assert/strict';
import test from 'node:test';

import { Job, JobStatus } from '../../src/db/entities/job.ts';
import { withJobProcesses, waitFor } from '../helpers/job-process.ts';
import { withJobTestDatabase, withJobUpdateTrigger } from '../helpers/job-test-database.ts';

function samples(text: string): number {
    const match = /^nodejs_event_loop_delay_samples (\d+)$/m.exec(text);
    assert.ok(match, '프로세스 Event Loop 표본 수가 있어야 합니다');
    return Number(match[1]);
}

for (const mode of ['Separated', 'Co-located'] as const) {
    test(`${mode} 실제 HTTP에서 cpu 처리·프로세스 계측과 정상 종료를 확인한다`, { timeout: 25_000 }, async (t) => {
        await withJobTestDatabase('job', 1, async ({ schema, connections }) => {
            const orm = connections[0]!;
            await withJobProcesses(schema, t.signal, async (start) => {
                const api = start(mode === 'Separated' ? 'api/server.ts' : 'co-located');
                const apiUrl = (await api.ready(/Server listening at (http:\/\/127\.0\.0\.1:\d+)/))[1]!;
                const worker = mode === 'Separated' ? start('worker/main.ts') : api;
                await worker.ready(/Worker 시작/);
                const started = worker.output().trim().split('\n').map((line) => JSON.parse(line))
                    .find((log) => log.event === 'worker.started');
                const workerUrl = `http://127.0.0.1:${started.metricsPort}`;
                assert.deepEqual(await (await fetch(`${apiUrl}/health`, { signal: t.signal })).json(), { status: 'ok' });
                const apiResponse = await fetch(`${apiUrl}/metrics`, { signal: t.signal });
                assert.equal(apiResponse.status, 200);
                const apiMetrics = await apiResponse.text();
                assert.match(apiMetrics, /async_job_enqueued_total 0\n/);
                if (mode === 'Separated') samples(apiMetrics);
                else assert.ok(!apiMetrics.includes('nodejs_event_loop_delay_'), '공유 Event Loop는 Worker Endpoint에서만 계측해야 합니다');
                const before = await waitFor(async () => {
                    const response = await fetch(`${workerUrl}/metrics`, { signal: t.signal });
                    assert.equal(response.status, 200);
                    const text = await response.text();
                    return samples(text) > 0 ? samples(text) : undefined;
                }, t.signal);

                const created = await fetch(`${apiUrl}/jobs`, {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ type: 'cpu', payload: { durationMs: 30 } }), signal: t.signal,
                });
                assert.equal(created.status, 201);
                const { id } = await created.json() as { id: string };
                const completed = await waitFor(async () => {
                    const response = await fetch(`${apiUrl}/jobs/${id}`, { signal: t.signal });
                    assert.equal(response.status, 200);
                    const job = await response.json() as Job;
                    assert.notEqual(job.status, JobStatus.failed);
                    return job.status === JobStatus.succeeded ? job : undefined;
                }, t.signal);
                assert.equal(completed.attempt, 1);
                assert.equal(completed.workerId, started.workerId);
                const result = completed.result as { durationMs: number; iterations: number; checksum: number };
                assert.equal(result.durationMs, 30);
                assert.ok(result.iterations > 0 && Number.isSafeInteger(result.checksum));
                assert.deepEqual((await orm.em.fork().findOneOrFail(Job, id)).result, result);
                await waitFor(async () => {
                    const text = await (await fetch(`${workerUrl}/metrics`, { signal: t.signal })).text();
                    assert.match(text, /async_job_succeeded_total 1\n/);
                    assert.match(text, /async_job_handler_duration_seconds_count 1\n/);
                    for (const suffix of ['max_seconds', 'p99_seconds', 'mean_seconds']) {
                        const match = new RegExp(`^nodejs_event_loop_delay_${suffix} (.+)$`, 'm').exec(text);
                        assert.ok(match);
                        assert.ok(Number.isFinite(Number(match[1])) && Number(match[1]) >= 0);
                    }
                    return samples(text) > before ? true : undefined;
                }, t.signal);

                assert.equal(worker.child.kill('SIGTERM'), true);
                assert.deepEqual(await worker.exited, { code: 0, signal: null }, worker.output());
                await assert.rejects(fetch(`${workerUrl}/metrics`));
                if (mode === 'Separated') {
                    assert.equal(api.child.kill('SIGINT'), true);
                    assert.deepEqual(await api.exited, { code: 0, signal: null }, api.output());
                }
                await assert.rejects(fetch(`${apiUrl}/health`));
            });
        });
    });
}

test('Co-located는 처리 중 SIGINT에도 결과 저장을 기다리고 두 Listener와 DB 연결을 정리한다', { timeout: 20_000 }, async (t) => {
    await withJobTestDatabase('job', 1, async ({ schema, connections }) => {
        const orm = connections[0]!;
        const connectionPids = () => orm.em.getConnection().execute<{ pid: number }[]>(
            'select pid from pg_stat_activity where datname = current_database() order by pid',
        );
        const before = await connectionPids();
        const em = orm.em.fork();
        const job = em.create(Job, { type: 'slow', payload: { durationMs: 1_000 }, maxAttempts: 3 });
        em.persist(job);
        await em.flush();
        await withJobProcesses(schema, t.signal, async (start) => {
            const process = start('co-located');
            const apiUrl = (await process.ready(/Server listening at (http:\/\/127\.0\.0\.1:\d+)/))[1]!;
            await process.ready(/Worker 시작/);
            const logs = process.output().trim().split('\n').map((line) => JSON.parse(line));
            const metricsPort = logs.find((log) => log.event === 'worker.started').metricsPort;
            await process.ready(/job.claimed/);
            assert.equal((await orm.em.fork().findOneOrFail(Job, job.id)).status, JobStatus.processing);
            assert.equal(process.child.kill('SIGINT'), true);
            await process.ready(/Worker 종료 요청/);
            assert.deepEqual(await process.exited, { code: 0, signal: null }, process.output());
            const saved = await orm.em.fork().findOneOrFail(Job, job.id);
            assert.equal(saved.status, JobStatus.succeeded);
            assert.equal(saved.attempt, 1);
            assert.deepEqual(saved.result, { durationMs: 1_000 });
            await assert.rejects(fetch(`${apiUrl}/health`));
            await assert.rejects(fetch(`http://127.0.0.1:${metricsPort}/metrics`));
            await waitFor(async () => JSON.stringify(await connectionPids()) === JSON.stringify(before) ? true : undefined, t.signal);
        });
    });
});

test('Co-located는 Worker DB 오류 시 실패로 종료하고 API·Metrics Listener도 정리한다', { timeout: 20_000 }, async (t) => {
    await withJobTestDatabase('job', 1, async (database) => {
        await withJobUpdateTrigger(database, 'reject_cpu_outcome', `
            if old.status = 'processing' and new.status = 'succeeded' then
                raise exception '검증용 결과 저장 오류';
            end if;
            return new;
        `, async () => {
            await withJobProcesses(database.schema, t.signal, async (start) => {
                const process = start('co-located');
                const apiUrl = (await process.ready(/Server listening at (http:\/\/127\.0\.0\.1:\d+)/))[1]!;
                await process.ready(/Worker 시작/);
                const started = process.output().trim().split('\n').map((line) => JSON.parse(line))
                    .find((log) => log.event === 'worker.started');
                const response = await fetch(`${apiUrl}/jobs`, {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ type: 'cpu', payload: { durationMs: 1 } }), signal: t.signal,
                });
                assert.equal(response.status, 201);
                assert.deepEqual(await process.exited, { code: 1, signal: null }, process.output());
                assert.match(process.output(), /검증용 결과 저장 오류/);
                const { id } = await response.json() as { id: string };
                const saved = await database.connections[0]!.em.fork().findOneOrFail(Job, id);
                assert.equal(saved.status, JobStatus.processing);
                assert.equal(saved.attempt, 1);
                assert.equal(saved.result, null, '저장 오류는 Handler 실패로 덮어쓰지 않아야 합니다');
                await assert.rejects(fetch(`${apiUrl}/health`));
                await assert.rejects(fetch(`http://127.0.0.1:${started.metricsPort}/metrics`));
            });
        });
    });
});
