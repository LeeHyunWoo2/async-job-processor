import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';

import type { MikroORM } from '@mikro-orm/postgresql';

import { createApp } from '../../src/api/app.ts';
import { Job, JobStatus } from '../../src/db/entities/job.ts';
import { executeHandler } from '../../src/jobs/handlers.ts';
import { claimJob, saveJobOutcome } from '../../src/jobs/store.ts';
import { runWorker } from '../../src/worker/run.ts';
import { withJobProcesses, waitFor } from '../helpers/job-process.ts';
import { withJobTestDatabase, withJobUpdateTrigger } from '../helpers/job-test-database.ts';

test('PostgreSQL 기본 Job 처리', { timeout: 60_000 }, async (t) => {
    await withJobTestDatabase('job', 3, async (database) => {
        const { schema, table, connections, reset } = database;
        const [orm, other, apiConnection] = connections as [MikroORM, MikroORM, MikroORM];
        const api = createApp(apiConnection, false);

        try {
            const insert = async (values: Partial<Job> = {}) => {
                const em = orm.em.fork();
                const job = em.create(Job, { type: 'slow', payload: { durationMs: 1 }, maxAttempts: 1, ...values });
                em.persist(job);
                await em.flush();
                return job;
            };
            const post = (payload: unknown) => api.inject({
                method: 'POST', url: '/jobs', headers: { 'content-type': 'application/json' },
                payload: JSON.stringify(payload),
            });
            const get = (id: string) => api.inject({ method: 'GET', url: `/jobs/${id}` });

            t.beforeEach(reset);

            await t.test('API는 등록·조회와 대표적인 입력 오류 및 경계값을 처리한다', async () => {
                const body = { type: 'slow', payload: { durationMs: 1 } };
                const first = await post(body);
                const second = await post(body);
                assert.equal(first.statusCode, 201);
                assert.equal(second.statusCode, 201);
                const created = first.json<{ id: string; status: string }>();
                assert.equal(created.status, JobStatus.queued);
                assert.notEqual(created.id, second.json().id);
                const found = await get(created.id);
                assert.equal(found.statusCode, 200);
                assert.equal(found.json().id, created.id);
                assert.deepEqual(found.json().payload, body.payload);
                assert.equal(found.json().attempt, 0);
                assert.equal(found.json().maxAttempts, 3);
                assert.equal((await get(randomUUID())).statusCode, 404);
                assert.equal((await get('invalid-id')).statusCode, 400);

                // slow Handler의 durationMs 허용 범위: 1~10,000ms
                const invalid = [
                    { payload: {} }, { type: 'unknown', payload: {} },
                    { ...body, status: 'succeeded' },
                    { ...body, attempt: 1 }, { ...body, maxAttempts: 5 },
                    { type: 'slow', payload: null }, { type: 'slow', payload: {} },
                    { type: 'slow', payload: { durationMs: '1' } },
                    { type: 'slow', payload: { durationMs: 1.5 } },
                    { type: 'slow', payload: { durationMs: 0 } },
                    { type: 'slow', payload: { durationMs: 10_001 } },
                    { type: 'slow', payload: { durationMs: 1, extra: true } },
                    { type: 'fail', payload: { extra: true } },
                    { type: 'flaky', payload: null },
                    { type: 'flaky', payload: { attempt: 3 } },
                ];
                for (const payload of invalid) {
                    assert.equal((await post(payload)).statusCode, 400, JSON.stringify(payload));
                }
                assert.equal(await orm.em.fork().count(Job), 2, '잘못된 요청은 Job을 생성하지 않아야 합니다');
                assert.equal((await post({ type: 'slow', payload: { durationMs: 10_000 } })).statusCode, 201);
                assert.equal((await post({ type: 'fail', payload: {} })).statusCode, 201);
                assert.equal((await post({ type: 'flaky', payload: {} })).statusCode, 201);
            });

            await t.test('Claim 잠금 해제 후 실제 Handler를 실행하고 실패에 이어 다음 Job의 성공 결과를 조회한다', async (t) => {
                const failure = await post({ type: 'fail', payload: {} });
                const success = await post({ type: 'slow', payload: { durationMs: 20 } });
                assert.equal(failure.statusCode, 201);
                assert.equal(success.statusCode, 201);
                const failedId = failure.json<{ id: string }>().id;
                const succeededId = success.json<{ id: string }>().id;
                await orm.em.fork().nativeUpdate(Job, { id: failedId }, { availableAt: new Date(0) });
                const controller = new AbortController();
                const handled: string[] = [];

                await runWorker(orm, 'worker-a', async (job) => {
                    handled.push(job.id);
                    try {
                        // NOWAIT 잠금 성공은 Handler 시작 전에 Claim 트랜잭션이 끝났음을 확인한다.
                        await other.em.fork().transactional(async (em) => {
                            await em.execute(`select id from ${table} where id = ? for update nowait`, [job.id]);
                        });
                        assert.equal((await get(job.id)).json().status, JobStatus.processing);
                        if (job.id === succeededId) {
                            assert.equal((await get(failedId)).json().status, JobStatus.failed);
                        }
                        return await executeHandler(job);
                    } finally {
                        if (handled.length >= 2) controller.abort();
                    }
                }, AbortSignal.any([controller.signal, t.signal]));

                assert.deepEqual(handled, [failedId, succeededId]);
                const failed = (await get(failedId)).json();
                assert.equal(failed.status, JobStatus.failed);
                assert.deepEqual(failed.lastError, { name: 'Error', message: '의도적인 Job 실패' });
                const succeeded = (await get(succeededId)).json();
                assert.equal(succeeded.status, JobStatus.succeeded);
                assert.deepEqual(succeeded.result, { durationMs: 20 });
                assert.equal(succeeded.lastError, null);
            });

            await t.test('Claim 조건과 정렬을 적용하고 상태·시도 횟수·실행 정보를 갱신한다', async () => {
                const [isolation] = await orm.em.getConnection().execute<{ transaction_isolation: string }[]>('show transaction_isolation');
                assert.equal(isolation?.transaction_isolation, 'read committed');
                const time = new Date(Date.now() - 10_000);
                await insert({ availableAt: new Date(Date.now() + 60_000) });
                await insert({ attempt: 1 });
                for (const status of [JobStatus.processing, JobStatus.succeeded, JobStatus.failed]) await insert({ status });
                const last = await insert({ availableAt: new Date(time.getTime() + 1) });
                const newer = await insert({ availableAt: time, createdAt: new Date(time.getTime() + 1) });
                const second = await insert({ id: '00000000-0000-4000-8000-000000000002', availableAt: time, createdAt: time });
                const first = await insert({ id: '00000000-0000-4000-8000-000000000001', availableAt: time, createdAt: time });
                for (const expected of [first, second, newer, last]) {
                    const claimed = await claimJob(orm, 'worker-a');
                    assert.ok(claimed);
                    assert.equal(claimed.id, expected.id);
                    assert.equal(claimed.attempt, 1);
                    assert.equal(claimed.maxAttempts, expected.maxAttempts);
                    assert.equal(claimed.workerId, 'worker-a');
                    assert.ok(claimed.processingStartedAt instanceof Date);
                    const saved = await other.em.fork().findOneOrFail(Job, claimed.id);
                    assert.equal(saved.status, JobStatus.processing);
                    assert.equal(saved.attempt, claimed.attempt);
                    assert.equal(saved.workerId, claimed.workerId);
                    assert.equal(saved.processingStartedAt?.getTime(), claimed.processingStartedAt.getTime());
                }
                assert.equal(await claimJob(orm, 'worker-a'), null);
            });

            await t.test('서로 다른 DB 연결의 동시 Claim은 동일 Job을 한 번만 가져온다', async () => {
                const job = await insert();
                const claims = await Promise.all([claimJob(orm, 'worker-a'), claimJob(other, 'worker-b')]);
                const claimed = claims.filter((value) => value !== null);
                assert.equal(claimed.length, 1);
                assert.equal(claimed[0]?.id, job.id);
                const saved = await orm.em.fork().findOneOrFail(Job, job.id);
                assert.equal(saved.attempt, 1);
                assert.equal(saved.workerId, claimed[0]?.workerId);
            });

            await t.test('다른 연결이 잠근 첫 행을 SKIP LOCKED로 건너뛰고 잠금 해제 후 가져온다', async () => {
                const locked = await insert({ availableAt: new Date(0) });
                const next = await insert();
                await orm.em.fork().transactional(async (em) => {
                    const [owner] = await em.execute<{ pid: number }[]>('select pg_backend_pid() as pid');
                    const [peer] = await other.em.getConnection().execute<{ pid: number }[]>('select pg_backend_pid() as pid');
                    assert.notEqual(owner?.pid, peer?.pid);
                    await em.execute(`select id from ${table} where id = ? for update`, [locked.id]);
                    assert.equal((await claimJob(other, 'worker-b'))?.id, next.id);
                    assert.equal(await claimJob(other, 'worker-b'), null);
                });
                assert.equal((await claimJob(other, 'worker-b'))?.id, locked.id);
            });

            await t.test('현재 status·workerId·attempt와 다른 실행의 결과 저장을 차단한다', async () => {
                const job = await insert({ maxAttempts: 2 });
                const claimed = await claimJob(orm, 'worker-a');
                assert.ok(claimed);
                const outcome = { status: 'succeeded', result: { ok: true } } as const;
                for (const stale of [{ ...claimed, workerId: 'worker-b' }, { ...claimed, attempt: 0 }]) {
                    assert.equal(await saveJobOutcome(orm, stale, outcome), false);
                }
                await orm.em.fork().nativeUpdate(Job, { id: job.id }, { status: JobStatus.queued });
                assert.equal(await saveJobOutcome(orm, claimed, outcome), false);
                const current = await claimJob(other, 'worker-b');
                assert.ok(current);
                assert.equal(current.attempt, 2);
                assert.equal(await saveJobOutcome(orm, claimed, outcome), false);
                assert.equal(await saveJobOutcome(other, current, outcome), true);
                assert.equal(await saveJobOutcome(other, current, { status: 'failed', lastError: '중복 완료' }), false);
                const saved = await orm.em.fork().findOneOrFail(Job, job.id);
                assert.equal(saved.status, JobStatus.succeeded);
                assert.deepEqual(saved.result, outcome.result);
            });

            await t.test('종료 요청은 현재 Job의 결과 저장을 기다리고 새 Claim을 중단한다', async (t) => {
                const current = await insert({ availableAt: new Date(0) });
                const next = await insert();
                const started = Promise.withResolvers<void>();
                const finish = Promise.withResolvers<void>();
                const controller = new AbortController();
                let stopped = false;
                const running = runWorker(orm, 'worker-a', async () => {
                    started.resolve();
                    await finish.promise;
                    return { completed: true };
                }, AbortSignal.any([controller.signal, t.signal])).finally(() => { stopped = true; });
                try {
                    await Promise.race([started.promise, running.then(() => {
                        throw new Error('Handler 시작 전에 Worker가 종료되었습니다');
                    })]);
                    controller.abort();
                    assert.equal(stopped, false);
                    assert.equal((await other.em.fork().findOneOrFail(Job, current.id)).status, JobStatus.processing);
                } finally {
                    controller.abort();
                    finish.resolve();
                    await running;
                }
                assert.deepEqual((await other.em.fork().findOneOrFail(Job, current.id)).result, { completed: true });
                assert.equal((await other.em.fork().findOneOrFail(Job, current.id)).status, JobStatus.succeeded);
                assert.equal((await other.em.fork().findOneOrFail(Job, next.id)).status, JobStatus.queued);
            });

            // Worker의 DB 장애 대응 정책은 후속 단계에서 결정할 예정이므로, 이번 테스트에서는 재시도 및 복구 동작을 검증하지 않는다.
            await t.test('Claim과 결과 저장의 DB 오류가 전파되고 변경이 부분 반영되지 않는다', async () => {
                const processing = await insert();
                const claimed = await claimJob(orm, 'worker-a');
                assert.ok(claimed);
                const queued = await insert();
                const snapshot = (id: string) => other.em.getConnection().execute(`select * from ${table} where id = ?`, [id]);
                const beforeClaim = await snapshot(queued.id);
                const beforeSave = await snapshot(processing.id);
                await withJobUpdateTrigger(database, 'reject_job_update', "raise exception '검증용 DB 저장 오류';", async () => {
                    // 저장 함수의 원자성과 오류 전파만 확인한다.
                    await assert.rejects(claimJob(orm, 'worker-b'), /검증용 DB 저장 오류/);
                    assert.deepEqual(await snapshot(queued.id), beforeClaim);
                    await assert.rejects(saveJobOutcome(orm, claimed, { status: 'succeeded', result: { ok: true } }), /검증용 DB 저장 오류/);
                    assert.deepEqual(await snapshot(processing.id), beforeSave);
                });
            });

            await t.test('독립 Worker 프로세스는 SIGTERM 이후 DB 연결을 정리하고 정상 종료한다', { timeout: 20_000 }, async (t) => {
                // 실행 중 종료 처리는 위에서 검증했으므로, 여기서는 독립 프로세스의 실제 Job 완료와 SIGTERM 종료를 확인한다.
                const job = await insert({
                    payload: { durationMs: 20 },
                    availableAt: new Date(0),
                });
                await withJobProcesses(schema, t.signal, async (start) => {
                    const worker = start('worker/main.ts');
                    await worker.ready(/Worker 시작/);
                    // 독립 Worker가 테스트 schema의 Job을 실제로 처리했는지 확인한다.
                    const completed = await waitFor(async () => {
                        const saved = await other.em.fork().findOneOrFail(Job, job.id);
                        if (saved.status === JobStatus.succeeded) return saved;
                        assert.notEqual(saved.status, JobStatus.failed, 'Job 실행 실패');
                        return undefined;
                    }, t.signal, 8_000);
                    assert.deepEqual(completed.result, { durationMs: 20 });
                    assert.equal(completed.attempt, 1);
                    assert.ok(completed, `Worker가 Job을 완료하지 못했습니다: ${worker.output()}`);

                    assert.equal(worker.child.kill('SIGTERM'), true);
                    assert.deepEqual(await worker.exited, { code: 0, signal: null }, worker.output());
                });
            });
        } finally {
            await api.close();
            assert.equal(await apiConnection.isConnected(), false, 'app.close()는 API ORM을 닫아야 합니다');
        }
    });
});
