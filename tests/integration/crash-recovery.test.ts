import assert from 'node:assert/strict';
import test from 'node:test';

import type { MikroORM } from '@mikro-orm/postgresql';

import { Job, JobStatus } from '../../src/db/entities/job.ts';
import { executeHandler } from '../../src/jobs/handlers.ts';
import { RetryableJobError } from '../../src/jobs/retry.ts';
import { claimJob, recoverStaleJobs, saveJobOutcome, scheduleRetry } from '../../src/jobs/store.ts';
import { processJob, runWorker } from '../../src/worker/run.ts';
import { withJobProcesses, waitFor } from '../helpers/job-process.ts';
import { withJobTestDatabase, withJobUpdateTrigger } from '../helpers/job-test-database.ts';
import { captureWorkerObservability, metricValue } from '../helpers/observability.ts';

test('PostgreSQL Crash Recovery / At-least-once', { timeout: 100_000 }, async (t) => {
    await withJobTestDatabase('recovery', 3, async (database) => {
        const { schema, table, connections, reset } = database;
        const [orm, other, peer] = connections as [MikroORM, MikroORM, MikroORM];
        const saved = (id: string) => other.em.fork().findOneOrFail(Job, id);
        const snapshot = (id: string) => other.em.getConnection().execute(`select * from ${table} where id = ?`, [id]);
        const makeStale = (id: string) => orm.em.getConnection().execute(`
            update ${table} set processing_started_at = now() - interval '31 seconds' where id = ?
        `, [id]);
        const insert = async (values: Partial<Job> = {}) => {
            const em = orm.em.fork();
            const job = em.create(Job, { type: 'slow', payload: { durationMs: 1 }, maxAttempts: 3, ...values });
            em.persist(job);
            await em.flush();
            return job;
        };

        // 두 UPDATE를 실제 Row Lock 뒤에 순서대로 대기시켜 조건 재평가를 검증한다.
        const compete = async <A, B>(id: string, first: () => Promise<A>, second: () => Promise<B>): Promise<[A, B]> => {
            const [a] = await other.em.getConnection().execute<{ pid: number }[]>('select pg_backend_pid() as pid');
            const [b] = await peer.em.getConnection().execute<{ pid: number }[]>('select pg_backend_pid() as pid');
            assert.ok(a && b);
            assert.notEqual(a.pid, b.pid);
            let firstResult: Promise<A> | undefined;
            let secondResult: Promise<B> | undefined;
            try {
                await orm.em.fork().transactional(async (em) => {
                    const [owner] = await em.execute<{ pid: number }[]>('select pg_backend_pid() as pid');
                    assert.notEqual(owner?.pid, a.pid);
                    assert.notEqual(owner?.pid, b.pid);
                    await em.execute(`select id from ${table} where id = ? for update`, [id]);
                    const waitUntilBlocked = (pid: number) => waitFor(async () => {
                        const [row] = await em.execute<{ blocked: boolean }[]>(
                            'select cardinality(pg_blocking_pids(?)) > 0 as blocked', [pid],
                        );
                        return row?.blocked ? true : undefined;
                    }, t.signal, 2_000);
                    firstResult = first();
                    void firstResult.catch(() => {});
                    await waitUntilBlocked(a.pid);
                    secondResult = second();
                    void secondResult.catch(() => {});
                    await waitUntilBlocked(b.pid);
                });
                return await Promise.all([firstResult!, secondResult!]);
            } finally {
                await Promise.allSettled([firstResult, secondResult]);
            }
        };

        t.beforeEach(reset);

        await t.test('30초를 초과한 processing만 복구하고 남은 Attempt에 따라 원자적으로 상태를 변경한다', async () => {
            const metadata = {
                status: JobStatus.processing, workerId: 'previous-worker',
                processingStartedAt: new Date(0), availableAt: new Date(0), updatedAt: new Date(0),
                lastError: { name: 'PreviousError', message: '이전 오류' },
            };
            const retry = await insert({ ...metadata, attempt: 1 });
            const exhausted = await insert({ ...metadata, attempt: 3 });
            const overLimit = await insert({ ...metadata, attempt: 4 });
            const recent = await insert({ ...metadata, attempt: 1 });
            await orm.em.getConnection().execute(`
                update ${table} set processing_started_at = now() - interval '29 seconds' where id = ?
            `, [recent.id]);
            const untouched = [recent, await insert({ ...metadata, processingStartedAt: null })];
            for (const status of [JobStatus.queued, JobStatus.succeeded, JobStatus.failed]) {
                untouched.push(await insert({ ...metadata, status }));
            }
            const before = await Promise.all(untouched.map((job) => snapshot(job.id)));
            const [start] = await orm.em.getConnection().execute<{ now: string }[]>('select now() as now');
            const recoveredJobs = await recoverStaleJobs(orm);
            assert.equal(recoveredJobs.length, 3);
            for (const original of [retry, exhausted, overLimit]) {
                const recovered = recoveredJobs.find((job) => job.jobId === original.id);
                assert.ok(recovered);
                assert.equal(recovered.workerId, original.workerId);
                assert.equal(recovered.attempt, original.attempt);
                assert.equal(recovered.jobType, original.type);
                assert.equal(recovered.status, original.id === retry.id ? 'queued' : 'failed');
                assert.equal(recovered.lastError.name, 'StaleJobError');
            }
            const [end] = await orm.em.getConnection().execute<{ now: string }[]>('select now() as now');
            assert.ok(start && end);

            const queued = await saved(retry.id);
            assert.equal(queued.status, JobStatus.queued);
            assert.equal(queued.availableAt.getTime(), queued.updatedAt.getTime());
            assert.ok(queued.availableAt.getTime() >= new Date(start.now).getTime());
            assert.ok(queued.availableAt.getTime() <= new Date(end.now).getTime());
            for (const original of [retry, exhausted, overLimit]) {
                const recovered = await saved(original.id);
                assert.equal(recovered.attempt, original.attempt);
                assert.equal(recovered.workerId, original.workerId);
                assert.equal(recovered.processingStartedAt?.getTime(), original.processingStartedAt?.getTime());
                assert.equal(recovered.result, null);
                assert.deepEqual(recovered.lastError, {
                    name: 'StaleJobError',
                    message: '마지막 Claim 이후 30초가 경과하여 복구했습니다. 이전 실행의 완료 여부와 결과를 확인할 수 없습니다',
                });
                assert.ok(recovered.updatedAt.getTime() >= new Date(start.now).getTime());
                if (original.id !== retry.id) {
                    assert.equal(recovered.status, JobStatus.failed);
                    assert.equal(recovered.availableAt.getTime(), original.availableAt.getTime());
                }
            }
            assert.deepEqual(await Promise.all(untouched.map((job) => snapshot(job.id))), before);
        });

        await t.test('Handler 완료 없이 Crash가 세 번 반복돼도 Claim 횟수를 유지하고 최종 실패한다', async () => {
            const job = await insert();
            for (const attempt of [1, 2, 3]) {
                const claimed = await claimJob(orm, `crashed-worker-${attempt}`);
                assert.ok(claimed);
                assert.equal(claimed.attempt, attempt);
                await makeStale(job.id);
                assert.equal((await recoverStaleJobs(other)).length, 1);
                const recovered = await saved(job.id);
                assert.equal(recovered.attempt, attempt);
                assert.equal(recovered.status, attempt < 3 ? JobStatus.queued : JobStatus.failed);
            }
            assert.equal(await claimJob(peer, 'extra-worker'), null);
        });

        await t.test('서로 다른 연결의 동시 Recovery는 Row Lock 이후 조건을 재평가하여 한 번만 적용된다', async () => {
            const job = await insert({ status: JobStatus.processing, attempt: 1, processingStartedAt: new Date(0) });
            const controllers = [new AbortController(), new AbortController()];
            const stopAfterRecovery = () => controllers.forEach((controller) => controller.abort());
            const a = captureWorkerObservability('recovery-a', stopAfterRecovery);
            const b = captureWorkerObservability('recovery-b', stopAfterRecovery);
            const handler = async () => { throw new Error('Recovery 이후 종료되어 Handler를 호출하지 않아야 합니다'); };
            await compete(job.id,
                () => runWorker(other, 'recovery-a', handler, AbortSignal.any([controllers[0]!.signal, t.signal]), a),
                () => runWorker(peer, 'recovery-b', handler, AbortSignal.any([controllers[1]!.signal, t.signal]), b));
            assert.deepEqual(a.logs.map((log) => log.event), ['job.recovered']);
            assert.deepEqual(b.logs, []);
            assert.equal(a.logs[0]?.jobId, job.id);
            assert.equal(await metricValue(a.registry, 'async_job_recovered_total'), 1);
            assert.equal(await metricValue(b.registry, 'async_job_recovered_total'), 0);
            assert.equal(await metricValue(a.registry, 'async_job_failed_total'), 0);
            const recovered = await saved(job.id);
            assert.equal(recovered.status, JobStatus.queued);
            assert.equal(recovered.attempt, 1);
            const before = await snapshot(job.id);
            assert.deepEqual(await recoverStaleJobs(peer), []);
            assert.deepEqual(await snapshot(job.id), before, '중복 검사로 수정 시각이나 오류를 다시 쓰지 않습니다');
        });

        await t.test('결과 저장과 Recovery의 경합은 먼저 상태를 변경한 실행을 보존한다', async () => {
            for (const recoveryFirst of [false, true]) {
                await reset();
                const job = await insert();
                const claimed = await claimJob(orm, 'worker-a');
                assert.ok(claimed);
                await makeStale(job.id);
                const outcome = { status: 'succeeded', result: { completed: true } } as const;
                if (recoveryFirst) {
                    assert.deepEqual(await compete(job.id,
                        () => recoverStaleJobs(other).then((jobs) => jobs.length), () => saveJobOutcome(peer, claimed, outcome)), [1, false]);
                    assert.equal((await saved(job.id)).status, JobStatus.queued);
                } else {
                    assert.deepEqual(await compete(job.id,
                        () => saveJobOutcome(other, claimed, outcome), () => recoverStaleJobs(peer).then((jobs) => jobs.length)), [true, 0]);
                    const completed = await saved(job.id);
                    assert.equal(completed.status, JobStatus.succeeded);
                    assert.deepEqual(completed.result, outcome.result);
                    assert.equal(completed.lastError, null);
                }
            }
        });

        await t.test('Recovery 직후와 재Claim 이후 이전 실행의 결과 저장·Retry 예약이 거부된다', async () => {
            for (const maxAttempts of [1, 3]) {
                await reset();
                const job = await insert({ maxAttempts });
                const previous = await claimJob(orm, 'worker-a');
                assert.ok(previous);
                await makeStale(job.id);
                await recoverStaleJobs(other);
                const assertPreviousRejected = async () => {
                    const before = await snapshot(job.id);
                    const observation = captureWorkerObservability('worker-a');
                    assert.equal(await saveJobOutcome(orm, previous, { status: 'succeeded', result: { old: true } }), false);
                    assert.equal(await scheduleRetry(orm, previous, { message: '이전 실행 오류' }), false);
                    await assert.rejects(processJob(orm, previous, async () => ({ old: true }), observation), /실행 상태가 변경되었습니다/);
                    await assert.rejects(processJob(orm, previous, async () => {
                        throw new RetryableJobError('이전 실행의 늦은 실패');
                    }, observation), /실행 상태가 변경되었습니다/);
                    await assert.rejects(processJob(orm, previous, async () => {
                        throw new Error('이전 실행의 최종 실패');
                    }, observation), /실행 상태가 변경되었습니다/);
                    // Handler 실행 Marker는 남지만 거부된 DB 상태 전이를 성공으로 기록하면 안 된다.
                    assert.deepEqual(observation.logs.map((log) => log.event), Array.from({ length: 3 },
                        () => ['job.handler_started', 'job.handler_finished']).flat());
                    assert.ok(observation.logs.every((log) => log.jobId === previous.id && log.workerId === 'worker-a' && log.attempt === previous.attempt));
                    for (const name of ['succeeded', 'failed', 'retry_scheduled']) {
                        assert.equal(await metricValue(observation.registry, `async_job_${name}_total`), 0);
                    }
                    assert.equal(await metricValue(observation.registry, 'async_job_handler_duration_seconds_count'), 3);
                    assert.deepEqual(await snapshot(job.id), before);
                };
                assert.equal((await saved(job.id)).status, maxAttempts === 1 ? JobStatus.failed : JobStatus.queued);
                await assertPreviousRejected();
                if (maxAttempts > 1) {
                    const current = await claimJob(other, 'worker-b');
                    assert.ok(current);
                    assert.equal(current.attempt, 2);
                    await assertPreviousRejected();
                    await processJob(other, current, executeHandler);
                    assert.equal((await saved(job.id)).status, JobStatus.succeeded);
                }
            }
        });

        await t.test('아직 실행 중인 Handler가 Stale로 오판되어도 이전 Worker는 현재 실행의 상태를 덮어쓰지 못한다', async (t) => {
            const job = await insert();
            const aStarted = Promise.withResolvers<void>();
            const bStarted = Promise.withResolvers<void>();
            const aFinish = Promise.withResolvers<void>();
            const bFinish = Promise.withResolvers<void>();
            const aController = new AbortController();
            const bController = new AbortController();
            let calls = 0;
            const aStopped = runWorker(orm, 'worker-a', async () => {
                calls++;
                aStarted.resolve();
                await aFinish.promise;
                return { worker: 'a' };
            }, AbortSignal.any([aController.signal, t.signal])).then(() => null, (error: unknown) => error);
            let bStopped: Promise<unknown> | undefined;
            try {
                await Promise.race([aStarted.promise, aStopped.then(() => { throw new Error('Worker A가 조기 종료했습니다'); })]);
                await makeStale(job.id);
                bStopped = runWorker(other, 'worker-b', async () => {
                    calls++;
                    bStarted.resolve();
                    await bFinish.promise;
                    bController.abort();
                    return { worker: 'b' };
                }, AbortSignal.any([bController.signal, t.signal])).then(() => null, (error: unknown) => error);
                await Promise.race([bStarted.promise, bStopped.then(() => { throw new Error('Worker B가 조기 종료했습니다'); })]);
                assert.equal(calls, 2, '정상 Handler와 복구 후 Handler가 동시에 실행됩니다');
                const before = await peer.em.getConnection().execute(`select * from ${table} where id = ?`, [job.id]);
                const current = await peer.em.fork().findOneOrFail(Job, job.id);
                assert.equal(current.status, JobStatus.processing);
                assert.equal(current.workerId, 'worker-b');
                assert.equal(current.attempt, 2);
                aFinish.resolve();
                const error = await aStopped;
                assert.ok(error instanceof Error);
                assert.match(error.message, /Job 결과를 저장할 수 없습니다: 실행 상태가 변경되었습니다/);
                assert.deepEqual(await peer.em.getConnection().execute(`select * from ${table} where id = ?`, [job.id]), before);
                bFinish.resolve();
                assert.equal(await bStopped, null);
                const completed = await saved(job.id);
                assert.equal(completed.status, JobStatus.succeeded);
                assert.deepEqual(completed.result, { worker: 'b' });
            } finally {
                aController.abort();
                bController.abort();
                aFinish.resolve();
                bFinish.resolve();
                await Promise.allSettled([aStopped, bStopped]);
            }
        });

        await t.test('실행 중인 Worker도 약 10초 뒤 Recovery 검사를 다시 수행한다', { timeout: 18_000 }, async (t) => {
            const initial = await insert();
            const started = Promise.withResolvers<void>();
            const controller = new AbortController();
            const beganAt = Date.now();
            const handled: string[] = [];
            const running = runWorker(orm, 'periodic-worker', async (job) => {
                handled.push(job.id);
                if (job.id === initial.id) started.resolve();
                else controller.abort();
                return executeHandler(job);
            }, AbortSignal.any([controller.signal, t.signal]));
            void running.catch(() => {});
            try {
                await Promise.race([started.promise, running.then(() => { throw new Error('Worker가 조기 종료했습니다'); })]);
                const stale = await insert({ status: JobStatus.processing, attempt: 1, processingStartedAt: new Date(0) });
                await waitFor(async () => (await saved(stale.id)).status === JobStatus.succeeded ? true : undefined, t.signal, 14_000);
                await running;
                assert.deepEqual(handled, [initial.id, stale.id]);
                assert.ok(Date.now() - beganAt >= 9_000, '매 Polling마다 Recovery를 실행하지 않습니다');
                assert.equal((await saved(stale.id)).attempt, 2);
            } finally {
                controller.abort();
                await Promise.allSettled([running]);
            }
        });

        await t.test('Recovery DB 오류는 전체 변경을 취소하고 Handler 실행 없이 Worker를 중단한다', async (t) => {
            const retry = await insert({ status: JobStatus.processing, attempt: 1, processingStartedAt: new Date(0) });
            const exhausted = await insert({ status: JobStatus.processing, attempt: 3, processingStartedAt: new Date(0) });
            const queued = await insert();
            const before = await Promise.all([retry, exhausted, queued].map((job) => snapshot(job.id)));
            await withJobUpdateTrigger(database, 'reject_recovery', `
                if old.status = 'processing' and new.status = 'failed' then
                    raise exception '검증용 Recovery DB 오류';
                end if;
                return new;
            `, async () => {
                await assert.rejects(recoverStaleJobs(orm), /검증용 Recovery DB 오류/);
                let calls = 0;
                const observation = captureWorkerObservability('worker-a');
                await assert.rejects(runWorker(orm, 'worker-a', async () => { calls++; return null; }, t.signal, observation), /검증용 Recovery DB 오류/);
                assert.equal(calls, 0);
                assert.deepEqual(observation.logs, []);
                assert.equal(await metricValue(observation.registry, 'async_job_recovered_total'), 0);
                assert.equal(await metricValue(observation.registry, 'async_job_failed_total'), 0);
                assert.deepEqual(await Promise.all([retry, exhausted, queued].map((job) => snapshot(job.id))), before);
            });
        });

        await t.test('API 등록 후 Claim 직후·Handler 실행 중 SIGKILL과 Production Worker 재시작으로 복구한다', { timeout: 30_000 }, async (t) => {
            await withJobProcesses(schema, t.signal, async (start) => {
                const api = start('api/server.ts');
                const address = (await api.ready(/Server listening at (http:\/\/127\.0\.0\.1:\d+)/))[1];
                assert.ok(address);
                for (const phase of ['after-claim', 'in-handler'] as const) {
                    await reset();
                    const response: Response = await fetch(`${address}/jobs`, {
                        method: 'POST', headers: { 'content-type': 'application/json' },
                        body: JSON.stringify({ type: 'slow', payload: { durationMs: 1500 } }), signal: t.signal,
                    });
                    assert.equal(response.status, 201);
                    const created = await response.json() as { id: string };
                    const crashed = start('crash-worker', phase);
                    const marker = phase === 'after-claim' ? 'Claim 완료: Handler 진입 전' : 'Handler 진입 확인';
                    await crashed.ready(new RegExp(`${marker} ${created.id}`));
                    const claimed = await saved(created.id);
                    assert.equal(claimed.status, JobStatus.processing);
                    assert.equal(claimed.attempt, 1);
                    const beforeKill = await snapshot(created.id);
                    assert.equal(crashed.child.kill('SIGKILL'), true);
                    assert.deepEqual(await crashed.exited, { code: null, signal: 'SIGKILL' }, crashed.output());
                    assert.deepEqual(await snapshot(created.id), beforeKill, '강제 종료 후에도 미완료 processing이 남습니다');
                    await makeStale(created.id);

                    const restarted = start('worker/main.ts');
                    await restarted.ready(/Worker 시작/);
                    const reclaimed = await waitFor(async () => {
                        const job = await saved(created.id);
                        return job.status === JobStatus.processing && job.attempt === 2 ? job : undefined;
                    }, t.signal);
                    assert.notEqual(reclaimed.workerId, claimed.workerId);
                    assert.equal((reclaimed.lastError as { name: string }).name, 'StaleJobError');
                    const completed = await waitFor(async () => {
                        const job = await saved(created.id);
                        assert.notEqual(job.status, JobStatus.failed);
                        return job.status === JobStatus.succeeded ? job : undefined;
                    }, t.signal);
                    assert.equal(completed.attempt, 2);
                    assert.deepEqual(completed.result, { durationMs: 1500 });
                    assert.equal(completed.lastError, null);
                    const found: Response = await fetch(`${address}/jobs/${created.id}`, { signal: t.signal });
                    assert.equal(found.status, 200);
                    assert.equal((await found.json() as { status: string }).status, JobStatus.succeeded);
                    assert.equal(restarted.child.kill('SIGTERM'), true);
                    assert.deepEqual(await restarted.exited, { code: 0, signal: null }, restarted.output());
                    const recoveryLines = restarted.output().trim().split('\n')
                        .filter((line) => JSON.parse(line).event === 'job.recovered');
                    assert.equal(recoveryLines.length, 1);
                    const recoveryLine = recoveryLines[0]!;
                    const recoveryLog = JSON.parse(recoveryLine);
                    assert.equal(recoveryLog.jobId, created.id);
                    assert.equal(recoveryLog.workerId, reclaimed.workerId);
                    assert.equal(recoveryLog.previousWorkerId, claimed.workerId);
                    // JSON.parse는 중복 키를 덮어쓰므로 원본 JSON에서도 각각 한 번만 기록되는지 확인한다.
                    assert.equal((recoveryLine.match(/"workerId"\s*:/g) ?? []).length, 1);
                    assert.equal((recoveryLine.match(/"previousWorkerId"\s*:/g) ?? []).length, 1);
                }
                assert.equal(api.child.kill('SIGTERM'), true);
                assert.deepEqual(await api.exited, { code: 0, signal: null }, api.output());
            });
        });

        await t.test('Side Effect 이후 결과 저장 누락으로 중복 실행되며 jobId Unique Key로만 중복 INSERT를 막는다', async () => {
            const effectsTable = `"${schema}".job_effects`;
            for (const idempotent of [false, true]) {
                await reset();
                await orm.em.getConnection().execute(`
                    create table ${effectsTable} (job_id uuid not null ${idempotent ? 'primary key' : ''}, attempt integer not null)
                `);
                try {
                    const job = await insert();
                    const calls: number[] = [];
                    const handler = async (claimed: { id: string; attempt: number }) => {
                        calls.push(claimed.attempt);
                        await other.em.getConnection().execute(`
                            insert into ${effectsTable} (job_id, attempt) values (?, ?)
                            ${idempotent ? 'on conflict (job_id) do nothing' : ''}
                        `, [claimed.id, claimed.attempt]);
                        return { completed: true };
                    };
                    const first = await claimJob(orm, 'worker-a');
                    assert.ok(first);
                    // Side Effect만 커밋하고 결과 저장을 생략하여 저장 전 중단을 재현한다.
                    await handler(first);
                    assert.equal((await saved(job.id)).status, JobStatus.processing);
                    assert.equal((await saved(job.id)).result, null);
                    const readEffects = () => other.em.getConnection().execute<{ job_id: string; attempt: number }[]>(`select * from ${effectsTable}`);
                    assert.deepEqual(await readEffects(), [{ job_id: job.id, attempt: 1 }]);
                    await makeStale(job.id);
                    assert.equal((await recoverStaleJobs(orm)).length, 1);
                    const second = await claimJob(other, 'worker-b');
                    assert.ok(second);
                    await processJob(other, second, handler);
                    assert.deepEqual(calls, [1, 2], 'Idempotency 적용 여부와 관계없이 Handler는 두 번 실행됩니다');
                    const effects = await readEffects();
                    assert.equal(effects.length, idempotent ? 1 : 2);
                    assert.ok(effects.every((effect) => effect.job_id === job.id));
                    assert.deepEqual(effects.map((effect) => effect.attempt).sort(), idempotent ? [1] : [1, 2]);
                    const completed = await saved(job.id);
                    assert.equal(completed.status, JobStatus.succeeded);
                    assert.equal(completed.attempt, 2);
                    assert.deepEqual(completed.result, { completed: true });
                } finally {
                    await orm.em.getConnection().execute(`drop table ${effectsTable}`);
                }
            }
        });
    });
});
