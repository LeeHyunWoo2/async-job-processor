import assert from 'node:assert/strict';
import test from 'node:test';

import type { MikroORM } from '@mikro-orm/postgresql';

import { Job, JobStatus } from '../../src/db/entities/job.ts';
import { executeHandler } from '../../src/jobs/handlers.ts';
import { RetryableJobError } from '../../src/jobs/retry.ts';
import { claimJob, enqueueJob, scheduleRetry } from '../../src/jobs/store.ts';
import { processJob, runWorker } from '../../src/worker/run.ts';
import { withJobProcesses, waitFor } from '../helpers/job-process.ts';
import { withJobTestDatabase, withJobUpdateTrigger } from '../helpers/job-test-database.ts';

test('PostgreSQL Retry / Backoff', { timeout: 90_000 }, async (t) => {
    await withJobTestDatabase('retry', 2, async (database) => {
        const { schema, table, connections, reset } = database;
        const [orm, other] = connections as [MikroORM, MikroORM];
        const saved = (id: string) => other.em.fork().findOneOrFail(Job, id);
        const makeDue = (id: string) => orm.em.fork().nativeUpdate(Job, { id }, { availableAt: new Date(0) });
        const insert = async (values: Partial<Job> = {}) => {
            const em = orm.em.fork();
            const job = em.create(Job, { type: 'slow', payload: { durationMs: 1 }, maxAttempts: 3, ...values });
            em.persist(job);
            await em.flush();
            return job;
        };
        const snapshot = (id: string) => other.em.getConnection().execute(`select * from ${table} where id = ?`, [id]);

        t.beforeEach(reset);

        await t.test('Retryable 실패 두 번 후 세 번째 실행에서 성공하며 DB Backoff는 1초·2초이다', async () => {
            const job = await enqueueJob(orm, 'slow', { durationMs: 1 });
            assert.equal((await saved(job.id)).maxAttempts, 3);
            const handled: number[] = [];
            const handler = async (claimed: { attempt: number }) => {
                handled.push(claimed.attempt);
                if (claimed.attempt < 3) throw new RetryableJobError('검증용 일시적 실패');
                return { completed: true };
            };

            for (const attempt of [1, 2]) {
                const claimed = await claimJob(orm, `worker-${attempt}`);
                assert.ok(claimed);
                assert.equal(claimed.id, job.id);
                assert.equal(claimed.attempt, attempt);
                assert.equal(claimed.maxAttempts, 3);
                await orm.em.fork().nativeUpdate(Job, { id: job.id }, { result: { stale: true }, updatedAt: new Date(0) });
                await processJob(orm, claimed, handler);

                const retry = await saved(job.id);
                assert.equal(retry.status, JobStatus.queued);
                assert.equal(retry.attempt, attempt, 'Retry 예약에서는 attempt를 증가시키지 않습니다');
                assert.equal(retry.availableAt.getTime() - retry.updatedAt.getTime(), attempt === 1 ? 1_000 : 2_000);
                assert.ok(retry.updatedAt.getTime() > 0);
                assert.equal(retry.result, null);
                assert.deepEqual(retry.lastError, { name: 'RetryableJobError', message: '검증용 일시적 실패' });
                assert.equal(retry.workerId, claimed.workerId);
                assert.equal(retry.processingStartedAt?.getTime(), claimed.processingStartedAt.getTime());
                assert.equal(await claimJob(other, 'early-worker'), null, 'availableAt 이전에는 Claim할 수 없습니다');
                await makeDue(job.id);
            }

            const claimed = await claimJob(other, 'worker-3');
            assert.ok(claimed);
            assert.equal(claimed.attempt, 3);
            await processJob(other, claimed, handler);
            const completed = await saved(job.id);
            assert.deepEqual(handled, [1, 2, 3]);
            assert.equal(completed.status, JobStatus.succeeded);
            assert.equal(completed.attempt, 3);
            assert.equal(completed.workerId, 'worker-3');
            assert.equal(completed.processingStartedAt?.getTime(), claimed.processingStartedAt.getTime());
            assert.deepEqual(completed.result, { completed: true });
            assert.equal(completed.lastError, null);
            assert.equal(await claimJob(orm, 'extra-worker'), null);
        });

        await t.test('Retryable 실패가 최대 세 번 발생하면 최종 실패하고 추가 Claim을 차단한다', async () => {
            const job = await insert();
            let calls = 0;
            for (const attempt of [1, 2, 3]) {
                const claimed = await claimJob(orm, 'worker-a');
                assert.ok(claimed);
                assert.equal(claimed.attempt, attempt);
                await processJob(orm, claimed, async () => {
                    calls++;
                    throw new RetryableJobError(`검증용 실패 ${attempt}`);
                });
                assert.equal((await saved(job.id)).status, attempt < 3 ? JobStatus.queued : JobStatus.failed);
                if (attempt < 3) await makeDue(job.id);
            }
            const failed = await saved(job.id);
            assert.equal(calls, 3);
            assert.equal(failed.attempt, 3);
            assert.equal(failed.result, null);
            assert.deepEqual(failed.lastError, { name: 'RetryableJobError', message: '검증용 실패 3' });
            assert.equal(await claimJob(other, 'worker-b'), null);
        });

        await t.test('일반 Error·위장한 name·알 수 없는 예외는 첫 실행에서 실패하고 maxAttempts 1도 준수한다', async () => {
            const disguised = new Error('오류 이름만으로 Retry하지 않습니다');
            disguised.name = 'RetryableJobError';
            for (const [error, maxAttempts] of [
                [new Error('영구적 실패'), 3], [disguised, 3],
                ['알 수 없는 예외', 3], [new RetryableJobError('추가 시도 불가'), 1],
            ] as const) {
                const job = await insert({ maxAttempts });
                const claimed = await claimJob(orm, 'worker-a');
                assert.ok(claimed);
                await processJob(orm, claimed, async () => { throw error; });
                const failed = await saved(job.id);
                assert.equal(failed.status, JobStatus.failed);
                assert.equal(failed.attempt, 1);
                assert.deepEqual(failed.lastError, {
                    name: error instanceof Error ? error.name : 'Error',
                    message: error instanceof Error ? error.message : error,
                });
                assert.equal(await claimJob(other, 'worker-b'), null);
            }
        });

        await t.test('서로 다른 DB 연결은 Retry가 가능한 Job을 한 번만 Claim하고 실행 정보를 교체한다', async () => {
            const job = await insert();
            const first = await claimJob(orm, 'first-worker');
            assert.ok(first);
            await processJob(orm, first, async () => { throw new RetryableJobError('다시 실행'); });
            await makeDue(job.id);
            const [owner] = await orm.em.getConnection().execute<{ pid: number }[]>('select pg_backend_pid() as pid');
            const [peer] = await other.em.getConnection().execute<{ pid: number }[]>('select pg_backend_pid() as pid');
            assert.notEqual(owner?.pid, peer?.pid);
            const claims = await Promise.all([claimJob(orm, 'worker-a'), claimJob(other, 'worker-b')]);
            const current = claims.filter((claimed) => claimed !== null);
            assert.equal(current.length, 1);
            assert.equal(current[0]?.id, job.id);
            assert.equal(current[0]?.attempt, 2);
            const processing = await saved(job.id);
            assert.equal(processing.status, JobStatus.processing);
            assert.equal(processing.attempt, 2);
            assert.equal(processing.workerId, current[0]?.workerId);
            assert.notEqual(processing.workerId, first.workerId);
            assert.equal(processing.processingStartedAt?.getTime(), current[0]?.processingStartedAt.getTime());
            assert.ok(processing.processingStartedAt!.getTime() >= first.processingStartedAt.getTime());
        });

        await t.test('Worker는 Retry를 DB에 예약한 뒤 대기 중인 다른 Job 처리를 계속한다', async (t) => {
            const retry = await insert({ availableAt: new Date(0) });
            const next = await insert();
            const handled: string[] = [];
            const controller = new AbortController();
            await runWorker(orm, 'worker-a', async (job) => {
                handled.push(job.id);
                if (job.id === retry.id) throw new RetryableJobError('다음 Claim에서 재시도');
                controller.abort();
                return { completed: true };
            }, AbortSignal.any([controller.signal, t.signal]));
            assert.deepEqual(handled, [retry.id, next.id]);
            assert.equal((await saved(retry.id)).status, JobStatus.queued);
            assert.equal((await saved(retry.id)).attempt, 1);
            assert.equal((await saved(next.id)).status, JobStatus.succeeded);
        });

        await t.test('Claim·Retry 예약·최종 결과 저장의 DB 오류는 Worker를 중단하고 상태를 부분 변경하지 않는다', async (t) => {
            await withJobUpdateTrigger(database, 'reject_outcome', `
                if old.status = 'processing' then raise exception '검증용 결과 저장 오류'; end if;
                return new;
            `, async (replaceBody) => {
                for (const outcome of ['retry', 'success', 'failure'] as const) {
                    await reset();
                    const job = await insert({ availableAt: new Date(0) });
                    const next = await insert();
                    let calls = 0;
                    let before: unknown;
                    await assert.rejects(runWorker(orm, 'worker-a', async (claimed) => {
                        calls++;
                        before = await snapshot(claimed.id);
                        if (outcome === 'retry') throw new RetryableJobError('검증용 일시적 실패');
                        if (outcome === 'failure') throw new Error('검증용 영구적 실패');
                        return { completed: true };
                    }, t.signal), /검증용 결과 저장 오류/);
                    assert.equal(calls, 1, 'DB 오류로 Handler를 즉시 다시 실행하지 않습니다');
                    assert.deepEqual(await snapshot(job.id), before);
                    assert.equal((await saved(job.id)).status, JobStatus.processing);
                    assert.equal((await saved(next.id)).attempt, 0);
                }

                await reset();
                const queued = await insert();
                const before = await snapshot(queued.id);
                await replaceBody("raise exception '검증용 Claim 오류';");
                let calls = 0;
                await assert.rejects(runWorker(orm, 'worker-a', async () => {
                    calls++;
                    return null;
                }, t.signal), /검증용 Claim 오류/);
                assert.equal(calls, 0, 'Claim 실패 시 Handler를 실행하지 않습니다');
                assert.deepEqual(await snapshot(queued.id), before);
            });
        });

        await t.test('Retry의 조건부 UPDATE 0건은 오류로 처리하고 다른 실행의 상태를 덮어쓰지 않는다', async () => {
            const job = await insert();
            const first = await claimJob(orm, 'worker-a');
            assert.ok(first);
            const lastError = { message: '이전 실행 실패' };
            for (const stale of [{ ...first, workerId: 'wrong-worker' }, { ...first, attempt: 0 }]) {
                const before = await snapshot(job.id);
                assert.equal(await scheduleRetry(orm, stale, lastError), false);
                await assert.rejects(processJob(orm, stale, async () => { throw new RetryableJobError('실패'); }), /실행 상태가 변경되었습니다/);
                assert.deepEqual(await snapshot(job.id), before);
            }

            assert.equal(await scheduleRetry(orm, first, lastError), true);
            const queued = await snapshot(job.id);
            assert.equal(await scheduleRetry(orm, first, lastError), false);
            assert.deepEqual(await snapshot(job.id), queued);
            await makeDue(job.id);
            // 같은 workerId를 재사용해도 이전 attempt는 소유권을 갖지 못한다.
            const current = await claimJob(other, 'worker-a');
            assert.ok(current);
            assert.equal(current.attempt, 2);
            const processing = await snapshot(job.id);
            await assert.rejects(processJob(orm, first, async () => { throw new RetryableJobError('늦은 실패'); }), /실행 상태가 변경되었습니다/);
            await assert.rejects(processJob(orm, first, async () => ({ stale: true })), /실행 상태가 변경되었습니다/);
            assert.deepEqual(await snapshot(job.id), processing);
            await processJob(other, current, executeHandler);
            const completed = await snapshot(job.id);
            await assert.rejects(processJob(orm, current, async () => { throw new RetryableJobError('완료 이후 실패'); }), /실행 상태가 변경되었습니다/);
            assert.deepEqual(await snapshot(job.id), completed);
        });

        await t.test('실제 API와 독립 Worker는 flaky를 1초·2초 Backoff 후 세 번째 실행에서 완료한다', { timeout: 25_000 }, async (t) => {
            await withJobProcesses(schema, t.signal, async (start) => {
                const api = start('api/server.ts');
                const address = (await api.ready(/Server listening at (http:\/\/127\.0\.0\.1:\d+)/))[1];
                assert.ok(address);
                const response = await fetch(`${address}/jobs`, {
                    method: 'POST', headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({ type: 'flaky', payload: {} }), signal: t.signal,
                });
                assert.equal(response.status, 201);
                const created = await response.json() as { id: string; status: string };
                assert.equal(created.status, JobStatus.queued);
                const registered = await saved(created.id);
                assert.equal(registered.type, 'flaky');
                assert.equal(registered.attempt, 0);
                assert.equal(registered.maxAttempts, 3);
                const worker = start('worker/main.ts');
                await worker.ready(/Worker 시작/);

                const retries = new Map<number, Job>();
                const completed = await waitFor(async () => {
                    assert.equal(worker.child.exitCode, null, worker.output());
                    const response = await fetch(`${address}/jobs/${created.id}`, { signal: t.signal });
                    assert.equal(response.status, 200);
                    const result = await response.json() as { status: string; attempt: number; result: unknown; lastError: unknown };
                    assert.notEqual(result.status, JobStatus.failed, JSON.stringify(result));
                    if (result.status === JobStatus.queued && result.attempt > 0) {
                        const retry = await saved(created.id);
                        if (retry.status === JobStatus.queued) retries.set(retry.attempt, retry);
                    }
                    return result.status === JobStatus.succeeded ? result : undefined;
                }, t.signal);
                assert.equal(completed.attempt, 3);
                assert.deepEqual(completed.result, { attempt: 3 });
                assert.equal(completed.lastError, null);
                assert.deepEqual([...retries.keys()], [1, 2]);
                const first = retries.get(1)!;
                const second = retries.get(2)!;
                assert.equal(first.availableAt.getTime() - first.updatedAt.getTime(), 1_000);
                assert.equal(second.availableAt.getTime() - second.updatedAt.getTime(), 2_000);
                assert.deepEqual(first.lastError, { name: 'RetryableJobError', message: '일시적인 Job 실패' });
                assert.deepEqual(second.lastError, first.lastError);
                assert.ok(second.processingStartedAt!.getTime() >= first.availableAt.getTime());
                assert.ok((await saved(created.id)).processingStartedAt!.getTime() >= second.availableAt.getTime());

                assert.equal(worker.child.kill('SIGTERM'), true);
                assert.deepEqual(await worker.exited, { code: 0, signal: null }, worker.output());
                assert.equal(api.child.kill('SIGTERM'), true);
                assert.deepEqual(await api.exited, { code: 0, signal: null }, api.output());
            });
        });

        await t.test('독립 Worker는 SIGINT·SIGTERM 요청 시 현재 Job 완료를 기다리고 다음 Claim을 중단한다', { timeout: 25_000 }, async (t) => {
            for (const signal of ['SIGINT', 'SIGTERM'] as const) {
                await reset();
                const current = await insert({ payload: { durationMs: 1_500 }, availableAt: new Date(0) });
                const next = await insert();
                await withJobProcesses(schema, t.signal, async (start) => {
                    const worker = start('worker/main.ts');
                    await worker.ready(/Worker 시작/);
                    await waitFor(async () => (await saved(current.id)).status === JobStatus.processing ? true : undefined, t.signal);
                    assert.equal(worker.child.kill(signal), true);
                    await worker.ready(/Worker 종료 요청/);
                    assert.equal((await saved(current.id)).status, JobStatus.processing);
                    assert.equal(worker.child.exitCode, null);
                    assert.deepEqual(await worker.exited, { code: 0, signal: null }, worker.output());
                    assert.equal((await saved(current.id)).status, JobStatus.succeeded);
                    assert.deepEqual((await saved(current.id)).result, { durationMs: 1_500 });
                    assert.equal((await saved(next.id)).status, JobStatus.queued);
                    assert.equal((await saved(next.id)).attempt, 0);
                });
            }
        });

        await t.test('독립 Worker는 Retry 예약의 DB 오류 시 실패 종료하고 Handler를 다시 실행하지 않는다', { timeout: 20_000 }, async (t) => {
            const job = await insert({ type: 'flaky', payload: {} });
            await withJobUpdateTrigger(database, 'reject_retry', `
                if old.status = 'processing' and new.status = 'queued' then
                    raise exception '검증용 Retry 예약 오류';
                end if;
                return new;
            `, async () => {
                await withJobProcesses(schema, t.signal, async (start) => {
                    const worker = start('worker/main.ts');
                    assert.deepEqual(await worker.exited, { code: 1, signal: null }, worker.output());
                    assert.match(worker.output(), /검증용 Retry 예약 오류/);
                    const processing = await saved(job.id);
                    assert.equal(processing.status, JobStatus.processing);
                    assert.equal(processing.attempt, 1);
                    assert.equal(processing.result, null);
                    assert.equal(processing.lastError, null);
                });
            });
        });
    });
});
