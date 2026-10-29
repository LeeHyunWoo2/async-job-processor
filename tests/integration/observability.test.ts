import assert from 'node:assert/strict';
import test from 'node:test';

import { Counter, Gauge } from '@prometheus-io/client';

import { createApp } from '../../src/api/app.ts';
import { createLogger } from '../../src/observability/logger.ts';
import { createMetricsRegistry } from '../../src/observability/metrics.ts';
import { Job, JobStatus } from '../../src/db/entities/job.ts';
import { executeHandler } from '../../src/jobs/handlers.ts';
import { RetryableJobError } from '../../src/jobs/retry.ts';
import { claimJob, enqueueJob } from '../../src/jobs/store.ts';
import { processJob, runWorker } from '../../src/worker/run.ts';
import { withJobProcesses, waitFor } from '../helpers/job-process.ts';
import { withJobTestDatabase, withJobUpdateTrigger } from '../helpers/job-test-database.ts';
import { captureWorkerObservability, metricValue } from '../helpers/observability.ts';

test('API Metrics와 요청별 Logger를 기존 PostgreSQL 연결에서 사용한다', async () => {
    await withJobTestDatabase('job', 2, async ({ connections }) => {
        const registry = createMetricsRegistry();
        let output = '';
        const logger = createLogger('api', { write: (chunk: string) => { output += chunk; } });
        const app = createApp(connections[0]!, logger, registry);
        const silent = createApp(connections[1]!, false);
        app.get('/test-request', async (request) => {
            request.log.info('요청별 Logger 확인');
            return { id: request.id };
        });
        try {
            const empty = await app.inject('/metrics');
            assert.equal(empty.statusCode, 200);
            assert.match(empty.body, /async_job_queue_depth 0\n/);
            assert.match(empty.body, /async_job_enqueued_total 0\n/);
            assert.match(empty.headers['content-type']!, /text\/plain; version=0\.0\.4/);
            new Gauge({ name: 'test_api', help: '검증용 API Gauge', registers: [registry] }).set(4);
            assert.match((await app.inject('/metrics')).body, /test_api 4\n/);
            assert.ok(!(await silent.inject('/metrics')).body.includes('test_api'));
            new Gauge({
                name: 'test_broken', help: '검증용 수집 오류', registers: [registry],
                collect() { throw new Error('검증용 수집 실패'); },
            });
            assert.equal((await app.inject('/metrics')).statusCode, 500);
            assert.equal((await app.inject('/health')).statusCode, 200);
            registry.removeSingleMetric('test_broken');
            const first = await app.inject({ url: '/test-request', headers: { authorization: '검증용 비밀 값' } });
            const second = await app.inject('/test-request');
            assert.notEqual(first.json().id, second.json().id);
            const created = await app.inject({ method: 'POST', url: '/jobs', payload: { type: 'slow', payload: { durationMs: 1 } } });
            assert.equal(created.statusCode, 201);
            const invalid = await app.inject({ method: 'POST', url: '/jobs', payload: { type: 'invalid', payload: {} } });
            assert.equal(invalid.statusCode, 400);
            assert.equal(await metricValue(registry, 'async_job_enqueued_total'), 1);
            assert.match((await silent.inject('/metrics')).body, /async_job_enqueued_total 0\n/);
            assert.match((await app.inject('/metrics')).body, /async_job_queue_depth 1\n/);
            const logs = output.trim().split('\n').map((line) => JSON.parse(line));
            const enqueued = logs.filter((log) => log.event === 'job.enqueued');
            assert.equal(enqueued.length, 1);
            assert.equal(enqueued[0].jobId, created.json().id);
            assert.equal(enqueued[0].jobType, 'slow');
            assert.equal(enqueued[0].status, 'queued');
            assert.equal(enqueued[0].level, 30);
            assert.ok(enqueued[0].reqId);
            assert.ok(logs.some((log) => log.reqId === enqueued[0].reqId && log.req?.method === 'POST'));
            assert.ok(!Object.hasOwn(enqueued[0], 'payload'));
            for (const response of [first, second]) {
                const requests = logs.filter((log) => log.reqId === response.json().id);
                assert.equal(requests.find((log) => log.req)?.component, 'api');
                assert.ok(requests.some((log) => log.msg === '요청별 Logger 확인'));
                assert.ok(requests.some((log) => log.res?.statusCode === 200));
            }
            assert.ok(!output.includes('검증용 비밀 값'));
        } finally {
            await app.close();
            await silent.close();
        }
    });
});

test('독립 Worker의 Metrics 서버는 SIGINT·SIGTERM과 DB 오류 종료 시 함께 정리된다', { timeout: 30_000 }, async (t) => {
    await withJobTestDatabase('job', 2, async ({ schema, connections, table }) => {
        await withJobProcesses(schema, t.signal, async (start) => {
            for (const signal of ['SIGINT', 'SIGTERM'] as const) {
                const worker = start('worker/main.ts');
                await worker.ready(/Worker 시작/);
                const logs = worker.output().trim().split('\n').map((line) => JSON.parse(line));
                const started = logs.find((log) => log.msg === 'Worker 시작');
                assert.equal(started.component, 'worker');
                assert.equal(started.event, 'worker.started');
                assert.ok(started.workerId);
                const url = `http://127.0.0.1:${started.metricsPort}/metrics`;
                assert.equal((await fetch(url)).status, 200);
                assert.equal(worker.child.kill(signal), true);
                assert.deepEqual(await worker.exited, { code: 0, signal: null }, worker.output());
                assert.match(worker.output(), /Worker 종료 요청/);
                const stopping = worker.output().trim().split('\n').map((line) => JSON.parse(line)).find((log) => log.event === 'worker.stopping');
                assert.equal(stopping.workerId, started.workerId);
                assert.equal(stopping.signal, signal);
                assert.equal(stopping.level, 30);
                await assert.rejects(fetch(url));
            }
            const worker = start('worker/main.ts');
            await worker.ready(/Worker 시작/);
            // 테스트 schema만 변경하여 다음 Polling의 DB 오류를 재현한다.
            await connections[0]!.em.getConnection().execute(`drop table ${table}`);
            assert.deepEqual(await worker.exited, { code: 1, signal: null }, worker.output());
            const logs = worker.output().trim().split('\n').map((line) => JSON.parse(line));
            const failed = logs.find((log) => log.level === 50);
            assert.equal(failed.component, 'worker');
            assert.equal(failed.event, 'worker.error');
            assert.ok(failed.err.message);
            const started = logs.find((log) => log.msg === 'Worker 시작');
            await assert.rejects(fetch(`http://127.0.0.1:${started.metricsPort}/metrics`));
        });
    });
});

test('Job Lifecycle 계측은 실제 PostgreSQL 상태 전이와 Handler 실행을 반영한다', { timeout: 45_000 }, async (t) => {
    await withJobTestDatabase('job', 3, async (database) => {
        const { connections, table, reset } = database;
        const orm = connections[0]!;
        const other = connections[1]!;
        const peer = connections[2]!;
        const saved = (id: string) => other.em.fork().findOneOrFail(Job, id);
        const insert = async (values: Partial<Job> = {}) => {
            const em = orm.em.fork();
            const job = em.create(Job, { type: 'slow', payload: { durationMs: 1 }, maxAttempts: 3, ...values });
            em.persist(job);
            await em.flush();
            return job;
        };
        t.beforeEach(reset);

        await t.test('DB Gauge는 Backoff·Attempt 제한·Stale을 구분하고 매 Scrape마다 다시 조회한다', async () => {
            const app = createApp(peer, false);
            try {
                const ready = await insert();
                await insert({ availableAt: new Date(Date.now() + 60_000) });
                await insert({ attempt: 3 });
                await insert({ status: JobStatus.processing, processingStartedAt: new Date(0) });
                await insert({ status: JobStatus.processing, processingStartedAt: new Date() });
                await insert({ status: JobStatus.succeeded });
                await insert({ status: JobStatus.failed });
                const scrape = await app.inject('/metrics');
                assert.equal(scrape.statusCode, 200);
                assert.match(scrape.body, /async_job_queue_depth 3\n/);
                assert.match(scrape.body, /async_job_ready_queue_depth 1\n/);
                assert.match(scrape.body, /async_job_processing_jobs 2\n/);
                const claim = await claimJob(other, 'gauge-worker');
                assert.equal(claim?.id, ready.id);
                const changed = await app.inject('/metrics');
                assert.match(changed.body, /async_job_queue_depth 2\n/);
                assert.match(changed.body, /async_job_ready_queue_depth 0\n/);
                assert.match(changed.body, /async_job_processing_jobs 3\n/);
                // 동일 연결의 jobs 접근만 차단해 Gauge SQL의 실제 DB 오류를 재현한다.
                await peer.em.getConnection().execute("set search_path = pg_catalog");
                assert.equal((await app.inject('/metrics')).statusCode, 500);
                assert.equal((await app.inject('/health')).statusCode, 200);
                const observation = captureWorkerObservability('gauge-worker');
                assert.ok(claim);
                await processJob(other, claim, executeHandler, observation);
                assert.equal((await saved(ready.id)).status, JobStatus.succeeded);
            } finally {
                await app.close();
            }
        });

        await t.test('성공·최종 실패·Retry는 각각의 Event와 Counter를 기록하고 Handler의 모든 종료를 측정한다', async (t) => {
            const slow = await insert();
            const fail = await insert({ type: 'fail', payload: {} });
            const flaky = await insert({ type: 'flaky', payload: {} });
            const observation = captureWorkerObservability('lifecycle-worker');
            const controller = new AbortController();
            await runWorker(orm, 'lifecycle-worker', async (job) => {
                assert.equal(observation.logs.at(-1)?.event, 'job.handler_started');
                const claimed = observation.logs.at(-2)!;
                assert.equal(claimed.event, 'job.claimed');
                assert.equal(claimed.jobId, job.id);
                assert.equal(claimed.attempt, job.attempt);
                assert.equal((await saved(job.id)).status, JobStatus.processing);
                if (job.id === flaky.id && job.attempt === 3) controller.abort();
                return executeHandler(job);
            }, AbortSignal.any([controller.signal, t.signal]), observation);
            const events = (id: string) => observation.logs.filter((log) => log.jobId === id).map((log) => log.event);
            assert.deepEqual(events(slow.id), ['job.claimed', 'job.handler_started', 'job.handler_finished', 'job.succeeded']);
            assert.deepEqual(events(fail.id), ['job.claimed', 'job.handler_started', 'job.handler_finished', 'job.failed']);
            assert.deepEqual(events(flaky.id), [
                'job.claimed', 'job.handler_started', 'job.handler_finished', 'job.retry_scheduled',
                'job.claimed', 'job.handler_started', 'job.handler_finished', 'job.retry_scheduled',
                'job.claimed', 'job.handler_started', 'job.handler_finished', 'job.succeeded',
            ]);
            for (const log of observation.logs) {
                assert.equal(log.component, 'worker');
                assert.equal(log.workerId, 'lifecycle-worker');
                assert.ok(log.attempt! >= 1);
                assert.equal(log.level, log.event === 'job.failed' || log.event === 'job.retry_scheduled' ? 40 : 30);
                if (log.event === 'job.handler_started' || log.event === 'job.handler_finished') {
                    assert.equal(log.status, undefined, 'Handler 종료 Marker는 DB 결과 저장을 뜻하지 않습니다');
                    if (log.event === 'job.handler_finished') assert.ok(Number.isFinite(log.handlerElapsedMs) && log.handlerElapsedMs! >= 0);
                } else {
                    assert.equal(log.status, log.event === 'job.claimed' ? 'processing' : log.event === 'job.retry_scheduled' ? 'queued' : log.event === 'job.failed' ? 'failed' : 'succeeded');
                }
                assert.ok(!Object.hasOwn(log, 'payload') && !Object.hasOwn(log, 'result'));
            }
            const retries = observation.logs.filter((log) => log.event === 'job.retry_scheduled');
            assert.deepEqual(retries.map((log) => log.attempt), [1, 2]);
            assert.ok(retries.every((log) => log.err?.message === '일시적인 Job 실패' && log.err.stack));
            assert.equal(observation.logs.find((log) => log.event === 'job.failed')?.err?.message, '의도적인 Job 실패');
            assert.equal(await metricValue(observation.registry, 'async_job_claimed_total'), 5);
            assert.equal(await metricValue(observation.registry, 'async_job_succeeded_total'), 2);
            assert.equal(await metricValue(observation.registry, 'async_job_failed_total'), 1);
            assert.equal(await metricValue(observation.registry, 'async_job_retry_scheduled_total'), 2);
            assert.equal(await metricValue(observation.registry, 'async_job_handler_duration_seconds_count'), 5);
            assert.ok(await metricValue(observation.registry, 'async_job_handler_duration_seconds_sum') > 0);
            const isolated = captureWorkerObservability('isolated-worker');
            assert.equal(await metricValue(isolated.registry, 'async_job_claimed_total'), 0);
            assert.ok(!(await observation.registry.metrics()).includes('async_job_enqueued_total'));
            assert.ok(!(await observation.registry.metrics()).includes('workerId='));
        });

        await t.test('Claim·Retry·최종 결과 저장의 DB 오류는 성공한 사건을 기록하지 않는다', async (t) => {
            await withJobUpdateTrigger(database, 'reject_observed_transition', `
                if old.status = 'processing' then raise exception '검증용 계측 결과 저장 오류'; end if;
                return new;
            `, async (replaceBody) => {
                for (const mode of ['success', 'failure', 'retry'] as const) {
                    await reset();
                    const job = await insert();
                    const observation = captureWorkerObservability('failed-write-worker');
                    await assert.rejects(runWorker(orm, 'failed-write-worker', async () => {
                        if (mode === 'failure') throw new Error('검증용 최종 실패');
                        if (mode === 'retry') throw new RetryableJobError('검증용 재시도');
                        return { completed: true };
                    }, t.signal, observation), /검증용 계측 결과 저장 오류/);
                    assert.equal((await saved(job.id)).status, JobStatus.processing);
                    assert.deepEqual(observation.logs.map((log) => log.event), ['job.claimed', 'job.handler_started', 'job.handler_finished']);
                    assert.equal(await metricValue(observation.registry, 'async_job_claimed_total'), 1);
                    for (const counter of ['succeeded', 'failed', 'retry_scheduled']) {
                        assert.equal(await metricValue(observation.registry, `async_job_${counter}_total`), 0);
                    }
                    assert.equal(await metricValue(observation.registry, 'async_job_handler_duration_seconds_count'), 1);
                }
                await reset();
                await insert();
                await replaceBody("raise exception '검증용 Claim 저장 오류';");
                const observation = captureWorkerObservability('failed-claim-worker');
                await assert.rejects(runWorker(orm, 'failed-claim-worker', executeHandler, t.signal, observation), /검증용 Claim 저장 오류/);
                assert.deepEqual(observation.logs, []);
                assert.equal(await metricValue(observation.registry, 'async_job_claimed_total'), 0);
                assert.equal(await metricValue(observation.registry, 'async_job_handler_duration_seconds_count'), 0);
            });
        });

        await t.test('결과 UPDATE가 Row Lock을 기다리는 동안에도 Handler Duration은 이미 확정된다', async (t) => {
            const job = await insert();
            const claimed = await claimJob(orm, 'duration-worker');
            assert.ok(claimed);
            const observation = captureWorkerObservability('duration-worker');
            let pending: Promise<void> | undefined;
            let duration = 0;
            try {
                await other.em.fork().transactional(async (em) => {
                    await em.execute(`select id from ${table} where id = ? for update`, [job.id]);
                    pending = processJob(orm, claimed, executeHandler, observation);
                    void pending.catch(() => {});
                    await waitFor(async () => await metricValue(observation.registry, 'async_job_handler_duration_seconds_count') === 1 ? true : undefined, t.signal);
                    duration = await metricValue(observation.registry, 'async_job_handler_duration_seconds_sum');
                    assert.ok(duration > 0);
                    assert.equal(await metricValue(observation.registry, 'async_job_succeeded_total'), 0);
                    assert.deepEqual(observation.logs.map((log) => log.event), ['job.handler_started', 'job.handler_finished']);
                });
                await pending;
                assert.equal(await metricValue(observation.registry, 'async_job_handler_duration_seconds_sum'), duration);
                assert.equal(await metricValue(observation.registry, 'async_job_succeeded_total'), 1);
                assert.deepEqual(observation.logs.map((log) => log.event), ['job.handler_started', 'job.handler_finished', 'job.succeeded']);
            } finally {
                await Promise.allSettled([pending]);
            }
        });

        await t.test('Recovery는 현재 Worker와 이전 소유자를 구분하고 최종 실패에 Recovered·Failed Counter만 함께 반영한다', async (t) => {
            const retry = await insert({ status: JobStatus.processing, workerId: 'previous-retry', attempt: 1, processingStartedAt: new Date(0) });
            const failed = await insert({ status: JobStatus.processing, workerId: 'previous-failed', attempt: 3, processingStartedAt: new Date(0) });
            const observation = captureWorkerObservability('recovery-worker');
            const controller = new AbortController();
            await runWorker(orm, 'recovery-worker', async (job) => {
                assert.equal(job.id, retry.id);
                controller.abort();
                return executeHandler(job);
            }, AbortSignal.any([controller.signal, t.signal]), observation);
            const recovered = observation.logs.filter((log) => log.event === 'job.recovered');
            assert.equal(recovered.length, 2);
            for (const original of [retry, failed]) {
                const log = recovered.find((log) => log.jobId === original.id)!;
                assert.equal(log.workerId, 'recovery-worker');
                assert.equal(log.previousWorkerId, original.workerId);
                assert.equal(log.attempt, original.attempt);
                assert.equal(log.status, original.id === retry.id ? 'queued' : 'failed');
                assert.equal(log.err?.name, 'StaleJobError');
                assert.equal(log.level, 40);
            }
            assert.ok(!observation.logs.some((log) => log.event === 'job.failed'));
            assert.equal(await metricValue(observation.registry, 'async_job_recovered_total'), 2);
            assert.equal(await metricValue(observation.registry, 'async_job_failed_total'), 1);
            assert.equal(await metricValue(observation.registry, 'async_job_retry_scheduled_total'), 0);
            assert.equal((await saved(failed.id)).status, JobStatus.failed);
        });

        await t.test('Logger·Counter·Histogram 오류는 성공·실패·Retry·Recovery의 DB 결과를 바꾸지 않는다', async (t) => {
            const slow = await insert();
            const fail = await insert({ type: 'fail', payload: {} });
            const flaky = await insert({ type: 'flaky', payload: {} });
            const stale = await insert({ status: JobStatus.processing, attempt: 3, processingStartedAt: new Date(0) });
            const observation = captureWorkerObservability('broken-observation-worker', () => { throw new Error('검증용 Logger 오류'); });
            const failObservation = () => { throw new Error('검증용 Metric 오류'); };
            for (const counter of [observation.metrics.claimed, observation.metrics.succeeded, observation.metrics.failed, observation.metrics.retryScheduled, observation.metrics.recovered]) {
                counter.inc = failObservation;
            }
            observation.metrics.handlerDuration.observe = failObservation;
            const controller = new AbortController();
            let calls = 0;
            await runWorker(orm, 'broken-observation-worker', async (job) => {
                calls++;
                if (job.id === flaky.id) controller.abort();
                return executeHandler(job);
            }, AbortSignal.any([controller.signal, t.signal]), observation);
            assert.equal(calls, 3);
            assert.equal((await saved(slow.id)).status, JobStatus.succeeded);
            assert.equal((await saved(fail.id)).status, JobStatus.failed);
            assert.equal((await saved(flaky.id)).status, JobStatus.queued);
            assert.equal((await saved(flaky.id)).attempt, 1);
            assert.equal((await saved(stale.id)).status, JobStatus.failed);
        });
    });
});

test('API는 INSERT 실패를 기록하지 않고 계측 오류 후에도 등록 성공을 응답한다', async () => {
    await withJobTestDatabase('job', 2, async ({ connections, table }) => {
        const registry = createMetricsRegistry();
        let output = '';
        const logger = createLogger('api', { write(chunk: string) { output += chunk; if (chunk.includes('"event":"job.enqueued"')) throw new Error('검증용 Logger 오류'); } });
        const app = createApp(connections[0]!, logger, registry);
        try {
            await connections[0]!.em.getConnection().execute('set search_path = pg_catalog');
            const failed = await app.inject({ method: 'POST', url: '/jobs', payload: { type: 'fail', payload: {} } });
            assert.equal(failed.statusCode, 500);
            assert.equal(await metricValue(registry, 'async_job_enqueued_total'), 0);
            assert.ok(!output.includes('"event":"job.enqueued"'));
            // schema를 명시한 search_path로 원상 복구한다.
            const schema = table.split('.')[0]!;
            await connections[0]!.em.getConnection().execute(`set search_path = ${schema}`);
            const enqueued = registry.getSingleMetric('async_job_enqueued_total');
            assert.ok(enqueued instanceof Counter);
            enqueued.inc = () => { throw new Error('검증용 Counter 오류'); };
            const created = await app.inject({ method: 'POST', url: '/jobs', payload: { type: 'fail', payload: {} } });
            assert.equal(created.statusCode, 201);
            assert.equal((await connections[1]!.em.fork().findOneOrFail(Job, created.json().id)).status, JobStatus.queued);
        } finally {
            await app.close();
        }
    });
});
