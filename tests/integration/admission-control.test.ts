import assert from 'node:assert/strict';
import test from 'node:test';

import { Counter } from '@prometheus-io/client';

import { createApp } from '../../src/api/app.ts';
import { Job, JobStatus } from '../../src/db/entities/job.ts';
import { claimJob, recoverStaleJobs, saveJobOutcome, scheduleRetry } from '../../src/jobs/store.ts';
import { createLogger } from '../../src/observability/logger.ts';
import { createMetricsRegistry } from '../../src/observability/metrics.ts';
import { waitFor, withJobProcesses } from '../helpers/job-process.ts';
import { withJobTestDatabase } from '../helpers/job-test-database.ts';
import { metricValue, type LogEntry } from '../helpers/observability.ts';

const input = { type: 'slow', payload: { durationMs: 1 } };
const lockSql = 'select pg_advisory_xact_lock(1668244581, 1)';

test('Admission은 모든 미완료 상태를 포함하고 99개에서 수락·100개에서 거부하며 입력 검증을 우선한다', async () => {
    await withJobTestDatabase('admission', 2, async ({ connections, table }) => {
        const [orm, observer] = connections;
        assert.ok(orm && observer);
        const registry = createMetricsRegistry();
        const logs: (LogEntry & { reason?: string; outstandingJobs?: number; maxOutstandingJobs?: number; res?: { statusCode: number } })[] = [];
        const logger = createLogger('api', { write(chunk: string) { logs.push(JSON.parse(chunk)); } });
        const app = createApp(orm, logger, registry);
        const post = (payload: unknown = input) => app.inject({
            method: 'POST', url: '/jobs', headers: { 'content-type': 'application/json' }, payload: JSON.stringify(payload),
        });
        try {
            await observer.em.execute(`
                insert into ${table} (type, payload, max_attempts)
                select 'slow', '{"durationMs":1}', 3 from generate_series(1, 95)
            `);
            await observer.em.execute(`
                insert into ${table} (type, payload, max_attempts, status, attempt, available_at, processing_started_at)
                values ('slow', '{"durationMs":1}', 3, 'queued', 0, now() + interval '1 day', null),
                       ('flaky', '{}', 3, 'queued', 1, now() + interval '1 second', null),
                       ('slow', '{"durationMs":1}', 3, 'processing', 1, now(), now()),
                       ('slow', '{"durationMs":1}', 3, 'processing', 1, now(), now() - interval '1 day'),
                       ('slow', '{"durationMs":1}', 3, 'succeeded', 1, now(), now()),
                       ('fail', '{}', 3, 'failed', 3, now(), now())
            `);
            const accepted = await post();
            assert.equal(accepted.statusCode, 201);
            assert.deepEqual(Object.keys(accepted.json()).sort(), ['id', 'status']);
            assert.equal(accepted.json().status, 'queued');
            assert.equal((await observer.em.fork().findOneOrFail(Job, accepted.json().id)).attempt, 0);
            const before = await observer.em.fork().count(Job);
            const rejected = await post();
            assert.equal(rejected.statusCode, 503);
            assert.deepEqual(rejected.json(), { error: '작업 처리 용량이 가득 찼습니다', code: 'QUEUE_CAPACITY_EXCEEDED' });
            assert.equal(rejected.headers['retry-after'], undefined);
            assert.equal((await post({ ...input, extra: true })).statusCode, 400);
            assert.equal((await app.inject({ method: 'POST', url: '/jobs', headers: { 'content-type': 'application/json' }, payload: '{' })).statusCode, 400);
            assert.equal(await observer.em.fork().count(Job), before, '거부와 입력 오류는 행을 추가하지 않아야 합니다');
            assert.equal(await observer.em.fork().count(Job, { status: { $in: ['queued', 'processing'] } }), 100);
            assert.equal(await metricValue(registry, 'async_job_enqueued_total'), 1);
            assert.equal(await metricValue(registry, 'async_job_enqueue_rejected_total'), 1);
            const events = logs.filter((log) => log.event);
            assert.deepEqual(events.map((log) => log.event), ['job.enqueued', 'job.enqueue_rejected']);
            const rejection = events[1]!;
            assert.equal(rejection.reason, 'QUEUE_CAPACITY_EXCEEDED');
            assert.equal(rejection.outstandingJobs, 100);
            assert.equal(rejection.maxOutstandingJobs, 100);
            assert.equal(rejection.level, 40);
            assert.equal(rejection.component, 'api');
            assert.ok(rejection.reqId);
            assert.notEqual(rejection.reqId, events[0]!.reqId);
            assert.ok(logs.some((log) => log.reqId === rejection.reqId && log.res?.statusCode === 503));
            for (const field of ['jobId', 'payload', 'result']) assert.equal(Object.hasOwn(rejection, field), false);
            assert.equal(events[0]!.jobId, accepted.json().id);
            await observer.em.fork().transactional(async (em) => {
                const [lock] = await em.execute<{ acquired: boolean }[]>('select pg_try_advisory_xact_lock(1668244581, 1) as acquired');
                assert.equal(lock?.acquired, true, '응답 이후에는 Admission 잠금이 해제되어야 합니다');
            });
        } finally {
            await app.close();
        }
    });
});

test('10개 실제 HTTP 요청은 서로 다른 DB 연결에서 잠금 경합 후 정확히 1개만 남은 슬롯을 사용한다', { timeout: 20_000 }, async (t) => {
    await withJobTestDatabase('admission', 12, async ({ connections, table, schema }) => {
        const blocker = connections[10]!;
        const observer = connections[11]!;
        const apps = connections.slice(0, 10).map((orm) => createApp(orm, false));
        const pending: Promise<{ status: number; body: { code?: string } }>[] = [];
        try {
            await observer.em.execute(`
                insert into ${table} (type, payload, max_attempts)
                select 'slow', '{"durationMs":1}', 3 from generate_series(1, 99)
            `);
            // DB 기본 격리 수준과 무관하게 Enqueue가 READ COMMITTED를 지정하는지 확인한다.
            for (const orm of connections.slice(0, 10)) {
                await orm.em.execute("set default_transaction_isolation = 'repeatable read'");
            }
            await observer.em.execute(`
                create function "${schema}".check_admission_transaction() returns trigger language plpgsql as $$
                begin
                    if current_setting('transaction_isolation') <> 'read committed' or not exists (
                        select 1 from pg_locks where pid = pg_backend_pid() and locktype = 'advisory'
                        and classid = 1668244581 and objid = 1 and objsubid = 2 and granted
                    ) then raise exception 'INSERT는 READ COMMITTED와 동일 연결의 Admission 잠금 안에서 실행되어야 합니다'; end if;
                    return new;
                end; $$
            `);
            await observer.em.execute(`
                create trigger check_admission_transaction before insert on ${table}
                for each row execute function "${schema}".check_admission_transaction()
            `);
            const urls = await Promise.all(apps.map((app) => app.listen({ host: '127.0.0.1', port: 0 })));
            await blocker.em.fork().transactional(async (em) => {
                await em.execute(lockSql);
                for (const url of urls) {
                    const request = fetch(`${url}/jobs`, {
                        method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(input),
                        signal: t.signal,
                    }).then(async (response) => ({ status: response.status, body: await response.json() }));
                    void request.catch(() => {});
                    pending.push(request);
                }
                const waiters = await waitFor(async () => {
                    const rows = await observer.em.execute<{ pid: number }[]>(`
                        select distinct l.pid from pg_locks l join pg_stat_activity a on a.pid = l.pid
                        where l.locktype = 'advisory' and l.classid = 1668244581 and l.objid = 1
                          and l.objsubid = 2 and not l.granted and a.wait_event = 'advisory'
                          and cardinality(pg_blocking_pids(l.pid)) > 0
                    `);
                    return rows.length === 10 ? rows : undefined;
                }, t.signal, 3_000);
                assert.equal(new Set(waiters.map((row) => row.pid)).size, 10, '10개의 물리 연결이 동시에 잠금을 기다려야 합니다');
                assert.equal(await observer.em.fork().count(Job), 99, '잠금 대기 중에는 INSERT가 없어야 합니다');
            });
            const results = await Promise.all(pending);
            assert.equal(results.filter((result) => result.status === 201).length, 1);
            assert.equal(results.filter((result) => result.status === 503).length, 9);
            assert.ok(results.filter((result) => result.status === 503).every((result) => result.body.code === 'QUEUE_CAPACITY_EXCEEDED'));
            assert.equal(await observer.em.fork().count(Job), 100);
            assert.equal(await observer.em.fork().count(Job, { status: { $in: ['queued', 'processing'] } }), 100);
            const metrics = await Promise.all(apps.map(async (app) => (await app.inject('/metrics')).body));
            const sum = (name: string) => metrics.reduce((total, text) => total + Number(text.match(new RegExp(`^${name} (\\d+)$`, 'm'))?.[1]), 0);
            assert.equal(sum('async_job_enqueued_total'), 1);
            assert.equal(sum('async_job_enqueue_rejected_total'), 9);
        } finally {
            await Promise.allSettled(pending);
            await Promise.all(apps.map((app) => app.close()));
        }
    });
});

test('정책 OFF는 Capacity COUNT·Advisory Lock 없이 100개를 넘어 등록하고 기존 Job을 실행한다', async () => {
    await withJobTestDatabase('admission', 2, async ({ connections, table }) => {
        const orm = connections[0]!;
        const blocker = connections[1]!;
        const registry = createMetricsRegistry();
        const app = createApp(orm, false, registry, { enableBackpressure: false, maxOutstandingJobs: 100 });
        const queries: string[] = [];
        const logger = orm.config.getLogger();
        const originalLogQuery = logger.logQuery;
        logger.logQuery = (context) => { queries.push(context.query ?? ''); originalLogQuery.call(logger, context); };
        try {
            await blocker.em.execute(`
                insert into ${table} (type, payload, max_attempts, available_at)
                select 'slow', '{"durationMs":1}', 3, now() + interval '1 day' from generate_series(1, 100)
            `);
            await blocker.em.fork().transactional(async (em) => {
                await em.execute(lockSql);
                const response = await app.inject({ method: 'POST', url: '/jobs', payload: input });
                assert.equal(response.statusCode, 201);
                assert.deepEqual(Object.keys(response.json()).sort(), ['id', 'status']);
                assert.equal(response.json().status, 'queued');
            });
            assert.ok(queries.some((sql) => /insert into jobs/i.test(sql)));
            assert.ok(!queries.some((sql) => /count\(|pg_advisory|begin/i.test(sql)), 'OFF는 기존 INSERT만 실행해야 합니다');
            assert.equal(await blocker.em.fork().count(Job, { status: 'queued' }), 101);
            const job = await claimJob(orm, 'off-worker');
            assert.ok(job);
            assert.equal(job.attempt, 1);
            assert.equal(await saveJobOutcome(orm, job, { status: 'succeeded', result: { durationMs: 1 } }), true);
            assert.equal(await metricValue(registry, 'async_job_enqueued_total'), 1);
            assert.equal(await metricValue(registry, 'async_job_enqueue_rejected_total'), 0);
        } finally {
            logger.logQuery = originalLogQuery;
            await app.close();
        }
    });
});

test('Claim·Retry·재시도 가능한 Recovery는 슬롯을 유지하고 성공·최종 실패·소진 Recovery만 반환한다', async () => {
    await withJobTestDatabase('admission', 2, async ({ connections, table }) => {
        const orm = connections[0]!;
        const worker = connections[1]!;
        const app = createApp(orm, false, undefined, { enableBackpressure: true, maxOutstandingJobs: 1 });
        const post = () => app.inject({ method: 'POST', url: '/jobs', payload: input });
        const full = async () => {
            assert.equal((await post()).statusCode, 503);
            assert.equal(await worker.em.fork().count(Job, { status: { $in: ['queued', 'processing'] } }), 1);
        };
        const stale = () => worker.em.execute(`update ${table} set processing_started_at = now() - interval '31 seconds' where status = 'processing'`);
        try {
            assert.equal((await post()).statusCode, 201);
            let claimed = await claimJob(worker, 'admission-worker');
            assert.ok(claimed);
            await full();
            assert.equal(await scheduleRetry(worker, claimed, { message: '검증용 Retry' }), true);
            await full();
            await worker.em.execute(`update ${table} set available_at = now() where id = ?`, [claimed.id]);
            claimed = await claimJob(worker, 'admission-worker');
            assert.ok(claimed);
            await stale();
            assert.equal((await recoverStaleJobs(worker))[0]?.status, 'queued');
            await full();
            claimed = await claimJob(worker, 'admission-worker');
            assert.ok(claimed);
            assert.equal(await saveJobOutcome(worker, claimed, { status: 'succeeded', result: {} }), true);
            assert.equal((await post()).statusCode, 201);
            claimed = await claimJob(worker, 'admission-worker');
            assert.ok(claimed);
            assert.equal(await saveJobOutcome(worker, claimed, { status: 'failed', lastError: { message: '검증용 최종 실패' } }), true);
            assert.equal((await post()).statusCode, 201);
            await worker.em.execute(`update ${table} set max_attempts = 1 where status = 'queued'`);
            assert.ok(await claimJob(worker, 'admission-worker'));
            await stale();
            assert.equal((await recoverStaleJobs(worker))[0]?.status, 'failed');
            assert.equal((await post()).statusCode, 201);
        } finally {
            await app.close();
        }
    });
});

test('COUNT·INSERT·COMMIT 오류는 500이며 성공·거부 관측을 남기지 않고 잠금을 해제한다', async () => {
    await withJobTestDatabase('admission', 2, async ({ connections, table, schema }) => {
        const orm = connections[0]!;
        const observer = connections[1]!;
        const registry = createMetricsRegistry();
        const logs: { event?: string }[] = [];
        const app = createApp(orm, createLogger('api', { write(chunk: string) { logs.push(JSON.parse(chunk)); } }), registry);
        try {
            await observer.em.execute(`
                create function "${schema}".reject_enqueue() returns trigger language plpgsql as $$
                begin raise exception '검증용 Job 등록 저장 오류'; end; $$
            `);
            for (const phase of ['count', 'insert', 'commit'] as const) {
                if (phase === 'count') await orm.em.execute('set search_path = pg_catalog');
                else await observer.em.execute(phase === 'insert' ? `
                    create trigger reject_enqueue before insert on ${table}
                    for each row execute function "${schema}".reject_enqueue()
                ` : `
                    create constraint trigger reject_enqueue after insert on ${table}
                    deferrable initially deferred for each row execute function "${schema}".reject_enqueue()
                `);
                try {
                    const response = await app.inject({ method: 'POST', url: '/jobs', payload: input });
                    assert.equal(response.statusCode, 500, phase);
                    assert.notEqual(response.json().code, 'QUEUE_CAPACITY_EXCEEDED');
                    assert.equal(await observer.em.fork().count(Job), 0);
                    assert.equal(await metricValue(registry, 'async_job_enqueued_total'), 0);
                    assert.equal(await metricValue(registry, 'async_job_enqueue_rejected_total'), 0);
                    assert.deepEqual(logs.filter((log) => log.event), []);
                    await observer.em.fork().transactional(async (em) => {
                        const [lock] = await em.execute<{ acquired: boolean }[]>('select pg_try_advisory_xact_lock(1668244581, 1) as acquired');
                        assert.equal(lock?.acquired, true, `${phase} 오류 후 잠금이 남아서는 안 됩니다`);
                    });
                } finally {
                    if (phase === 'count') await orm.em.execute(`set search_path = "${schema}"`);
                    else await observer.em.execute(`drop trigger reject_enqueue on ${table}`);
                }
            }
            assert.equal((await app.inject({ method: 'POST', url: '/jobs', payload: input })).statusCode, 201);
            assert.equal(await observer.em.fork().count(Job), 1);
            assert.equal(await metricValue(registry, 'async_job_enqueued_total'), 1);
        } finally {
            await app.close();
        }
    });
});

test('거부 Counter·Logger 오류가 발생해도 실제 용량 거부와 DB 행 수를 유지한다', async () => {
    await withJobTestDatabase('admission', 2, async ({ connections }) => {
        const registry = createMetricsRegistry();
        const logger = createLogger('api', { write(chunk: string) {
            if (chunk.includes('"event":"job.enqueue_rejected"')) throw new Error('검증용 거부 Logger 오류');
        } });
        const app = createApp(connections[0]!, logger, registry, { enableBackpressure: true, maxOutstandingJobs: 1 });
        try {
            assert.equal((await app.inject({ method: 'POST', url: '/jobs', payload: input })).statusCode, 201);
            const rejected = registry.getSingleMetric('async_job_enqueue_rejected_total');
            assert.ok(rejected instanceof Counter);
            rejected.inc = () => { throw new Error('검증용 거부 Counter 오류'); };
            const response = await app.inject({ method: 'POST', url: '/jobs', payload: input });
            assert.equal(response.statusCode, 503);
            assert.equal(response.json().code, 'QUEUE_CAPACITY_EXCEEDED');
            assert.equal(await connections[1]!.em.fork().count(Job), 1);
            assert.equal(await metricValue(registry, 'async_job_enqueued_total'), 1);
        } finally {
            await app.close();
        }
    });
});

test('독립 API 서버는 환경변수의 ON/OFF·제한값을 실제 HTTP 등록에 적용한다', { timeout: 15_000 }, async (t) => {
    const previous = {
        ENABLE_BACKPRESSURE: process.env.ENABLE_BACKPRESSURE,
        MAX_OUTSTANDING_JOBS: process.env.MAX_OUTSTANDING_JOBS,
    };
    try {
        await withJobTestDatabase('admission', 2, async ({ schema, reset }) => {
            await withJobProcesses(schema, t.signal, async (start) => {
                for (const enabled of ['true', 'false']) {
                    process.env.ENABLE_BACKPRESSURE = enabled;
                    process.env.MAX_OUTSTANDING_JOBS = '1';
                    const api = start('api/server.ts');
                    const match = await api.ready(/Server listening at (http:\/\/127\.0\.0\.1:\d+)/);
                    const post = () => fetch(`${match[1]}/jobs`, {
                        method: 'POST', headers: { 'content-type': 'application/json' },
                        body: JSON.stringify(input), signal: t.signal,
                    });
                    assert.equal((await post()).status, 201);
                    assert.equal((await post()).status, enabled === 'true' ? 503 : 201);
                    const metrics = await (await fetch(`${match[1]}/metrics`, { signal: t.signal })).text();
                    assert.match(metrics, new RegExp(`async_job_enqueue_rejected_total ${enabled === 'true' ? 1 : 0}\\n`));
                    assert.equal(api.child.kill('SIGTERM'), true);
                    assert.deepEqual(await api.exited, { code: 0, signal: null }, api.output());
                    await reset();
                }
            });
        });
    } finally {
        for (const [name, value] of Object.entries(previous)) {
            if (value === undefined) delete process.env[name];
            else process.env[name] = value;
        }
    }
});
