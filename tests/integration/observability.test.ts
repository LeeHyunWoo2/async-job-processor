import assert from 'node:assert/strict';
import test from 'node:test';

import { Gauge } from '@prometheus-io/client';

import { createApp } from '../../src/api/app.ts';
import { createLogger } from '../../src/observability/logger.ts';
import { createMetricsRegistry } from '../../src/observability/metrics.ts';
import { withJobProcesses } from '../helpers/job-process.ts';
import { withJobTestDatabase } from '../helpers/job-test-database.ts';

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
            assert.equal(empty.body.trim(), '');
            assert.match(empty.headers['content-type']!, /text\/plain; version=0\.0\.4/);
            new Gauge({ name: 'test_api', help: '검증용 API Gauge', registers: [registry] }).set(4);
            assert.match((await app.inject('/metrics')).body, /test_api 4\n/);
            assert.equal((await silent.inject('/metrics')).body.trim(), '');
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
            const logs = output.trim().split('\n').map((line) => JSON.parse(line));
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
                const url = `http://127.0.0.1:${started.metricsPort}/metrics`;
                assert.equal((await fetch(url)).status, 200);
                assert.equal(worker.child.kill(signal), true);
                assert.deepEqual(await worker.exited, { code: 0, signal: null }, worker.output());
                assert.match(worker.output(), /Worker 종료 요청/);
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
            assert.ok(failed.err.message);
            const started = logs.find((log) => log.msg === 'Worker 시작');
            await assert.rejects(fetch(`http://127.0.0.1:${started.metricsPort}/metrics`));
        });
    });
});
