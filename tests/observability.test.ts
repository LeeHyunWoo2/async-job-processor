import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { once } from 'node:events';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

import { Counter, Gauge, Histogram, register } from '@prometheus-io/client';

import { createLogger } from '../src/observability/logger.ts';
import { createEventLoopMetrics } from '../src/observability/event-loop.ts';
import { createApiMetrics, createMetricsRegistry, createWorkerMetrics } from '../src/observability/metrics.ts';
import { createWorkerMetricsServer } from '../src/worker/metrics-server.ts';
import { metricValue } from './helpers/observability.ts';

test('공통 Logger는 component와 Error를 JSON으로 출력하고 LOG_LEVEL을 적용한다', () => {
    const previous = process.env.LOG_LEVEL;
    let output = '';
    const destination = { write: (chunk: string) => { output += chunk; } };
    try {
        delete process.env.LOG_LEVEL;
        const api = createLogger('api', destination);
        assert.equal(api.level, 'info');
        api.debug('출력하지 않는 로그');
        api.info('API 확인');
        process.env.LOG_LEVEL = 'warn';
        const worker = createLogger('worker', destination);
        worker.info('출력하지 않는 로그');
        worker.error({ err: new Error('검증용 오류') }, 'Worker 확인');
        const logs = output.trim().split('\n').map((line) => JSON.parse(line));
        assert.equal(logs.length, 2);
        assert.equal(logs[0].component, 'api');
        assert.equal(logs[1].component, 'worker');
        assert.equal(logs[1].err.message, '검증용 오류');
        assert.match(logs[1].err.stack, /검증용 오류/);
        process.env.LOG_LEVEL = 'silent';
        createLogger('api', destination).info('출력하지 않는 로그');
        assert.equal(output.trim().split('\n').length, 2);
    } finally {
        if (previous === undefined) delete process.env.LOG_LEVEL;
        else process.env.LOG_LEVEL = previous;
    }
});

test('Registry는 빈 응답과 Counter·Gauge·Histogram을 지원하며 등록 상태를 격리한다', async () => {
    const api = createMetricsRegistry();
    const worker = createMetricsRegistry();
    const isolated = createMetricsRegistry();
    assert.equal((await isolated.metrics()).trim(), '');
    assert.match(isolated.contentType, /text\/plain; version=0\.0\.4/);
    const counter = new Counter({ name: 'test_total', help: '검증용 Counter', registers: [api] });
    new Counter({ name: 'test_total', help: '검증용 Counter', registers: [worker] }).inc(2);
    counter.inc();
    new Gauge({ name: 'test_gauge', help: '검증용 Gauge', registers: [api] }).set(3);
    new Histogram({ name: 'test_duration_seconds', help: '검증용 Histogram', registers: [api] }).observe(0.1);
    assert.match(await api.metrics(), /test_total 1\n/);
    assert.match(await api.metrics(), /test_gauge 3\n/);
    assert.match(await api.metrics(), /test_duration_seconds_count 1\n/);
    assert.match(await worker.metrics(), /test_total 2\n/);
    assert.equal((await isolated.metrics()).trim(), '');
    assert.equal(register.getSingleMetric('test_total'), undefined);
});

test('API와 Worker의 초기 설정 오류도 공통 JSON Logger로 출력된다', () => {
    for (const entry of ['api/server.ts', 'worker/main.ts']) {
        const child = spawnSync(process.execPath, [fileURLToPath(new URL(`../src/${entry}`, import.meta.url))], {
            env: { ...process.env, DB_HOST: '', LOG_LEVEL: 'info' }, encoding: 'utf8', timeout: 10_000,
        });
        assert.equal(child.status, 1, child.stderr);
        assert.equal(child.stderr, '');
        const log = JSON.parse(child.stdout.trim());
        assert.equal(log.component, entry.startsWith('api/') ? 'api' : 'worker');
        if (entry.startsWith('worker/')) assert.equal(log.event, 'worker.error');
        assert.match(log.err.message, /필수 환경 변수가 없습니다: DB_HOST/);
    }
});

test('분리 프로세스의 Job·Event Loop Metric 19개는 Registry를 격리하고 실행별 Label을 사용하지 않는다', async () => {
    const api = createMetricsRegistry();
    const worker = createMetricsRegistry();
    createApiMetrics(api);
    createWorkerMetrics(worker);
    const apiLoop = createEventLoopMetrics(api);
    const workerLoop = createEventLoopMetrics(worker);
    try {
        const metrics = [...await api.getMetricsAsJSON(), ...await worker.getMetricsAsJSON()];
        assert.equal(metrics.length, 19);
        assert.equal(metrics.filter((metric) => metric.type === 'counter').length, 7);
        assert.equal(metrics.filter((metric) => metric.type === 'gauge').length, 11);
        assert.equal(metrics.filter((metric) => metric.type === 'histogram').length, 1);
        assert.equal(api.getMetricsAsArray().length, 9);
        assert.equal(worker.getMetricsAsArray().length, 10);
        for (const metric of metrics) {
            assert.equal(register.getSingleMetric(metric.name), undefined);
            for (const sample of metric.values) {
                assert.ok(Object.keys(sample.labels).every((label) => label === 'le'));
            }
        }
    } finally {
        apiLoop.disable();
        workerLoop.disable();
    }
});

test('Worker HTTP는 지정 Registry를 사용하며 조회 실패 후에도 응답을 계속한다', async () => {
    const registry = createMetricsRegistry();
    const logger = createLogger('worker', { write() {} });
    const server = createWorkerMetricsServer(registry, logger);
    server.listen({ host: '127.0.0.1', port: 0 });
    await once(server, 'listening');
    const address = server.address();
    assert.ok(address && typeof address === 'object');
    const url = `http://127.0.0.1:${address.port}`;
    try {
        const empty = await fetch(`${url}/metrics`);
        assert.equal(empty.status, 200);
        assert.match(empty.headers.get('content-type')!, /text\/plain; version=0\.0\.4/);
        assert.equal((await empty.text()).trim(), '');
        new Gauge({ name: 'test_worker', help: '검증용 Worker Gauge', registers: [registry] }).set(7);
        assert.match(await (await fetch(`${url}/metrics?probe=1`)).text(), /test_worker 7\n/);
        assert.equal((await fetch(`${url}/other`)).status, 404);
        assert.equal((await fetch(`${url}/metrics`, { method: 'POST' })).status, 404);
        new Gauge({
            name: 'test_broken', help: '검증용 수집 오류', registers: [registry],
            collect() { throw new Error('검증용 수집 실패'); },
        });
        assert.equal((await fetch(`${url}/metrics`)).status, 500);
        registry.removeSingleMetric('test_broken');
        assert.equal((await fetch(`${url}/metrics`)).status, 200);
    } finally {
        await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    }
    await assert.rejects(fetch(`${url}/metrics`));
});

test('Event Loop Gauge는 실제 표본을 초로 변환하며 reset·종료와 Registry 격리를 지원한다', { timeout: 5_000 }, async (t) => {
    const registry = createMetricsRegistry();
    const isolated = createMetricsRegistry();
    const histogram = createEventLoopMetrics(registry);
    const names = ['max_seconds', 'p99_seconds', 'mean_seconds', 'samples'].map((suffix) => `nodejs_event_loop_delay_${suffix}`);
    try {
        assert.equal(histogram.count, 0);
        for (const name of names) assert.equal(await metricValue(registry, name), 0);
        const deadline = Date.now() + 2_000;
        while (histogram.count === 0 && Date.now() < deadline) await delay(20, undefined, { signal: t.signal });
        assert.ok(histogram.count > 0, 'Event Loop 지연 표본이 수집되어야 합니다');
        assert.equal(histogram.disable(), true, '활성화된 계측을 종료해야 합니다');
        assert.equal(await metricValue(registry, names[0]!), histogram.max / 1e9);
        assert.equal(await metricValue(registry, names[1]!), histogram.percentile(99) / 1e9);
        assert.equal(await metricValue(registry, names[2]!), histogram.mean / 1e9);
        assert.equal(await metricValue(registry, names[3]!), histogram.count);
        const count = histogram.count;
        await delay(30, undefined, { signal: t.signal });
        assert.equal(histogram.count, count, '종료 후 새 표본이 생기지 않아야 합니다');
        const metrics = await registry.getMetricsAsJSON();
        assert.equal(metrics.length, 4);
        assert.ok(metrics.every((metric) => metric.type === 'gauge'));
        for (const metric of metrics) {
            assert.equal(register.getSingleMetric(metric.name), undefined);
            assert.ok(metric.values.every((sample) => Object.keys(sample.labels).length === 0));
        }
        assert.equal((await isolated.metrics()).trim(), '');
        histogram.reset();
        for (const name of names) assert.equal(await metricValue(registry, name), 0);
        assert.equal(histogram.enable(), true);
    } finally {
        histogram.disable();
    }
});
