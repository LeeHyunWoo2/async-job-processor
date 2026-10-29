import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { test } from 'node:test';
import { clockDiagnostic, histogram, options, probeSummary, probeValidity, runOrder, startProbes } from './experiment.ts';

test('옵션 경계와 여섯 조건의 3회 교차 순서를 확인한다', () => {
    const o = options([]);
    const order = runOrder(o);
    assert.equal(order.length, 18);
    assert.deepEqual([0, 6, 12].map((i) => order[i]!.mode), ['co-located', 'separated', 'co-located']);
    for (const mode of o.modes) for (const condition of o.conditions) {
        assert.equal(order.filter((r) => r.mode === mode && r.condition === condition).length, 3);
    }
    for (const args of [['--repeats=0'], ['--modes=separated,separated'], ['--conditions=no'], ['--timeout-ms=2500'],
        ['--duration-ms=0'], ['--interval-ms=0'], ['--max-inflight=0'], ['--window-ms=100']]) assert.throws(() => options(args));
});

test('Histogram은 초에서 ms로 변환하고 같은 경계의 시계 경과 차이를 계산한다', () => {
    assert.deepEqual(histogram(`nodejs_event_loop_delay_max_seconds 2\nnodejs_event_loop_delay_p99_seconds 0.012\nnodejs_event_loop_delay_mean_seconds 0.02\nnodejs_event_loop_delay_samples 300`),
        { maxMs: 2000, p99Ms: 12, meanMs: 20, count: 300 });
    assert.throws(() => histogram(''));
    assert.deepEqual(clockDiagnostic({ monoMs: 10, wallMs: 100 }, { monoMs: 2010, wallMs: 2110 }),
        { monotonicElapsedMs: 2000, wallElapsedMs: 2010, differenceMs: 10 });
});

// 실제 HTTP 서버는 응답만 늦춘다. 클라이언트가 응답 전에 다음 예약을 발송했는지가 핵심 불변식이다.
test('독립 Probe는 응답 대기 중에도 예약 발송하며 지연·Timeout·상한 누락·저장 실패를 보존한다', async () => {
    const server = createServer((req, res) => {
        if (req.url === '/error') { res.destroy(); return; }
        setTimeout(() => res.end('ok'), req.url === '/timeout' ? 300 : 100);
    });
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    const url = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
    try {
        const controller = new AbortController();
        const origin = performance.now();
        const probes = startProbes({ intervalMs: 15, maxInflight: 64, timeoutMs: 200 }, { api: url }, 'test', origin, controller.signal, async () => {});
        await delay(180);
        const normal = await probes.stop();
        assert.ok(normal.rows.length > 2);
        assert.ok(normal.rows[1]!.sentMs! < normal.rows[0]!.finishedMs, '요청 완료를 기다리는 순차 발송이면 안 됩니다');
        assert.ok(normal.peakInflight > 1);
        assert.equal(probeSummary(normal.rows).success, normal.rows.length);
        assert.ok(normal.rows.every((r) => r.durationMs! > 0 && r.sentMs! >= r.scheduledMs - 2 && r.finishedWallMs >= r.sentWallMs!));

        const limited = startProbes({ intervalMs: 10, maxInflight: 1, timeoutMs: 50 },
            { api: `${url}/timeout`, worker: `${url}/error` }, 'failure', performance.now(), controller.signal, async () => {});
        await delay(90);
        const failures = await limited.stop();
        const http = probeSummary(failures.rows);
        assert.ok(http.skipped > 0 && http.timeouts > 0 && http.errors > 0);
        assert.equal(http.sent + http.skipped, http.planned);
        assert.equal(http.success + http.timeouts + http.errors + http.otherHttp, http.sent);
        assert.ok(probeValidity(failures.rows, 10, 100).length >= 3);
        const late = structuredClone(normal.rows[0]!);
        late.sentMs = late.scheduledMs + 500;
        assert.ok(probeValidity([late], 100, 0).some((r) => r.includes('발송 지연')));
        // 응답 시간의 크기는 유효성 실패 조건이 아니다.
        late.sentMs = late.scheduledMs;
        late.durationMs = 5000;
        assert.deepEqual(probeValidity([late], 100, 0), []);

        const failedWrite = startProbes({ intervalMs: 10, maxInflight: 2, timeoutMs: 200 }, { api: url },
            'save-failure', performance.now(), controller.signal, async () => { throw new Error('저장 실패'); });
        await delay(130);
        const saved = await failedWrite.stop();
        assert.match(saved.writeError!, /저장 실패/);
        assert.ok(saved.rows.every((r) => r.finishedMs > 0));
    } finally {
        server.closeAllConnections();
        await new Promise<void>((resolve) => server.close(() => resolve()));
    }
});
