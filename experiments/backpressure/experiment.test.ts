import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { performance } from 'node:perf_hooks';
import test from 'node:test';
import { options, produce, requestSummary, sampleSummary } from './experiment.ts';

test('옵션 범위와 중복 조건을 거부하고 기본 비교 조건을 유지한다', () => {
    const o = options([]);
    assert.deepEqual([o.modes, o.repeats, o.seconds, o.rate, o.durationMs, o.limit, o.intervalMs], [['off', 'on'], 2, 30, 10, 1000, 100, 1000]);
    for (const args of [['--modes=on,on'], ['--rate=0'], ['--seconds=1.5'], ['--max-inflight=11'], ['--unknown=1']]) assert.throws(() => options(args));
});

test('HTTP 결과·본문 오류를 중복 없이 분류해 합계를 발송 수와 맞추고 N-1 간격 발송률을 구분한다', () => {
    const summary = requestSummary([
        { index: 0, scheduledMs: 0, sentMs: 5, finishedMs: 20, status: 201, id: 'job-1' },
        { index: 1, scheduledMs: 100, sentMs: 105, finishedMs: 120, status: 503, code: 'QUEUE_CAPACITY_EXCEEDED' },
        { index: 2, scheduledMs: 200, sentMs: 205, finishedMs: 220, status: 503, code: 'OTHER' },
        { index: 3, scheduledMs: 300, sentMs: 305, finishedMs: 320, error: '연결 끊김' },
        { index: 4, scheduledMs: 400, sentMs: 405, finishedMs: 420, status: 201, error: '본문 유실' },
        { index: 5, scheduledMs: 500, sentMs: 505, finishedMs: 520, status: 503, code: 'QUEUE_CAPACITY_EXCEEDED', error: '본문 유실' },
    ], options(['--seconds=1', '--rate=6']));
    assert.deepEqual([summary.sent, summary.accepted, summary.rejected, summary.otherHttp, summary.networkOrBodyErrors], [6, 1, 1, 1, 3]);
    assert.equal(summary.accepted + summary.rejected + summary.otherHttp + summary.networkOrBodyErrors, summary.sent);
    assert.deepEqual(summary.ids, ['job-1']);
    assert.equal(summary.dispatchRate, 10);
    assert.equal(summary.windowDispatchRate, 6);
    assert.equal(summary.maxScheduleLagMs, 5);
    assert.equal(requestSummary([], options([])).dispatchRate, null);
    const sampled = sampleSummary([
        { elapsedMs: 0, queryMs: 2, phase: 'start', total: 0, queued: 0, processing: 0, outstanding: 0, succeeded: 0, failed: 0 },
        { elapsedMs: 1005, queryMs: 4, phase: 'periodic', total: 4, queued: 2, processing: 1, outstanding: 3, succeeded: 1, failed: 0 },
    ]);
    assert.equal(sampled.observedMaxOutstanding, 3);
    assert.equal(sampled.meanQueryMs, 3);
    assert.equal(sampled.maxActualIntervalMs, 1005);
});

test('실제 HTTP의 느린 응답 중에도 예약 송신을 계속하고 동시성 상한 안에서 모두 정산한다', async () => {
    let active = 0;
    let peak = 0;
    const server = createServer((_request, response) => {
        active++;
        peak = Math.max(peak, active);
        setTimeout(() => { active--; response.writeHead(201, { 'content-type': 'application/json' }); response.end(JSON.stringify({ id: 'job' })); }, 180);
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    try {
        const address = server.address();
        assert.ok(address && typeof address !== 'string');
        const o = options(['--seconds=1', '--rate=10', '--max-inflight=2']);
        const saved: number[] = [];
        const result = await produce(o, `http://127.0.0.1:${address.port}`, performance.now(), new AbortController().signal, async (r) => { saved.push(r.index); });
        assert.equal(result.requests.length, 10);
        assert.equal(saved.length, 10);
        assert.equal(peak, 2);
        assert.ok(result.peakInflight <= 2);
        assert.ok(result.requests[1]!.sentMs < result.requests[0]!.finishedMs, '앞 응답을 기다려 다음 송신을 지연하면 안 됩니다');
        assert.equal(requestSummary(result.requests, o).accepted, 10);
    } finally {
        server.closeAllConnections();
        await new Promise<void>((resolve) => server.close(() => resolve()));
    }
});
