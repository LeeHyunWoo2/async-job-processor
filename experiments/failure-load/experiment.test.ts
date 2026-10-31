import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { performance } from 'node:perf_hooks';
import test from 'node:test';
import { category, evaluate, loadStability, options, phaseSummary, postKillRequest, requestSummary, runOrder, schedule, startProducer, survivorSuccessRequest,
    type Evidence, type Event, type RequestRecord, type Sample } from './experiment.ts';

test('기본 288개 일반 예약·별도 대상·정상 장애 장애 정상 순서와 Worker 2개 Pilot 옵션을 유지한다', () => {
    const o = options([]);
    assert.deepEqual([o.workers, o.limit, o.seconds, o.rate, o.targetAtSeconds, o.targetDurationMs, o.drainSeconds], [3, 30, 120, 2.4, 20, 10_000, 300]);
    assert.deepEqual(runOrder(o), [{ mode: 'normal', repeat: 1 }, { mode: 'failure', repeat: 1 }, { mode: 'failure', repeat: 2 }, { mode: 'normal', repeat: 2 }]);
    const plan = schedule(120, 2.4);
    assert.equal(plan.length, 288);
    assert.equal(plan[1], 1000 / 2.4);
    assert.equal(plan.at(-1), 287 * 1000 / 2.4);
    assert.deepEqual(schedule(1, 2.4), [0, 1000 / 2.4, 2000 / 2.4]);
    assert.equal(o.purpose, 'main');
    assert.deepEqual(runOrder(options(['--purpose=pilot', '--workers=2', '--modes=normal', '--repeats=1'])), [{ mode: 'normal', repeat: 1 }]);
    for (const args of [['--rate=0'], ['--rate=NaN'], ['--rate=2x'], ['--seconds=1.5'], ['--target-at-seconds=120'],
        ['--modes=normal,normal'], ['--modes='], ['--workers=2'], ['--workers=4'], ['--purpose=wrong'], ['--purpose=pilot'], ['--purpose=smoke', '--workers=2', '--modes=normal'], ['--drain-seconds=301'], ['--max-inflight=0'], ['--unknown=1']])
        assert.throws(() => options(args), args.join(' '));
});

function request(index: number, patch: Partial<RequestRecord> = {}): RequestRecord {
    return { kind: 'regular', index, scheduledMs: index * 100, sentMs: index * 100 + 5, finishedMs: index * 100 + 25,
        status: 201, id: `job-${index}`, body: { status: 'queued', error: '작업 처리 용량이 가득 찼습니다' }, ...patch };
}
test('HTTP·본문 오류·미발송은 배타적이며 RTT와 대상 Job 집계를 분리한다', () => {
    const rows = [request(0), request(1, { status: 503, code: 'QUEUE_CAPACITY_EXCEEDED' }), request(2, { status: 500 }),
        request(3, { error: '연결 끊김' }), request(4, { error: '본문 유실', status: 503, code: 'QUEUE_CAPACITY_EXCEEDED' }),
        request(5, { sentMs: null, finishedMs: null, unsent: '중단' }), request(6, { kind: 'target' })];
    const noId = request(7); delete noId.id;
    assert.equal(category(noId), 'otherHttp');
    const s = requestSummary(rows.filter((r) => r.kind === 'regular'));
    assert.deepEqual([s.planned, s.sent, s.unsent, s.accepted, s.rejected, s.otherHttp, s.networkOrBodyErrors], [6, 5, 1, 1, 1, 1, 2]);
    assert.equal(s.sent, s.accepted + s.rejected + s.otherHttp + s.networkOrBodyErrors);
    assert.equal(s.planned, s.sent + s.unsent);
    assert.equal(s.rttMs.p95, 20);
    assert.equal(s.dispatchRate, 10);
    assert.equal(requestSummary(rows.filter((r) => r.kind === 'target')).accepted, 1);
});

test('실제 HTTP의 느린 응답과 무관하게 소수 rate로 발송하고 대상은 따로 정확히 한 번 보낸다', async () => {
    const payloads: number[] = [];
    const server = createServer((req, res) => {
        let body = '';
        req.on('data', (chunk) => { body += String(chunk); });
        req.on('end', () => {
            payloads.push(JSON.parse(body).payload.durationMs);
            const id = payloads.length;
            setTimeout(() => { res.writeHead(201, { 'content-type': 'application/json' }); res.end(JSON.stringify({ id: `id-${id}` })); }, 600);
        });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    try {
        const address = server.address(); assert.ok(address && typeof address !== 'string');
        const o = options(['--seconds=2', '--rate=2.4', '--target-at-seconds=0.5']);
        const saved: RequestRecord[] = [], errors: unknown[] = [];
        const p = startProducer(o, `http://127.0.0.1:${address.port}`, performance.now(), new AbortController().signal,
            async (r) => { saved.push(r); }, (e) => { errors.push(e); });
        const result = await p.done;
        assert.deepEqual(errors, []);
        assert.equal(saved.length, 6);
        assert.equal(payloads.filter((n) => n === 10_000).length, 1);
        assert.equal(payloads.filter((n) => n === 1000).length, 5);
        const regular = p.requests.filter((r) => r.kind === 'regular');
        assert.ok(regular[1]!.sentMs! < regular[0]!.finishedMs!, '응답 완료를 기다리지 않고 다음 예약을 발송해야 합니다');
        assert.deepEqual(regular.map((r) => r.scheduledMs), schedule(2, 2.4));
        assert.ok(p.requests.every((r) => r.sentMs! >= r.scheduledMs), '예약 시각보다 일찍 발송하면 안 됩니다');
        assert.ok(result.peakInflight <= o.maxInflight && result.peakInflight >= 2);
        assert.equal(category(await p.target), 'accepted');
    } finally { server.closeAllConnections(); await new Promise<void>((resolve) => server.close(() => resolve())); }
});

test('동시성 상한·중단·저장 오류에서 미발송과 전송한 요청을 끝까지 보존한다', async () => {
    const server = createServer((_req, res) => setTimeout(() => res.end('{"id":"one"}'), 500));
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    try {
        const address = server.address(); assert.ok(address && typeof address !== 'string');
        for (const saveFailure of [false, true]) {
            const abort = new AbortController();
            const errors: string[] = [];
            const o = options(['--seconds=2', '--rate=10', '--target-at-seconds=0.1', '--max-inflight=1']);
            const p = startProducer(o, `http://127.0.0.1:${address.port}`, performance.now(), abort.signal,
                async () => { if (saveFailure) throw new Error('저장 실패'); },
                (e) => { errors.push(String(e)); abort.abort(); });
            const timer = setTimeout(() => abort.abort(), 650);
            await p.done;
            clearTimeout(timer);
            const s = requestSummary(p.requests);
            assert.ok(s.unsent > 0);
            assert.ok(p.requests.some((r) => r.unsent === '동시 발송 상한'));
            assert.ok(p.requests.filter((r) => r.sentMs !== null).every((r) => r.finishedMs !== null));
            assert.equal(s.planned, 21);
            assert.equal(s.sent + s.unsent, s.planned);
            assert.equal(errors.length > 0, saveFailure);
        }
    } finally { server.closeAllConnections(); await new Promise<void>((resolve) => server.close(() => resolve())); }
});

function evidence(): Evidence {
    const row = { id: 'target', status: 'succeeded', type: 'slow', attempt: 1, worker_id: 'w1', payload: { durationMs: 10_000 }, result: { durationMs: 10_000 }, last_error: null, processing_started_at: '2026-10-31T00:00:00Z' };
    const events = ['job.claimed', 'job.handler_started', 'job.handler_finished', 'job.succeeded'].map((event, i): Event =>
        ({ event, jobId: row.id, workerId: 'w1', attempt: 1, containerId: 'c1', time: i, observedMs: 100 + i * 100, receivedWallMs: i }));
    const counts = { total: 1, queued: 0, processing: 0, outstanding: 0, succeeded: 1, failed: 0 };
    const samples: Sample[] = [0, 1000, 2000].map((elapsedMs, i) => ({ ...counts, elapsedMs, queryMs: 1, actualIntervalMs: i ? 1000 : null,
        wallMs: elapsedMs, clockDifferenceMs: 0, dbWallMs: elapsedMs, dbClockOffsetMs: 0, phase: i === 0 ? 'start' : i === 1 ? 'load-end' : 'completed', connections: 5, activeConnections: 1 }));
    const regular = schedule(2, 2.4).map((scheduledMs, index) => request(index, { scheduledMs, sentMs: scheduledMs + 5, finishedMs: scheduledMs + 25 }));
    const regularRows = regular.map((r) => ({ ...row, id: r.id!, payload: { durationMs: 1000 }, result: { durationMs: 1000 } }));
    const regularEvents = regularRows.flatMap((r) => events.map((e) => ({ ...e, jobId: r.id })));
    return { requests: [request(0, { kind: 'target', id: row.id }), ...regular], before: [], after: [row, ...regularRows], events: [...events, ...regularEvents], samples,
        workers: [{ workerId: 'w1', containerId: 'c1' }], observations: { 'handler-started': { elapsedMs: 200 }, 'control-confirmed': { elapsedMs: 220 },
            'target-succeeded': { elapsedMs: 400 }, 'load-end': { elapsedMs: 1000 }, 'drain-end': { elapsedMs: 2000 } },
        topologyVerified: true, finalTopologyVerified: true, collectionErrors: [] };
}
test('조건 미성립·기능 실패·필수 근거 누락·시간 분석 제한을 별도 판정하고 응답 유실 Commit을 드러낸다', () => {
    const o = options(['--purpose=smoke', '--seconds=2', '--target-at-seconds=0.1']);
    const d = evidence();
    d.workers.push({ workerId: 'w2', containerId: 'c2' });
    let result = evaluate(o, 'normal', d);
    assert.equal(result.scenario.valid, false, '모든 Worker의 처리 참여를 확인하지 못해 시나리오는 미성립');
    assert.equal(result.functional.status, 'passed', '성능 조건 미성립과 실제 기능 실패를 혼동하지 않는다');
    d.samples[1]!.clockDifferenceMs = 5000;
    result = evaluate(o, 'normal', d);
    assert.equal(result.measurement.valid, true);
    assert.equal(result.measurement.wallAlignment.valid, false, 'Wall Clock 정렬 제한만 별도로 표시한다');
    d.after[0]!.attempt = 3;
    assert.equal(evaluate(o, 'normal', d).functional.status, 'failed');
    d.after[0]!.attempt = 1;
    const originalStatus = d.requests[0]!.status;
    d.requests[0]!.status = 500;
    assert.ok(evaluate(o, 'normal', d).functional.failures.some((s) => s.includes('HTTP 계약')));
    d.requests[0]!.status = originalStatus!;
    d.requests[0]!.error = '응답 본문 유실';
    result = evaluate(o, 'normal', d);
    assert.equal(result.measurement.valid, false);
    assert.deepEqual(result.reconciliation.unknownResponseRows.map((r) => r.id), ['target']);
    const failure = evaluate(o, 'failure', evidence());
    assert.equal(failure.scenario.valid, false);
    assert.ok(failure.functional.missing.some((s) => s.includes('second-claim')));
    assert.ok(failure.scenario.reasons.some((s) => s.includes('503')));
    assert.ok(failure.functional.failures.every((s) => !s.includes('503')), '503 미관측을 기능 버그로 분류하지 않는다');
});

test('단계 경계는 Monotonic 반개구간으로 정산하고 동일 경계의 요청·완료를 이중 집계하지 않는다', () => {
    const d = evidence();
    d.requests = [request(0, { sentMs: 200 }), request(1, { kind: 'target', id: 'target', sentMs: 220 })];
    d.events = d.events.filter((e) => e.jobId === 'target');
    const phases = phaseSummary(d);
    assert.equal(phases.reduce((n, p) => n + p.regular.sent + p.target.sent, 0), 2);
    assert.equal(phases.reduce((n, p) => n + p.logObservedCompletedRegular + p.logObservedCompletedTarget, 0), 1);
    assert.equal(phases.reduce((n, p) => n + p.logObservedCompletedUnknown, 0), 0);
    assert.ok(phases.every((p) => p.seconds >= 0));
});

test('장애의 정상 Recovery 전이·소유권을 대조하고 중복 Claim·누락 사건·상한 초과를 구분한다', () => {
    const o = options(['--purpose=smoke', '--seconds=2', '--target-at-seconds=0.1']);
    const d = evidence();
    d.workers.push({ workerId: 'w2', containerId: 'c2' }, { workerId: 'w3', containerId: 'c3' });
    const target = d.after[0]!;
    const initial = { ...target, status: 'processing', result: null };
    target.attempt = 2; target.worker_id = 'w2';
    const first = d.events.filter((e) => e.jobId === 'target').slice(0, 2);
    const recovered: Event = { event: 'job.recovered', jobId: 'target', workerId: 'w2', previousWorkerId: 'w1', containerId: 'c2',
        status: 'queued', attempt: 1, time: 0, observedMs: 1500, receivedWallMs: 0 };
    const second = ['job.claimed', 'job.handler_started', 'job.handler_finished', 'job.succeeded'].map((event, i): Event =>
        ({ ...recovered, event, status: event === 'job.succeeded' ? 'succeeded' : 'processing', attempt: 2, observedMs: 1550 + i * 100 }));
    d.events = [...d.events.filter((e) => e.jobId !== 'target'), ...first, recovered, ...second];
    for (const [index, worker] of ['w2', 'w3'].entries()) {
        const row = d.after[index + 1]!;
        row.worker_id = worker;
        for (const e of d.events.filter((e) => e.jobId === row.id)) {
            e.workerId = worker; e.containerId = worker === 'w2' ? 'c2' : 'c3';
        }
    }
    // 마지막 일반 예약 한 건을 정책 거부로 바꾸고 DB 신규 행과 로그에서도 제외한다.
    const rejected = d.requests.at(-1)!;
    d.after = d.after.filter((r) => r.id !== rejected.id);
    d.events = d.events.filter((e) => e.jobId !== rejected.id);
    rejected.status = 503; rejected.code = 'QUEUE_CAPACITY_EXCEEDED';
    d.samples[1]!.queued = 1; d.samples[1]!.outstanding = 2;
    const survivorJob = d.after.find((r) => r.id === 'job-1')!;
    const success = d.events.find((e) => e.event === 'job.succeeded' && e.jobId === survivorJob.id)!;
    success.observedMs = 1000;
    Object.assign(d.observations, { 'handler-started': { elapsedMs: 200, row: initial },
        'kill-confirmed': { elapsedMs: 250, survivors: [{ id: 'c2' }, { id: 'c3' }] }, 'retained-after-kill': { elapsedMs: 260 },
        'survivor-before-recovery': { elapsedMs: 1005, event: success, completed: survivorJob, retainedTarget: initial },
        recovered: { elapsedMs: 1500, firstClaimAgeMsAtConfirmation: 30_001 }, 'second-claim': { elapsedMs: 1550 },
        'second-processing': { elapsedMs: 1580 }, 'target-succeeded': { elapsedMs: 1850 } });
    let report = evaluate(o, 'failure', d);
    assert.equal(report.scenario.valid, true, JSON.stringify(report.scenario));
    assert.equal(report.functional.status, 'passed', JSON.stringify(report.functional));
    assert.equal(report.measurement.valid, true, JSON.stringify(report.measurement));
    d.observations.recovered!.firstClaimAgeMsAtConfirmation = 29_999;
    assert.ok(evaluate(o, 'failure', d).functional.failures.some((s) => s.includes('30초 미만')));
    delete d.observations.recovered!.firstClaimAgeMsAtConfirmation;
    assert.equal(evaluate(o, 'failure', d).measurement.valid, false);
    d.observations.recovered!.firstClaimAgeMsAtConfirmation = 30_001;
    // 사망 Worker는 첫 Job이 대상이어도 된다. 실행 중 죽이므로 그 Worker의 일반 완료를 강제하지 않는다.
    for (const row of d.after.filter((r) => r.id !== 'target' && r.worker_id === 'w1')) {
        row.worker_id = 'w2';
        for (const e of d.events.filter((e) => e.jobId === row.id)) { e.workerId = 'w2'; e.containerId = 'c2'; }
    }
    assert.equal(evaluate(o, 'failure', d).scenario.valid, true);
    d.events.push({ ...first[1]!, event: 'job.handler_finished' });
    assert.equal(evaluate(o, 'failure', d).scenario.valid, false, '첫 Handler 종료 뒤 SIGKILL은 유효한 주입이 아니다');
    d.events.pop();
    d.events.push({ ...second[0]! });
    assert.equal(evaluate(o, 'failure', d).functional.status, 'failed');
    d.events.pop();
    delete d.observations['second-processing'];
    assert.equal(evaluate(o, 'failure', d).functional.status, 'incomplete');
    d.samples[1]!.outstanding = 31;
    report = evaluate(o, 'failure', d);
    assert.ok(report.functional.failures.some((s) => s.includes('상한 초과')));
});

test('중단으로 미완료인 Job과 요청 대응을 잃은 완료 사건을 기능 오류·대상 성공으로 보정하지 않는다', () => {
    const o = options(['--purpose=smoke', '--seconds=2', '--target-at-seconds=0.1']);
    const d = evidence();
    d.after[0]!.status = 'queued'; d.after[0]!.attempt = 0;
    delete d.observations['drain-end'];
    d.executionError = 'SIGTERM 중단';
    assert.equal(evaluate(o, 'normal', d).functional.status, 'incomplete');
    d.events.push({ ...d.events[0]!, event: 'job.succeeded', jobId: 'unknown', observedMs: 500 });
    const phases = phaseSummary(d);
    assert.equal(phases.reduce((n, p) => n + p.logObservedCompletedUnknown, 0), 1);
    assert.equal(phases.reduce((n, p) => n + p.logObservedCompletedTarget, 0), 1);
});

test('SIGKILL 이전 요청의 늦은 성공 로그는 제외하고 이후 수락 요청·생존 소유권·대상 잔류를 함께 검증한다', () => {
    const d = evidence();
    d.workers.push({ workerId: 'survivor', containerId: 'alive' });
    const target = { ...d.after[0]!, status: 'processing', result: null };
    const completed = { ...d.after[1]!, worker_id: 'survivor' };
    const event: Event = { ...d.events[0]!, event: 'job.succeeded', jobId: completed.id, workerId: 'survivor', containerId: 'alive', observedMs: 500 };
    d.observations['handler-started'] = { elapsedMs: 100, row: target };
    d.observations['kill-confirmed'] = { elapsedMs: 250, survivors: [{ id: 'alive' }] };
    const r = d.requests.find((r) => r.id === completed.id)!;
    // 이전 Job의 성공 로그가 늦게 수신되면 DB가 succeeded여도 장애 후 처리 증거로 사용할 수 없다.
    assert.equal(postKillRequest(d.requests, event, 250), undefined);
    assert.equal(survivorSuccessRequest(d, event, target, completed), undefined);
    d.events.push(event);
    d.observations['survivor-before-recovery'] = { elapsedMs: 510, event, completed, retainedTarget: target };
    const o = options(['--purpose=smoke', '--seconds=2', '--target-at-seconds=0.1']);
    assert.ok(evaluate(o, 'failure', d).functional.missing.some((s) => s.includes('SIGKILL 확인 후 발송')));
    r.sentMs = 300; r.finishedMs = 320;
    assert.equal(survivorSuccessRequest(d, event, target, completed)?.id, r.id);
    assert.equal(survivorSuccessRequest(d, { ...event, workerId: 'unknown' }, target, completed), undefined);
    assert.equal(survivorSuccessRequest(d, event, { ...target, attempt: 2 }, completed), undefined);
    r.status = 503; r.code = 'QUEUE_CAPACITY_EXCEEDED';
    assert.equal(survivorSuccessRequest(d, event, target, completed), undefined);
});

test('빠른 503이 혼합 RTT를 낮춰도 수락 201 RTT를 분리하고 DB 성공 변화와 로그 관측 완료를 교차 제시한다', () => {
    const s = requestSummary([request(0, { sentMs: 0, finishedMs: 100 }),
        request(1, { sentMs: 200, finishedMs: 205, status: 503, code: 'QUEUE_CAPACITY_EXCEEDED' })]);
    assert.equal(s.rttMsByOutcome.accepted201.mean, 100);
    assert.equal(s.rttMsByOutcome.policyRejected503.mean, 5);
    assert.equal(s.rttMs.mean, 52.5);
    const d = evidence();
    d.observations = { 'load-end': { elapsedMs: 2200 } };
    d.samples = [0, 1000, 2000].map((elapsedMs, i) => ({ ...d.samples[0]!, elapsedMs, queryMs: 10, succeeded: i }));
    d.events = [600, 1500].map((observedMs) => ({ ...d.events[0]!, event: 'job.succeeded', observedMs }));
    let [phase] = phaseSummary(d);
    assert.equal(phase!.completionBasis, 'log-observed');
    assert.equal(phase!.dbCompletionCrossCheck.dbSucceededDelta, 2);
    assert.equal(phase!.dbCompletionCrossCheck.logObservedSucceeded, 2);
    d.events[1]!.observedMs = 2500;
    [phase] = phaseSummary(d);
    assert.equal(phase!.dbCompletionCrossCheck.difference, -1, '로그 전달 지연의 불일치를 DB 완료량으로 보정하지 않는다');
});

function stabilityEvidence(o: ReturnType<typeof options>, levels: number[], rejectedWindows: number[] = []) {
    const d = evidence();
    d.workers = Array.from({ length: o.workers }, (_, i) => ({ workerId: `w${i + 1}`, containerId: `c${i + 1}` }));
    d.requests = [...schedule(o.seconds, o.rate).map((scheduledMs, i) => request(i, { scheduledMs, sentMs: scheduledMs + 5, finishedMs: scheduledMs + 25 })),
        request(0, { kind: 'target', id: 'target', scheduledMs: 1000, sentMs: 1005, finishedMs: 1025 })];
    for (const index of rejectedWindows) {
        const row = d.requests.find((r) => r.kind === 'regular' && r.sentMs! >= 10_000 + index * 10_000 && r.sentMs! < 20_000 + index * 10_000)!;
        row.status = 503; row.code = 'QUEUE_CAPACITY_EXCEEDED';
    }
    const seed = d.after[0]!;
    d.after = d.requests.filter((r) => category(r) === 'accepted').map((r, i) => ({ ...seed, id: r.id!, worker_id: d.workers[i % o.workers]!.workerId,
        payload: { durationMs: r.kind === 'target' ? 10_000 : 1000 }, result: { durationMs: r.kind === 'target' ? 10_000 : 1000 } }));
    d.events = d.after.flatMap((row) => ['job.claimed', 'job.handler_started', 'job.handler_finished', 'job.succeeded'].map((event, i): Event =>
        ({ event, jobId: row.id, workerId: row.worker_id!, containerId: d.workers.find((w) => w.workerId === row.worker_id)!.containerId,
            attempt: 1, time: 0, observedMs: 100 + i * 100, receivedWallMs: 0 })));
    const sampleSeed = d.samples[0]!;
    d.samples = [sampleSeed, ...levels.flatMap((outstanding, i) => Array.from({ length: 10 }, (_, j) =>
        ({ ...sampleSeed, phase: 'periodic', elapsedMs: 10_000 + i * 10_000 + j * 1000, queryMs: 1, outstanding, queued: Math.max(0, outstanding - o.workers), actualIntervalMs: 1000 })))];
    d.samples.push({ ...sampleSeed, elapsedMs: o.seconds * 1000, phase: 'load-end', outstanding: levels.at(-1)!, actualIntervalMs: 1000 },
        { ...sampleSeed, elapsedMs: o.seconds * 1000 + 5000, phase: 'completed', outstanding: 0, actualIntervalMs: 1000 });
    d.observations['target-succeeded'] = { elapsedMs: 10_000 };
    d.observations['load-end'] = { elapsedMs: o.seconds * 1000 };
    d.observations['drain-end'] = { elapsedMs: o.seconds * 1000 + 5000 };
    return d;
}

test('정상 3개 대조군은 지속 포화·증가 추세와 구간 부족을 판정하고 일시적 503·대상 실행 구간을 구분한다', () => {
    const o = options(['--seconds=60', '--rate=3', '--target-at-seconds=1']);
    const stable = stabilityEvidence(o, [2, 3, 2, 3, 2], [0]);
    assert.equal(evaluate(o, 'normal', stable).scenario.valid, true, '한 구간의 일시적 503만으로 부적합 처리하지 않는다');
    assert.equal(evaluate(o, 'normal', stable).functional.status, 'passed');
    const saturated = stabilityEvidence(o, [30, 30, 30, 30, 30], [1, 2]);
    const report = evaluate(o, 'normal', saturated);
    assert.equal(report.scenario.valid, false);
    assert.equal(report.functional.status, 'passed', '과부하 조건 부적합과 기능 실패는 다르다');
    assert.equal(report.scenario.suitability.requiresUserDecision, true);
    assert.ok(report.stability.saturation.length > 0);
    assert.equal(loadStability(o, stabilityEvidence(o, [2, 4, 6, 6, 6])).status, 'unsuitable');
    assert.equal(loadStability(o, stabilityEvidence(o, [2, 2.2, 2.4, 2.2, 2.1])).status, 'suitable', '작은 관측 변동은 증가 추세 임계값과 구분한다');
    const short = stabilityEvidence(o, [2, 2]);
    short.observations['load-end'] = { elapsedMs: 30_000 };
    assert.equal(evaluate(o, 'normal', short).scenario.suitability.status, 'inconclusive');
    stable.requests.push(request(999, { sentMs: 5000, finishedMs: 5010, status: 503, code: 'QUEUE_CAPACITY_EXCEEDED' }));
    assert.equal(loadStability(o, stable).status, 'suitable', '대상 Handler 실행 중의 과도 구간은 안정성 창에서 제외한다');
});

test('Pilot 3개 조건의 부적합은 사용자 판단으로 넘기고 Pilot 2개와 Smoke에는 정상 3개 기준을 강제하지 않는다', () => {
    const pilot3 = options(['--purpose=pilot', '--modes=normal', '--seconds=60', '--rate=3', '--target-at-seconds=1']);
    const bad = evaluate(pilot3, 'normal', stabilityEvidence(pilot3, [30, 30, 30, 30, 30], [1, 2]));
    assert.equal(bad.scenario.valid, true, '과부하를 관찰한 Pilot 자체는 유효하다');
    assert.equal(bad.scenario.suitability.status, 'unsuitable');
    assert.equal(bad.scenario.suitability.requiresUserDecision, true);
    const pilot2 = { ...pilot3, workers: 2 };
    const capacity = evaluate(pilot2, 'normal', stabilityEvidence(pilot2, [30, 30, 30, 30, 30], [1, 2]));
    assert.equal(capacity.scenario.valid, true);
    assert.equal(capacity.scenario.suitability.status, 'observation-only');
    assert.equal(capacity.scenario.suitability.requiresUserDecision, false);
    assert.equal(capacity.stability.applied, false);
    const smoke = { ...pilot3, purpose: 'smoke' as const };
    assert.equal(evaluate(smoke, 'normal', stabilityEvidence(smoke, [30, 30, 30, 30, 30], [1, 2])).scenario.suitability.requiresUserDecision, false);
});
