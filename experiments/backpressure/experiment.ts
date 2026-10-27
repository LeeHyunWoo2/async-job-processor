import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { parseArgs } from 'node:util';

export function options(args: string[]) {
    const { values } = parseArgs({ args, options: {
        modes: { type: 'string', default: 'off,on' },
        repeats: { type: 'string', default: '2' },
        seconds: { type: 'string', default: '30' },
        rate: { type: 'string', default: '10' },
        'duration-ms': { type: 'string', default: '1000' },
        limit: { type: 'string', default: '100' },
        'interval-ms': { type: 'string', default: '1000' },
        'max-inflight': { type: 'string', default: '4' },
        'drain-seconds': { type: 'string', default: '900' },
        out: { type: 'string', default: `artifacts/backpressure/${new Date().toISOString().replaceAll(':', '-')}` },
    } });
    function integer(value: string, name: string, max: number, min = 1) {
        assert.match(value, /^\d+$/, `${name}: 정수가 필요합니다`);
        const n = Number(value);
        assert.ok(Number.isSafeInteger(n) && n >= min && n <= max, `${name}: 허용 범위 ${min}~${max}`);
        return n;
    }
    const modes = values.modes.split(',');
    assert.ok(modes.every((mode) => mode === 'on' || mode === 'off') && new Set(modes).size === modes.length, 'modes: on/off를 중복 없이 지정하세요');
    return {
        modes, repeats: integer(values.repeats, 'repeats', 3), seconds: integer(values.seconds, 'seconds', 300),
        rate: integer(values.rate, 'rate', 100), durationMs: integer(values['duration-ms'], 'duration-ms', 10_000),
        limit: integer(values.limit, 'limit', 100_000), intervalMs: integer(values['interval-ms'], 'interval-ms', 10_000, 100),
        maxInflight: integer(values['max-inflight'], 'max-inflight', 10), drainSeconds: integer(values['drain-seconds'], 'drain-seconds', 3600),
        out: values.out,
    };
}
export type Options = ReturnType<typeof options>;
export interface RequestRecord {
    index: number;
    scheduledMs: number;
    sentMs: number;
    finishedMs: number;
    status?: number;
    code?: string;
    id?: string;
    error?: string;
}

// 예약 시각은 HTTP 완료와 독립적이다. 동시성 상한으로 늦어지면 지연을 그대로 기록한다.
export async function produce(o: Options, url: string, origin: number, signal: AbortSignal,
    record: (request: RequestRecord) => Promise<void>) {
    const requests: RequestRecord[] = [];
    const active = new Set<Promise<void>>();
    let writeError: unknown;
    let peakInflight = 0;
    for (let index = 0; index < o.seconds * o.rate && !signal.aborted && !writeError; index++) {
        const scheduledMs = index * 1000 / o.rate;
        try {
            await delay(Math.max(0, origin + scheduledMs - performance.now()), undefined, { signal });
            while (active.size >= o.maxInflight && !signal.aborted) await Promise.race(active);
        } catch { break; }
        if (signal.aborted || writeError) break;
        const request: RequestRecord = { index, scheduledMs, sentMs: performance.now() - origin, finishedMs: 0 };
        requests.push(request);
        const task = (async () => {
            try {
                const response = await fetch(`${url}/jobs`, { method: 'POST', headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({ type: 'slow', payload: { durationMs: o.durationMs } }),
                    signal: AbortSignal.any([signal, AbortSignal.timeout(5000)]),
                });
                request.status = response.status;
                const body = await response.json();
                if (typeof body.code === 'string') request.code = body.code;
                if (typeof body.id === 'string') request.id = body.id;
            } catch (error) { request.error = String(error); }
            request.finishedMs = performance.now() - origin;
            try { await record(request); } catch (error) { writeError = error; }
        })();
        active.add(task);
        peakInflight = Math.max(peakInflight, active.size);
        void task.then(() => active.delete(task));
    }
    // 마지막 예약은 구간 끝보다 한 주기 앞이다. 전체 구간이 지난 뒤 부하를 종료한다.
    try { await delay(Math.max(0, origin + o.seconds * 1000 - performance.now()), undefined, { signal }); } catch { /* 중단되어도 발송한 요청은 정산한다. */ }
    await Promise.all(active);
    if (writeError) throw writeError;
    return { requests, peakInflight, settledMs: performance.now() - origin };
}

export function requestSummary(requests: RequestRecord[], o: Options) {
    const errors = requests.filter((r) => r.error !== undefined || r.status === undefined);
    const responses = requests.filter((r) => r.error === undefined && r.status !== undefined);
    const accepted = responses.filter((r) => r.status === 201 && typeof r.id === 'string');
    const rejected = responses.filter((r) => r.status === 503 && r.code === 'QUEUE_CAPACITY_EXCEEDED');
    const firstSentMs = requests[0]?.sentMs ?? null;
    const lastSentMs = requests.at(-1)?.sentMs ?? null;
    return {
        planned: o.seconds * o.rate, sent: requests.length, accepted: accepted.length, rejected: rejected.length,
        otherHttp: responses.length - accepted.length - rejected.length,
        networkOrBodyErrors: errors.length,
        rejectionFraction: requests.length ? rejected.length / requests.length : null,
        firstSentMs, lastSentMs,
        // 첫 송신과 마지막 송신 사이에는 N-1개 간격이 있다. 구간 평균도 별도로 남긴다.
        dispatchRate: requests.length > 1 && lastSentMs! > firstSentMs! ? (requests.length - 1) * 1000 / (lastSentMs! - firstSentMs!) : null,
        windowDispatchRate: requests.length / Math.max(o.seconds, (lastSentMs ?? 0) / 1000),
        maxScheduleLagMs: Math.max(0, ...requests.map((r) => r.sentMs - r.scheduledMs)),
        ids: accepted.map((r) => r.id!),
    };
}

export interface Counts { total: number; queued: number; processing: number; outstanding: number; succeeded: number; failed: number }
export interface Sample extends Counts { elapsedMs: number; queryMs: number; phase: string }
export function sampleSummary(samples: Sample[]) {
    const intervals = samples.slice(1).map((sample, i) => sample.elapsedMs - samples[i]!.elapsedMs);
    return {
        count: samples.length, observedMaxOutstanding: Math.max(0, ...samples.map((s) => s.outstanding)),
        maxQueryMs: Math.max(0, ...samples.map((s) => s.queryMs)),
        meanQueryMs: samples.length ? samples.reduce((sum, s) => sum + s.queryMs, 0) / samples.length : null,
        maxActualIntervalMs: intervals.length ? Math.max(...intervals) : null,
    };
}
