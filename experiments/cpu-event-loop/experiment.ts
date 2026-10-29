import assert from 'node:assert/strict';
import { request as httpRequest } from 'node:http';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { parseArgs } from 'node:util';

export type Mode = 'co-located' | 'separated';
export type Condition = 'idle' | 'slow' | 'cpu';
export function options(args: string[]) {
    const { values: v } = parseArgs({ args, options: {
        modes: { type: 'string', default: 'co-located,separated' },
        conditions: { type: 'string', default: 'idle,slow,cpu' },
        repeats: { type: 'string', default: '3' },
        'duration-ms': { type: 'string', default: '2000' },
        'window-ms': { type: 'string', default: '2000' },
        'interval-ms': { type: 'string', default: '100' },
        'timeout-ms': { type: 'string', default: '10000' },
        'max-inflight': { type: 'string', default: '64' },
        out: { type: 'string', default: `artifacts/cpu-event-loop/${new Date().toISOString().replaceAll(':', '-')}` },
    } });
    function integer(value: string, name: string, min: number, max: number) {
        assert.match(value, /^\d+$/, `${name}: 정수가 필요합니다`);
        const n = Number(value);
        assert.ok(Number.isSafeInteger(n) && n >= min && n <= max, `${name}: 허용 범위 ${min}~${max}`);
        return n;
    }
    function choices<T extends string>(value: string, allowed: T[], name: string): T[] {
        const items = value.split(',');
        assert.ok(items.every((item) => allowed.includes(item as T)) && new Set(items).size === items.length,
            `${name}: 허용된 값을 중복 없이 지정하세요`);
        return items as T[];
    }
    const result = {
        modes: choices(v.modes, ['co-located', 'separated'], 'modes'),
        conditions: choices(v.conditions, ['idle', 'slow', 'cpu'], 'conditions'),
        repeats: integer(v.repeats, 'repeats', 1, 3),
        durationMs: integer(v['duration-ms'], 'duration-ms', 1, 10000),
        windowMs: integer(v['window-ms'], 'window-ms', 100, 10000),
        intervalMs: integer(v['interval-ms'], 'interval-ms', 10, 1000),
        timeoutMs: integer(v['timeout-ms'], 'timeout-ms', 100, 60000),
        maxInflight: integer(v['max-inflight'], 'max-inflight', 1, 256), out: v.out,
    };
    assert.ok(result.timeoutMs > result.durationMs + 1000, 'timeout-ms는 duration-ms보다 1초 이상 길어야 합니다');
    assert.ok(result.windowMs >= result.intervalMs * 2, 'window-ms는 Probe 두 주기 이상이어야 합니다');
    return result;
}
export type Options = ReturnType<typeof options>;
export function runOrder(o: Options) {
    return Array.from({ length: o.repeats }, (_, i) => {
        const modes = i % 2 ? [...o.modes].reverse() : o.modes;
        return modes.flatMap((mode) => o.conditions.map((condition) => ({ repeat: i + 1, mode, condition })));
    }).flat();
}
export interface Probe {
    index: number; target: string; run: string;
    scheduledMs: number; sentMs: number | null; finishedMs: number;
    sentWallMs: number | null; finishedWallMs: number;
    durationMs: number | null; status: number | null;
    error: string | null; errorKind: 'timeout' | 'network' | 'aborted' | null;
    skipped: 'inflight-limit' | null;
}

// Node.js 클라이언트는 대상 프로세스와 독립적이다. 연결 Pool의 숨은 대기를 피하고 요청별 총 Timeout을 잰다.
export async function probe(url: string, row: Probe, origin: number, timeoutMs: number, signal: AbortSignal) {
    await new Promise<void>((resolve) => {
        const req = httpRequest(url, { agent: false, signal }, (res) => {
            row.status = res.statusCode ?? null;
            res.resume();
            res.on('end', () => finish());
            res.on('error', (error) => finish(error));
        });
        let settled = false;
        const timer = setTimeout(() => {
            row.errorKind = 'timeout';
            req.destroy(new Error('Probe 응답 제한 시간 초과'));
        }, timeoutMs);
        function finish(error?: Error) {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            if (error) {
                row.error = String(error);
                row.errorKind ??= signal.aborted ? 'aborted' : 'network';
            }
            row.finishedMs = performance.now() - origin;
            row.finishedWallMs = Date.now();
            row.durationMs = row.finishedMs - row.sentMs!;
            resolve();
        }
        req.on('error', (error) => finish(error));
        req.end();
    });
}

export function startProbes(
    o: Pick<Options, 'intervalMs' | 'timeoutMs' | 'maxInflight'>,
    targets: Record<string, string>, run: string, origin: number, signal: AbortSignal,
    record: (row: Probe) => Promise<void>,
) {
    const rows: Probe[] = [];
    const active = new Set<Promise<void>>();
    const counts = new Map<string, number>();
    let stopped = false;
    let stopRequestedMs = Infinity;
    let writeError: unknown;
    let peakInflight = 0;
    const scheduler = (async () => {
        for (let index = 0; !signal.aborted && !writeError; index++) {
            const scheduledMs = index * o.intervalMs;
            if (stopped && scheduledMs >= stopRequestedMs) break;
            try { await delay(Math.max(0, origin + scheduledMs - performance.now()), undefined, { signal }); }
            catch { break; }
            if (signal.aborted || (stopped && scheduledMs >= stopRequestedMs)) break;
            for (const [target, url] of Object.entries(targets)) {
                const row: Probe = { index, target, run, scheduledMs, sentMs: null, sentWallMs: null,
                    finishedMs: performance.now() - origin, finishedWallMs: Date.now(), durationMs: null,
                    status: null, error: null, errorKind: null, skipped: null };
                rows.push(row);
                const full = (counts.get(target) ?? 0) >= o.maxInflight;
                if (full) row.skipped = 'inflight-limit';
                else { row.sentMs = performance.now() - origin; row.sentWallMs = Date.now(); }
                counts.set(target, (counts.get(target) ?? 0) + (full ? 0 : 1));
                const task = (async () => {
                    if (!full) await probe(url, row, origin, o.timeoutMs, signal);
                    try { await record(row); } catch (error) { writeError ??= error; }
                    if (!full) counts.set(target, counts.get(target)! - 1);
                })();
                active.add(task);
                peakInflight = Math.max(peakInflight, [...counts.values()].reduce((a, b) => a + b, 0));
                void task.then(() => active.delete(task));
            }
        }
    })();
    return { rows, async stop() {
        stopRequestedMs = Math.min(stopRequestedMs, performance.now() - origin);
        stopped = true;
        await scheduler;
        await Promise.all(active);
        return { rows, peakInflight, stopRequestedMs, expectedPerTarget: Math.ceil(stopRequestedMs / o.intervalMs), writeError: writeError ? String(writeError) : null };
    } };
}

export function distribution(values: number[]) {
    const sorted = [...values].sort((a, b) => a - b);
    const percentile = (p: number) => sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)] ?? null;
    return { count: sorted.length, min: sorted[0] ?? null, p50: percentile(.5), p95: percentile(.95), p99: percentile(.99),
        max: sorted.at(-1) ?? null, mean: sorted.length ? sorted.reduce((a, b) => a + b, 0) / sorted.length : null };
}
export function probeSummary(rows: Probe[]) {
    const sent = rows.filter((r) => r.sentMs !== null);
    const success = sent.filter((r) => !r.error && r.status === 200);
    const timeouts = sent.filter((r) => r.errorKind === 'timeout').length;
    const errors = sent.filter((r) => r.error && r.errorKind !== 'timeout').length;
    return { planned: rows.length, sent: sent.length, skipped: rows.length - sent.length,
        success: success.length, timeouts, errors, otherHttp: sent.length - success.length - timeouts - errors,
        // 전송된 모든 요청의 완료/실패까지 경과를 포함한다. 성공 응답 분포도 따로 제공한다.
        durationMs: distribution(sent.map((r) => r.durationMs!)), successDurationMs: distribution(success.map((r) => r.durationMs!)),
        scheduleLagMs: distribution(sent.map((r) => r.sentMs! - r.scheduledMs)),
        largest: [...sent].sort((a, b) => b.durationMs! - a.durationMs!).slice(0, 3) };
}
export function metric(text: string, name: string) {
    const match = new RegExp(`^${name} ([^\\s]+)$`, 'm').exec(text);
    assert.ok(match, `Metric 누락: ${name}`);
    const value = Number(match[1]);
    assert.ok(Number.isFinite(value), `Metric이 유한수가 아닙니다: ${name}`);
    return value;
}
export function histogram(text: string) {
    return {
        maxMs: metric(text, 'nodejs_event_loop_delay_max_seconds') * 1000,
        p99Ms: metric(text, 'nodejs_event_loop_delay_p99_seconds') * 1000,
        meanMs: metric(text, 'nodejs_event_loop_delay_mean_seconds') * 1000,
        count: metric(text, 'nodejs_event_loop_delay_samples'),
    };
}
export function clockDiagnostic(start: { monoMs: number; wallMs: number }, end: { monoMs: number; wallMs: number }) {
    const monotonicElapsedMs = end.monoMs - start.monoMs;
    const wallElapsedMs = end.wallMs - start.wallMs;
    return { monotonicElapsedMs, wallElapsedMs, differenceMs: wallElapsedMs - monotonicElapsedMs };
}
// 성능의 예상 방향은 유효성 기준이 아니다. 시계/발송 품질만 검사한다.
export function probeValidity(rows: Probe[], intervalMs: number, clockDifferenceMs: number) {
    const reasons: string[] = [];
    if (!rows.length) reasons.push('Probe 표본 없음');
    if (rows.some((r) => r.skipped)) reasons.push('In-flight 상한으로 발송 누락');
    if (rows.some((r) => r.sentMs !== null && r.sentMs - r.scheduledMs > intervalMs)) reasons.push('한 주기를 초과한 발송 지연');
    if (rows.some((r) => r.error || r.status !== 200)) reasons.push('Probe Timeout·오류·비정상 HTTP');
    if (Math.abs(clockDifferenceMs) > intervalMs) reasons.push('시계 경과 차이가 한 주기를 초과하여 Wall Clock 중첩 해석 불확실');
    return reasons;
}
