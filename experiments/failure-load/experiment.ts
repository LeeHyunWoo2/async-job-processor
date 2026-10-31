import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { parseArgs } from 'node:util';

export type Mode = 'normal' | 'failure';
export type Purpose = 'main' | 'pilot' | 'smoke';
export function options(args: string[]) {
    const { values } = parseArgs({ args, options: {
        purpose: { type: 'string', default: 'main' },
        modes: { type: 'string', default: 'normal,failure' }, repeats: { type: 'string', default: '2' },
        workers: { type: 'string', default: '3' }, seconds: { type: 'string', default: '120' },
        rate: { type: 'string', default: '2.4' }, 'duration-ms': { type: 'string', default: '1000' },
        'target-at-seconds': { type: 'string', default: '20' }, limit: { type: 'string', default: '30' },
        'interval-ms': { type: 'string', default: '1000' }, 'max-inflight': { type: 'string', default: '4' },
        'drain-seconds': { type: 'string', default: '300' },
        out: { type: 'string', default: `artifacts/failure-load/${new Date().toISOString().replaceAll(':', '-')}` },
    } });
    function number(value: string, name: string, min: number, max: number, integer = true) {
        assert.match(value, integer ? /^\d+$/ : /^\d+(\.\d+)?$/, `${name}: 잘못된 숫자입니다`);
        const n = Number(value);
        assert.ok(Number.isFinite(n) && (!integer || Number.isSafeInteger(n)) && n >= min && n <= max,
            `${name}: 허용 범위 ${min}~${max}`);
        return n;
    }
    const modes = values.modes.split(',') as Mode[];
    assert.ok(modes.every((m) => m === 'normal' || m === 'failure') && new Set(modes).size === modes.length,
        'modes: normal/failure를 중복 없이 지정하세요');
    const workers = number(values.workers, 'workers', 2, 3);
    assert.ok(workers === 3 || !modes.includes('failure'), 'failure 조건은 Worker 3개가 필요합니다. Worker 2개는 normal Pilot에서만 사용하세요');
    const purpose = values.purpose as Purpose;
    assert.ok(['main', 'pilot', 'smoke'].includes(purpose), 'purpose: main/pilot/smoke를 지정하세요');
    assert.ok(purpose !== 'pilot' || modes.every((m) => m === 'normal'), 'Pilot은 normal 모드에서 Worker 2·3개를 따로 관찰하세요');
    assert.ok(workers !== 2 || purpose === 'pilot', 'Worker 2개는 --purpose=pilot에서만 사용하세요');
    const seconds = number(values.seconds, 'seconds', 2, 300);
    const targetAtSeconds = number(values['target-at-seconds'], 'target-at-seconds', 0, seconds, false);
    assert.ok(targetAtSeconds < seconds, '대상 Job은 생산 구간 안에서 발송해야 합니다');
    assert.ok(values.out.trim(), 'out: 자료 경로가 필요합니다');
    return { purpose, modes, workers, seconds, targetAtSeconds, targetDurationMs: 10_000,
        repeats: number(values.repeats, 'repeats', 1, 2), rate: number(values.rate, 'rate', .1, 100, false),
        durationMs: number(values['duration-ms'], 'duration-ms', 1, 10_000), limit: number(values.limit, 'limit', 1, 100_000),
        intervalMs: number(values['interval-ms'], 'interval-ms', 100, 5000),
        maxInflight: number(values['max-inflight'], 'max-inflight', 1, 32),
        drainSeconds: number(values['drain-seconds'], 'drain-seconds', 1, 300), out: values.out };
}
export type Options = ReturnType<typeof options>;
export function runOrder(o: Options) {
    return Array.from({ length: o.repeats }, (_, i) => (i % 2 ? [...o.modes].reverse() : o.modes)
        .map((mode) => ({ mode, repeat: i + 1 }))).flat();
}
export function schedule(seconds: number, rate: number) {
    // [0, seconds * 1000) 안의 예약만 만든다. 기본 조건은 288개다.
    return Array.from({ length: Math.ceil(seconds * rate) }, (_, i) => i * 1000 / rate)
        .filter((ms) => ms < seconds * 1000);
}
export interface RequestRecord {
    kind: 'regular' | 'target'; index: number; scheduledMs: number;
    sentMs: number | null; finishedMs: number | null;
    status?: number; code?: string; id?: string; body?: unknown; error?: string; unsent?: string;
}
export function category(r: RequestRecord) {
    if (r.sentMs === null) return 'unsent';
    if (r.error !== undefined || r.status === undefined || r.finishedMs === null) return 'networkOrBodyErrors';
    if (r.status === 201 && typeof r.id === 'string') return 'accepted';
    if (r.status === 503 && r.code === 'QUEUE_CAPACITY_EXCEEDED') return 'rejected';
    return 'otherHttp';
}
export function distribution(values: number[]) {
    const sorted = [...values].sort((a, b) => a - b);
    const percentile = (p: number) => sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)] ?? null;
    return { count: sorted.length, min: sorted[0] ?? null, p50: percentile(.5), p95: percentile(.95),
        p99: percentile(.99), max: sorted.at(-1) ?? null,
        mean: sorted.length ? sorted.reduce((a, b) => a + b, 0) / sorted.length : null };
}
export function requestSummary(rows: RequestRecord[]) {
    const count = (name: ReturnType<typeof category>) => rows.filter((r) => category(r) === name).length;
    const sent = rows.filter((r) => r.sentMs !== null);
    const times = sent.map((r) => r.sentMs!).sort((a, b) => a - b);
    const rtt = (outcome: ReturnType<typeof category>) => distribution(sent.filter((r) => category(r) === outcome && r.finishedMs !== null)
        .map((r) => r.finishedMs! - r.sentMs!));
    return { planned: rows.length, sent: sent.length, unsent: count('unsent'), accepted: count('accepted'),
        rejected: count('rejected'), otherHttp: count('otherHttp'), networkOrBodyErrors: count('networkOrBodyErrors'),
        acceptanceFraction: sent.length ? count('accepted') / sent.length : null,
        dispatchRate: times.length > 1 && times.at(-1)! > times[0]! ? (times.length - 1) * 1000 / (times.at(-1)! - times[0]!) : null,
        rttMs: distribution(sent.filter((r) => r.finishedMs !== null).map((r) => r.finishedMs! - r.sentMs!)),
        rttMsByOutcome: { accepted201: rtt('accepted'), policyRejected503: rtt('rejected'), otherHttp: rtt('otherHttp'), networkOrBodyErrors: rtt('networkOrBodyErrors') },
        scheduleLagMs: distribution(sent.map((r) => r.sentMs! - r.scheduledMs)),
        ids: rows.filter((r) => category(r) === 'accepted').map((r) => r.id!) };
}

// 일반 부하와 대상 Job은 같은 Clock·동시성 상한을 사용하되 별도의 예약이다.
// 상한에 막힌 예약은 늦은 Burst로 보정하지 않고 미발송으로 보존한다.
export function startProducer(o: Options, url: string, origin: number, signal: AbortSignal,
    record: (row: RequestRecord) => Promise<void>, onError: (error: unknown) => void) {
    const requests: RequestRecord[] = schedule(o.seconds, o.rate).map((scheduledMs, index) =>
        ({ kind: 'regular', index, scheduledMs, sentMs: null, finishedMs: null }));
    const targetRow: RequestRecord = { kind: 'target', index: 0, scheduledMs: o.targetAtSeconds * 1000, sentMs: null, finishedMs: null };
    requests.push(targetRow);
    const active = new Set<Promise<void>>();
    let peakInflight = 0;
    const persist = async (row: RequestRecord) => { try { await record(row); } catch (error) { onError(error); } };
    const send = async (row: RequestRecord) => {
        row.sentMs = performance.now() - origin;
        try {
            const response = await fetch(`${url}/jobs`, { method: 'POST', headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ type: 'slow', payload: { durationMs: row.kind === 'target' ? o.targetDurationMs : o.durationMs } }),
                signal: AbortSignal.any([signal, AbortSignal.timeout(5000)]) });
            row.status = response.status;
            const body = await response.json();
            row.body = body;
            if (typeof body?.code === 'string') row.code = body.code;
            if (typeof body?.id === 'string') row.id = body.id;
        } catch (error) { row.error = String(error); }
        row.finishedMs = performance.now() - origin;
        await persist(row);
    };
    const wait = async (ms: number) => {
        // Timer의 소수점 절삭으로 예약보다 일찍 발송하지 않도록 남은 시간을 재확인한다.
        try {
            while (performance.now() < origin + ms)
                await delay(Math.max(1, origin + ms - performance.now()), undefined, { signal });
        } catch { /* 중단은 미발송 정산에서 처리한다. */ }
    };
    const dispatch = (row: RequestRecord) => {
        if (signal.aborted) row.unsent = '중단';
        else if (performance.now() - origin >= o.seconds * 1000) row.unsent = '생산 구간 종료';
        else if (active.size >= o.maxInflight) row.unsent = '동시 발송 상한';
        if (row.unsent) return persist(row);
        const task = send(row);
        active.add(task);
        peakInflight = Math.max(peakInflight, active.size);
        void task.then(() => active.delete(task));
        return task;
    };
    const target = (async () => { await wait(targetRow.scheduledMs); await dispatch(targetRow); return targetRow; })();
    const done = (async () => {
        for (const row of requests.filter((r) => r.kind === 'regular')) {
            // HTTP 완료와 독립적으로 다음 예약 시각까지 기다린다.
            await wait(row.scheduledMs);
            const task = dispatch(row);
            if (row.unsent) await task;
        }
        await wait(o.seconds * 1000);
        await target;
        await Promise.all(active);
        return { peakInflight, settledMs: performance.now() - origin };
    })();
    return { requests, target, done };
}

export interface JobRow {
    id: string; type: string; payload: unknown; status: string; attempt: number;
    worker_id: string | null; result: unknown; last_error: unknown; processing_started_at: string | null;
}
export interface Event {
    event: string; jobId?: string; workerId?: string; previousWorkerId?: string; attempt?: number; status?: string;
    time: number; observedMs: number; receivedWallMs: number; containerId: string;
}
export interface Sample {
    elapsedMs: number; queryMs: number; actualIntervalMs: number | null; wallMs: number; clockDifferenceMs: number;
    dbWallMs: number; dbClockOffsetMs: number; phase: string;
    total: number; queued: number; processing: number; outstanding: number; succeeded: number; failed: number;
    connections: number; activeConnections: number;
}
export interface Observation { elapsedMs: number; [key: string]: unknown }
export interface Evidence {
    requests: RequestRecord[]; samples: Sample[]; events: Event[]; before: JobRow[]; after: JobRow[];
    workers: { containerId: string; workerId: string }[]; observations: Record<string, Observation>;
    collectionErrors: string[]; executionError?: string; topologyVerified: boolean; finalTopologyVerified: boolean;
}

// SIGKILL 확인 후 발송된 요청만 선택해 장애 전 완료 로그의 늦은 도착을 배제한다.
export function postKillRequest(requests: RequestRecord[], event: Event, killMs: number) {
    return event.event === 'job.succeeded' && event.observedMs > killMs ? requests.find((r) =>
        r.kind === 'regular' && category(r) === 'accepted' && r.id === event.jobId && r.sentMs! > killMs && event.observedMs >= r.sentMs!) : undefined;
}

export function survivorSuccessRequest(d: Pick<Evidence, 'requests' | 'workers' | 'observations'>,
    event: Event, retained: JobRow, completed: JobRow) {
    const killed = d.observations['kill-confirmed'];
    const first = d.observations['handler-started']?.row as JobRow | undefined;
    const survivors = killed?.survivors as { id: string }[] | undefined;
    if (!killed || !first || !survivors?.some((c) => c.id === event.containerId)
        || !d.workers.some((w) => w.containerId === event.containerId && w.workerId === event.workerId)
        || retained.id !== first.id || retained.status !== 'processing' || retained.attempt !== 1
        || retained.worker_id !== first.worker_id || retained.processing_started_at !== first.processing_started_at
        || event.workerId === first.worker_id || completed.id !== event.jobId || completed.id === first.id
        || completed.status !== 'succeeded' || completed.attempt !== 1 || completed.worker_id !== event.workerId) return undefined;
    return postKillRequest(d.requests, event, killed.elapsedMs);
}

// 정상 3개 대조군의 적합성에 사용한다. 기능 판정이나 Worker 2개 Pilot에 강제하지 않는다.
export function loadStability(o: Options, d: Evidence) {
    const targetEnd = d.observations['target-succeeded']?.elapsedMs;
    const loadEnd = Math.min(o.seconds * 1000, d.observations['load-end']?.elapsedMs ?? 0);
    const windowMs = 10_000;
    const windows: { startMs: number; endMs: number; samples: number; meanOutstanding: number | null; sent: number; rejected: number }[] = [];
    if (targetEnd !== undefined) for (let startMs = targetEnd; startMs + windowMs <= loadEnd; startMs += windowMs) {
        const endMs = startMs + windowMs;
        const samples = d.samples.filter((s) => s.elapsedMs >= startMs && s.elapsedMs + s.queryMs < endMs);
        const requests = d.requests.filter((r) => r.kind === 'regular' && r.sentMs !== null && r.sentMs >= startMs && r.sentMs < endMs);
        windows.push({ startMs, endMs, samples: samples.length,
            meanOutstanding: samples.length >= 2 ? samples.reduce((n, s) => n + s.outstanding, 0) / samples.length : null,
            sent: requests.length, rejected: requests.filter((r) => category(r) === 'rejected').length });
    }
    const saturation = windows.slice(1).flatMap((w, i) => windows[i]!.rejected > 0 && w.rejected > 0 ? [{ startMs: windows[i]!.startMs, endMs: w.endMs }] : []);
    const growth = windows.slice(2).flatMap((c, i) => {
        const a = windows[i]!, b = windows[i + 1]!;
        return a.meanOutstanding !== null && b.meanOutstanding !== null && c.meanOutstanding !== null
            && a.meanOutstanding < b.meanOutstanding && b.meanOutstanding < c.meanOutstanding && c.meanOutstanding - a.meanOutstanding >= 3
            ? [{ startMs: a.startMs, endMs: c.endMs, increase: c.meanOutstanding - a.meanOutstanding }] : [];
    });
    const reasons: string[] = [];
    if (saturation.length) reasons.push('대상 완료 후 10초 구간 2개 연속 정책상 503 발생');
    if (growth.length) reasons.push('대상 완료 후 10초 구간 3개 연속 outstanding 평균 증가 및 첫·끝 차이 3개 이상');
    const sufficient = windows.length >= 3 && windows.every((w) => w.meanOutstanding !== null && w.sent > 0);
    const status = reasons.length ? 'unsuitable' : sufficient ? 'suitable' : 'inconclusive';
    if (!sufficient) reasons.push('대상 완료 후 요청·DB 표본이 있는 온전한 10초 구간 3개 부족');
    return { status, reasons, windowMs, targetEndMs: targetEnd ?? null, loadEndMs: loadEnd, windows, saturation, growth,
        unassessedTailMs: Math.max(0, loadEnd - (windows.at(-1)?.endMs ?? targetEnd ?? loadEnd)) };
}

function completionCrossCheck(samples: Sample[], events: Event[], startMs: number, endMs: number) {
    const contained = samples.filter((s) => s.elapsedMs >= startMs && s.elapsedMs + s.queryMs <= endMs).sort((a, b) => a.elapsedMs - b.elapsedMs);
    const first = contained[0], last = contained.at(-1);
    if (!first || !last || first === last) return { available: false, reason: '구간 내부 DB 표본 2개 미만' };
    const logObservedSucceeded = events.filter((e) => e.event === 'job.succeeded'
        && e.observedMs >= first.elapsedMs + first.queryMs && e.observedMs < last.elapsedMs + last.queryMs).length;
    const dbSucceededDelta = last.succeeded - first.succeeded;
    return { available: true, first: { readStartedMs: first.elapsedMs, queryMs: first.queryMs, succeeded: first.succeeded },
        last: { readStartedMs: last.elapsedMs, queryMs: last.queryMs, succeeded: last.succeeded },
        dbSucceededDelta, logObservedSucceeded, difference: logObservedSucceeded - dbSucceededDelta,
        note: '전체 Job 기준의 내부 DB 관측 구간 비교. 조회 시점·로그 전달 차이로 일시 불일치할 수 있으며 단계 전체 완료 수와 같지 않습니다' };
}
export function sampleSummary(samples: Sample[]) {
    return { count: samples.length, observedMaxOutstanding: Math.max(0, ...samples.map((s) => s.outstanding)),
        observedMaxQueued: Math.max(0, ...samples.map((s) => s.queued)), queryMs: distribution(samples.map((s) => s.queryMs)),
        actualIntervalMs: distribution(samples.flatMap((s) => s.actualIntervalMs === null ? [] : [s.actualIntervalMs])),
        maxAbsClockDifferenceMs: Math.max(0, ...samples.map((s) => Math.abs(s.clockDifferenceMs))) };
}
export function phaseSummary(data: Evidence) {
    const points = [{ name: 'load-start', elapsedMs: 0 }, ...Object.entries(data.observations)
        .filter(([name]) => ['handler-started', 'control-confirmed', 'kill-confirmed', 'recovered', 'second-claim', 'target-succeeded', 'load-end', 'drain-end'].includes(name))
        .map(([name, value]) => ({ name, elapsedMs: value.elapsedMs }))].sort((a, b) => a.elapsedMs - b.elapsedMs);
    return points.slice(1).map((end, i) => {
        const start = points[i]!;
        const seconds = (end.elapsedMs - start.elapsedMs) / 1000;
        const inside = (ms: number) => ms >= start.elapsedMs && ms < end.elapsedMs;
        const requests = data.requests.filter((r) => r.sentMs !== null && inside(r.sentMs));
        const regular = requestSummary(requests.filter((r) => r.kind === 'regular'));
        const target = requestSummary(requests.filter((r) => r.kind === 'target'));
        const ids = new Set(data.requests.filter((r) => r.kind === 'regular' && category(r) === 'accepted').map((r) => r.id));
        const targetIds = new Set(data.requests.filter((r) => r.kind === 'target' && category(r) === 'accepted').map((r) => r.id));
        const completed = data.events.filter((e) => e.event === 'job.succeeded' && inside(e.observedMs));
        return { from: start.name, to: end.name, startMs: start.elapsedMs, endMs: end.elapsedMs, seconds,
            regular, target, completionBasis: 'log-observed', logObservedCompletedRegular: completed.filter((e) => ids.has(e.jobId)).length,
            logObservedCompletedTarget: completed.filter((e) => targetIds.has(e.jobId)).length,
            logObservedCompletedUnknown: completed.filter((e) => !ids.has(e.jobId) && !targetIds.has(e.jobId)).length,
            acceptedRegularPerSecond: seconds > 0 ? regular.accepted / seconds : null,
            logObservedCompletedRegularPerSecond: seconds > 0 ? completed.filter((e) => ids.has(e.jobId)).length / seconds : null,
            dbCompletionCrossCheck: completionCrossCheck(data.samples, data.events, start.elapsedMs, end.elapsedMs),
            sampling: sampleSummary(data.samples.filter((s) => inside(s.elapsedMs))) };
    });
}

export function evaluate(o: Options, mode: Mode, d: Evidence) {
    const scenario: string[] = [], failures: string[] = [], missing: string[] = [], measurement: string[] = [];
    const regular = requestSummary(d.requests.filter((r) => r.kind === 'regular'));
    const targetRequests = d.requests.filter((r) => r.kind === 'target');
    const target = requestSummary(targetRequests);
    const targetId = target.ids[0];
    const ids = [...regular.ids, ...target.ids];
    const added = d.after.filter((r) => !d.before.some((b) => b.id === r.id));
    const unknownResponseRows = added.filter((r) => !ids.includes(r.id));
    const absentIds = ids.filter((id) => !added.some((r) => r.id === id));
    const observed = (name: string) => d.observations[name];
    const logs = (id: string, name: string) => d.events.filter((e) => e.jobId === id && e.event === name);
    const applies = o.purpose !== 'smoke' && mode === 'normal' && o.workers === 3;
    const stability = { ...loadStability(o, d), criterion: 'normal-three-stable', applied: applies };
    const suitability = { purpose: o.purpose, criterion: applies ? 'normal-three-stable' : o.purpose === 'pilot' ? 'normal-two-capacity' : 'tool-or-failure-path',
        status: applies ? stability.status : 'observation-only', requiresUserDecision: applies && stability.status !== 'suitable' };
    if (o.purpose === 'main' && suitability.requiresUserDecision) scenario.push(...stability.reasons);
    if (!d.topologyVerified || !d.finalTopologyVerified) scenario.push('실제 Worker 구성·프로세스 유지 확인 미완료');
    if (regular.sent !== schedule(o.seconds, o.rate).length || regular.unsent || (regular.scheduleLagMs.max ?? Infinity) > 1000 / o.rate)
        scenario.push('계획한 일반 부하의 발송 수·한 주기 이내 발송 지연 조건 미충족');
    if (targetRequests.length !== 1 || target.accepted !== 1 || !observed('handler-started')) scenario.push('별도 대상 Job 1개 수락·Handler 실행 확인 미완료');
    const participatingWorkers = new Set(d.events.filter((e) => ids.includes(e.jobId!) && (mode === 'normal'
        ? e.event === 'job.succeeded'
        : e.event === 'job.handler_started' && e.observedMs < (observed('kill-confirmed')?.elapsedMs ?? -Infinity))).map((e) => e.workerId));
    if (!d.workers.length || !d.workers.every((w) => participatingWorkers.has(w.workerId)))
        scenario.push(mode === 'normal' ? '정상 Worker 전체의 Job 완료 관측 부족' : 'SIGKILL 전 Worker 전체의 Handler 진입 관측 부족');
    if (mode === 'failure') {
        if (!observed('kill-confirmed') || !observed('retained-after-kill')) scenario.push('실행 중 SIGKILL·ExitCode 137·생존 2개·대상 잔류 확인 미완료');
        if (targetId && logs(targetId, 'job.handler_finished').some((e) => e.attempt === 1)) scenario.push('대상 첫 Handler가 종료되어 실행 중 SIGKILL 조건 미성립');
        if (!d.requests.some((r) => r.kind === 'regular' && r.sentMs !== null && r.sentMs > (observed('kill-confirmed')?.elapsedMs ?? Infinity))) scenario.push('SIGKILL 뒤 지속 유입 확인 부족');
        if (!d.requests.some((r) => r.kind === 'regular' && category(r) === 'rejected' && r.sentMs! > (observed('kill-confirmed')?.elapsedMs ?? Infinity))
            || !d.samples.some((s) => s.queued > 0 && s.elapsedMs > (observed('kill-confirmed')?.elapsedMs ?? Infinity))) scenario.push('장애 구간의 Queue 누적·정책상 503 관측 부족: 실험 조건 미성립');
        const proof = observed('survivor-before-recovery');
        const event = proof?.event as Event | undefined, retained = proof?.retainedTarget as JobRow | undefined, completed = proof?.completed as JobRow | undefined;
        if (!event || !retained || !completed || !survivorSuccessRequest(d, event, retained, completed)
            || !d.events.some((e) => e.event === event.event && e.jobId === event.jobId && e.workerId === event.workerId
                && e.containerId === event.containerId && e.observedMs === event.observedMs))
            missing.push('Recovery 전 생존 Worker가 SIGKILL 확인 후 발송·수락된 일반 Job을 완료한 DB·로그 근거 없음');
        const recoveredAge = observed('recovered')?.firstClaimAgeMsAtConfirmation;
        if (observed('recovered')) {
            if (typeof recoveredAge !== 'number' || !Number.isFinite(recoveredAge)) {
                measurement.push('Recovery 확인 시 DB 기준 최초 Claim 경과 누락'); missing.push('30초 Stale 기준 확인 근거 없음');
            } else if (recoveredAge < 30_000) failures.push('Recovery 확인 시 DB 기준 최초 Claim 경과가 30초 미만');
        }
        for (const name of ['recovered', 'second-claim', 'second-processing', 'target-succeeded']) if (!observed(name)) missing.push(`대상 사건 누락: ${name}`);
    } else if (!observed('control-confirmed')) scenario.push('정상 대조군의 대상 실행 확인 미완료');
    if (d.samples.some((s) => s.outstanding > o.limit)) failures.push('DB 표본에서 Admission 상한 초과');
    if (!d.samples.length || !observed('drain-end')) missing.push('최종 전역 정산 확인 미완료');
    else if (d.samples.at(-1)!.outstanding !== 0) failures.push('최종 전역 미완료 Job 잔류');
    if (regular.otherHttp + regular.networkOrBodyErrors + target.otherHttp + target.networkOrBodyErrors) measurement.push('예상 외 HTTP·네트워크/본문 오류');
    if (regular.unsent || target.unsent || regular.planned !== schedule(o.seconds, o.rate).length) measurement.push('요청 예약·발송 누락');
    if (new Set(ids).size !== ids.length) failures.push('수락 ID 중복');
    if (unknownResponseRows.length || absentIds.length) measurement.push('신규 DB 행과 수락 ID 불일치: 응답 유실 후 Commit·외부 등록·누락 확인 필요');
    if (absentIds.length) missing.push('수락 ID의 최종 DB 행 누락');
    if (JSON.stringify(d.after.filter((r) => d.before.some((b) => b.id === r.id))) !== JSON.stringify(d.before)) failures.push('이전 완료 행 변경·누락');
    for (const r of d.requests) {
        const body = r.body as { status?: string; error?: unknown } | null | undefined;
        if (category(r) === 'otherHttp') failures.push(`유효한 등록 요청의 예상 외 HTTP 계약: ${r.kind}/${r.index}, HTTP ${r.status}`);
        if (category(r) === 'accepted' && body?.status !== 'queued') failures.push(`201 수락 계약의 queued 상태 불일치: ${r.id}`);
        if (category(r) === 'rejected' && typeof body?.error !== 'string') failures.push('503 정책 거부 계약의 error 메시지 누락');
    }
    for (const row of added) {
        if (!ids.includes(row.id)) { missing.push(`신규 행의 요청 대응 확인 불가: ${row.id}`); continue; }
        const expectedAttempt = mode === 'failure' && row.id === targetId ? 2 : 1;
        const durationMs = row.id === targetId ? o.targetDurationMs : o.durationMs;
        if (row.status === 'queued' || row.status === 'processing') {
            missing.push(`수락 Job 미완료: ${row.id} (${row.status}, attempt=${row.attempt})`);
            if (row.attempt > expectedAttempt) failures.push(`미완료 Job의 예상 외 attempt: ${row.id}`);
            continue;
        }
        if (row.status !== 'succeeded' || row.attempt !== expectedAttempt || row.last_error !== null || row.type !== 'slow'
            || JSON.stringify(row.payload) !== JSON.stringify({ durationMs }) || JSON.stringify(row.result) !== JSON.stringify({ durationMs }))
            failures.push(`Job 최종 상태·result·attempt 불일치: ${row.id}`);
        for (const event of ['job.claimed', 'job.handler_started', 'job.handler_finished', 'job.succeeded']) {
            const found = logs(row.id, event);
            const attempts = event === 'job.claimed' || event === 'job.handler_started' ? (expectedAttempt === 2 ? [1, 2] : [1]) : [expectedAttempt];
            if (found.length < attempts.length) { measurement.push(`${row.id}: ${event} 필수 로그 누락`); continue; }
            if (JSON.stringify(found.map((e) => e.attempt).sort()) !== JSON.stringify(attempts)) failures.push(`${row.id}: ${event} 중복·예상 외 attempt`);
            if (found.some((e) => !d.workers.some((w) => w.workerId === e.workerId && w.containerId === e.containerId))) failures.push(`${row.id}: 로그 Worker·컨테이너 불일치`);
            if (found.some((e) => e.attempt === expectedAttempt && e.workerId !== row.worker_id)) failures.push(`${row.id}: 최종 DB·로그 소유권 불일치`);
        }
        const recoveries = logs(row.id, 'job.recovered');
        if (mode === 'failure' && row.id === targetId) {
            const first = logs(row.id, 'job.claimed').find((e) => e.attempt === 1);
            if (!recoveries.length) measurement.push('대상 Recovery 로그 누락');
            else if (recoveries.length !== 1 || recoveries[0]!.attempt !== 1 || recoveries[0]!.status !== 'queued'
                || recoveries[0]!.previousWorkerId !== first?.workerId || recoveries[0]!.workerId === first?.workerId
                || row.worker_id === first?.workerId
                || !d.workers.some((w) => w.workerId === recoveries[0]!.workerId && w.containerId === recoveries[0]!.containerId)) failures.push('대상 Recovery·이전 소유자·재Claim 소유권 불일치');
        } else if (recoveries.length) failures.push(`예상 외 Recovery: ${row.id}`);
    }
    if (d.events.some((e) => ['job.failed', 'job.retry_scheduled', 'worker.error'].includes(e.event))) failures.push('예상 외 실패·Retry·Worker 오류 사건');
    if (!d.samples.some((s) => s.phase === 'start') || !d.samples.some((s) => s.phase === 'load-end')) measurement.push('시작·생산 종료 DB 표본 누락');
    if (!observed('target-succeeded')) measurement.push('대상 최종 완료 관측 누락');
    measurement.push(...d.collectionErrors);
    if (d.executionError) measurement.push(d.executionError);
    const timingReasons: string[] = [];
    const maxInterval = sampleSummary(d.samples).actualIntervalMs.max;
    if (maxInterval === null || maxInterval > o.intervalMs * 3) timingReasons.push('DB 표본 간격이 예약 주기의 3배 초과 또는 간격 없음');
    if (mode === 'failure' && observed('recovered') && observed('second-claim')
        && observed('recovered')!.elapsedMs > observed('second-claim')!.elapsedMs) timingReasons.push('서로 다른 로그 Stream 도착 순서 역전: 세부 Recovery 경과 해석 제외');
    const wallReasons = sampleSummary(d.samples).maxAbsClockDifferenceMs > 1000 ? ['Wall/Monotonic 경과 차이 1초 초과: Wall Clock 자료 정렬 제외'] : [];
    const dbSucceededDelta = d.after.filter((r) => r.status === 'succeeded').length - d.before.filter((r) => r.status === 'succeeded').length;
    const logObservedSucceeded = d.events.filter((e) => e.event === 'job.succeeded').length;
    return { scenario: { valid: scenario.length === 0, reasons: scenario, suitability }, stability,
        functional: { status: failures.length ? 'failed' : missing.length ? 'incomplete' : 'passed', failures, missing },
        measurement: { valid: measurement.length === 0, reasons: measurement,
            timing: { valid: timingReasons.length === 0, reasons: timingReasons }, wallAlignment: { valid: wallReasons.length === 0, reasons: wallReasons } },
        http: { regular, target }, reconciliation: { newRows: added.length, unknownResponseRows, absentIds },
        sampling: sampleSummary(d.samples), phases: phaseSummary(d), completion: {
            basis: 'log-observed', dbSucceededDelta, logObservedSucceeded, matches: dbSucceededDelta === logObservedSucceeded,
        } };
}
