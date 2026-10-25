import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

export interface JobRow {
    id: string;
    type: string;
    payload: { durationMs: number };
    status: string;
    attempt: number;
    workerId: string | null;
    processingStartedAt: string | null;
    updatedAt: string;
    ready: boolean;
    lastError: unknown;
    result: unknown;
}

export function batchTiming(rows: JobRow[], ids: string[], durationMs: number) {
    assert.equal(new Set(ids).size, ids.length, 'Batch ID가 중복되었습니다');
    assert.ok(ids.length > 0, '빈 Batch는 측정할 수 없습니다');
    assert.equal(rows.length, ids.length, 'Batch Job이 누락되었습니다');
    assert.deepEqual(new Set(rows.map((row) => row.id)), new Set(ids), 'Batch 외 Job이 섞였습니다');
    for (const row of rows) {
        assert.ok(row.status === 'succeeded' && row.attempt === 1 && row.processingStartedAt && row.workerId
            && row.lastError === null && row.type === 'slow' && row.payload.durationMs === durationMs,
        `정상 Concurrency 비교에서 제외합니다: ${row.id}`);
        assert.deepEqual(row.result, { durationMs }, '정상 Handler 결과가 아닙니다');
        assert.ok(new Date(row.updatedAt).getTime() >= new Date(row.processingStartedAt!).getTime(), 'Job 시각이 역전되었습니다');
    }
    const firstClaim = new Date(Math.min(...rows.map((row) => new Date(row.processingStartedAt!).getTime()))).toISOString();
    const lastSuccess = new Date(Math.max(...rows.map((row) => new Date(row.updatedAt).getTime()))).toISOString();
    const elapsedSeconds = (Date.parse(lastSuccess) - Date.parse(firstClaim)) / 1_000;
    assert.ok(elapsedSeconds > 0, '처리 경과 시간이 0 이하입니다');
    return { firstClaim, lastSuccess, elapsedSeconds, throughput: rows.length / elapsedSeconds };
}

export function spread(values: number[]) {
    assert.ok(values.length > 0 && values.every(Number.isFinite), '유효한 관측값이 필요합니다');
    const sorted = [...values].sort((a, b) => a - b);
    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const middle = Math.floor(sorted.length / 2);
    return {
        count: values.length,
        median: sorted.length % 2 ? sorted[middle]! : (sorted[middle - 1]! + sorted[middle]!) / 2,
        min: sorted[0]!, max: sorted.at(-1)!,
        // 이번에 실제 수행한 반복 집합의 산포이며 모집단 성능의 신뢰구간이 아니다.
        populationStddev: Math.sqrt(values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length),
    };
}

export interface RunResult {
    name: string;
    workers: number;
    repeat: number;
    count: number;
    durationMs: number;
    valid: boolean;
    error?: string;
    timing?: ReturnType<typeof batchTiming>;
    resources?: Record<string, ReturnType<typeof spread>>;
    targetWaitSeconds?: number;
    gateReleasedAt?: string;
    handler?: { count: number; sumSeconds: number; meanSeconds: number;
        workers: { instance: string; count: number; sumSeconds: number; meanSeconds: number }[] };
    sampling?: { total: number; errors: number; insideWindow: number };
    workerContainers?: { id: string; workerId: string; instance: string; startedAt: string; imageId: string; limits: Record<string, unknown> }[];
}

export async function writeSummary(directory: string, results: RunResult[]) {
    const baselines = results.filter((run) => run.valid && run.workers === 1 && run.timing);
    const baselineMedian = baselines.length ? spread(baselines.map((run) => run.timing!.throughput)).median : undefined;
    const rows = results.map((run) => {
        const baseline = baselines.find((one) => one.repeat === run.repeat && one.count === run.count && one.durationMs === run.durationMs);
        const speedup = run.valid && run.timing && baseline ? run.timing.throughput / baseline.timing!.throughput : undefined;
        return { ...run, speedup, efficiencyPercent: speedup === undefined ? undefined : speedup / run.workers * 100 };
    });
    const groups = [...new Set(results.map((run) => run.workers))].map((workers) => {
        const valid = rows.filter((run) => run.workers === workers && run.valid && run.timing);
        if (!valid.length) return { workers, validRuns: 0 };
        const throughput = spread(valid.map((run) => run.timing!.throughput));
        const speedup = baselineMedian === undefined ? undefined : throughput.median / baselineMedian;
        return { workers, validRuns: valid.length, throughput, speedup,
            efficiencyPercent: speedup === undefined ? undefined : speedup / workers * 100 };
    });
    const environment = JSON.parse(await readFile(join(directory, 'environment.json'), 'utf8'));
    await writeFile(join(directory, 'summary.json'), JSON.stringify({ environment, runs: rows, groups }, null, 2) + '\n');
    const number = (value: number | undefined) => value === undefined ? ''
        : value !== 0 && Math.abs(value) < 0.0001 ? value.toExponential(2) : value.toFixed(4);
    await writeFile(join(directory, 'runs.csv'), [
        'run,workers,repeat,count,duration_ms,valid,elapsed_seconds,jobs_per_second,speedup,efficiency_percent',
        ...rows.map((run) => [run.name, run.workers, run.repeat, run.count, run.durationMs, run.valid,
            number(run.timing?.elapsedSeconds), number(run.timing?.throughput), number(run.speedup), number(run.efficiencyPercent)].join(',')),
    ].join('\n') + '\n');
    const report = [
        '# Worker 수에 따른 실측 결과', '',
        '상세 환경은 `summary.json`, 개별 원자료는 각 실행 디렉터리에 있다. 실패·미완료 실행은 집계에서 제외한다.', '',
        '| 실행 | Worker | Job | 대기 ms | 유효 | 경과 초 | Job/s | Speedup | Efficiency % |',
        '|---|---:|---:|---:|---|---:|---:|---:|---:|',
        ...rows.map((run) => `| ${run.name} | ${run.workers} | ${run.count} | ${run.durationMs} | ${run.valid} | ${number(run.timing?.elapsedSeconds)} | ${number(run.timing?.throughput)} | ${number(run.speedup)} | ${number(run.efficiencyPercent)} |`),
        '', '반복별 Speedup은 같은 반복의 1 Worker를 기준으로 한다. 아래 Speedup은 처리량 중앙값끼리 비교한다.', '',
        '| Worker | 유효 반복 | Job/s 중앙값 | 최소 | 최대 | 표준편차 | Speedup | Efficiency % |',
        '|---|---:|---:|---:|---:|---:|---:|---:|',
        ...groups.map((group) => `| ${group.workers} | ${group.validRuns} | ${number(group.throughput?.median)} | ${number(group.throughput?.min)} | ${number(group.throughput?.max)} | ${number(group.throughput?.populationStddev)} | ${number(group.speedup)} | ${number(group.efficiencyPercent)} |`),
        '', '경과 초 = (마지막 성공 updatedAt − 첫 processingStartedAt) / 1000. PostgreSQL timestamptz를 UTC 밀리초로 변환한다.',
        'Job/s = 이번 Batch 성공 수 / 경과 초. Speedup = N Worker Job/s / 1 Worker Job/s. Efficiency = Speedup / N × 100.',
        '표준편차는 실제 반복 집합의 모집단 표준편차이며, 적은 반복의 범위·산포를 표시할 뿐 성능 신뢰구간이 아니다.',
        '', '자원 집계는 첫 Claim 이후 시작해 마지막 성공 이전 끝난 복수 샘플만 포함한다. 원자료에는 경계 밖 샘플도 보존한다.',
        'Docker CPU 100%는 논리 CPU 하나에 해당하며 전체 Worker CPU는 같은 샘플의 합이다. 메모리는 Docker stats 사용량이다.',
        'DB Connection은 해당 DB의 client backend 수이며 관찰 연결도 포함한다. idle/active 등 상태별 값은 원자료에 남긴다.',
        '', '| 실행 | 구간 내 샘플 | 전체 Worker CPU % 중앙값/최대 | 전체 Worker MiB 중앙값/최대 | PostgreSQL CPU % 중앙값/최대 | PostgreSQL MiB 중앙값/최대 | DB 연결 중앙값/최대 |',
        '|---|---:|---:|---:|---:|---:|---:|',
        ...rows.filter((run) => run.valid).map((run) => {
            const resource = (name: string, divisor = 1) => {
                const value = run.resources?.[name];
                return value ? `${number(value.median / divisor)}/${number(value.max / divisor)}` : '측정 구간 내 샘플 없음';
            };
            return `| ${run.name} | ${run.resources?.workerCpuPercent?.count ?? 0} | ${resource('workerCpuPercent')} | ${resource('workerMemoryBytes', 1024 ** 2)} | ${resource('postgresCpuPercent')} | ${resource('postgresMemoryBytes', 1024 ** 2)} | ${resource('databaseConnections')} |`;
        }), '',
        '| 실행 | Handler 관측 수 | Handler 평균 초 | Target 준비 초 | 전체/오류/구간 내 자원 샘플 |',
        '|---|---:|---:|---:|---|',
        ...rows.filter((run) => run.valid).map((run) => `| ${run.name} | ${run.handler?.count ?? ''} | ${number(run.handler?.meanSeconds)} | ${number(run.targetWaitSeconds)} | ${run.sampling ? `${run.sampling.total}/${run.sampling.errors}/${run.sampling.insideWindow}` : ''} |`), '',
        'Prometheus는 Worker별 rate()를 구한 뒤 합산한다. Counter reset·Scrape 누락·2분 창의 구간 밖 값 때문에 DB Batch 완료 수와 처리량을 대체하지 않는다.',
        'Handler Histogram은 Handler 호출~종료만 포함한다. DB 저장 시간·전체 Job 시간은 제외한다. Processing Gauge는 DB 상태이며 실행 Handler 수와 항상 같지는 않다.',
        'Timer 기반 slow의 관찰값이다. Connection 증가만으로 PostgreSQL 병목을 단정할 수 없으며 병목 원인은 별도 증거가 없으면 원인 미확정이다.', '',
    ];
    await writeFile(join(directory, 'report.md'), report.join('\n'));
}
