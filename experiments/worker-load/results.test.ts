import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import { batchTiming, spread, writeSummary, type JobRow, type RunResult } from './results.ts';

// 산식 검증용 입력이며 실제 측정 결과 파일에는 기록하지 않는다.
const rows: JobRow[] = ['one', 'two'].map((id, index) => ({
    id, type: 'slow', payload: { durationMs: 1000 }, status: 'succeeded', attempt: 1,
    workerId: 'worker', processingStartedAt: `2026-01-01T00:00:0${index}.000Z`,
    updatedAt: `2026-01-01T00:00:0${index + 1}.000Z`, ready: false, lastError: null, result: { durationMs: 1000 },
}));

test('이번 ID의 첫 Claim부터 마지막 성공까지 계산하며 Retry·누락·다른 Batch는 거부한다', () => {
    const timing = batchTiming(rows, ['one', 'two'], 1000);
    assert.equal(timing.elapsedSeconds, 2);
    assert.equal(timing.throughput, 1);
    assert.throws(() => batchTiming([{ ...rows[0]!, attempt: 2 }, rows[1]!], ['one', 'two'], 1000));
    assert.throws(() => batchTiming(rows.slice(0, 1), ['one', 'two'], 1000));
    assert.throws(() => batchTiming(rows, ['one', 'other'], 1000));
    assert.throws(() => batchTiming(rows, ['one', 'one'], 1000));
});

test('반복 중앙값과 실제 반복 집합의 표준편차를 계산한다', () => {
    assert.deepEqual(spread([4, 2]), { count: 2, median: 3, min: 2, max: 4, populationStddev: 1 });
    assert.equal(spread([3, 1, 2]).median, 2);
});

test('실패한 반복은 집계에서 제외하고 같은 반복의 1 Worker만 Speedup 기준으로 쓴다', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'worker-load-summary-'));
    try {
        await writeFile(join(directory, 'environment.json'), '{}');
        const make = (workers: number, repeat: number, throughput: number): RunResult => ({
            name: `r${repeat}-w${workers}`, workers, repeat, count: 2, durationMs: 1000, valid: true,
            timing: { firstClaim: rows[0]!.processingStartedAt!, lastSuccess: rows[1]!.updatedAt, elapsedSeconds: 2 / throughput, throughput },
        });
        await writeSummary(directory, [make(1, 1, 1), make(1, 2, 2), make(2, 1, 2),
            { ...make(2, 2, 100), valid: false }, make(4, 3, 4)]);
        const summary = JSON.parse(await readFile(join(directory, 'summary.json'), 'utf8'));
        assert.equal(summary.runs[2].speedup, 2);
        assert.equal(summary.runs[2].efficiencyPercent, 100);
        assert.equal(summary.runs[3].speedup, undefined);
        assert.equal(summary.runs[4].speedup, undefined);
        assert.equal(summary.groups[1].validRuns, 1);
        assert.equal(summary.groups[1].throughput.median, 2);
        assert.equal(summary.groups[1].speedup, 2 / 1.5);
    } finally {
        await rm(directory, { recursive: true });
    }
});
