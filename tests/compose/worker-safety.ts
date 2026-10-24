import assert from 'node:assert/strict';

import { withComposeSafety, type JobRow, type Worker } from '../helpers/compose-safety.ts';

const controller = new AbortController();
const onSignal = () => controller.abort(new Error('검증 중단 요청'));
process.once('SIGINT', onSignal);
process.once('SIGTERM', onSignal);

try {
    await withComposeSafety(AbortSignal.any([controller.signal, AbortSignal.timeout(10 * 60_000)]), async (env) => {
        async function normalClaims(rows: JobRow[], active: Worker[]) {
            assert.ok(rows.every((row) => row.status === 'succeeded' && row.attempt === 1));
            assert.deepEqual(new Set(rows.map((row) => row.workerId)), new Set(active.map((worker) => worker.workerId)),
                '모든 Worker가 실제 완료에 참여해야 합니다');
            const events = await env.until('정상 Claim·완료 로그 수집', async () => {
                const found = active.flatMap((worker) => env.logs(worker));
                return rows.every((row) => found.some((log) => log.event === 'job.succeeded' && log.jobId === row.id)) ? found : undefined;
            });
            for (const row of rows) {
                const claims = events.filter((log) => log.event === 'job.claimed' && log.jobId === row.id);
                const completed = events.filter((log) => log.event === 'job.succeeded' && log.jobId === row.id);
                assert.equal(claims.length, 1, `${row.id}: 정상 실행에서 중복 Claim이 없어야 합니다`);
                assert.equal(completed.length, 1);
                assert.equal(claims[0]!.workerId, row.workerId);
                assert.equal(claims[0]!.attempt, 1);
                assert.equal(completed[0]!.workerId, row.workerId);
                assert.equal(completed[0]!.attempt, 1);
            }
            return events.filter((log) => rows.some((row) => row.id === log.jobId));
        }

        console.log('1/3 정상 분산 처리');
        const three = await env.scale(3);
        await env.targets(three, '정상 실행 전 3개 Target 수집');
        const normal = await env.succeeded(await env.enqueue(12, 1_000));
        const normalEvents = await normalClaims(normal, three);
        env.record('정상 분산 처리 통과', { rows: normal, events: normalEvents });
        await env.targets(three, '정상 실행 후 Worker별 성공 Counter 수집');

        console.log('2/3 단일 Worker SIGKILL 후 처리 지속과 Recovery');
        const crashBatch = await env.enqueue(3, 10_000);
        const inFlight = await env.until('3개 Worker의 processing 진입', async () => {
            const found = await env.rows(crashBatch);
            return found.every((row) => row.status === 'processing')
                && new Set(found.map((row) => row.workerId)).size === 3 ? found : undefined;
        });
        const interrupted = inFlight[0]!;
        const victim = three.find((worker) => worker.workerId === interrupted.workerId)!;
        await env.until('종료 대상 Claim 로그', async () => env.logs(victim).some((log) =>
            log.event === 'job.claimed' && log.jobId === interrupted.id && log.attempt === 1) ? true : undefined);
        env.record('강제 종료 전 소유권', { victim, interrupted });
        await env.docker('kill', '--signal', 'SIGKILL', victim.containerId);
        const [killed] = JSON.parse(await env.docker('inspect', victim.containerId));
        assert.equal(killed.State.Running, false);
        assert.equal(killed.State.ExitCode, 137);
        assert.deepEqual((await env.rows([interrupted.id]))[0], interrupted, '강제 종료 직후 DB 상태는 유지되어야 합니다');
        const survivors = three.filter((worker) => worker !== victim);
        assert.deepEqual(new Set((await env.activeWorkers(2)).map((worker) => worker.workerId)), new Set(survivors.map((worker) => worker.workerId)));
        // Crash 이후 새로 등록한 Job의 완료를 Recovery보다 먼저 확인한다.
        const continued = await env.succeeded(await env.enqueue(4, 500), 22_000);
        assert.deepEqual(new Set(continued.map((row) => row.workerId)), new Set(survivors.map((worker) => worker.workerId)));
        assert.ok(continued.every((row) => row.attempt === 1));
        assert.deepEqual((await env.rows([interrupted.id]))[0], interrupted);
        env.record('Recovery 전 생존 Worker의 새 Claim·완료 통과', { continued, interrupted: (await env.rows([interrupted.id]))[0] });
        const reclaimed = await env.until('실제 Fixed Timeout 이후 다른 Worker의 두 번째 Claim', async () => {
            const [row] = await env.rows([interrupted.id]);
            return row?.status === 'processing' && row.attempt === 2 ? row : undefined;
        }, 80_000);
        assert.notEqual(reclaimed.workerId, interrupted.workerId);
        assert.ok(survivors.some((worker) => worker.workerId === reclaimed.workerId));
        const recovered = await env.succeeded([interrupted.id]);
        await env.succeeded(crashBatch);
        const crashEvents = await env.until('Recovery·재Claim·완료 로그 수집', async () => {
            const found = three.flatMap((worker) => env.logs(worker)).filter((log) => log.jobId === interrupted.id);
            return found.some((log) => log.event === 'job.succeeded' && log.attempt === 2) ? found : undefined;
        });
        assert.deepEqual(crashEvents.filter((log) => log.event === 'job.claimed').map((log) => log.attempt).sort(), [1, 2]);
        const recovery = crashEvents.filter((log) => log.event === 'job.recovered');
        assert.equal(recovery.length, 1);
        assert.equal(recovery[0]!.status, 'queued');
        assert.equal(recovery[0]!.attempt, 1);
        assert.equal(recovery[0]!.previousWorkerId, victim.workerId);
        assert.ok(survivors.some((worker) => worker.workerId === recovery[0]!.workerId));
        assert.equal(recovered[0]!.workerId, reclaimed.workerId);
        assert.equal(recovered[0]!.attempt, 2);
        env.record('Crash Recovery 통과', { interrupted, reclaimed, recovered, events: crashEvents });
        await env.targets(survivors, '종료된 Target 제거와 생존 2개 Target 수집');

        console.log('3/3 Scale-up·Scale-down과 SIGTERM');
        const one = await env.scale(1);
        const single = await env.succeeded(await env.enqueue(2, 300));
        await normalClaims(single, one);
        await env.targets(one, '축소된 1개 Target과 처리 지속');
        const expanded = await env.scale(3);
        const expandedRows = await env.succeeded(await env.enqueue(12, 1_000));
        const expandedEvents = await normalClaims(expandedRows, expanded);
        const added = expanded.filter((worker) => !one.some((before) => before.workerId === worker.workerId));
        assert.equal(added.length, 2);
        env.record('1→3 Scale-up 통과', { added, rows: expandedRows, events: expandedEvents });
        await env.targets(expanded, '추가 Worker를 포함한 3개 Target 수집');

        const finishing = await env.enqueue(3, 10_000);
        const beforeStop = await env.until('축소 전 모든 Worker의 processing 진입', async () => {
            const found = await env.rows(finishing);
            return found.every((row) => row.status === 'processing')
                && new Set(found.map((row) => row.workerId)).size === 3 ? found : undefined;
        });
        const queued = await env.enqueue(6, 500);
        const since = new Date().toISOString();
        const remaining = await env.scale(1);
        const removed = expanded.filter((worker) => worker.workerId !== remaining[0]!.workerId);
        assert.equal(removed.length, 2);
        const finished = await env.succeeded(finishing);
        assert.ok(finished.every((row) => row.attempt === 1
            && row.workerId === beforeStop.find((before) => before.id === row.id)!.workerId));
        const afterStop = await env.succeeded(queued);
        assert.ok(afterStop.every((row) => row.attempt === 1 && row.workerId === remaining[0]!.workerId),
            '축소 후 대기 Job은 남은 Worker가 처리해야 합니다');
        const deaths = (await env.docker('events', '--since', since, '--until', new Date().toISOString(),
            '--filter', 'type=container', '--filter', 'event=die', '--filter', 'label=com.docker.compose.project=async-job-worker-safety',
            '--format', '{{json .}}')).trim().split('\n').filter(Boolean).map((line) => JSON.parse(line));
        const shutdown = removed.map((worker) => {
            const logs = env.logs(worker);
            const stopping = logs.findIndex((log) => log.event === 'worker.stopping' && 'signal' in log && log.signal === 'SIGTERM');
            assert.ok(stopping >= 0, 'SIGTERM 종료 요청을 기록해야 합니다');
            assert.equal(logs.slice(stopping + 1).some((log) => log.event === 'job.claimed'), false, '종료 요청 이후 새 Claim이 없어야 합니다');
            const held = beforeStop.find((row) => row.workerId === worker.workerId)!;
            assert.ok(logs.slice(stopping + 1).some((log) => log.event === 'job.succeeded' && log.jobId === held.id && log.attempt === 1),
                '종료 요청 이후 기존 Job의 결과를 저장해야 합니다');
            const death = deaths.find((event) => event.Actor.ID === worker.containerId);
            assert.equal(death?.Actor.Attributes.exitCode, '0', '30초 Grace Period 안에 정상 종료해야 합니다');
            return { worker, held, logs: logs.slice(stopping), death };
        });
        env.record('3→1 Scale-down·Graceful Shutdown 통과', { shutdown, finished, afterStop });
        await env.targets(remaining, '축소 후 1개 Target 수집과 성공 Counter 갱신');
        env.record('전체 시나리오 통과', { scenarios: 3 });
    });
} finally {
    process.off('SIGINT', onSignal);
    process.off('SIGTERM', onSignal);
}
