import assert from 'node:assert/strict';

import { createOrm } from '../../src/db/orm.ts';
import { claimJob } from '../../src/jobs/store.ts';
import { runWorker } from '../../src/worker/run.ts';

// 테스트 전용 프로세스에서 SIGKILL할 경계를 고정한다.
const orm = await createOrm();
const workerId = `crash-worker-${process.pid}`;
const blocked = () => new Promise<never>(() => {
    // 미완료 Promise만으로는 Node.js가 종료되므로 프로세스를 유지한다.
    setInterval(() => {}, 1_000);
});

try {
    if (process.env.TEST_CRASH_PHASE === 'after-claim') {
        const job = await claimJob(orm, workerId);
        assert.ok(job, 'Crash 검증용 Job이 없습니다');
        console.log('Claim 완료: Handler 진입 전', job.id);
        await blocked();
    } else {
        assert.equal(process.env.TEST_CRASH_PHASE, 'in-handler');
        await runWorker(orm, workerId, async (job) => {
            console.log('Handler 진입 확인', job.id);
            return blocked();
        }, new AbortController().signal);
    }
} finally {
    await orm.close();
}
