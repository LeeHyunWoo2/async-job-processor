import { randomUUID } from 'node:crypto';

import { createOrm } from '../db/orm.ts';
import { executeHandler } from '../jobs/handlers.ts';
import { runWorker } from './run.ts';

async function main() {
    const workerId = randomUUID();
    const controller = new AbortController();
    const shutdown = (signal: NodeJS.Signals) => {
        if (!controller.signal.aborted) {
            console.log('Worker 종료 요청: 현재 Job 완료 대기', { signal, workerId });
            controller.abort();
        }
    };
    const onSigint = () => shutdown('SIGINT');
    const onSigterm = () => shutdown('SIGTERM');
    process.on('SIGINT', onSigint);
    process.on('SIGTERM', onSigterm);

    try {
        const orm = await createOrm();
        try {
            console.log('Worker 시작', { workerId });
            await runWorker(orm, workerId, executeHandler, controller.signal);
        } finally {
            await orm.close();
        }
    } finally {
        process.off('SIGINT', onSigint);
        process.off('SIGTERM', onSigterm);
    }
}

main().catch((error) => {
    console.error('Worker 실행 실패: DB 또는 결과 저장 오류로 중단합니다', error);
    process.exitCode = 1;
});
