import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { promisify } from 'node:util';

import { MikroORM } from '@mikro-orm/postgresql';

import config from '../../src/db/mikro-orm.config.ts';

const execute = promisify(execFile);
const project = 'async-job-worker-load';
const composeArgs = ['compose', '-p', project, '-f', 'compose.yml', '-f', 'experiments/worker-load/compose.yml'];
async function docker(...args: string[]) {
    return (await execute('docker', args, { timeout: 90_000, maxBuffer: 2 * 1024 ** 2 })).stdout;
}
const compose = (...args: string[]) => docker(...composeArgs, ...args);

test('이전 Batch의 Ready·Stale Job이 있으면 Worker 생성 전에 거부하고 기존 행을 그대로 보존한다', { timeout: 180_000 }, async () => {
    assert.equal((await compose('ps', '--all', '--quiet')).trim(), '', '같은 실험 프로젝트가 이미 실행 중입니다');
    const directory = await mkdtemp(join(tmpdir(), 'worker-load-preflight-'));
    const ids = [randomUUID(), randomUUID()];
    let orm: MikroORM | undefined;
    async function connect() {
        const port = Number((await compose('port', 'postgres', '5432')).trim().split(':').at(-1));
        return MikroORM.init({ ...config, host: '127.0.0.1', port, contextName: `load-preflight-${randomUUID()}` });
    }
    const snapshot = () => orm!.em.getConnection().execute('select to_jsonb(job) as row from jobs job where id in (?, ?) order by id', ids);
    try {
        await compose('up', '-d', '--no-build', 'postgres', 'migrate');
        assert.equal((await docker('wait', (await compose('ps', '--all', '--quiet', 'migrate')).trim())).trim(), '0');
        orm = await connect();
        await orm.em.getConnection().execute(`
            insert into jobs (id, type, payload, status, attempt, max_attempts, worker_id, processing_started_at)
            values (?, 'slow', '{"durationMs":1}'::jsonb, 'queued', 0, 3, null, null),
                   (?, 'slow', '{"durationMs":1}'::jsonb, 'processing', 1, 3, 'previous-worker', now() - interval '31 seconds')
        `, ids);
        const before = await snapshot();
        await orm.close();
        orm = undefined;
        await compose('down');
        const since = new Date().toISOString();
        await assert.rejects(execute(process.execPath, ['--env-file=.env', 'experiments/worker-load/run.ts',
            '--workers=1', '--repeats=1', '--count=1', '--duration-ms=1', `--out=${directory}`], { timeout: 90_000 }),
        (error: unknown) => error instanceof Error && 'stderr' in error && String(error.stderr).includes('실험 전용 DB에 미완료 Job이 있습니다'));
        const failure = JSON.parse(await readFile(join(directory, 'failure.json'), 'utf8'));
        assert.deepEqual(failure.results, [], '사전 검사 실패 시 Batch를 등록하면 안 됩니다');
        const created = await docker('events', '--since', since, '--until', new Date().toISOString(), '--filter', 'event=create',
            '--filter', `label=com.docker.compose.project=${project}`, '--filter', 'label=com.docker.compose.service=worker', '--format', '{{json .}}');
        assert.equal(created.trim(), '', '기존 Job 확인 중 Worker를 생성하면 안 됩니다');
        await compose('up', '-d', '--no-build', '--wait', 'postgres');
        orm = await connect();
        assert.deepEqual(await snapshot(), before, '실행 거부 전후 전체 행과 시각이 같아야 합니다');
    } finally {
        try {
            if (!orm) {
                await compose('up', '-d', '--no-build', '--wait', 'postgres');
                orm = await connect();
            }
            await orm.em.getConnection().execute('delete from jobs where id in (?, ?)', ids);
        } finally {
            if (orm) await orm.close();
            await compose('down');
            await rm(directory, { recursive: true });
        }
    }
});
