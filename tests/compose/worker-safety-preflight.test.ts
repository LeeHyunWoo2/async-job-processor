import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';

import { MikroORM } from '@mikro-orm/postgresql';

import config from '../../src/db/mikro-orm.config.ts';
import { withComposeSafety } from '../helpers/compose-safety.ts';

test('이전 실험의 Job이 있으면 Worker를 시작하지 않고 전체 행을 보존한다', { timeout: 120_000 }, async (t) => {
    const ids = [randomUUID(), randomUUID()];
    let compose: ((...args: string[]) => Promise<string>) | undefined;
    let docker: ((...args: string[]) => Promise<string>) | undefined;

    async function withDatabase<T>(run: (orm: MikroORM) => Promise<T>): Promise<T> {
        assert.ok(compose);
        const port = Number((await compose('port', 'postgres', '5432')).trim().split(':').at(-1));
        const orm = await MikroORM.init({
            ...config, host: '127.0.0.1', port, contextName: `preflight_${randomUUID()}`,
            driverOptions: { connectionTimeoutMillis: 5_000, options: '-c statement_timeout=5000' },
        });
        try { return await run(orm); }
        finally { await orm.close(); }
    }
    const snapshot = (orm: MikroORM) => orm.em.getConnection().execute<{ row: Record<string, unknown> }[]>(`
        select to_jsonb(job) as row from jobs job where id in (?, ?) order by id
    `, ids);

    try {
        // 빈 전용 환경을 한 번 준비한 뒤 Worker를 멈춰 이전 실행의 잔여 데이터를 만든다.
        let before: Awaited<ReturnType<typeof snapshot>> = [];
        await withComposeSafety(t.signal, async (env) => {
            compose = env.compose;
            docker = env.docker;
            await compose('stop', 'worker');
            before = await withDatabase(async (orm) => {
                await orm.em.getConnection().execute(`
                    insert into jobs (id, type, payload, status, attempt, max_attempts, worker_id, processing_started_at)
                    values (?, 'slow', '{"durationMs":1}'::jsonb, 'queued', 0, 3, null, null),
                           (?, 'slow', '{"durationMs":1}'::jsonb, 'processing', 1, 3, 'previous-worker', now() - interval '31 seconds')
                `, ids);
                return snapshot(orm);
            });
            assert.equal(before.length, 2);
        });
        assert.ok(compose);
        assert.ok(docker);

        // down 이후에도 Named Volume에 남아 있는 두 Job 때문에 재실행이 거부되어야 한다.
        const since = new Date().toISOString();
        await assert.rejects(withComposeSafety(t.signal, async () => {
            assert.fail('기존 Job이 있으면 시나리오를 시작하면 안 됩니다');
        }), /검증 전용 DB에 기존 Job이 있어 실행을 중단합니다/);
        const createdWorkers = await docker('events', '--since', since, '--until', new Date().toISOString(),
            '--filter', 'type=container', '--filter', 'event=create',
            '--filter', 'label=com.docker.compose.project=async-job-worker-safety',
            '--filter', 'label=com.docker.compose.service=worker', '--format', '{{json .}}');

        await compose('up', '-d', '--no-build', '--wait', 'postgres');
        const after = await withDatabase(snapshot);
        assert.deepEqual(after, before, '실행 거부 전후 status·attempt·소유권·시각·결과를 포함한 전체 행이 같아야 합니다');
        assert.equal(createdWorkers.trim(), '', '기존 Job을 검사할 때 Worker 컨테이너를 생성하면 안 됩니다');
    } finally {
        if (compose) {
            try {
                await compose('up', '-d', '--no-build', '--wait', 'postgres');
                await withDatabase(async (orm) => {
                    await orm.em.getConnection().execute('delete from jobs where id in (?, ?)', ids);
                });
            } finally {
                await compose('down');
            }
        }
    }
});
