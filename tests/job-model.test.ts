import assert from 'node:assert/strict';
import test from 'node:test';

import { Job, JobStatus } from '../src/db/entities/job.ts';
import { createOrm } from '../src/db/orm.ts';

test('PostgreSQL Job 모델', async (t) => {
    const orm = await createOrm();

    try {
        await t.test('migration이 적용되어 있고 Entity와 schema가 일치한다', async () => {
            assert.equal((await orm.migrator.getPending()).length, 0);
            assert.equal((await orm.migrator.getExecuted()).length, 1);
            assert.equal(await orm.schema.getUpdateSchemaSQL({ wrap: false }), '');

            const columns = await orm.em.getConnection().execute<{
                column_name: string;
                data_type: string;
                is_nullable: string;
            }[]>(
                "select column_name, data_type, is_nullable from information_schema.columns where table_schema = 'public' and table_name = 'jobs'",
            );

            assert.equal(columns.length, 13);

            for (const name of ['available_at', 'created_at', 'updated_at']) {
                const column = columns.find((column) => column.column_name === name);
                assert.equal(column?.data_type, 'timestamp with time zone');
                assert.equal(column?.is_nullable, 'NO');
            }

            assert.equal(columns.find((column) => column.column_name === 'type')?.data_type, 'text');
        });

        await t.test('Job 생성 시 기본값이 올바르게 저장된다', async () => {
            const rollback = new Error('검증 데이터 rollback');

            await assert.rejects(orm.em.fork().transactional(async (em) => {
                const before = Date.now();
                const job = em.create(Job, { type: 'slow', payload: {}, maxAttempts: 1 });
                em.persist(job);
                await em.flush();
                em.clear();

                const saved = await em.findOneOrFail(Job, job.id);
                assert.match(saved.id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
                assert.equal(saved.type, 'slow');
                assert.equal(saved.status, JobStatus.queued);
                assert.equal(saved.attempt, 0);
                assert.equal(saved.maxAttempts, 1);
                assert.ok(saved.availableAt.getTime() >= before);
                assert.ok(saved.createdAt instanceof Date);
                assert.ok(saved.updatedAt instanceof Date);

                const [raw] = await em.execute<{
                    id: string;
                    status: string;
                    attempt: number;
                    available_at: string;
                }[]>(
                    'insert into jobs (type, payload, max_attempts) values (?, ?::jsonb, ?) returning id, status, attempt, available_at',
                    ['slow', '{}', 1],
                );

                assert.ok(raw);
                assert.equal(raw.id[14], '4');
                assert.equal(raw.status, JobStatus.queued);
                assert.equal(raw.attempt, 0);
                assert.ok(Number.isFinite(new Date(raw.available_at).getTime()));

                throw rollback;
            }), (error) => error === rollback);
        });

        await t.test('JSONB와 nullable 필드가 올바르게 저장된다', async () => {
            const rollback = new Error('검증 데이터 rollback');

            await assert.rejects(orm.em.fork().transactional(async (em) => {
                const job = em.create(Job, {
                    type: 'slow',
                    payload: { input: [1, true, null] },
                    maxAttempts: 1,
                });
                em.persist(job);
                await em.flush();
                em.clear();

                const saved = await em.findOneOrFail(Job, job.id);
                assert.deepEqual(saved.payload, { input: [1, true, null] });

                for (const value of [saved.processingStartedAt, saved.workerId, saved.result, saved.lastError]) {
                    assert.equal(value, null);
                }

                saved.result = { output: [2, false] };
                saved.lastError = { message: 'verification' };
                await em.flush();
                em.clear();

                const updated = await em.findOneOrFail(Job, saved.id);
                assert.deepEqual(updated.result, { output: [2, false] });
                assert.deepEqual(updated.lastError, { message: 'verification' });

                throw rollback;
            }), (error) => error === rollback);
        });

        await t.test('Job 수정 시 updatedAt이 갱신된다', async () => {
            const rollback = new Error('검증 데이터 rollback');

            await assert.rejects(orm.em.fork().transactional(async (em) => {
                const job = em.create(Job, { type: 'slow', payload: {}, maxAttempts: 1 });
                em.persist(job);
                await em.flush();

                job.updatedAt = new Date(0);
                await em.flush();

                job.result = { output: [2, false] };
                await em.flush();
                em.clear();

                const updated = await em.findOneOrFail(Job, job.id);
                assert.ok(updated.updatedAt.getTime() > 0);

                throw rollback;
            }), (error) => error === rollback);
        });

        await t.test('DB는 새로운 Job type 문자열을 허용한다', async () => {
            const rollback = new Error('검증 데이터 rollback');

            await assert.rejects(orm.em.fork().transactional(async (em) => {
                const [saved] = await em.execute<{ type: string }[]>(
                    'insert into jobs (type, payload, max_attempts) values (?, ?::jsonb, ?) returning type',
                    ['future-job-type', '{}', 1],
                );

                assert.equal(saved?.type, 'future-job-type');

                throw rollback;
            }), (error) => error === rollback);
        });

        await t.test('DB가 잘못된 maxAttempts와 status를 거부한다', async () => {
            for (const maxAttempts of [0, -1]) {
                await assert.rejects(orm.em.fork().transactional(async (em) => {
                    em.persist(em.create(Job, { type: 'slow', payload: {}, maxAttempts }));
                    await em.flush();
                }), /jobs_max_attempts_check/);
            }

            await assert.rejects(orm.em.fork().transactional(async (em) => {
                await em.execute(
                    'insert into jobs (type, payload, status, max_attempts) values (?, ?::jsonb, ?, ?)',
                    ['slow', '{}', 'retrying', 1],
                );
            }), /invalid input value for enum job_status/);
        });
    } finally {
        await orm.close();
    }
});