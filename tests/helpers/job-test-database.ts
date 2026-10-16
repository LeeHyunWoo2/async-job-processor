import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';

import { MikroORM } from '@mikro-orm/postgresql';

import config from '../../src/db/mikro-orm.config.ts';
import { createOrm } from '../../src/db/orm.ts';

export const resolvedJobsSchemaSql = `
    select n.nspname as schema from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where c.oid = to_regclass('jobs')
`;

interface JobTestDatabase {
    schema: string;
    table: string;
    connections: MikroORM[];
    reset: () => Promise<void>;
}

export async function withJobTestDatabase(
    prefix: 'job' | 'retry' | 'recovery',
    connectionCount: 2 | 3,
    run: (database: JobTestDatabase) => Promise<void>,
): Promise<void> {
    const admin = await createOrm();
    const schema = `${prefix}_test_${randomUUID().replaceAll('-', '')}`;
    const table = `"${schema}".jobs`;
    const connections: MikroORM[] = [];

    try {
        // Migration은 모델 테스트에서 검증하며 여기서는 기존 테이블을 복제한다.
        await admin.em.getConnection().execute(`create schema "${schema}"`);
        await admin.em.getConnection().execute(`create table ${table} (like public.jobs including all)`);
        for (let i = 0; i < connectionCount; i++) {
            const connection = await MikroORM.init({
                ...config, schema, contextName: `${schema}_${i}`, pool: { min: 0, max: 1 },
                // public fallback을 없애고 각 연결의 실제 테이블 해석을 확인한다.
                driverOptions: {
                    options: `-c search_path=${schema} -c statement_timeout=5000`,
                    connectionTimeoutMillis: 5_000,
                },
            });
            connections.push(connection);
            const [resolved] = await connection.em.getConnection().execute<{ schema: string }[]>(resolvedJobsSchemaSql);
            assert.equal(resolved?.schema, schema, 'jobs는 테스트 schema로 해석되어야 합니다');
        }

        await run({
            schema, table, connections,
            reset: async () => {
                // public.jobs를 비우지 않도록 schema를 명시한다.
                await connections[0]!.em.getConnection().execute(`truncate ${table}`);
            },
        });
    } finally {
        try {
            const closed = await Promise.allSettled(connections.map((connection) => connection.close()));
            const failure = closed.find((result) => result.status === 'rejected');
            if (failure?.status === 'rejected') throw failure.reason;
        } finally {
            try {
                await admin.em.getConnection().execute(`drop schema if exists "${schema}" cascade`);
            } finally {
                await admin.close();
            }
        }
    }
}

export async function withJobUpdateTrigger(
    database: JobTestDatabase,
    name: string,
    body: string,
    run: (replaceBody: (body: string) => Promise<void>) => Promise<void>,
): Promise<void> {
    const execute = (sql: string) => database.connections[0]!.em.getConnection().execute(sql);
    const replaceBody = async (body: string) => {
        await execute(`
            create or replace function "${database.schema}".${name}() returns trigger language plpgsql as $$
            begin ${body} end;
            $$
        `);
    };

    try {
        await replaceBody(body);
        await execute(`
            create trigger ${name} before update on ${database.table}
            for each row execute function "${database.schema}".${name}()
        `);
        await run(replaceBody);
    } finally {
        try {
            await execute(`drop trigger if exists ${name} on ${database.table}`);
        } finally {
            await execute(`drop function if exists "${database.schema}".${name}()`);
        }
    }
}
