import { Migrator } from '@mikro-orm/migrations';
import { defineConfig } from '@mikro-orm/postgresql';

import { env } from '../config/env.ts';
import { Job } from './entities/job.ts';

export default defineConfig({
    host: env.db.host,
    port: env.db.port,
    dbName: env.db.name,
    user: env.db.user,
    password: env.db.password,

    entities: [Job],
    extensions: [Migrator],
    migrations: {
        path: './src/db/migrations',
    },
});
