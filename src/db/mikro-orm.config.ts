import { defineConfig } from '@mikro-orm/postgresql';

import { env } from '../config/env.ts';

export default defineConfig({
    host: env.db.host,
    port: env.db.port,
    dbName: env.db.name,
    user: env.db.user,
    password: env.db.password,

    discovery: {
        // TODO: Job Entity 추가 후 제거 — 현재는 Entity 없이 DB 연결만 확인하기 위해 비활성화
        warnWhenNoEntities: false,
    },
});