import { MikroORM } from '@mikro-orm/postgresql';

import config from './mikro-orm.config.ts';

export function createOrm() {
    return MikroORM.init(config);
}