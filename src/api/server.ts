import { createLogger } from '../observability/logger.ts';
import { createApp } from './app.ts';

const logger = createLogger('api');

async function start() {
    const { env } = await import('../config/env.ts');
    const { createOrm } = await import('../db/orm.ts');
    const orm = await createOrm();
    const app = createApp(orm, logger, undefined, env.api.enqueue);
    let closing = false;

    async function shutdown(signal: NodeJS.Signals) {
        if (closing) {
            return;
        }
        closing = true;
        app.log.info({ signal }, '서버 종료 중');
        try {
            await app.close();
        } catch (error) {
            app.log.error({ err: error }, '서버 종료 실패');
            process.exitCode = 1;
        }
    }

    process.on('SIGINT', () => { void shutdown('SIGINT'); });
    process.on('SIGTERM', () => { void shutdown('SIGTERM'); });

    try {
        await app.listen({
            host: env.api.host,
            port: env.api.port,
        });
    } catch (error) {
        app.log.error({ err: error }, 'API Listen 실패');
        process.exitCode = 1;
        await app.close();
    }
}

start().catch((error) => {
    logger.error({ err: error }, 'API 실행 실패');
    process.exitCode = 1;
});
