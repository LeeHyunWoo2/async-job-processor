import { env } from '../config/env.ts';
import { createOrm } from '../db/orm.ts';
import { createApp } from './app.ts';

async function start() {
    const orm = await createOrm();
    const app = createApp(orm);
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
            app.log.error(error, '서버 종료 실패');
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
        app.log.error(error);
        process.exitCode = 1;
        await app.close();
    }
}

start().catch((error) => {
    console.error('API 실행 실패', error);
    process.exitCode = 1;
});
