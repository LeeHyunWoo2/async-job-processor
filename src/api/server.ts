import { env } from '../config/env.ts';
import { createApp } from './app.ts';

const app = createApp();

async function start() {
    try {
        await app.listen({
            host: env.api.host,
            port: env.api.port,
        });
    } catch (error) {
        app.log.error(error);
        process.exitCode = 1;
    }
}

async function shutdown(signal: NodeJS.Signals) {
    app.log.info({ signal }, 'shutting down');
    await app.close();
}

process.once('SIGINT', () => {
    void shutdown('SIGINT');
});

process.once('SIGTERM', () => {
    shutdown('SIGTERM');
});

await start();