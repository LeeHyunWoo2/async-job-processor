import pino, { type DestinationStream } from 'pino';

export function createLogger(component: 'api' | 'worker', destination?: DestinationStream) {
    return pino({
        level: process.env.LOG_LEVEL ?? 'info',
    }, destination).child({ component });
}
