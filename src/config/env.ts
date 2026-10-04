function requiredEnv(name: string): string {
    const value = process.env[name];

    if (!value) {
        throw new Error(`Missing required environment variable: ${name}`);
    }

    return value;
}

export const env = {
    api: {
        host: process.env.API_HOST ?? '127.0.0.1',
        port: Number(process.env.API_PORT ?? 3000),
    },

    db: {
        host: requiredEnv('DB_HOST'),
        port: Number(process.env.DB_PORT ?? 5432),
        name: requiredEnv('DB_NAME'),
        user: requiredEnv('DB_USER'),
        password: requiredEnv('DB_PASSWORD'),
    },
};