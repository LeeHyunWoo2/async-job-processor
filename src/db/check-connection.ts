import { createOrm } from './orm.ts';

async function main() {
    const orm = await createOrm();

    try {
        await orm.connect();

        console.log('Database connection OK');
    } finally {
        await orm.close();
    }
}

main().catch((error) => {
    console.error('Database connection failed');
    console.error(error);

    process.exitCode = 1;
});