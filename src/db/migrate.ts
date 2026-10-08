import { createOrm } from './orm.ts';

async function main() {
    const action = process.argv[2];

    if (action !== 'create' && action !== 'up') {
        throw new Error('사용법: migrate.ts <create|up>');
    }

    const orm = await createOrm();

    try {
        if (action === 'create') {
            const migration = await orm.migrator.create();
            console.log(migration.fileName || 'schema 변경 사항이 없습니다');
        } else {
            await orm.migrator.up();
        }
    } finally {
        await orm.close();
    }
}

main().catch((error) => {
    console.error('migration 실행 실패', error);
    process.exitCode = 1;
});
