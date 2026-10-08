import { createOrm } from './orm.ts';

async function main() {
    const orm = await createOrm();

    try {
        await orm.connect();

        console.log('데이터베이스 연결 확인 완료');
    } finally {
        await orm.close();
    }
}

main().catch((error) => {
    console.error('데이터베이스 연결 실패');
    console.error(error);

    process.exitCode = 1;
});
