async function main() {
    console.log('Worker 시작');
}

main().catch((error) => {
    console.error('Worker 실행 실패', error);
    process.exitCode = 1;
});
