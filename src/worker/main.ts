async function main() {
    console.log('Worker started');
}

main().catch((error) => {
    console.error('Worker failed', error);
    process.exitCode = 1;
});