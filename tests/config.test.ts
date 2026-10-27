import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

const configUrl = new URL('../src/config/env.ts', import.meta.url).href;

function readConfig(values: NodeJS.ProcessEnv = {}) {
    const environment: NodeJS.ProcessEnv = { ...process.env, DB_HOST: '127.0.0.1', DB_NAME: 'config_test', DB_USER: 'test', DB_PASSWORD: 'test' };
    delete environment.ENABLE_BACKPRESSURE;
    delete environment.MAX_OUTSTANDING_JOBS;
    return spawnSync(process.execPath, ['--input-type=module', '--eval', `
        const { env } = await import(${JSON.stringify(configUrl)});
        console.log(JSON.stringify(env.api.enqueue));
    `], { env: { ...environment, ...values }, encoding: 'utf8' });
}

test('Admission 설정은 기본 ON·100을 사용하고 명시한 ON/OFF와 양의 정수를 검증한다', () => {
    const defaults = readConfig();
    assert.equal(defaults.status, 0, defaults.stderr);
    assert.deepEqual(JSON.parse(defaults.stdout), { enableBackpressure: true, maxOutstandingJobs: 100 });
    for (const value of ['true', 'false']) {
        const configured = readConfig({ ENABLE_BACKPRESSURE: value, MAX_OUTSTANDING_JOBS: '1' });
        assert.equal(configured.status, 0, configured.stderr);
        assert.deepEqual(JSON.parse(configured.stdout), { enableBackpressure: value === 'true', maxOutstandingJobs: 1 });
    }
    for (const value of ['', 'TRUE', '1']) {
        const invalid = readConfig({ ENABLE_BACKPRESSURE: value });
        assert.equal(invalid.status, 1);
        assert.match(invalid.stderr, /환경 변수는 true 또는 false여야 합니다: ENABLE_BACKPRESSURE/);
    }
    for (const value of ['', '0', '-1', '1.5', 'abc', 'Infinity', '9007199254740992', ' 2 ', '1e2']) {
        // OFF여도 잘못된 제한값을 임의 보정하지 않는다.
        const invalid = readConfig({ ENABLE_BACKPRESSURE: 'false', MAX_OUTSTANDING_JOBS: value });
        assert.equal(invalid.status, 1, value);
        assert.match(invalid.stderr, /환경 변수는 양의 정수여야 합니다: MAX_OUTSTANDING_JOBS/);
    }
});

test('createApp 직접 사용은 DB 환경변수나 환경변수 설정 파싱을 요구하지 않는다', () => {
    const environment: NodeJS.ProcessEnv = { ...process.env, ENABLE_BACKPRESSURE: 'invalid', MAX_OUTSTANDING_JOBS: 'invalid' };
    for (const name of ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']) delete environment[name];
    const result = spawnSync(process.execPath, ['--input-type=module', '--eval', `
        const { MikroORM } = await import('@mikro-orm/postgresql');
        const { Job } = await import(${JSON.stringify(new URL('../src/db/entities/job.ts', import.meta.url).href)});
        const { createApp } = await import(${JSON.stringify(new URL('../src/api/app.ts', import.meta.url).href)});
        const orm = await MikroORM.init({ dbName: 'config_test', entities: [Job], connect: false });
        const app = createApp(orm, false);
        try {
            const response = await app.inject('/health');
            if (response.statusCode !== 200) throw new Error('Health 응답 실패');
        } finally { await app.close(); }
    `], { env: environment, encoding: 'utf8' });
    assert.equal(result.status, 0, result.stderr);
});
