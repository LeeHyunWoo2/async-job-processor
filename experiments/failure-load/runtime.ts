import { execFile, spawn } from 'node:child_process';
import { appendFile } from 'node:fs/promises';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';
import { createInterface } from 'node:readline';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import type { Event } from './experiment.ts';

export const project = 'async-job-failure-load';
export const dbName = 'async_job_failure_load';
export const composeArgs = ['compose', '-p', project, '-f', 'compose.yml', '-f', 'experiments/failure-load/compose.yml'];
export const execute = promisify(execFile);
export async function docker(args: string[], limit = 30) {
    return (await execute('docker', args, { timeout: 60_000, maxBuffer: 32 * 1024 ** 2,
        env: { ...process.env, FAILURE_LOAD_LIMIT: String(limit) } })).stdout;
}
export const compose = (args: string[], limit = 30) => docker([...composeArgs, ...args], limit);
export async function port(service: string, internal: string) {
    return Number((await compose(['port', service, internal])).trim().split(':').at(-1));
}
export interface Container {
    Id: string; Image: string; RestartCount: number; Path: string; Args: string[];
    Config: { Env: string[]; Cmd: string[]; Labels: Record<string, string> };
    State: { Running: boolean; StartedAt: string; ExitCode: number; FinishedAt: string; OOMKilled: boolean };
    HostConfig: Record<string, unknown>;
    NetworkSettings: { Networks: Record<string, { IPAddress: string }> };
}
export const service = (c: Container) => c.Config.Labels['com.docker.compose.service'];
export async function inspect(): Promise<Container[]> {
    const ids = (await compose(['ps', '--all', '--quiet'])).trim().split('\n').filter(Boolean);
    return ids.length ? JSON.parse(await docker(['inspect', ...ids])) : [];
}
// 인증정보가 들어 있는 전체 Env/Compose config는 저장하지 않는다.
export function metadata(containers: Container[]) {
    return containers.map((c) => ({ id: c.Id, service: service(c), imageId: c.Image, command: [c.Path, ...c.Args],
        startedAt: c.State.StartedAt, finishedAt: c.State.FinishedAt, running: c.State.Running,
        exitCode: c.State.ExitCode, oomKilled: c.State.OOMKilled, restartCount: c.RestartCount,
        settings: c.Config.Env.filter((v) => /^(DB_NAME|DB_HOST|DB_PORT|LOG_LEVEL|ENABLE_BACKPRESSURE|MAX_OUTSTANDING_JOBS|API_HOST|API_PORT|WORKER_METRICS_HOST|WORKER_METRICS_PORT)=/.test(v)),
        limits: Object.fromEntries(['NanoCpus', 'CpuQuota', 'CpuPeriod', 'CpusetCpus', 'CpuShares', 'Memory', 'MemorySwap', 'RestartPolicy'].map((key) => [key, c.HostConfig[key]])),
        networks: c.NetworkSettings.Networks }));
}

// 각 로그의 최초 수신을 실행기의 단일 Monotonic Clock으로 기록한다.
// 프로세스 내부 기록/Flush부터 전송까지의 지연 상한은 알 수 없다. time은 진단용 원자료다.
export function followWorker(c: Container, path: string, elapsed: () => number,
    events: Event[], onError: (error: unknown) => void) {
    const child = spawn('docker', ['logs', '--follow', '--since', c.State.StartedAt, c.Id], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stopping = false;
    let writes = Promise.resolve();
    const lines = [createInterface({ input: child.stdout }), createInterface({ input: child.stderr })];
    for (const input of lines) input.on('line', (line) => {
        let row: Event;
        try { row = JSON.parse(line); }
        catch { onError(new Error(`${c.Id}: JSON 로그 해석 실패: ${line}`)); return; }
        row = { ...row, containerId: c.Id, observedMs: elapsed(), receivedWallMs: Date.now() };
        events.push(row);
        writes = writes.then(() => appendFile(join(path, 'events.jsonl'), JSON.stringify(row) + '\n')).catch(onError);
    });
    child.on('error', onError);
    const closed = new Promise<void>((resolve) => child.on('close', (code, signal) => {
        // SIGKILL 대상의 docker logs가 EOF로 끝나는 것은 정상이다.
        if (!stopping && code !== 0) onError(new Error(`Worker 로그 수집 종료: ${c.Id}, code=${code}, signal=${signal}`));
        resolve();
    }));
    return { async stop() {
        stopping = true;
        child.kill('SIGTERM');
        await closed;
        for (const input of lines) input.close();
        await writes;
    } };
}

export function resources(containers: Container[], path: string, elapsed: () => number, onError: (error: unknown) => void) {
    let stopped = false;
    const perContainer: Record<string, number> = {};
    const task = (async () => {
        do {
            const requestedMs = elapsed();
            try {
                // 사망 컨테이너는 이후 표본에서 빠져도 된다. 자료로 남기고 Counter 정산에 쓰지 않는다.
                const output = await docker(['stats', '--no-stream', '--no-trunc', '--format', '{{json .}}', ...containers.map((c) => c.Id)]);
                for (const line of output.trim().split('\n').filter(Boolean)) {
                    const row = { ...JSON.parse(line), requestedMs, observedMs: elapsed(), wallMs: Date.now() };
                    perContainer[row.ID] = (perContainer[row.ID] ?? 0) + 1;
                    await appendFile(join(path, 'resources.jsonl'), JSON.stringify(row) + '\n');
                }
            } catch (error) { onError(error); break; }
            if (!stopped) await delay(500);
        } while (!stopped);
    })();
    return { async stop() { stopped = true; await task; return { perContainer }; } };
}

export const boundary = () => ({ monoMs: performance.now(), wallMs: Date.now() });
