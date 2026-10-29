import { monitorEventLoopDelay } from 'node:perf_hooks';

import { Gauge, type Registry } from '@prometheus-io/client';

export function createEventLoopMetrics(registry: Registry) {
    const histogram = monitorEventLoopDelay({ resolution: 10 });
    const summaries = [
        ['max_seconds', 'Event Loop 지연 최댓값(초)', () => histogram.max / 1e9],
        ['p99_seconds', 'Event Loop 지연 p99(초)', () => histogram.percentile(99) / 1e9],
        ['mean_seconds', 'Event Loop 지연 평균(초)', () => histogram.mean / 1e9],
        ['samples', 'Event Loop 지연 표본 수', () => histogram.count],
    ] as const;

    for (const [suffix, help, read] of summaries) {
        new Gauge({
            name: `nodejs_event_loop_delay_${suffix}`, help, registers: [registry],
            collect() { this.set(histogram.count === 0 ? 0 : read()); },
        });
    }
    histogram.enable();
    // 호출자가 프로세스당 한 객체를 소유하며 reset()으로 구간을 나누고 disable()로 종료한다.
    return histogram;
}
