import { Registry } from '@prometheus-io/client';

export function createMetricsRegistry() {
    // 전역 Registry를 사용하지 않아 프로세스와 테스트마다 등록 상태를 분리한다.
    return new Registry();
}
