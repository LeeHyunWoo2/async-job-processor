import { Counter, Gauge, Histogram, Registry } from '@prometheus-io/client';

export function createMetricsRegistry() {
    // 전역 Registry를 사용하지 않아 프로세스와 테스트마다 등록 상태를 분리한다.
    return new Registry();
}

export function createApiMetrics(registry: Registry) {
    return {
        enqueued: new Counter({ name: 'async_job_enqueued_total', help: '등록에 성공한 Job 수', registers: [registry] }),
        enqueueRejected: new Counter({ name: 'async_job_enqueue_rejected_total', help: '미완료 Job 용량 초과로 거부한 등록 요청 수', registers: [registry] }),
        queueDepth: new Gauge({ name: 'async_job_queue_depth', help: 'Backoff 대기를 포함한 queued Job 수', registers: [registry] }),
        readyQueueDepth: new Gauge({ name: 'async_job_ready_queue_depth', help: '현재 Claim 가능한 queued Job 수', registers: [registry] }),
        processingJobs: new Gauge({ name: 'async_job_processing_jobs', help: 'Stale을 포함한 processing Job 수', registers: [registry] }),
    };
}

export function createWorkerMetrics(registry: Registry) {
    return {
        claimed: new Counter({ name: 'async_job_claimed_total', help: 'Claim에 성공한 Job 수', registers: [registry] }),
        succeeded: new Counter({ name: 'async_job_succeeded_total', help: '성공 결과를 저장한 Job 수', registers: [registry] }),
        failed: new Counter({ name: 'async_job_failed_total', help: 'Recovery를 포함해 최종 실패가 확정된 Job 수', registers: [registry] }),
        retryScheduled: new Counter({ name: 'async_job_retry_scheduled_total', help: 'Retry 예약에 성공한 Job 수', registers: [registry] }),
        recovered: new Counter({ name: 'async_job_recovered_total', help: 'Stale Recovery에 성공한 Job 수', registers: [registry] }),
        handlerDuration: new Histogram({ name: 'async_job_handler_duration_seconds', help: '정상 반환 또는 예외로 끝난 Handler 실행 시간(초)', registers: [registry] }),
    };
}
