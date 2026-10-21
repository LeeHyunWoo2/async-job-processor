> **개발 진행 중 (Work in Progress)**
>
> 현재 프로젝트는 개발 중이며, 이 README는 실행 방법과 구현 상태를 기록하기 위한 임시 문서입니다.


# Generic Async Job Processor

Node.js native TypeScript, Fastify, PostgreSQL을 사용하는 독립 API·Worker다. Job 등록·조회, PostgreSQL Claim, Retry / Backoff, Crash Recovery와 실행 소유권 보호를 제공한다.

## 실행

`.env.example`을 `.env`로 복사하고 DB 접속 값을 설정한다. Node.js 24와 Docker Compose가 필요하다.

### Docker Compose 통합 실행

```bash
docker compose up -d --build
docker compose ps -a
docker compose logs -f api worker
```

PostgreSQL이 준비되면 일회성 `migrate` 서비스가 기존 migration을 적용한다. Migration이 성공한 뒤 API와 Worker가 같은 애플리케이션 이미지로 시작한다. 실패하면 `docker compose logs migrate`로 원인을 확인한다. 컨테이너에서는 `.env` 파일을 읽는 대신 Compose가 환경변수를 전달한다.

- API: 기본 `http://localhost:3000` (`API_PORT`로 호스트 포트 변경)
- API Metrics: `http://localhost:3000/metrics`
- Worker Metrics: Compose 내부 `http://worker:9464/metrics`, 호스트 포트는 공개하지 않음
- Prometheus: 기본 `http://localhost:9090` (`PROMETHEUS_PORT`로 호스트 포트 변경)

```bash
curl -X POST http://localhost:3000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"slow","payload":{"durationMs":100}}'
curl http://localhost:3000/jobs/<반환된-id>
curl -i http://localhost:3000/metrics
docker compose exec api node --input-type=module -e \
  'const response = await fetch("http://worker:9464/metrics"); console.log(response.status, response.headers.get("content-type")); console.log(await response.text());'
```

Prometheus Web UI의 Targets에서 `api`, `worker` 상태를 확인하고 PromQL `up{job=~"api|worker"}`를 조회한다. 두 Target의 값이 모두 `1`이면 수집에 성공한 상태다. Static Target, 수집 주기 15초, Timeout 5초, 보관 기간 15일로 구성한다.

```bash
docker compose exec prometheus promtool check config /etc/prometheus/prometheus.yml
docker compose stop prometheus
# 이 상태에서도 API 등록·조회와 Worker 처리를 사용할 수 있다.
docker compose start prometheus
docker compose down
```

기존 `postgres_data`와 추가한 `prometheus_data` Named Volume은 일반적인 종료·재시작·컨테이너 재생성 이후에도 유지된다. `docker compose down -v`는 이 데이터를 삭제한다.

### 로컬 Node.js 직접 실행

```bash
npm install
docker compose up -d postgres
npm run db:migration:up
npm run start:api
# 다른 터미널에서 실행
npm run start:worker
```

Compose에서 API·Worker를 이미 실행 중이라면 먼저 `docker compose stop api worker`로 중지한다. 로컬 프로세스는 `.env`를 읽으며 API와 Worker Metrics는 기본적으로 `127.0.0.1:3000`, `127.0.0.1:9464`에서 응답한다. Worker Metrics 주소는 `WORKER_METRICS_HOST`, `WORKER_METRICS_PORT`로 변경할 수 있다. Compose의 Prometheus 설정은 컨테이너 API·Worker를 수집 대상으로 한다.

## Logger와 Registry 재사용

- `src/observability/logger.ts`의 `createLogger('api' | 'worker')`는 Pino 인스턴스를 반환한다. 표준 출력의 기본 JSON 형식, 기본 `info` 레벨과 `component` 필드를 공유하며 `LOG_LEVEL`로 레벨을 변경한다. 오류는 `{ err: error }`로 기록한다.
- API는 같은 인스턴스를 Fastify의 `loggerInstance`로 전달하여 `request.log`와 Request ID를 유지한다. 초기화 실패에도 공통 Logger를 사용한다. 기존 `createApp(orm, false)`는 로그를 비활성화한다.
- `src/observability/metrics.ts`의 `createMetricsRegistry()`는 호출마다 새 라이브러리 `Registry`를 반환한다. 전역 Registry나 별도 Metrics Service는 사용하지 않는다.
- API는 `createApp(orm, logger, registry)`에서 API Metric을 등록한다. Worker는 `main.ts`에서 Worker Metric을 등록하고 같은 Registry를 Metrics HTTP 서버에 전달한다. `processJob()`의 네 번째 인자와 `runWorker()`의 다섯 번째 인자에 선택적으로 `{ logger, metrics }`를 전달한다.
- 두 Endpoint는 `registry.metrics()`와 `registry.contentType`으로 응답한다. 조회 실패는 HTTP 500이며 Worker의 수집 오류는 Polling을 중단시키지 않는다. Worker Metrics 서버는 현재 Job 처리가 끝난 뒤 DB 연결과 함께 정리된다.

Job Lifecycle은 DB 상태 변경 성공 후 `job.enqueued`, `job.claimed`, `job.retry_scheduled`, `job.succeeded`, `job.failed`, `job.recovered`로 기록한다. API 등록 로그의 `reqId + jobId`, Worker 로그의 `jobId + workerId + attempt`로 요청과 실행을 연결한다. Recovery 로그의 `workerId`는 복구를 수행한 현재 Worker의 Context를 유지하며, `previousWorkerId`와 `attempt`는 복구 이전 Claim 소유자와 시도 번호다. Worker 시작·종료 요청·치명적 오류에는 `worker.started`, `worker.stopping`, `worker.error`를 사용한다. Payload·result·인증 헤더·DB 접속 설정을 로그 필드로 추가하지 않는다.

## Job Metrics

| Endpoint | Metric | 의미 |
|---|---|---|
| API | `async_job_queue_depth` | 최초 대기와 Backoff 대기를 포함한 전체 `queued` 수 |
| API | `async_job_ready_queue_depth` | `queued`, `availableAt <= now()`, `attempt < maxAttempts`인 수 |
| API | `async_job_processing_jobs` | Stale을 포함한 DB의 `processing` 수 |
| API | `async_job_enqueued_total` | INSERT 성공 횟수 |
| Worker | `async_job_claimed_total` | Claim 성공 횟수 |
| Worker | `async_job_succeeded_total` | 최종 성공 결과 저장 횟수 |
| Worker | `async_job_failed_total` | Handler 또는 Recovery에 의한 최종 실패 확정 횟수 |
| Worker | `async_job_retry_scheduled_total` | Retry 예약 성공 횟수 |
| Worker | `async_job_recovered_total` | Stale Recovery 성공 횟수 |
| Worker | `async_job_handler_duration_seconds` | 정상 반환·예외로 완료된 Handler 시간(초), 기본 Histogram Bucket 사용 |

Gauge는 `/metrics` 요청마다 단일 PostgreSQL 집계 SQL로 조회한다. 조회 실패는 HTTP 500이며 이전 값이나 `0`으로 대체하지 않는다. Processing Gauge는 실제 실행 중인 Handler 수와 다를 수 있다. Counter는 해당 프로세스가 관측한 사건의 누적값으로 프로세스 재시작 시 초기화된다. 별도 Metric Label은 추가하지 않으며 Worker 구분에는 Prometheus Target의 `instance`를 사용한다.

Retry는 Retry Counter만 증가한다. `processing → failed` Recovery는 Recovered·Failed Counter를 모두 증가시키지만 로그는 `job.recovered`만 기록한다. Duration은 `performance.now()`로 Handler 호출부터 반환 또는 예외까지만 측정하며 DB 결과 저장 시간은 제외한다. Handler가 끝났다면 결과 저장 실패나 실행 소유권 상실 시에도 Duration은 남을 수 있다.

PostgreSQL이 Job 상태의 Source of Truth다. 로그·Counter·Histogram 기록 오류는 Job 처리 결과에 영향을 주지 않으며, 상태 변경 직후 강제 종료 시 기록이 누락될 수 있는 Best-effort 관측이다. DB 오류와 소유권 상실에 따른 기존 Worker 중단 정책은 유지한다. 강제 종료로 끝나지 못한 Handler의 Duration은 보장하지 않는다.

Prometheus에서 다음 PromQL로 현재 상태와 프로세스별 누적값을 조회할 수 있다.

```promql
up{job=~"api|worker"}
async_job_queue_depth{job="api"}
async_job_ready_queue_depth{job="api"}
async_job_failed_total{job="worker"}
async_job_retry_scheduled_total{job="worker"}
async_job_recovered_total{job="worker"}
async_job_handler_duration_seconds_count{job="worker"}
```

## 검증

Migration이 적용된 PostgreSQL에서 다음 명령을 실행한다. 공유 DB를 사용하는 테스트는 직렬로 실행한다.

```bash
npm run typecheck
npm run test:observability
npm run test:db
```

관측 테스트는 JSON Event·Request ID·실행 정보, 10개 Metric과 Registry 격리, 실제 DB Gauge·저장 실패·소유권 상실·동시 Recovery의 계측, 결과 저장과 분리된 Handler Duration, 계측 오류의 격리와 독립 Worker의 종료 Event·서버 정리를 확인한다. 기존 PostgreSQL 테스트는 Claim·Retry·Recovery·실행 소유권·DB 오류 중단·Graceful Shutdown을 검증한다.

## 구현 범위

Job Lifecycle Logging, PostgreSQL Gauge·프로세스별 Counter·Handler Duration Histogram과 Compose·Prometheus 수집을 제공한다. 기존 상태 전이·Retry·Crash Recovery·실행 소유권 보호를 유지하며, 다중 Worker Target Discovery나 관측 기록의 원자적 저장·재집계는 제공하지 않는다.
