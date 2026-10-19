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
- API는 `createApp(orm, logger, registry)`의 Registry를 사용한다. Worker는 `main.ts`에서 생성한 Registry를 `createWorkerMetricsServer(registry, logger)`에 전달한다. 후속 계측에서는 같은 Registry를 Counter·Gauge·Histogram의 `registers: [registry]`에 지정한다.
- 두 Endpoint는 `registry.metrics()`와 `registry.contentType`으로 응답한다. 조회 실패는 HTTP 500이며 Worker의 수집 오류는 Polling을 중단시키지 않는다. Worker Metrics 서버는 현재 Job 처리가 끝난 뒤 DB 연결과 함께 정리된다.

현재 Registry에는 실제 Metric을 등록하지 않는다. 따라서 `/metrics`의 본문은 빈 줄이어도 정상이며 Prometheus의 `up`은 수집할 수 있다. Payload·result·인증 헤더·DB 접속 설정을 로그 필드로 추가하지 않는다.

## 검증

Migration이 적용된 PostgreSQL에서 다음 명령을 실행한다. 공유 DB를 사용하는 테스트는 직렬로 실행한다.

```bash
npm run typecheck
npm run test:observability
npm run test:db
```

관측 기반 테스트는 JSON Logger·레벨·초기화 오류, Registry 격리, API 요청별 로그와 Request ID, Metrics 응답·수집 실패, 독립 Worker의 종료 및 서버 정리를 확인한다. 기존 PostgreSQL 테스트는 Claim·Retry·Recovery·실행 소유권·DB 오류 중단·Graceful Shutdown을 검증한다.

## 구현 범위

공통 Logger, Registry, 두 Metrics Endpoint와 Compose·Prometheus 실행 기반을 제공한다. Job Lifecycle Event, Job Counter·Gauge·Histogram 10개, Handler Duration 측정, PostgreSQL Queue 집계 SQL 및 다중 Worker Target Discovery는 후속 범위다. Job 처리 상태 전이에는 이번 기반을 연결하지 않는다.
