# Worker 수와 처리량·자원 사용 실험

기존 `slow` Handler를 실제 API로 사전 등록하고, 독립 Worker 프로세스 수만 바꿔 비교한다. 기본값은 **1 / 2 / 4 / 8 Worker, 각각 480 Job, durationMs=1000, 두 번 반복**이다. 최고 TPS나 최적 Worker 수를 결정하는 도구가 아니다. Timer 대기이므로 실제 네트워크 I/O나 CPU 부하를 재현하지 않는다.

[2026-10-25 실측 결과](results/2026-10-25/report.md)에 기본 조건 두 반복과 자원 비용, 환경 시각 변동·Prometheus 수집 해상도의 한계를 정리했다.

## 실행

저장소 루트에서 Node.js 24, 기존 의존성, Docker Compose 2.24.4 이상과 `.env`의 DB 접속 설정을 준비한다. 애플리케이션 이미지가 현재 소스와 일치해야 한다.

```bash
docker compose build
npm run typecheck
node --test experiments/worker-load/results.test.ts
node --env-file=.env --test experiments/worker-load/preflight.test.ts
node --env-file=.env experiments/worker-load/run.ts
```

기본 조건의 Handler 대기 시간 합계만 약 30분이며, 등록·시작·DNS 탐색·Scrape·종료 시간이 추가된다. 각 반복은 1→2→4→8 순서다. 순서 효과를 무작위화한 실험은 아니며 Host의 다른 작업도 결과에 영향을 줄 수 있다.

입력은 다음처럼 조절한다. `--repeats`는 1~3이며 기본 비교는 2~3회를 권장한다. `--workers`에는 Speedup 기준인 1이 필요하다. 미리 사용할 출력 경로를 지정할 수 있으며, 기존 `environment.json`이 있는 경로의 재사용은 거부한다.

```bash
# 실행 흐름 확인용 소규모 실험. 기본 비교 결과와 구분해서 보관한다.
node --env-file=.env experiments/worker-load/run.ts \
  --workers=1,2 --repeats=1 --count=12 --duration-ms=500 \
  --out=artifacts/worker-load/smoke

# 필요할 때 별도로 수행하는 짧은 Handler 실험 예시. 기본 필수 실행 대상은 아니다.
node --env-file=.env experiments/worker-load/run.ts \
  --workers=1,2,4,8 --repeats=3 --count=4800 --duration-ms=50 \
  --interval-ms=5000 --out=artifacts/worker-load/short-wait
```

## 등록·시작·완료 확인 흐름

1. 전용 `async-job-worker-load` Compose 프로젝트에 PostgreSQL·Migration만 시작한다. 기존 프로젝트 컨테이너가 있으면 손대지 않고 거부한다. 보존된 DB에 `queued` 또는 `processing`이 있으면 API·Worker를 시작하지 않고 거부한다. 이전의 최종 완료 행은 남아 있어도 된다.
2. API·Prometheus만 시작하고, Worker가 없는 상태에서 `POST /jobs`로 Batch를 순차 등록한다. 반환된 ID를 매 응답마다 `batch.json`에 기록한다. 그 ID의 모든 행이 Ready·attempt=0·미실행인지 확인한다.
3. 도구의 별도 트랜잭션으로 이번 ID의 행만 `FOR UPDATE` 잠근 뒤 지정 개수의 Worker를 시작한다. Worker는 기존 `SKIP LOCKED`로 이 행을 건너뛴다. API Ready Queue가 Batch 수이고, Worker UUID·컨테이너 수·현재 IP의 Target 수·각 첫 Scrape·Claim Counter=0을 확인한 뒤 COMMIT한다. 애플리케이션 코드나 실행 모델은 바꾸지 않는다.
4. 잠금 해제 후 기본 5초 간격으로 이번 ID의 상태, Docker stats, `pg_stat_activity`, Targets를 수집한다. 주기는 이전 수집 시작부터 다음 시작까지이며 수집이 길면 그만큼 늘어난다. API Gauge도 기존 15초 Scrape마다 DB 집계 쿼리를 수행한다. 관찰 연결은 최대 2개이며 Batch 잠금은 준비 구간에만 유지한다.
5. 이번 ID가 전부 `succeeded`, `attempt=1`, 정상 result·lastError=null인지 확인한다. 모든 지정 Worker가 참여해야 한다. 프로세스 재시작 여부와 Job별 Claim·성공 로그도 확인한다. Retry·Recovery·Crash·미완료·소유권 오류가 있으면 유효 결과로 집계하지 않고 중단한다.
6. 마지막 성공 이후 API·모든 Worker가 다시 Scrape될 때까지 기다리고 기존 Counter·Histogram·Queue 시계열을 JSON으로 저장한다. Worker를 정상 종료·제거한 뒤 다음 조건을 준비한다. 마지막에는 컨테이너와 Network만 정리하며 DB·Prometheus Named Volume은 보존한다. **행을 전체 삭제하거나 볼륨을 초기화하지 않는다.**

준비 중 Row Lock 유지 시간과 Target 발견 지연은 `gate.json`·`result.json`에 남긴다. 준비에는 1초 Target 조회가 있지만 Job 처리 구간의 DB 완료 조회는 기본 5초다. 첫 Claim 전 기존 idle polling의 최대 약 1초 대기가 생길 수 있다. 그 대기는 아래 Batch 경과 시간에 포함되지 않는다. 측정 중 Docker stats 자체의 수집 비용과 DB 관찰 쿼리는 추가 부하이며 무계측 성능과 같다고 단정하지 않는다.

## 시점·산식·해석

기존 코드에서 `processingStartedAt`은 Claim 시 DB `now()`이고, `updatedAt`은 성공 상태 UPDATE의 DB `now()`다. 정상 첫 시도에서는 그 사이 다시 Claim하거나 Retry·Recovery로 필드 의미가 바뀌지 않는다. 최종 성공·attempt=1과 로그를 확인한 결과에만 다음 산식을 적용한다.

- `elapsedSeconds = (max(updatedAt) - min(processingStartedAt)) / 1000`: PostgreSQL timestamptz를 UTC 밀리초로 변환한다. 끝점은 성공 UPDATE에 저장된 시각이며 트랜잭션 COMMIT의 별도 타임스탬프는 아니다.
- `Batch Throughput = 이번 Batch 성공 Job 수 / elapsedSeconds`
- `Speedup = N Worker Throughput / 1 Worker Throughput`
- `Scaling Efficiency = Speedup / N × 100`

각 반복의 Speedup은 같은 반복의 1 Worker와 비교한다. 그룹 표는 각 Worker 수의 처리량 중앙값끼리 비교한다. 최소·최대·모집단 표준편차로 실제 반복 집합의 산포를 표시하며, 두세 반복으로 성능의 신뢰구간을 추정하지 않는다.

자원 요약은 첫 Claim 이후 수집을 시작하고 마지막 성공 이전 수집을 끝낸 샘플만 사용한다. CPU는 Docker stats의 논리 CPU 하나를 100%로 보는 값이며 전체 Worker는 같은 샘플을 합산한다. 메모리는 Docker stats 사용량(캐시 처리 등 Docker의 정의를 따름)이다. Worker별/전체 및 PostgreSQL의 중앙값·최대값을 남긴다. `pg_stat_activity`는 현재 DB의 `client backend` 연결을 상태별로 집계하며 API·Worker·관찰 연결을 포함하고 PostgreSQL 내부 프로세스는 제외한다. 실패한 샘플과 경계 밖 샘플도 원자료에 보존한다. 짧은 실험에 구간 내 자원 샘플이 없으면 측정값을 채워 넣지 않는다.

Prometheus 원자료의 주요 쿼리는 다음과 같다. 프로세스 Counter에 **각각 rate()를 적용한 뒤 합산**한다.

```promql
rate(async_job_succeeded_total{job="worker"}[2m])
sum(rate(async_job_succeeded_total{job="worker"}[2m]))
sum(rate(async_job_handler_duration_seconds_sum{job="worker"}[2m]))
  / sum(rate(async_job_handler_duration_seconds_count{job="worker"}[2m]))
async_job_queue_depth{job="api"}
async_job_ready_queue_depth{job="api"}
async_job_processing_jobs{job="api"}
```

시계열 응답의 step은 5초지만 원래 Scrape 간격은 15초다. 이는 5초마다 새로 관측했다는 뜻이 아니다. 기본 8 Worker의 약 60초 Batch는 2분 rate 창보다 짧으며 창에 준비·이전 실행·종료 후 구간이 섞일 수 있다. Counter reset과 Scrape 누락도 가능하므로 **최종 비교는 DB Batch 기록**을 기준으로 한다. Histogram은 Handler 호출부터 종료까지이며 DB 저장 시간·Job 전체 완료 시간은 제외한다. Processing Gauge는 DB의 `processing` 수로 실제 실행 Handler 수와 항상 같지 않다.

`query_range`의 마지막 평가 시점은 5초 격자에 맞춰져 요청 종료 시각보다 앞설 수 있다. 마지막 Scrape가 그 사이에 있으면 실제 완료 이후 Scrape를 기다렸어도 저장된 시계열 끝에는 직전 Queue 값이 남을 수 있다. 이를 DB의 미완료 Job으로 해석하지 않고 원자료의 평가·Scrape 시각을 함께 확인한다.

Connection 증가만으로 PostgreSQL 병목을 단정하지 않는다. 처리량 정체·CPU·쿼리·대기 증거가 부족하면 `원인 미확정`으로 남긴다. 도구는 추가 Query/Claim 계측이나 최적화를 하지 않는다. 실제 관찰값의 최종 기술적 해석과 포트폴리오 결론은 사용자와 ChatGPT가 별도로 결정한다.

## 자료 위치와 버전 관리

기본 출력은 Git에서 제외하는 `artifacts/worker-load/<UTC 실행 시각>/`다. 입력 옵션·코드 Hash·Host/WSL·Docker CPU/RAM·DB 설정·컨테이너별 제한값은 `environment.json`, Worker별 실제 제한값과 이미지 ID는 각 `gate.json`에 남긴다. `0`인 Docker CPU/메모리 제한은 개별 제한 미설정이며 Docker VM 전체 한도와는 구분한다. Windows Host의 물리 RAM 등 WSL 밖에서 확인하지 않은 값은 추정하지 않는다.

DB는 반복 사이에도 완료 행을 보존하므로 전체 테이블은 Batch마다 커진다. 시작 시 기존 행 수를 환경 자료에 남기고 Batch는 ID로만 구분한다. 관찰 쿼리·기존 Queue Gauge 집계가 커진 테이블을 읽는 영향은 통제하지 않으므로 결과 해석에 이 조건을 함께 고려한다.

- `summary.json`, `runs.csv`, `report.md`: 실제 반복별 결과, 중앙값·산포, Speedup·Efficiency, 자원 요약
- `<실행>/batch.json`, `before.json`, `gate.json`, `jobs.json`: 이번 ID와 준비·완료 근거
- `<실행>/samples.jsonl`: 타임스탬프가 있는 복수 자원·연결·Target 샘플과 수집 오류
- `<실행>/prometheus.json`, `targets-final.json`, `<컨테이너 ID>.jsonl`: 기존 Metrics 시계열·마지막 Scrape·프로세스 로그
- `<실행>/metrics-final.json`: 마지막 Scrape의 Worker별 성공 수와 Handler Histogram count/sum. 정상 단일 시도 Batch에서는 DB의 Worker별 완료 수와 비교하고 Handler 평균 시간을 집계한다.
- 실패 시 `failure.json`, `<실행>/incomplete-jobs.json`, `latest.json`, `result.json`: 실패 이유와 실제 관측된 상태. 유효한 반복만 집계한다.

도구·사용법과 선별한 실측 `summary.json`·짧은 결과 문서는 버전 관리한다. 대량 Job ID·로그·시계열 원자료는 기본 출력에 보존하고 일반 코드와 함께 추가하지 않는다. 원자료를 공유할 때는 해당 출력 디렉터리를 별도로 전달한다. 실제로 실행하지 않은 조건이나 예상 성능값은 결과 파일에 넣지 않는다.

SIGINT/SIGTERM은 완료 확인을 중단하고 `finally`에서 Worker를 종료한다. 미완료 Job은 보존되며 다음 실행은 이를 감지해 거부한다. Host 종료·SIGKILL로 정리가 안 된 경우 아래 **전용 프로젝트만** 확인하고 중단한다.

```bash
docker compose -p async-job-worker-load -f compose.yml -f experiments/worker-load/compose.yml ps -a
docker compose -p async-job-worker-load -f compose.yml -f experiments/worker-load/compose.yml stop worker
docker compose -p async-job-worker-load -f compose.yml -f experiments/worker-load/compose.yml down
```

이전 Batch ID로 미완료 행과 원인을 확인한 뒤 필요하다면 해당 ID만 명시적으로 정리한다. 도구는 자동 DELETE하지 않는다. API INSERT와 응답 저장 사이의 강제 중단으로 ID가 누락될 수 있으므로 실패 뒤 전역 미완료 확인을 건너뛰지 않는다.
