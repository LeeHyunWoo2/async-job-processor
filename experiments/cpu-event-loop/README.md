# CPU Job / Event Loop 비교 실험

동일한 `createApp()`·`runWorker()`·`executeHandler()` 및 PostgreSQL Claim·결과 저장 경로를 별도 프로세스(Separated)와 한 프로세스(Co-located)에서 실행한다. 전용 Compose와 독립 Node.js 클라이언트로 HTTP 응답·Event Loop·Docker 자원·시계를 함께 기록한다.

## 반복 실험 실행

선행조건은 저장소 의존성 설치, `.env`의 PostgreSQL 접속 필수값, 동작하는 Docker Engine·Compose(`!override` 지원), 로컬 Node.js의 native TypeScript 지원이다. 새 의존성을 추가하지 않는다. 저장소 루트에서 실행한다.

```bash
docker compose -p async-job-cpu-event-loop \
  -f compose.yml -f experiments/cpu-event-loop/compose.yml build migrate
node --env-file=.env experiments/cpu-event-loop/run.ts \
  --out=artifacts/cpu-event-loop/my-run
```

기존 개발 이미지·컨테이너·고정 포트를 변경하지 않는다. 전용 프로젝트는 `async-job-cpu-event-loop`, DB는 `async_job_cpu_event_loop`, 이미지는 `async-job-cpu-event-loop:local`이다. Host 포트는 loopback의 임의 포트이며 전용 Named Volume을 재사용한다. 같은 프로젝트 컨테이너나 실행 잠금이 있으면 변경 없이 거부한다. PostgreSQL·Migration만 먼저 시작하고 미완료 Job이 없음을 확인한 뒤 API·Worker를 시작한다. 이전 완료 행을 보존하고 각 조건 전후 동일함을 확인한다.

기본값은 6조건 × 3회다. 매 조건의 API·Worker 컨테이너를 제거하고 새 프로세스를 만들어 Counter와 Histogram을 분리한다. 이미지는 같은 ID이고 실행 중 이미지 소스 SHA-256과 작업 소스도 비교한다. Docker CPU 상한·cpuset을 추가하지 않는다.

| 반복 | 실행 순서 |
|---|---|
| 1 | Co-located Idle → slow → cpu, Separated Idle → slow → cpu |
| 2 | Separated Idle → slow → cpu, Co-located Idle → slow → cpu |
| 3 | Co-located Idle → slow → cpu, Separated Idle → slow → cpu |

각 실행은 준비 확인 → 약 2초 Baseline → Idle 약 2초 또는 Job 1개의 실제 완료 확인 → 약 2초 재개·지표 반영 구간 순서다. 준비에는 API `/health`, Worker `/metrics`, DB, 프로세스 수와 단일 Worker, 실제 Event Loop 표본을 확인한다. Job은 기존 POST·GET API와 DB 행, Claim·Handler 시작/종료·완료 로그, 기존 Counter로 교차 확인한다. Job 실행 대기는 sleep으로 대체하지 않는다. Handler 경과는 Monotonic으로 측정한 기존 Duration이며 Marker의 JSON 로그 시각은 Wall Clock이다.

| 옵션 | 기본값 | 의미 |
|---|---|---|
| `--modes` | `co-located,separated` | 중복 없이 실행할 구조 |
| `--conditions` | `idle,slow,cpu` | 중복 없이 실행할 부하 |
| `--repeats` | `3` | 1~3회, 반복마다 구조 순서 반전 |
| `--duration-ms` | `2000` | Job 목표 경과 / Idle 구간, 1~10000ms |
| `--window-ms` | `2000` | Baseline·완료 후 최소 관찰, 100~10000ms, Probe 두 주기 이상 |
| `--interval-ms` | `100` | Probe 예약 간격, 10~1000ms |
| `--timeout-ms` | `10000` | 요청별 총 Timeout, Job 목표보다 1000ms 초과 여유 필요 |
| `--max-inflight` | `64` | 대상별 동시 요청 상한, 1~256개 |
| `--out` | `artifacts/cpu-event-loop/<UTC시각>` | 기존 `environment.json`을 덮어쓰지 않는 자료 경로 |

독립 클라이언트는 `node:http`로 연결별 요청을 만들고 이전 응답을 기다리지 않는다. 예약/발송/완료 경과·RTT는 `performance.now()`로 계산한다. 상한에 걸린 예약은 기다리거나 숨기지 않고 `skipped=inflight-limit`로 기록한다. 응답 본문을 모두 읽은 때까지 RTT에 포함하며, Timeout·네트워크 오류도 집계한다. 발송·완료 Wall Clock은 사건 정렬 근거로만 함께 보존한다.

## 유효성 및 결과 읽기

`summary.json`의 `valid`는 측정 유효성이다. 실행 구조·이미지·Job·Counter·로그 불일치, 실제 표본 부재, 완료 후 표본 증가 없음, Docker 자원 결측, Probe 누락·Timeout·오류·비정상 HTTP, 한 예약 주기 초과 발송 지연은 유효성 실패로 기록한다. 클라이언트 동일 시작·종료 경계의 Wall/Monotonic 경과 차이가 한 주기를 초과하면 사건 중첩 해석을 불확실하게 표시하고 유효한 비교에서 제외한다. 모든 데이터는 그대로 보존한다.

성능 결과의 예상 방향은 유효성 기준에 포함하지 않는다. 예를 들어 Separated API의 RTT가 크더라도 다른 수집 조건을 만족하면 유효하다. Job 조건에서는 각 대상의 실제 발송 Wall Clock이 Handler Marker 사이에 있는 요청이 하나 이상인지 확인한다. 예약만 해당 구간에 있고 실제 발송은 늦어진 경우도 구분된다. `cleanup.succeeded`는 자료 수집·컨테이너 종료 성공이며 `valid`와 별개다. 요약 파일 자체의 저장이 실패하면 별도 조건 디렉터리의 `cleanup.json`에도 남긴다. 유효성/정리 실패가 있거나 중단하면 종료 코드 1이다.

`http`는 대상별 모든 전송의 경과 분포와 성공 RTT 분포(min/p50/p95/p99/max/mean), Timeout·오류 수, 발송 지연 분포, 가장 큰 지연 3건을 제공한다. 표본 분위수는 nearest rank다. 실제 원자료에서 원하는 구간을 다시 선택할 수 있으며 전체 관찰 분포를 순수 Handler 구간 분포로 해석하지 않는다.

`histograms`는 **프로세스 계측 활성화 이후 누적** Max/p99/Mean(ms)/Count다. 시작·준비 시간도 포함하며 조건별 동일 길이의 정확한 Window가 아니다. Max를 주 지표로 보고 p99/Mean/Count를 함께 읽는다. Co-located는 `shared` 하나이고 Separated는 `api`, `worker` 각각 하나다. 두 Endpoint를 두 개의 독립 공유 표본처럼 합산하지 않는다. `completion`은 완료 직후 참고값이고, `final`은 추가 관찰 후 표본 증가까지 확인한 값이다. 누적 Max·분위수·평균을 빼서 구간 Histogram을 만들지 않는다.

프로세스 간 `performance.now()` 절댓값을 결합하지 않는다. Handler Marker와 Probe의 중첩은 Wall Clock 추정이며 별도 offset·정렬 오차 상한을 측정하지 않는다. Marker는 Handler 호출 직전/반환 직후이고 logging·Promise 처리의 작은 간극이 있다. DB `processing_started_at`~`updated_at`은 Handler 자체의 정확한 계산 경계가 아니다. 클라이언트의 시계 차이 진단은 과거 Wall Clock 변동의 원인이나 해결 여부를 입증하지 않는다.

Docker Stats는 수신 시각이 붙은 보조 표본이며 짧은 2초 부하의 정확한 CPU 사용량 적분이나 최대치를 보장하지 않는다. 한 컨테이너의 API+Worker와 두 컨테이너의 자원을 단순 일대일 비교하지 않는다. 프로세스를 분리해도 CPU 자원은 공유하며 OS 스케줄링·다른 부하로 API가 지연될 수 있다. Prometheus는 이 최소 실험에서 기동하지 않는다. 기존 15초 Scrape/5초 Timeout은 짧은 차단의 핵심 근거로 사용하지 않는다. 실측 결과의 최종 해석·추가 CPU 경합 조건은 별도 검토 대상이다.

## 자료와 중단·정리

Git 제외 원자료 경로에 다음을 남긴다.

- `environment.json`, `source/`, `source.diff`: Commit·미커밋 상태·소스 사본/해시·이미지 ID·호스트/Docker CPU 수·환경·조건 순서
- 각 조건의 `ready.json`, `stopped-containers.json`: 실제 Node.js 프로세스·Worker 수·시작 시각·Docker CPU/메모리 제한·종료 상태
- `requests.jsonl`, `requests.json`: 대상·조건·예약/발송/완료 경과·Wall Clock·상태·Timeout/오류·RTT·발송 누락의 요청별 원자료
- `*-api.prom`, `*-worker.prom`, `snapshots.json`: Baseline·실행 전·완료 직후·재개 후 직접 Metrics Snapshot 및 Histogram 수집 시점
- `phases.jsonl`, `db-samples.jsonl`, `enqueue.json`, `job-api.json`, `jobs.json`: 관찰 구간/시계 진단과 실제 API·DB 결과
- `resources.jsonl`, `resource-status.json`, `events.jsonl`, `services.log`: Docker CPU·메모리, 수집 오류, Handler 사건·전체 로그
- `result.json`, `summary.json`, `cleanup.json`, `final-jobs.json`, 실패 시 `failure.json`: 측정 유효성·정리 결과·실패/중단 근거

`Ctrl+C` 또는 실행 도구에 `SIGTERM`을 보내면 요청을 정산하고 가능한 자료를 저장한 뒤 전용 컨테이너만 내린다. Job 행과 볼륨은 삭제하지 않는다. 자료 저장이 실패해도 다른 종료 단계는 계속 시도한다. `SIGKILL`·Host 종료에는 정리를 보장하지 못한다. 그 경우 실행 도구가 살아 있지 않은지 확인한 뒤 아래 전용 명령으로 로그를 먼저 보존하고 컨테이너만 정리한다. 미완료 행은 다음 실행의 preflight가 거부하므로 자료 확인 후 처리 방향을 결정한다.

```bash
docker compose -p async-job-cpu-event-loop \
  -f compose.yml -f experiments/cpu-event-loop/compose.yml logs --no-color > /tmp/cpu-event-loop-preserved.log
docker compose -p async-job-cpu-event-loop \
  -f compose.yml -f experiments/cpu-event-loop/compose.yml down
# 강제 종료로 잠금만 남았고 실행 프로세스가 없는 것을 확인한 경우에만:
rmdir artifacts/cpu-event-loop/.lock
```

`down -v`나 Job 일괄 삭제는 사용하지 않는다. Git의 `results/`에는 선별한 작은 기계적 요약·재현 조건·확인 사실만 둔다.

## 도구 검증

이미지 빌드 후 순서대로 실행한다. Smoke는 전용 프로젝트를 사용하므로 반복 실험과 동시에 실행하지 않는다.

```bash
node --test experiments/cpu-event-loop/experiment.test.ts
node --env-file=.env --test experiments/cpu-event-loop/smoke.test.ts
npm run typecheck
npm run test:observability
```

순수 집계·단위/시계 변환·옵션과 실제 지연 HTTP 서버의 독립 예약/Timeout/누락/저장 실패를 검증한다. Compose Smoke는 두 구조의 cpu 1개, 실제 DB 결과와 기존 행·개발 컨테이너 보존, 실제 저장 실패와 SIGTERM 시 자료 보존·정리 분리를 확인한다. 기능 Assertions에 정확한 ms 성능 임계값이나 예상 성능 방향을 강제하지 않는다.

## CPU Job

```bash
curl -fsS http://127.0.0.1:3000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"cpu","payload":{"durationMs":100}}'
curl -fsS http://127.0.0.1:3000/jobs/<반환된-id>
```

최상위 필드는 `type`·`payload`, payload 필드는 정수 `durationMs` 하나만 허용한다. 허용 범위는 `slow`와 같은 1~10,000ms다. 누락·잘못된 형식·추가 필드·범위 초과는 HTTP 400이며 Job을 생성하지 않는다. Admission Control과 Retry·Recovery·실행 소유권 검증은 기존 경로를 사용한다.

현재 JavaScript 스레드에서 `Math.imul()`·덧셈·32bit 변환을 1,000회씩 반복하며 `performance.now()`의 경과 시간이 목표 이상이면 종료한다. 중간에 `await`로 양보하거나 Timer·Thread Pool로 계산을 넘기지 않는다. `{ durationMs, iterations, checksum }` JSON을 반환하고, `checksum`은 실제 산술 계산 결과다. `iterations`는 수행한 산술 반복 횟수다.

`durationMs`는 Monotonic **경과 시간 목표**다. 정확한 CPU 사용 시간·고정된 반복 횟수·정확한 종료 시각을 보장하지 않는다. OS 스케줄링·JIT·계산 묶음 등에 따라 달라진다. `slow`의 Timer 대기와 달리 CPU 계산 중에는 같은 프로세스의 HTTP 처리·Timer·종료 Signal 처리가 지연될 수 있다.

## 실행과 종료

기존 `.env`의 DB·API·Worker Metrics·Admission 설정을 그대로 사용한다. Migration이 적용된 PostgreSQL을 준비하고 같은 DB를 사용하는 다른 Worker는 중지한 상태에서 둘 중 하나만 실행한다. 실행 시 기존 queued·stale processing도 Claim·Recovery 대상이므로 비교용 DB를 구분한다.

Separated에서는 터미널 두 개에서 실행한다.

```bash
npm run start:api
# 다른 터미널
npm run start:worker
```

Co-located에서는 다음 명령 하나를 사용한다.

```bash
npm run start:co-located
# 환경변수가 이미 전달된 컨테이너 등의 직접 진입점
node experiments/cpu-event-loop/co-located.ts
```

기본 주소는 두 조건 모두 API `http://127.0.0.1:3000`의 `/health`·`POST /jobs`·`GET /jobs/:id`·`/metrics`, Worker `http://127.0.0.1:9464/metrics`다. `API_HOST`·`API_PORT`·`WORKER_METRICS_HOST`·`WORKER_METRICS_PORT`로 변경한다. Docker 이미지에도 통합 진입점을 포함하지만 기본 Compose 명령과 프로세스 배치는 유지한다. 아래 전용 Overlay는 기본 개발 Compose와 분리해서 사용한다.

통합 진입점은 API ORM과 Worker ORM을 따로 생성한다. API와 Worker Metrics Listener가 모두 준비된 다음 Recovery → Claim → Handler → 결과 저장의 기존 순차 Loop를 시작한다. `SIGINT`·`SIGTERM`은 Worker 중단을 요청하고, 이미 Claim한 Job의 결과 저장을 기다린 뒤 Metrics Listener·Worker ORM·API Listener를 닫는다. API ORM은 `app.close()`의 기존 Hook이 닫는다. Worker DB 오류도 두 Listener와 연결을 정리하고 종료 코드 1로 끝난다. CPU 계산 중 Signal Callback은 Event Loop가 재개된 뒤 처리된다.

## Event Loop 지표

`src/observability/event-loop.ts`의 `createEventLoopMetrics(registry)`가 `monitorEventLoopDelay({ resolution: 10 })`를 활성화한다. 전역 Registry·실행별 Label 없이 다음 **Gauge 4개**로 Native Histogram 요약값을 노출한다.

| 지표 | 단위·의미 |
|---|---|
| `nodejs_event_loop_delay_max_seconds` | 최대 지연, ns 원본을 1e9로 나눈 초 |
| `nodejs_event_loop_delay_p99_seconds` | p99 지연, 초 |
| `nodejs_event_loop_delay_mean_seconds` | 평균 지연, 초 |
| `nodejs_event_loop_delay_samples` | 표본 수, 단위 없는 개수 |

Separated에서는 API와 Worker 진입점이 각자 한 객체를 소유하고 **각 프로세스의 `/metrics`**에 노출한다. Co-located에서는 하나의 공유 Event Loop를 한 객체로 계측하며 **Worker `/metrics`만 통합 프로세스의 단일 관측 출처**다. 통합 API `/metrics`는 기존 Job 지표를 제공한다. API·Worker의 Registry는 계속 분리된다.

요약 범위는 **계측 활성화 이후 또는 마지막 `reset()` 이후 전체 표본**이다. Scrape로 초기화하거나 이동 구간을 만들지 않는다. 표본이 없으면 네 Gauge 모두 0이며 `samples = 0`은 유효한 지연 관측이 아직 없다는 뜻이다. p99는 Native Histogram의 근사 분위수이며 Prometheus 요청 시간 Histogram이나 HTTP 응답 지연이 아니다. 기존 `async_job_handler_duration_seconds`는 DB 결과 저장을 제외한 Handler 실행 시간으로 유지한다.

Factory 반환값은 Node.js Native Histogram이다. 같은 프로세스 안에서 사용하는 후속 도구는 `reset()`으로 표본을 비우고, 종료 시 `disable()`로 계측을 정리할 수 있다. `reset()`은 계측을 중지하지 않으며 다음 표본부터 새 구간을 수집한다. 현재 독립 실행 진입점에는 HTTP 초기화 API를 추가하지 않았다. 외부 도구만 사용할 때는 프로세스를 재시작하여 초기화하며 기동·준비 구간도 요약 범위에 포함한다. 프로세스 내부 도구에서 동일 Event Loop의 계측 객체를 추가로 만들지 않고 기존 객체를 재사용해야 한다.

CPU 계산 직후에는 지연된 Timer 표본이 아직 반영되지 않을 수 있다. 실험 도구는 먼저 `samples > 0`으로 계측이 준비되었는지 확인하고, CPU 완료 이후 Event Loop가 Timer를 실행할 시간을 준 뒤 다시 `/metrics`를 읽어야 한다. 프로세스 내부에서는 `await setTimeout(20)` 같은 Timer 대기 이후 조회할 수 있다. 표본 수 증가만으로 특정 CPU 구간의 지연 반영이나 실험 유효성을 확정하지 않는다. Max·p99·Mean의 누적 요약값을 빼서 구간 통계를 만들 수도 없다.

## 검증

```bash
npm run typecheck
npm run test:observability
npm run test:db
```

실제 PostgreSQL에서 입력 경계·행 생성 여부·첫 시도 성공 결과와 기존 slow/fail/flaky 경로를 확인한다. 계측 테스트는 실제 표본·ns→초 변환·Registry 격리·reset·disable을 검사한다. 독립 프로세스 테스트는 두 배치 모두의 실제 HTTP·cpu 성공·계측·종료를 확인하고, 통합 실행의 진행 중 Job 종료 대기 및 결과 저장 오류의 정리도 검증한다. CPU 시간에 비례한 Max Delay 같은 환경 의존 성능 임계값은 사용하지 않는다.
