# CPU Job과 Event Loop 관측 기반

동일한 `createApp()`·`runWorker()`·`executeHandler()` 및 PostgreSQL Claim·결과 저장 경로를 별도 프로세스(Separated)와 한 프로세스(Co-located)에서 실행한다. 현재 단계는 기능과 계측 기반까지이며 반복 부하 실험·Probe 도구·실측 보고서는 포함하지 않는다.

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

기본 주소는 두 조건 모두 API `http://127.0.0.1:3000`의 `/health`·`POST /jobs`·`GET /jobs/:id`·`/metrics`, Worker `http://127.0.0.1:9464/metrics`다. `API_HOST`·`API_PORT`·`WORKER_METRICS_HOST`·`WORKER_METRICS_PORT`로 변경한다. Docker 이미지에도 통합 진입점을 포함하지만 기본 Compose 명령과 프로세스 배치는 유지한다. 전용 Compose Overlay는 후속 범위다.

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

CPU 계산 직후에는 지연된 Timer 표본이 아직 반영되지 않을 수 있다. 후속 실험은 먼저 `samples > 0`으로 계측이 준비되었는지 확인하고, CPU 완료 이후 Event Loop가 Timer를 실행할 시간을 준 뒤 다시 `/metrics`를 읽어야 한다. 프로세스 내부에서는 `await setTimeout(20)` 같은 Timer 대기 이후 조회할 수 있다. 표본 수 증가만으로 특정 CPU 구간의 지연 반영이나 실험 유효성을 확정하지 않는다. Max·p99·Mean의 누적 요약값을 빼서 구간 통계를 만들 수도 없다.

## 검증

```bash
npm run typecheck
npm run test:observability
npm run test:db
```

실제 PostgreSQL에서 입력 경계·행 생성 여부·첫 시도 성공 결과와 기존 slow/fail/flaky 경로를 확인한다. 계측 테스트는 실제 표본·ns→초 변환·Registry 격리·reset·disable을 검사한다. 독립 프로세스 테스트는 두 배치 모두의 실제 HTTP·cpu 성공·계측·종료를 확인하고, 통합 실행의 진행 중 Job 종료 대기 및 결과 저장 오류의 정리도 검증한다. CPU 시간에 비례한 Max Delay 같은 환경 의존 성능 임계값은 사용하지 않는다.
