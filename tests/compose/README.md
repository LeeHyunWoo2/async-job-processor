# 실제 다중 Worker 검증

실제 Compose 컨테이너와 PostgreSQL을 사용해 정상 분산 처리, Worker 하나의 Crash 이후 처리 지속·Recovery, Scale 변경과 종료 정책을 확인한다. 기본 테스트에는 포함하지 않는 선택 실행이며 보통 수 분이 걸린다. Worker의 Claim·Retry·Recovery 코드는 변경하지 않는다.

## 사전 조건과 실행

- Node.js 24, 설치된 프로젝트 의존성, Docker Engine과 Docker Compose 2.24.4 이상(`!override` 지원)이 필요하다.
- 저장소 루트의 `.env`에 DB 접속 값을 설정한다. 실행 위치는 저장소 루트다.
- 먼저 기존 PostgreSQL 테스트를 실행한다. 기존 개발용 PostgreSQL에 Migration이 적용되어 있어야 한다.

```bash
npm run typecheck
npm run test:observability
npm run test:db
docker compose build
npm run test:multi-worker
```

스크립트는 기본 설정에 `tests/compose/worker-safety.yml`을 덧씌워 `async-job-worker-safety` 프로젝트를 시작한다. API·PostgreSQL·Prometheus는 `127.0.0.1`의 임의 호스트 포트를 사용하며, Network·Named Volume은 개발용 Compose와 분리된다. 별도 의존성이나 신규 애플리케이션 Metric은 추가하지 않는다. 로그 관찰을 위해 이 프로젝트의 API·Worker만 `LOG_LEVEL=info`로 실행한다.

시나리오 시작 전 API의 `/health`와 Prometheus의 `/-/ready`를 최대 30초씩 반복 확인한다. Prometheus의 연결 오류·요청 Timeout·준비 중 응답은 이 대기 시간 안에서 재시도한다. Target의 Scrape 시각과 DB의 Job 완료 시각은 밀리초 값으로 변환해 비교한다.

같은 검증 프로젝트의 컨테이너가 이미 있거나 전용 DB에 기존 Job이 있으면 실행을 중단한다. 동일 명령을 동시에 실행하지 않는다. 이전 실행이 비정상 중단되었다면 아래 정리 안내를 먼저 확인한다.

## 시나리오별 관찰과 성공 조건

### 1. Worker 3개의 정상 작업 분담

- 사전 조건: 독립 Worker 3개가 시작되고 Prometheus에서 서로 다른 `instance`의 Target 3개와 API Target이 `up`이다.
- 실행: `durationMs=1000`인 `slow` Job 12개를 API로 등록한다. 반환된 ID만 추적한다.
- DB·로그: 모든 대상 Job이 `succeeded`, `attempt=1`이고 완료한 `workerId` 집합이 실행 중인 Worker 3개의 UUID와 같다. 각 ID의 `job.claimed`와 `job.succeeded`가 각각 한 번이며 DB의 Worker·Attempt와 일치한다.
- Metric·성공 조건: 각 Worker의 기존 `async_job_succeeded_total`이 DB에서 확인한 해당 프로세스의 완료 수 이상으로 수집된다. 이는 장애 없는 이번 실행에서 중복 Claim이 관찰되지 않았다는 근거다.

### 2. Worker 하나의 SIGKILL 이후 지속성과 Recovery

- 사전 조건: `durationMs=10000`인 `slow` Job 3개가 서로 다른 Worker에서 `processing`이다. 종료할 Job의 DB 소유권과 Claim 로그를 확보한다. `processing`만으로 Handler 진입이나 외부 Side Effect 발생을 단정하지 않는다.
- 실행: 해당 컨테이너에 `SIGKILL`을 보내고 종료 코드 `137`을 확인한다. 종료한 Worker는 재시작하지 않는다. 이후 새로운 `slow` Job 4개를 등록한다.
- 먼저 관찰할 상태: 생존 Worker 2개가 새 Job을 Claim·완료하고 모두 `attempt=1`이다. 이 시점에 종료된 Worker의 Job은 이전 `processing`, `workerId`, `attempt=1`을 유지한다. 생존 Worker 처리와 중단된 Job을 따로 기록한다.
- Recovery·성공 조건: 실제 DB 시각과 기존 Recovery 주기에 맡겨 기다린다. `job.recovered`의 `status=queued`, `previousWorkerId`, `attempt=1`을 확인하고, 다른 Worker의 두 번째 Claim을 DB의 `processing`, `attempt=2`에서 관찰한 뒤 최종 `succeeded`를 확인한다. `queued`는 즉시 재Claim되어 짧게 끝날 수 있어 복구 로그와 이후 DB 상태를 함께 사용한다. 종료한 Target은 제거되고 생존 Target 2개와 API 수집은 유지된다.

`processing_started_at`을 인위적으로 과거로 바꾸지 않는다. Recovery는 정확히 30초 또는 10초에 실행된다고 가정하지 않으며 두 번째 Claim은 최대 80초, Target 수렴은 최대 90초 동안 조건을 반복 조회한다.

### 3. 1→3→1 Scale 변경과 SIGTERM

- 사전 조건: Worker 1개에서 Job 완료와 Target 1개 수집을 확인한다.
- 확장: Compose로 3개까지 늘려 `slow` Job 12개를 처리한다. 추가된 프로세스 2개를 포함해 모두 실제 Claim·완료에 참여하고 Target 3개와 성공 Counter가 수집되어야 한다.
- 축소: Worker 3개가 각각 10초 Job을 Claim한 상태에서 짧은 Job 6개를 추가 등록하고 Compose로 1개까지 줄인다. 기본 `stop_grace_period: 30s`를 그대로 사용한다.
- 로그·DB·성공 조건: 제거된 Worker 각각이 `worker.stopping`의 `SIGTERM` 이후 기존 Job의 성공 결과를 저장하고 종료 코드 `0`으로 종료한다. 종료 요청 뒤 새 `job.claimed`가 없어야 한다. 기존 Job의 소유권·`attempt=1`은 유지되고, 대기 Job 6개는 남은 Worker가 처리한다. 제거된 Target이 빠진 뒤 Target 1개와 API 수집, 성공 Counter 갱신을 확인한다.

## 검증 자료와 정리

실행 시 출력하는 `/tmp/worker-safety-*` 경로에 다음 자료를 보관한다.

- `evidence.json`: 실험 Job ID 전체, 단계별 DB 상태·Target·Counter·Worker 매핑, Claim·완료·Recovery·종료 근거, 실패 또는 전체 통과 결과
- `<container-id>-<실행번호>.jsonl`: 프로세스별 원본 로그. Scale-down으로 컨테이너가 제거되어도 남는다.
- `services.log`: 정리 시점에 남아 있는 Compose 서비스 로그

성공·실패와 SIGINT/SIGTERM 모두 `finally`에서 Worker 종료를 기다리고, 이번 실행에서 반환받은 Job ID만 DELETE한 뒤 해당 프로젝트를 `down`한다. 볼륨은 삭제하지 않는다. 전체 DB·테이블을 비우는 명령이나 `down -v`는 사용하지 않는다. 결과 자료는 저장소 밖에 남아 다음 실행과 구분된다.

호스트 종료나 스크립트 자체의 SIGKILL은 `finally`를 실행할 수 없다. 이 경우 먼저 해당 프로젝트만 중단하고 상태를 확인한다.

```bash
docker compose -p async-job-worker-safety -f compose.yml -f tests/compose/worker-safety.yml ps -a
docker compose -p async-job-worker-safety -f compose.yml -f tests/compose/worker-safety.yml stop worker
```

이전 실행의 `evidence.json`에 있는 ID를 기준으로 전용 DB의 행을 확인·정리한 뒤 같은 Compose 명령의 `down`으로 컨테이너와 Network를 정리한다. Job 등록과 응답 사이에 스크립트가 강제 중단되면 ID가 자료에 남지 않을 수 있으므로, 남은 행은 개별 확인한다. 기존 데이터가 있으면 다음 실행은 계속 거부하며 자동으로 삭제하지 않는다.

## 기존 검증과 이번 검증의 구분

`npm run test:db`의 서로 다른 DB 연결·Row Lock 테스트가 동시 Claim, Retry/Recovery 경합, 원자성, 실행 소유권을 검증한다. 기존 독립 프로세스 테스트는 Claim 직후·Handler 진입 이후 SIGKILL과 재시작, DB 오류 종료, Metrics 서버 정리도 검증한다. 이 SQL·장애 테스트는 복사하지 않는다.

이번 스크립트는 실제 Production Worker 컨테이너 3개의 동시 실행, 하나의 Crash 동안 나머지 프로세스의 지속성, 자연 시간 경과에 따른 복구, Compose Scale 변경·SIGTERM·개별 Prometheus Target 수집을 추가로 검증한다.

`FOR UPDATE SKIP LOCKED`는 동일 Job의 동시 Claim을 보호한다. 30초 Fixed Timeout은 정상 Handler를 Stale로 오판할 수 있고, DB 소유권 검증은 이미 발생한 외부 Side Effect를 취소하지 못한다. 이번 정상·Crash 시나리오의 결과로 모든 장애에서 Handler 중복 실행이 없거나 Exactly-once라고 주장할 수 없다. DB와 원자적이지 않은 로그·Metric은 강제 종료 시 누락될 수 있으며, `processing` 수는 실제 실행 중인 Handler 수와 다를 수 있다.

## 실제 실행 확인

2026-10-24에 위 명령으로 다음 결과를 확인했다. 이 수치는 성능 측정이나 시간 보장이 아니라 한 번의 시나리오 실행 결과다.

| 시나리오 | 확인 결과 |
|---|---|
| 정상 분담 | Job 12개가 Worker 3개에 분산되어 모두 `attempt=1`로 성공, ID별 Claim·완료 로그 각각 1개 |
| Crash 중 처리 지속 | SIGKILL 종료 코드 `137`, 생존 Worker 2개가 새 Job 4개를 완료할 때 중단 Job은 이전 `processing` 유지 |
| 자연 Recovery | 최초 Claim 약 30.2초 뒤 `queued` 복구·다른 Worker의 재Claim, `attempt=2`로 성공 |
| Scale 변경 | 1→3에서 추가 Worker 2개 모두 처리 참여, 3→1에서 제거된 2개가 SIGTERM 후 기존 Job 완료·종료 코드 `0`, 종료 요청 뒤 새 Claim 없음 |
| Prometheus | 단계별 Worker Target 3→2→1→3→1개 모두 `up`, API Target과 완료 이후 성공 Counter 수집 확인 |
| 정리 | 전체 대상 Job 42개 성공 후 해당 ID만 삭제, 검증 컨테이너·Network 정리, Named Volume과 개발용 Compose 유지 |

`npm run typecheck`, `npm run test:observability` 5개, `npm run test:db` 49개도 통과했다. 검증 프로젝트가 실행 중인 상태의 중복 실행은 기존 컨테이너를 변경하지 않고 거부하는 것을 확인했다. 이 지시서의 세 시나리오에 미검증 항목은 없다.
