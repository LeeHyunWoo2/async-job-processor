# 통합 Failure / Load 실험 도구

지속 유입 중 Worker 하나를 잃었을 때 Admission 상한, 생존 Worker의 처리, 대상 Job의 Recovery·재Claim·완료를 함께 관찰한다. 기존 API·Job·Admission·Recovery 정책을 그대로 실행한다. 이번 산출물은 도구와 작은 실제 Smoke의 검증이며 **120초 기본 4회 실측 결과가 아니다**. [초기 검증](results/2026-10-31/report.md)과 [리뷰 보완 검증](results/2026-11-01/report.md)을 구분해 보존한다.

## 준비와 실행

저장소 루트, Node.js 24, 기존 의존성, `.env`의 DB 접속 값, Docker Engine과 `!override`를 지원하는 Compose가 필요하다. 새 의존성은 없다. 전용 이미지를 현재 소스로 한 번 빌드한다. 비교 중 이미지를 재빌드하거나 코드를 변경하지 않는다. 실행기는 이미지 안의 전체 `src`·package Hash를 현재 파일과 대조한다.

```bash
docker compose -p async-job-failure-load \
  -f compose.yml -f experiments/failure-load/compose.yml build migrate
node --test experiments/failure-load/experiment.test.ts
node --env-file=.env --test experiments/failure-load/smoke.test.ts
```

기본 실측 명령은 아래와 같다. PR 1 검증에서는 실행하지 않았다. PR 2에서 먼저 아래 Pilot 결과를 확인한다. Pilot 3개 조건이 부적합·판단 보류이면 사용자 판단을 받은 뒤 조건과 후속 실행을 결정한다. Pilot 명령이 자동으로 main을 시작하지는 않는다.

```bash
node --env-file=.env experiments/failure-load/run.ts \
  --purpose=main --out=artifacts/failure-load/main-run
```

기본은 API 1개, Worker 3개, Backpressure ON, 미완료 상한 30, 일반 `slow(durationMs=1000)` 2.4 jobs/s × 120초다. 일반 Job 예약은 **288개**이며 20초에 별도 `slow(durationMs=10000)` **1개**를 추가한다. 정상→장애→장애→정상 순서이며 반복마다 API·Worker를 새로 만든다. 장애 조건의 사망 Worker는 해당 반복 중 재시작하지 않는다. 2.4 jobs/s는 Pilot 전의 초기 후보이며 PR 2에서 검토한다.

작은 정상·장애 Smoke를 수동으로 실행할 수도 있다. 이는 Queue와 거부를 빨리 관찰하기 위한 별도 조건이며 기본 조건의 성능 결과로 사용하지 않는다.

```bash
node --env-file=.env experiments/failure-load/run.ts \
  --purpose=smoke --modes=normal,failure --repeats=1 --seconds=12 --rate=6 \
  --target-at-seconds=1 --limit=8 --interval-ms=500 --drain-seconds=90 \
  --out=artifacts/failure-load/manual-smoke
```

PR 2의 Worker 2·3개 처리 능력 Pilot은 `--purpose=pilot`과 `normal`로 각각 관찰한다. Worker 2개는 이 목적에서만 허용하고 과부하를 유효한 용량 관측으로 취급한다. 대상 10초 Job은 유지되므로 대상 Handler 실행 구간과 그 밖의 구간을 나눠 비교한다. 한 번의 전체 평균을 순수 일반 Job 처리 능력으로 해석하지 않는다.

```bash
node --env-file=.env experiments/failure-load/run.ts \
  --purpose=pilot --modes=normal --repeats=1 --workers=2 --out=artifacts/failure-load/pilot-two
node --env-file=.env experiments/failure-load/run.ts \
  --purpose=pilot --modes=normal --repeats=1 --workers=3 --out=artifacts/failure-load/pilot-three
```

| 옵션 | 기본값·범위 |
|---|---|
| `--purpose` | `main` / `pilot` / `smoke`, 기본 `main`; Pilot은 normal만 허용 |
| `--modes` | `normal,failure`, 중복 없이 지정 |
| `--repeats` | 2, 1~2; 두 번째 반복은 modes 역순 |
| `--workers` | 3, 2~3; Worker 2개는 Pilot의 normal만 허용 |
| `--seconds` / `--rate` | 120초 / 2.4 jobs/s; 정수 2~300초 / 소수 0.1~100 |
| `--duration-ms` | 일반 Job 1000ms, 정수 1~10000 |
| `--target-at-seconds` | 20초, 생산 구간 안의 0 이상 소수; 대상 duration은 10000ms 고정 |
| `--limit` / `--interval-ms` | 30 / 1000ms; 정수 1~100000 / 100~5000ms |
| `--max-inflight` | 4, 1~32; 대상과 일반 요청의 공통 상한 |
| `--drain-seconds` | 300, 정수 1~300; 생산 종료·발송 요청 정산 후 최대 대기 |
| `--out` | Git 제외 `artifacts/failure-load/<UTC 시각>`; 기존 environment.json이 있는 경로 재사용 거부 |

## 실행과 데이터 보존

1. 실행 잠금과 동일 Compose 프로젝트의 기존 컨테이너를 확인한다. 컨테이너가 남아 있으면 실행·중단 상태와 무관하게 변경 없이 거부한다.
2. 프로젝트 `async-job-failure-load`, DB `async_job_failure_load`, 이미지 `async-job-failure-load:local`을 사용한다. localhost 동적 포트와 프로젝트별 PostgreSQL·Prometheus Named Volume으로 개발 환경과 분리한다. PostgreSQL·Migration만 시작한 뒤 전체 행을 읽어 `queued`·`processing`이 있으면 API·Worker **생성 전에 거부**한다.
3. 매 반복에 새 API·Worker를 시작한다. 이전 완료 행의 모든 필드를 보존·대조하고, 실제 컨테이너 ID·Worker UUID·이미지·명령·Node 프로세스·시작 시각·환경변수·자원 제한·재시작 횟수를 기록한다. Prometheus의 현재 Worker IP Target과 Scrape를 확인하고 부하를 시작한다.
4. `performance.now()` 기준 `i × 1000 / rate` ms에 일반 요청을 독립 예약한다. 소수 Timer의 조기 깨움도 재확인한다. 요청 완료를 기다리지 않으며, 동시성 상한·구간 종료·중단으로 보낼 수 없는 요청은 미발송으로 남긴다. 생산 구간 뒤 늦은 Burst로 보정하지 않는다. HTTP Timeout은 5초다. 대상 Job은 같은 동시성 상한 안에서 별도 예약으로 정확히 한 번 시도한다.
5. 대상 201 ID의 DB `processing`·attempt=1·소유 UUID, 해당 컨테이너의 `job.handler_started`·실행 상태를 교차 확인한다. 장애 조건은 직전 DB 행이 유지되고 DB 기준 Claim 경과와 추가 조회 비용이 합쳐 2초 미만일 때만 SIGKILL한다. 실제 ExitCode 137·OOM 아님·생존 2개·대상 행 잔류를 확인한다. 이미 완료됐거나 대상이 불명확하거나 주입이 늦으면 조건 미성립으로 남긴다.
6. Producer는 원래 구간까지 계속한다. 기본 약 1초 DB 상태·연결 수 표본, 약 200ms 대상 상태 관측, Worker 로그 Stream, Docker Stats를 병행한다. **SIGKILL 확인 후 발송·201 수락된 일반 Job**의 성공 로그·DB 소유권을 확인한 다음 대상의 첫 processing 잔류를 다시 읽는다. 확인된 생존 컨테이너·UUID가 일치해야 하며 장애 전 요청의 늦은 성공 로그는 증거로 인정하지 않는다. 원래 `processing_started_at`의 30초 Stale 기준과 기존 약 10초 Recovery 검사 주기를 그대로 사용한다.
7. 생산 종료 후 Worker를 유지하고 전역 미완료 0까지 최대 300초 기다린다. 대상의 Recovery·두 번째 Claim·두 번째 processing·성공과 모든 수락 Job의 최종 행·예상 attempt·result, 전체 신규 행과 수락 ID를 대조한다. 응답 유실 후 Commit된 미추적 행도 원자료에 드러낸다. 완료된 기존 행은 삭제하지 않는다.
8. API·Worker를 정상 종료하고 각 자료 저장·DB 조회·컨테이너 제거를 독립적으로 시도한다. 마지막에는 전용 프로젝트만 `down`하며 **볼륨과 Job 행은 유지**한다. Smoke 테스트만 자신이 만든 사전 검사·중단 fixture를 ID로 한정해 정리한다.

## 네 가지 판정

- `scenario.valid`: 실제 구성·정상 Worker 전체의 Job 완료(대상 포함), 일반 예약 수·한 주기 이내 발송 지연, 대상 수락·실행을 확인한다. 장애에서는 SIGKILL 전 전체 Worker의 Handler 진입(대상 포함)과 실행 중 SIGKILL·지속 유입·장애 이후 Queue·정책상 503를 확인한다. `main`의 정상 3개 조건에는 아래 안정성 기준도 적용한다. 사망 Worker에게 장애 전 일반 Job 완료를 강제하지 않는다. 503 미관측은 실험 조건 미성립이며 시스템 버그의 근거로 쓰지 않는다.
- `functional.status`: `passed` / `failed` / `incomplete`를 구분한다. 표본 상한, HTTP 계약, 기존 행 보존, 모든 수락 행의 상태·result·attempt·소유권과 Job별 Claim·Handler·완료 로그를 대조한다. 장애 대상만 attempt=2, 나머지는 attempt=1이다. 장애 후 신규 수락 Job의 생존 처리 증거와 Recovery 확인 시 DB 기준 최초 Claim 경과 30초 이상도 판정한다. 누락 근거와 실제 위반은 각각 `missing`·`failures`로 남긴다.
- `measurement.valid`: 계획·미발송·HTTP 결과·신규 행/ID 대응·필수 사건/표본·수집 오류를 확인한다. `timing`은 큰 DB 표본 간격·서로 다른 로그 Stream의 도착 순서 역전을, `wallAlignment`는 Wall/Monotonic 경과 차이 1초 초과를 별도 표시한다. 시간 분석만 제한될 사유를 전체 측정 무효와 구분한다.
- `cleanup.succeeded`: 반복의 종료·자료 보존과 최종 전용 환경 정리를 따로 기록한다. 상위 `cleanup.json`은 전체 환경 종료 결과다. 측정 성공 뒤 저장/정리가 실패하면 측정 판정을 바꾸지 않는다. `result.json` 자체 저장이 실패하면 상위 `summary.json`과 독립 근거 파일을 확인한다.

정상 3개 Worker의 안정성은 대상 Job 완료 관측 후 생산 종료까지의 온전한 **10초 구간**으로 판단한다. 2구간 연속 정책상 503가 발생하거나, 3구간의 outstanding 표본 평균이 연속 증가하면서 첫·끝 평균 차이가 3개 이상이면 `unsuitable`이다. 단일 구간의 503나 작은 평균 변동만으로 부적합 처리하지 않는다. 위반을 아직 확인하지 못한 경우, 구간마다 DB 표본 2개 이상·일반 발송이 있고 온전한 구간이 3개 이상이어야 적합을 판단하며, 부족하면 `inconclusive`다. 구간·평균·거부 수·증가 근거·미평가 꼬리 구간 길이는 `stability`에 남긴다. 대상 Handler 실행 중 과도 구간은 이 기준에서 제외한다.

`scenario.suitability`는 실험 목적에 따른 적합성을 구분한다. `main`의 정상 3개가 부적합·판단 보류이면 시나리오를 미성립으로 기록한다. Pilot 3개는 용량 관측 자체의 유효성을 유지하면서 별도 적합성을 보고한다. 두 경우 모두 `requiresUserDecision=true`, exit code 1로 끝내고 **다음 반복을 실행하지 않는다**. 사용자 판단 후 조건을 정해 새 Pilot과 본 실측을 진행한다. Pilot 2개와 Smoke에서는 정상 3개 기준을 강제하지 않으며 `stability.applied=false`, 적합성은 `observation-only`다. 따라서 과부하 Smoke 통과는 정상 대조군의 부하 설계 적합성을 인증하지 않는다.

전체 실행의 exit code는 시나리오·기능·필수 측정·정리 또는 사용자 판단 요구가 있으면 1이다. RTT/TPS의 예상 방향이나 특정 성능 수치를 기능 성공 조건으로 강제하지 않는다. 시간 분석 제한은 해당 하위 판정으로 보고한다.

## 자료와 측정 경계

원자료는 `artifacts/failure-load/`, 선별한 작은 검증 기록만 `results/`에 둔다.

| 파일 | 근거 |
|---|---|
| 상위 `environment.json`, `preflight.json` | revision·branch·dirty 상태·소스 Hash·이미지·Host/Docker/Node·옵션·순서·DB 설정·시작 전 전체 행 |
| 반복 `before.json`, `ready.json`, `containers-*.json` | 기존 전체 행, 컨테이너↔UUID, 실제 프로세스·환경·제한·생존·재시작 상태 |
| `requests.jsonl`, `requests.json` | kind별 예약·발송·완료 ms, 원본 HTTP 본문·ID·오류·미발송 사유; JSON은 중단 시 복구 저장도 겸함 |
| `samples.jsonl` | queued·processing·outstanding·succeeded·failed·전체 행·DB 연결/활성 연결, 실제 표본 간격·조회 RTT·Clock 진단 |
| `events.jsonl`, `events.json`, `observations.jsonl`, `target-states.jsonl` | 로그 원본과 실행기 최초 수신 시각, 주입 요청/확인·잔류·생존 처리·Recovery·재Claim·완료, DB 상태 변경의 조회 시작/확인 비용 |
| `clock.json`, `resources.jsonl`, `prometheus.json` | Monotonic/Wall anchor·진단, Docker Stats 원자료, 현재 Target과 원본/현재 시계열·15초 range query |
| `load-end-jobs.json`, `jobs.json`, `stopped-jobs.json`, 상위 `final-jobs.json` | 생산 종료·정산/중단·서비스 종료 시 전체 Job 행 |
| `evidence.json`, `result.json`, 상위 `summary.json`, `cleanup.json`, 실패 시 `failure.json`, `services.log` | 재집계 가능한 근거·네 축 판정·단계별 결과·수집/정리 오류·서비스 원본 로그 |

HTTP 분류는 먼저 네트워크/본문 오류, 그 외 응답을 수락 201+ID·정책 거부 503+code·기타 HTTP로 나눈다. 따라서 `sent = accepted + rejected + otherHttp + networkOrBodyErrors`, `planned = sent + unsent`다. 일반과 대상은 항상 별도 집계한다. `dispatchRate`는 `(N-1) / 첫 발송~마지막 발송 경과초`, RTT는 발송부터 본문 처리 완료까지의 클라이언트 경과다. `rttMsByOutcome`으로 수락 `accepted201`·정책 거부 `policyRejected503`·기타 HTTP·네트워크/본문 오류의 분포를 분리한다. 기존 `rttMs`는 전체 혼합 분포다. PR 2에서는 201과 503을 각각 비교해 빠른 거부 응답의 비중 증가를 API 응답 성능 개선으로 해석하지 않는다.

단계별 결과는 **실행기에서 관측한 사건** 사이의 Monotonic 반개구간 `[start, end)`에 실제 발송을 배치한다. 수락률은 해당 구간 발송 중 수락 비율, `logObservedCompleted*`는 해당 구간에 수신한 `job.succeeded` 수와 초당 수신량인 **로그 관측 완료량/완료율**이다. 일반·대상·요청 미대응을 분리한다. `dbCompletionCrossCheck`는 단계 안에 완전히 포함된 DB 표본 2개 사이의 전체 성공 수 변화와 같은 관측 구간의 성공 로그 수·차이를 제시한다. 조회 비용·표본 시각과 로그 전달 때문에 일시 불일치할 수 있으며 단계 전체의 정확한 DB 완료 수로 대체하지 않는다. 두 표본이 없는 짧은 단계는 비교 불가로 표시한다. 최종 `completion`에는 전체 DB 성공 수 변화와 전체 성공 로그 수·일치 여부를 별도로 남긴다. 정상에서도 대상 Handler가 실행되는 10초 동안 일반 Job Worker는 2개이므로 사건 전후를 나눠 비교한다.

DB 관찰 pool은 최대 4개다. 상태 표본과 대상 조회·정산 자체도 DB 부하다. 표본은 순간 상한 보장의 새 증명이 아니며 원자적 Admission의 근거는 기존 [동시성 Integration Test](../../tests/integration/admission-control.test.ts)다. 대상 DB 상태 조회는 약 200ms 간격이며 `readStartedMs`·`observedMs`·query 비용을 기록한다. 로그의 최초 Stream 수신과 후속 확인 시각은 구분하지만 프로세스 내부 Flush·전송 지연의 상한은 알 수 없어 `observationDelayMs=null`로 명시한다. DB·다른 프로세스 Wall Clock을 차감해 정밀 Recovery 시간을 계산하지 않는다. DB 안에서 원래 Claim부터 지난 시간을 조회해 Recovery **확인 시점**에 30초 미만이면 기능 위반, 값이 누락되면 근거 부족으로 판정한다. 30초 이상이라는 확인만으로 Recovery 트랜잭션의 정확한 발생 시각까지 증명하지는 않는다.

Prometheus의 15초 Scrape·DNS 갱신 사이에 사망 Worker의 마지막 Counter가 누락될 수 있다. `current`는 현재 Target만 필터링하고 `raw`/`range`에는 과거·사망 인스턴스를 남긴다. 종료 Target 제거는 기다리지만 사망 Counter 합계를 수락·완료 판정에 쓰지 않는다. 짧은 Smoke에는 충분한 Scrape나 `rate()` 창이 없을 수 있다. Docker Stats도 보조 표본이며 사망 후 해당 컨테이너 표본이 사라질 수 있다. 반복 간 완료 행·캐시·Prometheus 데이터는 유지되므로 순서·테이블 성장·관측 부하의 영향이 남는다.

## 중단과 재실행

예외·SIGINT/SIGTERM·수집 저장 오류에서는 발송 요청을 정산하고 가능한 요청·행·사건·컨테이너·로그를 저장한 뒤 나머지 정리를 계속한다. Worker는 현재 Job만 마치므로 중단 후 queued가 남을 수 있다. 정산 제한 초과 시에도 잔여 행을 보존한다. 다음 실행은 미완료 행을 발견하면 거부한다. 도구는 자동 삭제·재처리로 결과를 보정하지 않는다.

Host 종료나 도구 자체 SIGKILL 뒤에는 기존 실행이 없는지 확인하고 전용 프로젝트만 조사한다. 인증정보가 포함될 수 있는 전체 Compose config/inspect Env는 자료에 저장하지 않는다.

```bash
docker compose -p async-job-failure-load -f compose.yml -f experiments/failure-load/compose.yml ps -a
docker compose -p async-job-failure-load -f compose.yml -f experiments/failure-load/compose.yml logs --no-color > /tmp/failure-load-preserved.log
docker compose -p async-job-failure-load -f compose.yml -f experiments/failure-load/compose.yml down
# 실행 프로세스와 컨테이너·DB 상태를 확인한 뒤 잠금만 남은 경우:
rmdir artifacts/failure-load/.lock
```

`down -v`·전역 DELETE는 사용하지 않는다. 보존된 미완료 행의 처리 방향은 근거를 확인한 뒤 결정한다. 날짜별 `results/YYYY-MM-DD/report.md`·`summary.json`만 Git 추적 예외로 허용하고 원자료는 계속 제외한다. PR 2에는 Pilot으로 2.4 후보·Worker 2/3 처리 구간을 확인하고 기본 4회 실측 및 결과 보고서를 작성하는 작업이 남아 있다.
