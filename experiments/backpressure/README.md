# 지속 유입과 Queue 용량 제한 실험

Worker가 처리하는 동안 실제 `POST /jobs`를 지속 호출해, 수락량과 완료량의 차이가 미완료 Job으로 누적되는지 비교한다. Admission SQL·API·Worker는 변경하지 않는다. Worker 수별 성능이나 병목을 확정하는 실험이 아니다.

[2026-10-27 실측 결과](results/2026-10-27/report.md)에 기본 ON/OFF 두 반복의 누적·거부·최종 처리 상태와 관측 한계를 정리했다.

## 실행

저장소 루트에서 Node.js 24, 기존 의존성, Docker Compose 2.24.4 이상, `.env` DB 접속 설정이 필요하다. 전용 애플리케이션 이미지를 **현재 소스로 한 번 빌드**하고, 같은 이미지로 ON/OFF를 실행한다. 개발용 이미지·DB·컨테이너는 사용하지 않는다.

```bash
docker compose -p async-job-backpressure -f compose.yml -f experiments/backpressure/compose.yml build migrate
npm run typecheck
node --test experiments/backpressure/experiment.test.ts
node --env-file=.env --test experiments/backpressure/smoke.test.ts
node --env-file=.env experiments/backpressure/run.ts
```

기본 조건은 **Worker 1개, slow durationMs=1000, 10 jobs/s, 30초, MAX_OUTSTANDING_JOBS=100, ON/OFF 각각 두 번**이다. 첫 반복은 OFF→ON, 다음은 ON→OFF다. 시작·Scrape 준비·수락 Job 완료 대기가 추가되므로 전체는 약 16분 이상 걸린다. 같은 Commit·이미지 ID를 기록하고 반복마다 일치 여부를 검사한다. 실험 도중 이미지 재빌드나 소스 변경을 하지 않는다.

작은 조건으로 실행할 수도 있다. 이는 기본 비교의 실측과 구분한다.

```bash
node --env-file=.env experiments/backpressure/run.ts \
  --modes=off,on --repeats=1 --seconds=2 --rate=10 \
  --duration-ms=100 --limit=2 --interval-ms=100 \
  --out=artifacts/backpressure/manual-smoke
```

`--modes` 기본 `off,on`, `--repeats` 1~3, `--seconds` 1~300, `--rate` 1~100, `--duration-ms` 1~10000, `--limit` 1~100000, `--interval-ms` 100~10000을 지원한다. `--max-inflight`는 기본 4·최대 10이며 응답 지연 시에도 이 수를 넘겨 동시 요청하지 않는다. `--drain-seconds`는 부하 종료 뒤 전역 미완료가 0이 될 때까지 기다리는 Monotonic 제한으로 기본 900초다. 기존 `environment.json`이 있는 출력 경로는 재사용하지 않는다.

## 실행과 안전 조건

1. 프로세스 간 `.lock`과 기존 프로젝트 컨테이너를 확인한다. 같은 도구가 실행 중이거나 컨테이너가 남아 있으면 기존 환경을 중단하지 않고 거부한다.
2. 전용 프로젝트 `async-job-backpressure`의 PostgreSQL·Migration만 시작한다. DB 이름은 `async_job_backpressure`, 포트는 임의 localhost 포트, DB·Prometheus 볼륨은 프로젝트별 Named Volume이다. 이전 `queued`·`processing`이 있으면 **API·Worker 생성 전에 거부**한다. 이전의 완료 행은 허용하고 행 수·ID를 보존한다.
3. Prometheus와 API·Worker 1개를 시작한다. API health, 컨테이너 이미지·설정, 현재 Worker IP의 단일 Target, 새 프로세스 시작 이후의 성공 Scrape와 모든 Counter·Queue 초기값 0을 확인한다. 이 준비 시간은 생산 구간에서 제외한다.
4. Worker가 실행 중인 상태에서 Producer와 DB 샘플링을 시작한다. `performance.now()` 기준으로 i번째 요청을 `i × 1000 / rate` ms에 예약한다. 응답 완료를 기다려 다음 예약을 미루지 않는다. 동시성 상한 때문에 늦어지면 실제 발송 지연을 기록하며 요청을 버리지 않는다. SIGINT/SIGTERM 때도 발송된 요청을 모두 정산한다. 요청별 Timeout은 5초다.
5. DB 단일 집계로 전체 `queued`, `processing`, 합계, 성공·실패·전체 행 수를 기본 약 1초 간격으로 조회한다. Monotonic 경과·조회 왕복 비용과 실제 간격을 남긴다. 시작·부하 종료·처리 완료에도 추가 조회한다. 관찰 pool은 최대 2개이며 별도 완료 확인 조회도 같은 간격으로 수행한다. 관측 쿼리 자체가 추가 부하다.
6. 부하 종료 뒤 전역 미완료가 0이 될 때까지 기존 Worker를 유지한다. 이번 응답 ID와 실제 신규 DB 행을 대조하고 모두 `succeeded`, `attempt=1`, 정상 result·last_error=null인지 확인한다. 응답 유실로 커밋된 미추적 행도 검출한다. 이전 완료 행의 전체 필드도 대조한다. Retry·Recovery·실패·중복 Claim 로그·재시작·예상 외 HTTP·1초 이상 발송 지연은 정상 비교에서 제외하고 중단한다.
7. 완료 이후의 Scrape를 기다려 등록·거부·Claim·성공 Counter를 HTTP/DB와 대조하고 나머지 오류 Counter와 Queue 0을 확인한다. 기존 Metrics 시계열을 보존한다. API·Worker를 정상 종료한 뒤 로그·전역 행을 저장하고 다음 조건으로 이동한다.
8. 최종적으로 사용한 프로젝트의 컨테이너·Network만 `down`한다. DB·Prometheus 볼륨과 행은 보존하며 **전체 DELETE·down -v는 실행하지 않는다**. Smoke 테스트만 자신이 만든 사전 검사·중단 fixture ID를 개별 삭제하고 근거를 보존한다.

Smoke 테스트는 실제 Compose에서 작은 ON/OFF, 기존 프로젝트 무간섭, Ready·Stale 전체 행 보존 및 Worker 생성 전 거부, Producer 실행 중 SIGTERM 후 요청 정산·자료·미완료 보존과 전용 컨테이너 정리를 검증한다. 실제 로그 저장 실패도 재현해 측정 유효성·반복 정리 실패·최종 정리 성공의 구분과 후속 자료 수집을 확인한다. 집계·옵션 테스트는 잘못된 503 code·네트워크/본문 오류의 배타적 분류와 합계, 샘플 비용 산식, 느린 실제 HTTP 응답 중 예약 송신과 동시성 상한을 확인한다.

## 자료와 산식

기본 출력은 Git 제외 경로 `artifacts/backpressure/<UTC 시각>/`다. 출력 경로를 별도로 지정해도 대량 원자료를 Git에 추가하지 않는다. 선별한 작은 요약과 한국어 실측 문서만 `results/`에 둔다.

- `environment.json`, `preflight.json`: Commit·작업 트리·소스 Hash·이미지 ID, Host/Docker, DB 설정, 옵션·실행 순서·초기 전체 행 수.
- `<반복>/before.json`, `ready.json`: 이전 행 ID·상태 합계, 실제 이미지·환경변수·자원 제한, 현재 Targets와 초기 Metrics.
- `requests.jsonl`, `accepted-ids.json`: 목표 요청 수와 각 요청의 예약·발송·완료 시각(ms), status·503 code·ID·오류. 마지막 예약은 29.9초이며 전체 30초를 경과하고 발송 요청을 정산한 시점이 부하 종료다.
- `samples.jsonl`, `load-end-jobs.json`, `jobs.json`: Monotonic 시각의 전역 상태·조회 비용, 부하 종료와 최종 신규 Job 전체 필드.
- `result.json`, 상위 `summary.json`: HTTP 201·정책상 503·기타 응답·네트워크/본문 오류, 평균 발송률·지연·동시성, 유입 구간 수락/완료량과 누적 차이, 관측 최대 미완료·수집 비용·간격, 최종 상태·정상 여부. 네트워크/본문 오류를 먼저 분류하고 나머지를 HTTP 상태별로 분류하므로 네 항목의 합계가 발송 수와 일치한다. 201·503 응답이라도 본문 처리에 실패하면 오류 항목에만 집계한다.
- `metrics-final.json`, `prometheus.json`, `worker.jsonl`, `services.log`: 현재 Target의 마지막 Metrics와 원본 시계열, Claim·성공·거부 Event 근거.
- 실패 시 `failure.json`, `sampling-error.json`, `stopped-global-*.json`, 최종 `final-global-*.json`, `containers-stopped.json`, `cleanup-errors.json`: 비정상 원인과 종료 후 전역 상태·정리 오류. 부하 중 강제 종료 등으로 `jobs.json`이 없어도 DB 전체 행과 서비스 로그를 수집한다.

`valid`는 측정·상태·Metrics 검증의 통과 여부다. 각 반복의 `cleanup.succeeded`는 서비스 종료·로그 및 종료 상태 저장·컨테이너 제거의 성공 여부이며, `null`이면 종료 처리 완료가 아직 기록되지 않은 상태다. 단계별 오류는 `cleanup.errors`에 남기고 나머지 정리도 시도한다. 측정 성공 뒤 정리가 실패하면 `valid=true`, `cleanup.succeeded=false`로 보존하고 실행을 실패 코드로 종료한다. 최상위 `cleanup.json`은 전체 전용 환경의 최종 정리 결과다. 반복 정리 실패 뒤 최종 정리가 성공할 수도 있으므로 둘을 함께 확인한다. 이 필드·파일이 없는 이전 실측은 당시 로그·종료 자료로 정리 상태를 확인하며 원자료를 소급 변경하지 않는다.

평균 발송률 `dispatchRate = (N-1) / ((마지막 발송ms - 첫 발송ms)/1000)`이며 한 요청 이하이면 정의하지 않는다. `windowDispatchRate = N / max(설정 구간초, 마지막 발송ms/1000)`도 남긴다. 수락·완료의 구간 평균은 각각 HTTP 수락 수와 DB 성공 행 증가량을 **Monotonic 부하 종료 경과초**로 나눈 값이다. 이는 해당 구간의 수량 비교이며 정밀 서버 TPS·지연·Speedup 산식이 아니다. `loadEnd`와 `completed`는 서로 다른 시점이다.

정상 기본 조건은 OFF에서 거부 없이 100개 초과 누적, ON에서 정책 거부와 샘플의 상한 유지를 확인해야 통과한다. 표본 사이 순간까지 상한을 지켰다는 증거는 별도의 [Admission 동시성 Integration Test](../../tests/integration/admission-control.test.ts)에 있다. 이 도구의 시간별 표본과 구분한다.

## 해석의 한계와 중단 후 확인

Prometheus는 기존 15초 Scrape·15초 평가로 보조 수집한다. 과거 Worker IP의 시계열은 현재 Target으로 판단하지 않으며 Scrape 사이 변화가 모두 관찰됐다고 주장하지 않는다. DB wall clock 차이와 짧은 PromQL `rate()` 창으로 TPS·Speedup·서버 지연을 계산하지 않는다. 최종 요청 수는 HTTP 결과, 최종 상태는 PostgreSQL을 기준으로 한다.

조건마다 프로세스 Counter를 초기화하지만 완료 행·DB 캐시·Prometheus 데이터는 유지한다. OFF→ON→ON→OFF도 순서 효과를 완전히 제거하지 않는다. 전체 테이블 성장, 관측 조회, 요청·거부 로그가 추가 부하를 만들 수 있다. 정책은 요청 자체의 순간 폭주를 제한하지 않으며 완료 행 보존으로 전체 저장량은 계속 증가한다. CPU·DB 병목, 최적 Worker 수, 다른 부하 패턴의 성능은 이 결과로 확정하지 않는다.

SIGINT/SIGTERM은 Producer를 중단하고 이미 발송한 요청을 정산한 후 API·Worker를 정상 종료한다. Worker는 현재 Job만 마치므로 queued가 남을 수 있다. 실패·중단 실행은 정상 실측에 합치지 않는다. Host 종료·SIGKILL로 정리되지 않았다면 전용 프로젝트만 확인한다.

```bash
docker compose -p async-job-backpressure -f compose.yml -f experiments/backpressure/compose.yml ps -a
docker compose -p async-job-backpressure -f compose.yml -f experiments/backpressure/compose.yml stop api worker
docker compose -p async-job-backpressure -f compose.yml -f experiments/backpressure/compose.yml down
```

실제 DB와 보존 자료를 확인하기 전에 `.lock`을 제거하거나 미완료 Job을 정리하지 않는다. API INSERT 커밋 뒤 응답만 유실될 수 있어 받은 ID만 조회해서는 충분하지 않다. 의도한 실행의 ID와 전역 상태를 함께 확인한다. 도구는 자동으로 미완료 Job을 삭제하거나 재실행하지 않는다.
