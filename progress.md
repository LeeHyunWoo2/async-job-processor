# 진행 상황

## 완료

- 실행 환경 구축 완료
  - Node.js native TypeScript, Fastify API, 독립 Worker
  - Docker Compose 기반 PostgreSQL 및 MikroORM 연결
  - `GET /health` 제공

- Job persistence model 및 migration 구현
  - `defineEntity` 기반 Job entity
  - PostgreSQL native enum 기반 Job status
  - retry / crash recovery / 관측성을 고려한 Job schema
  - MikroORM Migrator 구성 및 실제 PostgreSQL migration 적용

- Job Processing 및 Retry / Backoff 구현 완료
  - `POST /jobs`, `GET /jobs/:id` 및 type별 payload 검증, 추가 필드 거부
  - `slow`는 대기 후 성공, `fail`은 즉시 최종 실패, `flaky`는 빈 payload로 세 번째 실행에서 성공
  - 등록 시 DB 초기값 사용 및 독립 Job 생성, 최초 실행을 포함한 기본 최대 시도 횟수는 3
  - PostgreSQL 단일 SQL 및 `FOR UPDATE SKIP LOCKED` 기반 Claim
  - 순차 polling으로 Handler 실행, `RetryableJobError`만 남은 시도 횟수 내에서 재시도
  - PostgreSQL 시각 기준 1초·2초 Backoff를 `queued + availableAt`으로 예약하고 다음 Job 처리 계속
  - Retry 예약과 최종 결과 저장을 분리하고 단일 조건부 UPDATE로 실행 소유권 및 원자성 보호
  - Retry 예약 시 마지막 실행 정보를 유지하며 Claim에서만 attempt와 실행 정보 갱신
  - DB 오류 시 재실행 없이 Worker 중단, 종료 요청 시 현재 Job 완료 후 DB 연결 정리

- Crash Recovery 구현 완료
  - Worker 시작 시와 이후 약 10초 간격으로 기존 순차 Polling Loop에서 복구 검사
  - PostgreSQL 시각 기준 마지막 Claim 이후 30초를 초과한 `processing`을 단일 조건부 UPDATE로 복구
  - 남은 Claim 횟수가 있으면 즉시 실행 가능한 `queued`, 소진했으면 `failed`로 전환
  - Recovery는 마지막 Claim 정보와 attempt를 유지하고 이전 실행 결과의 불확실성을 오류 정보로 기록
  - Row Lock과 조건 재평가로 동시 Recovery를 보호하며 기존 소유권 검증으로 이전 실행의 결과 저장·Retry 예약 차단

- Job Lifecycle Observability 및 Compose 통합 실행 환경 구축 완료
  - 공통 Pino JSON Logger로 Job Lifecycle·등록 거부 및 Worker Event 기록
  - API의 Request ID·jobId와 Worker의 jobId·workerId·attempt로 요청과 실행을 추적하며 Recovery 수행 Worker와 이전 Claim 소유자를 구분
  - 독립 Registry와 API·Worker `/metrics`에서 Gauge 3개·Counter 7개·Histogram 1개 제공
  - Scrape마다 단일 PostgreSQL 집계로 Queue·Ready Queue·Processing 수 조회, 조회 오류는 응답 실패로 구분
  - Retry와 Recovery·최종 실패를 구분해 집계하며 Handler 시간은 DB 결과 저장과 분리
  - 계측 오류를 Job 처리에서 격리하고 기존 Graceful Shutdown·오류 종료에 맞춰 Metrics 서버 정리
  - PostgreSQL 준비·Migration 이후 API·Worker를 시작하는 공통 이미지 구성
  - Prometheus의 API·Worker 수집, Named Volume 저장 및 15일 보관
  - Compose의 다중 Worker 실행과 DNS A 기반 개별 Target 탐색·제거, 기본 `job`·`instance` Label로 Metrics 구분
  - 기존 로컬 Node.js 직접 실행 유지

- Worker 수별 처리량·자원 사용 반복 실험 도구 및 기본 실측 완료
  - 기존 API로 Batch를 사전 등록하고 ID별로 분리하며, 전용 Compose에서 Worker·Prometheus 준비 이후 처리 시작
  - Worker 1·2·4·8개를 조건별 두 번 측정하고 총 3,840개 Job의 첫 시도 성공·프로세스별 단일 Claim·완료를 확인
  - DB 기준 처리량·Speedup·Efficiency와 반복 산포, Handler·Queue 추세, 복수 CPU·메모리·DB 연결 샘플 보존
  - 실측 요약과 원자료를 분리하고 wall clock 변동·짧은 Prometheus 창·평가 해상도의 한계를 결과 문서에 기록

- PostgreSQL 기반 Enqueue Admission Control 구현 완료
  - 기본 ON·최대 100개의 `queued + processing` 상한과 실험 비교용 OFF 설정 제공
  - 동일 READ COMMITTED 트랜잭션에서 Advisory Lock 획득 후 별도 COUNT·INSERT로 동시 등록 보호
  - 기존 입력 검증·성공 응답·Worker 상태 전이를 유지하며 용량 초과만 503·거부 Counter·요청별 로그로 구분
  - 로컬·Compose 설정 전달 및 잘못된 설정의 시작 오류 처리

- Producer/Consumer 불균형과 Admission Control 지속 유입 실험 완료
  - 전용 Compose에서 Worker 처리와 동시에 Monotonic 기준 지속 등록·DB 상태 표본·배타적인 HTTP 결과 집계·기존 Metrics 수집
  - 기본 조건 ON/OFF 각각 두 번에서 OFF의 미완료 누적과 ON의 503 거부·관측 상한 유지, 모든 수락 Job의 첫 시도 성공 확인
  - 측정 유효성과 종료 처리 결과를 구분하고 시작 조건 거부·중단·로그 저장 실패 시 자료·기존 행 보존 검증, 재현 도구·선별한 실측 요약과 원자료 분리

- 검증 완료
  - 실제 PostgreSQL에서 schema·기본값·제약, API 입력·등록·조회, Handler와 Retry / Backoff 확인
  - 서로 다른 DB 연결과 실제 Row Lock으로 동시 Claim·Recovery, 결과 저장과 Recovery 경합 및 소유권 보호 확인
  - Timeout 전후 상태 전이, 반복 Crash의 Attempt 소진, 정상 Handler의 Stale 오판과 중복 실행 시 DB 상태 보호 확인
  - 실제 약 10초 주기 검사, DB 오류의 원자성과 Worker 중단, 정상 종료 시 현재 Job 완료 대기 확인
  - 독립 테스트 Worker의 Claim 직후·Handler 진입 이후 `SIGKILL`과 Production Worker 재시작으로 복구·두 번째 Claim·성공 확인
  - 테스트 전용 PostgreSQL Side Effect에서 결과 저장 전 중단을 재현하고 중복 발생 및 jobId Unique Key의 중복 INSERT 방지 확인
  - typecheck와 전체 PostgreSQL 테스트 통과, 기존 Claim·Retry / Backoff·독립 프로세스 종료 처리 회귀 없음
  - JSON Event·실행 정보·Registry 격리와 실제 DB Gauge, Retry·Recovery 집계 및 Handler Duration 확인
  - DB 저장 실패·소유권 상실·동시 Recovery의 관측 정확성과 Logger·Metric 오류의 Job 처리 격리 확인
  - Compose의 실제 Job 생성·성공·실패·Retry·Recovery 후 API·Worker Prometheus Target에서 새 지표 수집 확인
  - Prometheus 중단 중 Job 처리 유지와 재시작 후 지표 수집 재개 확인
  - 실제 Compose Worker 3개의 정상 분담·Job별 단일 Claim, SIGKILL 중 생존 Worker의 새 Job 처리와 자연 Timeout 이후 다른 Worker의 두 번째 Claim·성공 확인
  - Compose Worker 1·3·5개 개별 수집과 1→3→1 변경 시 신규 Worker 참여·현재 Job 완료 후 SIGTERM 정상 종료·Target 제거, Worker별 완료 수와 Counter 수집 확인
  - 동일 시각의 인스턴스별 성공 처리율 합과 전체 집계 PromQL 결과 일치 확인
  - 기본 Compose와 로컬 단일 Worker의 Job 성공·Metrics 제공 및 정상 종료 확인
  - 실험 집계 산식·비정상 Batch 제외와 실제 잔여 Ready·Stale Job의 Worker 시작 전 거부·행 보존 확인
  - Admission의 상태 집계·슬롯 유지와 반환·ON/OFF·입력 검증 우선순위, 서로 다른 10개 연결의 실제 잠금 경합에서 잔여 1개 슬롯만 수락함을 확인
  - COUNT·INSERT·Commit 오류의 Rollback·잠금 해제·관측 정확성과 거부 계측 오류 격리, 독립 API의 환경변수 적용 확인

## 현재 범위

- Handler 실패의 Retry와 Worker Crash 이후 재실행을 지원하는 At-least-once 지향 처리이며, 최대 Claim 횟수 제한으로 Handler 실행을 무조건 한 번 이상 보장하지는 않음
- Fixed Timeout은 정상 Worker도 Stale로 오판할 수 있으며, 실행 소유권은 DB Job 상태를 보호하지만 Handler 중복 실행이나 외부 Side Effect를 취소하지는 못함
- Recovery는 정상 또는 재시작한 Worker가 Loop로 돌아올 때 수행되므로 모든 Worker 중단·Handler 정체 시 지연될 수 있음
- Idempotency는 테스트 전용 PostgreSQL Unique Key 예시로 검증했으며 외부 시스템의 Exactly-once나 범용 중복 방지는 제공하지 않음
- PostgreSQL이 Job 상태의 기준이며 로그·Metrics는 상태 변경과 원자적이지 않은 Best-effort 관측이다. 강제 종료 시 기록이 누락될 수 있고 Counter는 프로세스 재시작 시 초기화됨
- Admission 상한은 동일 DB에서 모든 API가 ON·동일 제한값·공통 잠금 키를 사용하는 등록 경로에 한정되며 OFF·직접 INSERT는 우회 가능함. COUNT·잠금 대기·거부 로그 비용은 남으며 순간 요청 폭주나 완료 행을 포함한 전체 저장량은 제한하지 않음
- 지속 유입 실측의 DB 표본과 기존 동시성 테스트의 상한 보장은 별도 근거이며, 반복 순서·완료 행 누적·관측 및 로그 부하의 영향은 통제하지 않음
- 기본 실험은 Timer 기반 대기에 한정하며, 관측된 시각 변동을 고려한 처리량의 정밀 해석·병목 원인·최적 Worker 수는 확정하지 않음

## 막힌 문제

- 없음
