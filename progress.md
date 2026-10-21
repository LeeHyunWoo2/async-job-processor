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
  - 공통 Pino JSON Logger로 DB 상태 변경 후 Job Event 6개와 Worker Event 3개 기록
  - API의 Request ID·jobId와 Worker의 jobId·workerId·attempt로 요청과 실행을 추적하며 Recovery 수행 Worker와 이전 Claim 소유자를 구분
  - 독립 Registry와 API·Worker `/metrics`에서 Gauge 3개·Counter 6개·Histogram 1개 제공
  - Scrape마다 단일 PostgreSQL 집계로 Queue·Ready Queue·Processing 수 조회, 조회 오류는 응답 실패로 구분
  - Retry와 Recovery·최종 실패를 구분해 집계하며 Handler 시간은 DB 결과 저장과 분리
  - 계측 오류를 Job 처리에서 격리하고 기존 Graceful Shutdown·오류 종료에 맞춰 Metrics 서버 정리
  - PostgreSQL 준비·Migration 이후 API·Worker를 시작하는 공통 이미지 구성
  - Prometheus의 API·Worker 수집, Named Volume 저장 및 15일 보관
  - 기존 로컬 Node.js 직접 실행 유지

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
  - Compose의 실제 Job 생성·성공·실패·Retry·Recovery 후 두 Prometheus Target에서 새 지표 수집 확인
  - Prometheus 중단 중 Job 처리 유지와 재시작 후 지표 수집 재개 확인

## 현재 범위

- Handler 실패의 Retry와 Worker Crash 이후 재실행을 지원하는 At-least-once 지향 처리이며, 최대 Claim 횟수 제한으로 Handler 실행을 무조건 한 번 이상 보장하지는 않음
- Fixed Timeout은 정상 Worker도 Stale로 오판할 수 있으며, 실행 소유권은 DB Job 상태를 보호하지만 Handler 중복 실행이나 외부 Side Effect를 취소하지는 못함
- Recovery는 정상 또는 재시작한 Worker가 Loop로 돌아올 때 수행되므로 모든 Worker 중단·Handler 정체 시 지연될 수 있음
- Idempotency는 테스트 전용 PostgreSQL Unique Key 예시로 검증했으며 외부 시스템의 Exactly-once나 범용 중복 방지는 제공하지 않음
- PostgreSQL이 Job 상태의 기준이며 로그·Metrics는 상태 변경과 원자적이지 않은 Best-effort 관측이다. 강제 종료 시 기록이 누락될 수 있고 Counter는 프로세스 재시작 시 초기화됨

## 막힌 문제

- 없음
