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

- 검증 완료
  - entity와 schema 일치, ORM 저장·조회, DB 기본값 및 제약 확인
  - API 입력 검증, 등록·조회, 기존 Handler 및 Retry 후 성공·최대 횟수 소진·즉시 실패 확인
  - 실제 PostgreSQL의 두 연결에서 동시 Claim 및 Row Lock 충돌을 유도하여 중복 방지와 `SKIP LOCKED` 확인
  - DB Backoff 및 실행 가능 시각 조건, Retry 이후 동시 Claim의 중복 방지 확인
  - Retry·최종 결과의 실행 소유권 보호, 다음 Job 처리, DB 오류 전파와 부분 변경 방지 확인
  - 독립 Worker 프로세스의 `SIGINT`·`SIGTERM` 처리 및 현재 Job 완료 대기 확인
  - 실제 API·Worker 프로세스와 HTTP 통신으로 기본 실행과 Retry 전체 흐름 및 DB 오류 시 실패 종료 확인
  - typecheck 및 기존·신규 DB 테스트 통과

## 현재 범위

- Handler 실패의 Retry를 처리하며 Worker Crash Recovery와 중복 Side Effect 방지는 이번 단계에 포함하지 않음

## 막힌 문제

- 없음
