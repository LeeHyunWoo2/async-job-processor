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

- Basic Job Processing 구현 완료
  - `POST /jobs`, `GET /jobs/:id` 및 type별 payload 검증, 추가 필드 거부
  - `slow`는 1~10,000ms 정수만 받아 비동기 대기 후 성공하고, `fail`은 빈 payload로 의도적인 실패 발생
  - 등록 시 DB 초기값 사용 및 독립 Job 생성, 기본 실행의 최대 시도 횟수는 1
  - PostgreSQL 단일 SQL 및 `FOR UPDATE SKIP LOCKED` 기반 Claim
  - 순차 polling으로 Handler 실행 후 성공 결과 또는 실패 정보 저장
  - 현재 status·workerId·attempt를 확인하여 이전 실행의 결과 저장 방지
  - DB 오류 시 재실행 없이 Worker 중단, 종료 요청 시 현재 Job 완료 후 DB 연결 정리

- 검증 완료
  - entity와 schema 일치, ORM 저장·조회, DB 기본값 및 제약 확인
  - API 입력 거부와 등록부터 정적 Handler 성공·실패 및 결과 조회까지 확인
  - 실제 PostgreSQL의 두 연결에서 동시 Claim 및 Row Lock 충돌을 유도하여 중복 방지와 `SKIP LOCKED` 확인
  - 실행 소유권 보호, Handler 실패 후 다음 Job 처리, DB 오류 시 중단 확인
  - 독립 Worker 프로세스의 `SIGINT`·`SIGTERM` 처리 및 현재 Job 완료 대기 확인
  - 실제 API·Worker 프로세스와 HTTP 통신으로 기본 실행 흐름 및 정상 종료 확인
  - typecheck 및 기존·신규 DB 테스트 통과

## 막힌 문제

- 없음
