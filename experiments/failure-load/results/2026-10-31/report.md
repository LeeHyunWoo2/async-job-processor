# PR 1 도구 검증 — 2026-10-31

전용 Failure / Load 실행기·집계·Compose·테스트를 구현하고 작은 실제 정상/장애 경로와 오류 경계를 검증했다. **120초 기본 4회 실측과 Pilot은 수행하지 않았다.** 아래 수량은 도구 검증 자료이며 성능 비교 결론으로 사용하지 않는다. 선별 수치는 [summary.json](summary.json), 실행법과 판정 기준은 [README](../../README.md)를 따른다.

## 실제 환경

- Node.js v24.21.0, Docker Engine 29.8.2, Compose 5.5.1, Docker Desktop/WSL2 Linux.
- Intel i7-7700K, 논리 CPU 8개, Docker 메모리 약 7.8GiB. 컨테이너별 실제 CPU·메모리 제한과 프로세스 정보는 각 `ready.json`에 보존했다.
- 브랜치 `feature/failure-load`, 작업 시작 revision `4b33907e26e076c73f1f79adc1d2d6d3d0bd4c94`. 미커밋 도구의 파일 Hash·작업 트리·실행 옵션은 각 `environment.json`에 남겼다. 애플리케이션 이미지 ID는 `sha256:c0ce0a1e8a322189cf1e387318ce8500d6ebc59c867aa7d7bb60ce9e4f70435a`이며 API·Worker의 이미지와 실제 `src`/package Hash를 대조했다.
- 개발 PostgreSQL이 실행 중인 상태에서 별도 프로젝트·DB·동적 localhost 포트·Named Volume으로 Smoke를 실행했다. 정상/장애 실행 전후 다른 프로젝트의 컨테이너 상태가 같았다. 회귀 검증 전후 개발 DB의 기존 전체 35개 행이 같고 임시 테스트 schema가 남지 않았다. 검증용으로 시작한 개발 PostgreSQL은 종료 후 원래 정지 상태로 복원했다.

## 작은 실제 실행

정상·장애 Smoke는 Worker 3개, 일반 `slow(1000ms)` 6 jobs/s × 12초(72개 예약), 1초의 별도 `slow(10000ms)` 1개, 상한 8, DB 표본 500ms, 정산 제한 90초다. 추가 소수 rate Smoke는 같은 구조에서 2.4 jobs/s × 12초(29개 예약), 상한 4인 장애 단일 실행이다. 정상과 장애 쌍의 입력·일정·이미지·설정은 같고 SIGKILL 유무만 다르다.

| 실행 | 일반 발송 / 수락 / 정책 거부 | 별도 대상 수락 | 관측 최대 미완료 | 대상 최종 attempt |
|---|---|---|---|---|
| 정상 | 72 / 31 / 41 | 1 | 8 | 1 |
| 장애 | 72 / 30 / 42 | 1 | 8 | 2 |
| 소수 rate 장애 | 29 / 24 / 5 | 1 | 4 | 2 |

세 실행 모두 시나리오 유효·기능 통과·필수 측정 유효·정리 성공이었다. 일반과 대상 요청을 별도 정산했고 미발송·기타 HTTP·네트워크/본문 오류·신규 행/수락 ID 불일치가 없었다. 모든 수락 행은 예상 result로 성공했고 대상 이외에는 attempt=1이었다. 기존 완료 행의 전체 필드가 유지됐으며 정산 후 전역 미완료는 0이었다.

장애 쌍에서는 실행기 관측 기준 생산 시작 약 3.50초에 ExitCode 137·생존 2개를 확인했다. 약 3.84초에 생존 Worker의 일반 Job 성공과 대상 첫 processing 잔류를 함께 확인했고, 약 39.59초에 Recovery, 그 뒤 두 번째 Claim, 약 49.61초에 대상 성공을 관측했다. Recovery 확인 시 DB에서 원래 `processing_started_at` 이후 약 37.10초가 지났음을 확인했다. 추가 소수 rate 실행에서도 원래 Claim 이후 약 33.16초에 Recovery를 확인했다. 기존 30초 Stale 기준·약 10초 검사 주기는 변경하지 않았다.

위 시각은 **실행기에서 관측한 경과**다. 로그 Flush·전송 지연은 알 수 없고 후속 DB 확인도 지연되므로 정밀 장애/복구 시간으로 해석하지 않는다. DB 표본 상한은 순간 상한의 새 증명이 아니며 기존 Admission 동시성 Integration Test와 구분한다. Prometheus·Docker Stats는 보조 관측이고 사망 Worker의 최종 Counter 합계로 완료를 판정하지 않았다.

## 오류 경계와 회귀

- 동일 프로젝트에 PostgreSQL이 이미 있으면 기존 컨테이너를 변경하지 않고 거부했다. 이전 queued·31초 이상 된 processing fixture가 있으면 Worker 생성 전에 거부했고, 기존 완료 행을 포함한 전체 행이 유지됐다. 사전 거부 때도 실행 옵션·revision 메타데이터가 남았다.
- `services.log`·`result.json` 경로를 실제 디렉터리로 만들어 EISDIR을 발생시켰다. 측정은 유효·기능은 통과인 채 반복 정리만 실패로 기록했고, 후속 DB 저장·컨테이너 제거·최종 환경 정리가 계속됐다. 결과 파일 저장 실패는 상위 summary와 독립 근거로 확인했다.
- Producer 실행 중 SIGTERM을 보내 발송 요청 정산·미발송 사유·전체 DB 상태·잔여 queued와 로그를 보존했다. 전용 컨테이너만 종료했으며 도구가 Job 행·볼륨을 삭제하지 않았다. Smoke만 자신이 만든 사전 검사/중단 fixture를 ID로 한정해 정리했다.
- 구현 중 2초 fixture의 Worker 참여 관측 부족과, 자원 수집 종료를 기다리는 동안 Worker가 새 Job을 가져가던 종료 순서 문제를 발견했다. Fixture를 보강하고 요청 정산 직후 API·Worker에 먼저 종료를 요청하도록 수정했다. 추가 소수 rate 검증에서는 사망 Worker에게 일반 Job 완료를 요구하던 과도한 조건을 발견했다. 사망 Worker는 주입 전 대상 Handler 진입으로 참여를 입증하고 정상 Worker 전체 완료·생존 처리·복구 검증을 유지하도록 바로잡았다.
- 최종 집계 함수로 기존 원자료를 재검증했다. 수락·거부 수량과 정상/장애 성공 판정은 같았다. 중단된 미완료는 `incomplete`, 요청과 대응하지 않는 완료는 `completedUnknown`으로 분리했다. 원본 실행 파일을 덮어쓰지 않고 별도 `recheck-final.json`에 집계 Hash와 결과를 남겼다.

| 실행한 검증 | 최종 결과 |
|---|---|
| `npm run typecheck` | 통과 |
| `node --test experiments/failure-load/experiment.test.ts experiments/backpressure/experiment.test.ts experiments/cpu-event-loop/experiment.test.ts` | 14개 통과: 신규 도구 8개·기존 실험 6개 |
| `npm run test:observability` | 6개 통과 |
| `npm run test:db` | 실제 PostgreSQL 64개 통과; 10개 연결 Admission 경합·Claim·Retry·Recovery·소유권·프로세스 종료 포함 |
| `node --env-file=.env --test experiments/failure-load/smoke.test.ts` | 실제 Compose 3개 통과 |
| `node --env-file=.env --test --test-name-pattern='기존 실행 거부' experiments/failure-load/smoke.test.ts` | 사전 거부 메타데이터 보강 후 해당 경계 1개 추가 통과 |
| README 작은 Smoke 명령의 `--modes=failure --rate=2.4 --limit=4` 실행 | exit code 0; 실제 소수 rate 발송·SIGKILL·자연 Recovery·최종 정산 통과 |
| 전용 `docker compose ... config --quiet`, `git diff --check` | 통과 |

개발 중 실패 원자료도 보존했다. 첫 전체 Smoke는 저장 실패 fixture의 관측 부족으로, 두 번째는 SIGTERM 이후 잔여 queued 검증에서 실패했다. 수정 후 전체 Smoke 3개가 모두 통과했다. 최초 소수 rate 실행은 기능·측정·정리가 통과했으나 Worker 참여 판정 때문에 시나리오가 미성립이었고, 판정 수정 후 소수 rate를 실제로 재실행해 통과했다.

## 보존 위치와 남은 범위

- 전체 정상/장애·오류 경계 원자료: `artifacts/failure-load/smoke-1793456575759/`.
- 사전 거부 메타데이터 추가 검증: `artifacts/failure-load/smoke-1793456870701/preflight/`.
- 최종 소수 rate 실제 실행: `artifacts/failure-load/fractional-smoke-final/`.
- 최종 함수 재검증: `artifacts/failure-load/smoke-1793456575759/recheck-final.json`.
- DB 회귀 출력·행 보존 확인: `artifacts/failure-load/regression-db.log`, `regression-db-preservation.json`.

대량 원자료는 Git 제외 경로에 유지하며 이 보고서·선별 summary만 결과로 둔다. 코어 정책 변경이 필요한 문제는 발견하지 않았다. PR 2에서 Worker 2·3개 Pilot으로 2.4 jobs/s 후보를 검토하고 120초 정상→장애→장애→정상 실측과 최종 결과 보고서를 작성해야 한다. 이번 Smoke의 수락량·자원·시각으로 기본 성능이나 유입률을 확정하지 않는다.
