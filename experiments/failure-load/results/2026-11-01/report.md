# 통합 Failure / Load Pilot·기본 실측 — 2026-11-01

2.4 jobs/s에서 Worker 3개는 유입을 감당했고 Worker 2개는 Queue 포화와 정책상 503을 보였다. 같은 소스·이미지의 정상→장애→장애→정상 네 반복에서 Admission·생존 처리·Stale Recovery·최종 저장이 함께 작동했다. 본 실측 수락 1,107개는 모두 성공했고 장애 대상 두 개만 두 번째 Claim을 사용했다. [선별 수치와 산식](summary.json), [실행법](../../README.md)을 함께 확인한다.

## 질문과 가설

- API 1개·Worker 3개에 일반 `slow(1000ms)`를 2.4 jobs/s로 계속 넣을 때 정상 조건은 안정적이고, Worker 하나를 잃으면 미완료 Job이 증가하는가?
- 같은 부하에서 `queued + processing` 관측 상한 30과 HTTP 201/정책상 `503 QUEUE_CAPACITY_EXCEEDED`가 유지되는가?
- 생존 Worker가 Recovery 전에 새 수락 Job을 완료하고, 사망 대상은 processing 잔류→Recovery→재Claim→성공하는가?
- 이 장애가 완료율·수락량·HTTP RTT·Queue·자원 표본에 어떤 비용을 남기는가?

이번에 검증하는 범위는 지속 부하와 Worker 장애가 겹친 상황의 통합 동작이다. 원자적 Admission, Retry, 기본 Crash Recovery, 다중 Worker 자체의 새 증명으로 확대하지 않는다.

## 실험 조건과 재현 방법

| 항목 | 실제 조건 |
|---|---|
| 기준 소스 | `968b753d5d7a91a7e9aeeb86b0e3e5a2107e9a6e`, `feature/failure-check`; Pilot·본 실측 시작 시 모두 clean |
| 공통 이미지 | `sha256:feeca08866b4059114abb9bb665ee4c6f272c12378acb67441ffd70510b93d40`; 측정 사이 재빌드·코드 변경 없음 |
| 환경 | Node.js v24.21.0, Docker 29.8.2, Compose 5.5.1; WSL2/Docker Desktop, Intel i7-7700K, 논리 CPU 8개, 약 7.76GiB |
| 구성 | 전용 `async-job-failure-load` / `async_job_failure_load`, API 1개, 초기 Worker 3개, Admission ON·상한 30 |
| 일반 Job | `slow`, `{"durationMs":1000}`, 2.4 jobs/s × 120초; 계획 288개/반복 |
| 대상 Job | 생산 시작 20초에 별도 `slow`, `{"durationMs":10000}` 1개; 201·첫 processing·Handler 시작·소유 컨테이너 확인 |
| 주입·순서 | 정상→장애→장애→정상; 장애에서 해당 Worker만 SIGKILL, 재시작 없이 생존 2개 유지 |
| 정산 | 생산 종료 후 수락 Job 최종 상태까지 최대 300초; 이번에는 모두 제한 안에 완료 |
| 자원·DB | CPU·메모리 추가 제한 없음; PostgreSQL max_connections=100, shared_buffers=128MiB, work_mem=4MiB |

실제 컨테이너 ID·Worker UUID·Node 명령·이미지 내 소스 Hash·환경·시작 시각·RestartCount와 현재 Prometheus Target을 반복마다 확인했다. PostgreSQL·Migration과 기존 행 검사를 API·Worker보다 먼저 실행했다. API·Worker만 매 반복 새로 생성했고 완료 행·PostgreSQL/Prometheus Volume은 유지했다. 준비 중 꺼져 있던 Docker Desktop을 시작했으며, 측정 중에는 Engine이나 기존 프로젝트 서비스를 재시작하지 않았다.

다음 명령을 저장소 루트에서 실행했다. 원자료 경로는 비어 있어야 하므로 재실행 시 새 `--out` 경로를 사용한다. 빌드는 비교 전에 한 번만 수행했다.

```bash
docker compose -p async-job-failure-load \
  -f compose.yml -f experiments/failure-load/compose.yml build migrate
node --env-file=.env experiments/failure-load/run.ts \
  --purpose=pilot --modes=normal --repeats=1 --workers=3 --seconds=90 \
  --out=artifacts/failure-load/2026-11-01-measurement/pilot-three
node --env-file=.env experiments/failure-load/run.ts \
  --purpose=pilot --modes=normal --repeats=1 --workers=2 --seconds=90 \
  --out=artifacts/failure-load/2026-11-01-measurement/pilot-two
# 두 Pilot의 실제 발송·완료·Queue 추세와 정산을 확인한 뒤 실행
node --env-file=.env experiments/failure-load/run.ts \
  --purpose=main --out=artifacts/failure-load/2026-11-01-measurement/main
```

실험 제어 프로세스의 Monotonic Clock만 경과 계산에 사용했다. 비교 구간은 첫 대상 Handler 시작 전, 시작 후 첫 10초, 그 이후 지속 유입, Producer 종료 후 정산으로 구분했다. 정상 대상의 실제 성공은 시작 약 10.015초 뒤 관측됐으므로 첫 10초 경계는 비교용 고정 경계이며 완료 사건 자체와 다르다. 단계별 수락/거부는 발송 시각의 반개구간으로 분류했다.

DB 관측 평균 완료율은 구간 내부 두 표본의 `succeeded` 증가량 / 두 조회 완료 사이 Monotonic 경과다. 조회 시작·비용·성공 수와 산식을 summary에 남겼다. 전체 성공 수이며 대상 완료가 포함될 수 있다. 로그 관측 완료량과 구분하며 서버의 정밀 TPS나 순수 일반 Job 최대 처리 능력으로 해석하지 않는다.

## Pilot 근거와 공통 유입률 결정

같은 이미지·일반 Job·상한·대상 예약을 사용해 사고 없는 normal Pilot을 Worker 3개→2개 순서로 각각 90초 실행했다. 90초는 대상 완료 이후 온전한 10초 구간 3개 이상과 이후 추세를 보기 위한 길이다. 본 실측과 달리 Pilot은 생산 90초·일반 계획 216개다. 두 Pilot의 시나리오·기능·측정·정리가 모두 통과했다.

| Worker | 일반 발송 | 201 | 503 | 실제 발송 jobs/s | 대상 시작+10초 이후 DB 완료율 | 최대 outstanding | 대상 포함 최종 성공 |
|---|---|---|---|---|---|---|---|
| 3 | 216 | 216 | 0 | 2.40086 | 2.474 | 8 | 217 |
| 2 | 216 | 196 | 20 | 2.40057 | 1.993 | 30 | 197 |

Worker 3개는 대상 완료 후 온전한 10초 구간 5개의 outstanding 평균이 **4.7→2.7→2.6→2.5→2.6**, 각 구간 일반 발송 24개·거부 0개였다. 일시적 최대 outstanding 8과 이후 안정 상태를 구분했다. 기존 안정성 기준에서 `suitable`이었다.

Worker 2개는 대상 Handler 시작 전에도 outstanding 최대 13으로 증가했다. 대상 시작 후 첫 10초 평균은 20.1, 이후 지속 유입 구간 평균은 28.65였다. 대상 완료 후 10초 구간 평균도 **28.4→28.8→28.7→28.8→28.6**으로 높게 유지됐고 해당 구간 거부는 2·4·4·4·4개였다. 전체 거부는 20개, 정산 직전까지의 관측 최대 outstanding은 30이었다. 단일 표본이나 대상 10초 Job의 일시적 영향만으로 용량 부족을 판정하지 않았다.

따라서 초기 후보 **2.4 jobs/s를 공통 유입률로 확정**하고 기본 조건을 그대로 사용했다. 판단 근거는 `pilot-decision.json`에 본 실측 시작 전에 저장했다. 과거 Worker Load의 DB Wall 시각 기반 정밀 성능·Speedup은 결정 근거로 사용하지 않았다.

## 반복별 원자료 요약

아래 일반 요청과 별도로 대상 1개가 매번 201로 수락됐다. 네 반복 모두 일반 계획·실제 발송 288개, 미발송 0개, 기타 HTTP 0개, 네트워크/본문 오류 0개였다. 대상도 누락·거부·오류가 없었다. 정산초는 `load-end`→`drain-end`의 관측 경과다.

| 반복 | 일반 발송 | 201 | 정책 503 | 실제 jobs/s | 최대 outstanding | 최대 queued | 대상 포함 성공 | 정산초 |
|---|---|---|---|---|---|---|---|---|
| r1-normal | 288 | 288 | 0 | 2.40083 | 8 | 5 | 289 | 0.965 |
| r1-failure | 288 | 264 | 24 | 2.40043 | 30 | 28 | 265 | 14.362 |
| r2-failure | 288 | 263 | 25 | 2.40050 | 30 | 28 | 264 | 13.942 |
| r2-normal | 288 | 288 | 0 | 2.40051 | 7 | 4 | 289 | 1.482 |

각 반복은 시나리오 유효, 기능 `passed`, 필수 측정 유효, 시간 분석·Wall 정렬 진단 유효, 반복 정리 성공이었다. 정상 두 반복의 대상 완료 후 안정성도 `suitable`이었다. 실패·무효 실행이나 조건을 바꾼 재시도는 없었다.

정상 Worker별 완료 수는 첫 반복 111·115·63개, 마지막 반복 120·108·61개로 세 Worker 모두 참여했다. 이는 균등 분담을 보장한다는 의미는 아니다. 장애 조건은 처음 세 Worker의 Handler 참여, 대상 소유 컨테이너만 ExitCode 137·OOM 아님, 생존 두 컨테이너의 시작 시각 유지, 마지막까지 재시작·Worker 추가 없음이 확인됐다.

### 장애 대상 사건과 Recovery 관측 경과

단위는 생산 시작부터 실행기가 관측한 초다. 컨테이너↔UUID↔대상 ID는 각 `ready.json`, `observations.jsonl`, `target-states.jsonl`과 summary에 보존했다.

| 사건 | r1-failure | r2-failure |
|---|---|---|
| 첫 Handler 시작 | 20.034 | 20.507 |
| SIGKILL 종료·생존 2개 확인 | 20.910 | 21.324 |
| 장애 후 새 수락 Job 성공·대상 잔류 확인 | 22.872 | 23.275 |
| Recovery 로그 수신 | 57.907 | 59.343 |
| 두 번째 Claim 로그 수신 | 65.952 | 68.400 |
| 대상 성공 로그 수신 | 75.972 | 78.417 |

SIGKILL 확인 이후 발송·201 수락된 별도 일반 Job의 성공 DB 행을 읽은 다음, 대상이 여전히 attempt=1·사망 Worker 소유의 processing인지 다시 확인했다. 이 성공이 Recovery 이전이며 실제 생존 컨테이너·Worker UUID와 일치했다. 장애 전 요청의 늦은 로그를 생존 처리 증거로 사용하지 않았다.

대상 DB 조회에서도 두 장애 모두 `processing(1)→queued(1)→processing(2)→succeeded(2)`가 관측됐다. queued 관측은 각각 생산 시작 58.097초·59.516초, 두 번째 processing 관측은 65.965초·68.593초였다. 로그와 DB 표본의 시각을 같은 사건 발생 시각으로 대체하지 않는다.

| 구간(관측초) | r1-failure | r2-failure |
|---|---|---|
| SIGKILL 확인 → Recovery 관측 | 36.997 | 38.018 |
| Recovery 관측 → 재Claim 관측 | 8.045 | 9.057 |
| 재Claim 관측 → 성공 관측 | 10.020 | 10.017 |
| SIGKILL 확인 → 성공 관측 | 55.062 | 57.093 |

Recovery 확인 시 DB의 원래 `processing_started_at`부터 지난 시간은 각각 **38.147초·39.111초**였다. 이는 확인 시점의 DB 경과로, Recovery UPDATE의 정확한 시각은 아니다. Stale 기준은 최초 Claim 후 30초 초과이고, 생존 Worker가 순차 Loop에서 약 10초 주기 검사를 실행해야 한다. 복구 뒤에도 `available_at, created_at, id` 정렬과 생존 Worker의 다음 Claim 시점에 따라 대기할 수 있으며, 이번 재Claim 대기는 약 8~9초로 관측됐다. SIGKILL→Recovery→재Claim→완료를 구분하며 30~40초 내 완료를 보장하지 않는다.

## 정상과 장애 비교

### 완료율·Queue·수락량

대상 시작 전 DB 관측 평균 완료율은 정상 약 2.291~2.292, 장애 약 2.242~2.307 jobs/s였다. 대상 시작 직후 첫 10초는 **네 반복 모두 약 1.993 jobs/s**였다. 정상도 10초 대상 Job 동안 일반 Job을 처리하는 Worker는 잠시 둘이므로, 이 구간을 장기간 3 대 2의 성능 차이로 해석하지 않는다.

이후 지속 유입 구간의 실제 DB 관측 증가량과 경과는 다음과 같다. 첫·끝 DB 조회가 구간 안에 완전히 들어와야 하므로 구간 전체 길이와 DB 산식의 경과는 조금 다르다.

| 반복 | 비교 구간초 | DB 성공 증가/관측초 | DB 관측 jobs/s | 일반 201/503 | 평균 outstanding | queued 범위 | processing 범위 |
|---|---|---|---|---|---|---|---|
| r1-normal | 30.321–120.001 | 217 / 88.311 | 2.457 | 215 / 0 | 2.99 | 0–4 | 2–3 |
| r1-failure | 30.034–120.001 | 169 / 89.337 | 1.892 | 191 / 24 | 23.30 | 5–28 | 2–3 |
| r2-failure | 30.507–120.000 | 167 / 88.371 | 1.890 | 189 / 25 | 24.20 | 6–28 | 2–3 |
| r2-normal | 30.101–120.001 | 218 / 89.343 | 2.440 | 215 / 0 | 3.61 | 0–4 | 1–3 |

정상은 일시적으로 쌓인 Queue를 해소하고 유입을 수용했다. 장애는 두 Worker가 계속 완료를 만들지만 유입보다 느려 outstanding이 30에 도달했고 24·25개를 정책상 거부했다. 장애 후 DB processing=3인 표본은 생존 Worker 2개와 사망 대상의 processing 잔류 1개를 포함하며, Worker가 세 개 실행 중이라는 뜻은 아니다. 잔류 Job도 Admission 슬롯을 점유했다. 수락된 Job의 유실은 없었다. Producer 종료 후 정산은 정상 약 0.965~1.482초, 장애 약 13.942~14.362초였다. 짧은 정산 구간의 순간 완료율을 최대 처리 능력으로 해석하지 않는다.

### HTTP 지연

일반 요청 RTT의 nearest-rank percentile이며 단위는 ms다. 발송부터 응답 본문 처리 완료까지의 클라이언트 경과를 사용했다.

| 반복 | 201 p50 | 201 p95 | 201 p99 | 201 max | 503 p50 | 503 p95 |
|---|---|---|---|---|---|---|
| r1-normal | 4.892 | 7.516 | 19.705 | 44.320 | — | — |
| r1-failure | 4.912 | 7.195 | 18.438 | 40.676 | 3.616 | 5.147 |
| r2-failure | 4.839 | 7.424 | 11.865 | 37.718 | 3.684 | 5.167 |
| r2-normal | 4.800 | 7.177 | 18.454 | 58.901 | — | — |

201 RTT p95는 약 7.18~7.52ms로 비슷했으며 반복별 tail 차이는 그대로 보존했다. 503은 201과 다른 처리 경로이며 더 빨랐다. 거부 비중이 늘면서 혼합 RTT가 작아져도 API 성공 응답 성능 개선으로 결론내리지 않는다. 수락률은 정상 100%, 장애 91.67%·91.32%였고 실제 발송률은 모두 약 2.4004~2.4008 jobs/s였다.

### CPU·메모리·DB 연결

지속 유입 구간에서 같은 Docker Stats 요청에 속한 role별 CPU%와 사용 MiB를 합한 뒤 표본 평균을 냈다. 셀의 `CPU% / MiB`는 시간 가중 평균이 아닌 표본 평균이다. Worker는 정상 3개, 장애 생존 2개의 합이며 죽은 컨테이너의 0 표본은 합에 영향을 주지 않는다. API·Worker·PostgreSQL은 각 반복 44개 Snapshot이었다. 상세 분포·최댓값·수집 비용은 summary에 있다.

| 반복 | API CPU% / MiB | Worker 합 CPU% / MiB | PostgreSQL CPU% / MiB | DB 전체 연결 범위(평균) | 활성 연결 범위 |
|---|---|---|---|---|---|
| r1-normal | 1.18 / 71.04 | 2.36 / 181.68 | 2.32 / 38.38 | 5–6 (5.30) | 1–2 |
| r1-failure | 1.15 / 66.21 | 1.65 / 124.40 | 2.69 / 36.65 | 4–5 (4.33) | 1–2 |
| r2-failure | 1.34 / 66.69 | 1.61 / 117.74 | 2.06 / 36.52 | 4–5 (4.16) | 1–1 |
| r2-normal | 1.18 / 67.88 | 2.29 / 189.21 | 3.42 / 39.05 | 5–6 (5.19) | 1–2 |

Worker 수 감소와 함께 Worker 합 메모리·CPU 표본 및 DB 연결이 낮아졌다. PostgreSQL CPU 표본은 정상 두 번끼리도 달랐다. 이 값만으로 실제 병목, CPU 부족, 최적 Worker 수나 보편적 성능 우위를 확정하지 않는다. DB 연결 수에는 최대 4개 Pool의 실험 관찰 연결도 포함된다.

## 검증된 사실과 전역 정산

- 본 실측 총 1,156회 발송 = 1,107회 201 + 49회 정책상 503. 대상은 4개 모두 수락됐고 일반 1,152개는 1,103개 수락됐다. `sent = accepted + rejected + otherHttp + networkOrBodyErrors`, `planned = sent + unsent`가 맞는다.
- Pilot을 합하면 1,590회 발송 = 1,521회 수락 + 69회 거부다. 신규 1,521행 전부 올바른 `slow` payload/result로 succeeded이며 attempt=1이 1,519개, 장애 대상 attempt=2가 2개다.
- 최초 기존 389행에서 최종 1,910행이 됐다. 각 반복 수락 ID와 전체 신규 DB 행의 대조에서 미대응 행·누락 ID·응답 유실 후 INSERT만 Commit된 행이 없었다. 기존 389행과 앞선 반복의 완료 행은 전체 필드가 그대로 유지됐다. 전역 outstanding은 매 반복 말에 0이었다.
- 새 failed·Retry·추가 Recovery·Worker 오류·API/Worker 재시작이 없었다. 장애 대상만 한 번씩 Recovery됐고 다른 Worker가 재Claim했다. 정상 대상은 첫 Claim·Handler·성공 로그와 DB attempt=1이 일치했다.
- 전체 DB 표본은 outstanding≤30이었다. 표본 사이 순간까지 이 실측으로 새로 증명하지 않는다. 원자성 근거는 별도 기존 Advisory Lock 동시성 테스트이며 이번 회귀 검증에서도 실행했다.
- 최종 DB 성공 증가량과 성공 사건 로그 수가 반복별 289/289, 265/265, 264/264, 289/289로 일치했다. Prometheus의 죽은 Worker Counter 합은 성공 정산에 사용하지 않았다.
- 요청·DB·사건·자원 수집 오류는 없었고 세 실행의 최종 정리와 여섯 반복의 정리가 모두 성공했다. 기존 다른 프로젝트 컨테이너 상태와 완료 행·Volume은 보존했다. 회귀 테스트 전후 public.jobs도 동일했다.

## 해석과 한계

관측 결과는 합의한 지속 부하에서 Admission·생존 처리·Recovery가 함께 동작했음을 뒷받침한다. 장애 비용은 이 환경에서의 수락 감소, Queue 포화, 약 1.89 jobs/s의 지속 구간 관측 완료율, 길어진 정산과 대상 완료 대기였다. 각 조건 두 번만으로 통계적 유의성·보편적 지연 보장이나 성능 보장을 주장하지 않는다.

DB 표본은 약 1초, 대상 상태 조회는 약 200ms다. 본 실측 DB 조회 최대 비용은 약 22.96ms, 실제 표본 간격 최대는 약 1028.80ms였다. Docker Stats는 요청·응답 사이 약 1.5초가 걸리는 표본이며 경계를 걸친 표본은 단계 자원 집계에서 제외했다. 로그 Flush·전송 지연의 상한은 미상이고 Worker 사건 수신 순서가 실제 사건 간격을 정확히 보장하지 않는다. DB·로그 수집과 보존 자체도 부하다.

본 실측 Wall/Monotonic 상대 경과 차이의 최대 절댓값은 반복별 169.007·359.992·437.972·441.602ms였다. 이번 1초 진단 기준은 넘지 않았지만 시계가 완전히 일치한다는 뜻은 아니다. 과거 약 1,908초 동안 차이가 약 158초 변한 원인은 여전히 미확정이다. Wall Clock은 사건 연결 참고로만 사용했고, 다른 프로세스의 performance.now 절댓값·DB/로그 Wall 시각 차로 정밀 TPS·Speedup·복구 시간을 계산하지 않았다.

Prometheus는 15초 Scrape와 DNS 갱신 지연이 있고 사망 Worker의 마지막 Counter가 누락될 수 있다. raw/current/range와 현재 Target을 보존했지만 PostgreSQL 최종 상태와 Worker 사건을 우선했다. 완료 행·Prometheus Volume·캐시 유지, 고정 실행 순서와 테이블 성장, 외부 OS 부하의 영향도 남는다. CPU·메모리·연결 표본은 병목을 확정하는 근거가 아니다.

핵심 Job·Admission·Recovery 로직이나 실험 도구의 결함은 관찰되지 않아 수정하지 않았다. 현재 사용자 판단을 요구하는 막힌 문제는 없다. Fixed Timeout·재실행 지연과 At-least-once의 중복 실행 가능성은 기존 설계의 한계이며, Timeout 정책 변경·병목 진단·최적 Worker 수 결정은 이번 결과만으로 선택하지 않는다.

## 원자료 보존과 검증 명령·실행 결과

원자료 루트는 **`artifacts/failure-load/2026-11-01-measurement/`**이며 Git에서 제외한다. `pilot-three/`, `pilot-two/`, `main/` 아래 environment/preflight/final-jobs/summary/cleanup, 각 반복의 전체 요청·DB 행·표본·사건·대상 상태·자원·Prometheus·컨테이너 진단·서비스 로그를 보존했다. `verification/`에는 빌드·typecheck·테스트 로그, 회귀 검증 전후 전체 Job 행·보존 판정을 남겼다. `plan.json`, `pilot-decision.json`, `analyze.py`, `analysis.json`, `select_results.py`, `write_report.py`로 결정과 재집계를 추적할 수 있다. summary는 source 경로·주요 원자료 SHA-256·산식·실제 관측값만 선별한다.

같은 날짜의 기존 PR 1 리뷰 보완 report/summary는 기준 Commit `968b753d5d7a91a7e9aeeb86b0e3e5a2107e9a6e`와 원자료 루트의 `prior-review-record/`에 원문 그대로 보존했다. 현재 날짜의 report/summary는 Pilot·본 실측을 반영한 최신 기록이다. [10월 31일 초기 도구 검증](../2026-10-31/report.md)도 유지했다. 내부 의사결정·면접 노트와 루트 README는 수정하지 않았다.

| 명령·검증 | 실제 결과 |
|---|---|
| 위 이미지 빌드·Worker 3/2 Pilot 두 번 | 모두 exit 0; Pilot 가정 충족·기능/측정/정리 통과 |
| 위 `--purpose=main` | exit 0; 정상→장애→장애→정상 네 번 모두 시나리오/기능/측정/정리 통과 |
| `npm run typecheck` | 통과 |
| `node --test experiments/failure-load/experiment.test.ts experiments/backpressure/experiment.test.ts experiments/cpu-event-loop/experiment.test.ts` | 18개 통과 |
| `npm run test:observability` | 6개 통과 |
| 전용 DB 접속 값으로 `npm run test:db` (`--test-concurrency=1`) | 64개 통과; Claim·Admission·Retry·Recovery·독립 프로세스·CPU/Event Loop·관측 회귀 포함 |
| 회귀 전후 public.jobs 전체 행·임시 Schema 검사 | 1910행 그대로 유지, 잔여 테스트 Schema 0 |
| 소스·이미지 동일성, 전역 정산, 원자료 Hash, 다른 프로젝트 상태, `git diff --check` | 통과 |

기존 Compose Smoke는 이번에 다시 실행하지 않았다. 도구 TypeScript·Compose 변경 없이 실제 Pilot 두 번과 기본 실측 네 번을 수행했으며, 과거 Smoke의 장애 정리 테스트 결과를 이번 실행 결과로 합산하지 않았다. 회귀 검증은 본 실측을 모두 마친 뒤 전용 PostgreSQL/Migration만 다시 시작해 실행하고 해당 컨테이너·Network를 종료했으며 Volume과 Job 행은 유지했다.
