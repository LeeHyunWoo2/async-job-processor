# CPU Job / Event Loop 기본 실측 (2026-10-29)

전용 Compose에서 6조건을 각각 3회 실행했다. 18/18회가 수집·기능 유효성 기준을 만족했고 실행하지 못한 기본 조건은 없다. 모든 수치의 원자료는 `artifacts/cpu-event-loop/2026-10-29-default/`에 있으며 선별한 기계적 요약은 [summary.json](summary.json)이다. 최종 기술적 해석과 추가 CPU 경합 조건은 별도 검토 대상으로 남긴다.

## 재현 조건과 명령

기준 Commit은 `36728af478babae90b6d3905e5c403f0b58db106`, 브랜치는 `feature/cpu-check`다. 미커밋 구현을 포함하며 실제 실행 파일 사본·파일별 SHA-256·diff를 원자료 `source/`, `environment.json`, `source.diff`에 보존했다. 비교한 이미지 ID는 `sha256:0b6cbc17f308dad26842bbb2cf7a20cc653d7d4b53a13ab344738d7ff190d394`다. 각 조건에서 이미지 내부 소스 해시도 비교했다. 실측 후에는 요약 파일 자체의 저장 실패를 정리 실패로 기록하는 처리와 해당 Smoke를 보완했다. 실측 구간 코드는 동일하며 당시 실행 도구는 원자료에서 확인할 수 있다.

실제 실행한 주요 명령은 다음과 같다. 이미지 빌드는 Windows 자격 증명 도우미 경로 오류 때문에 Python `TemporaryDirectory`로 만든 `{"auths":{}}` 임시 Docker 설정을 `DOCKER_CONFIG`에 전달해 실행했다. 사용자 Docker 설정은 변경하지 않았다.

```bash
# 아래 build에만 임시 DOCKER_CONFIG를 전달
docker compose -p async-job-cpu-event-loop \
  -f compose.yml -f experiments/cpu-event-loop/compose.yml build migrate
node --env-file=.env experiments/cpu-event-loop/run.ts \
  --out=artifacts/cpu-event-loop/2026-10-29-default
```

Worker는 1개이고 Job도 동시에 하나다. `durationMs=2000`, Baseline·완료 후 관찰 최소 2000ms, Probe 간격 100ms, 요청별 Timeout 10000ms, 대상별 In-flight 상한 64개다. 각 조건을 새 프로세스로 실행했다. 반복 1·3은 Co-located의 Idle→slow→cpu 다음 Separated의 Idle→slow→cpu, 반복 2는 두 구조의 순서를 반대로 했다. 부하 종류의 순서는 고정했다.

Host/Docker는 WSL2 Linux, 논리 CPU 8개, Intel Core i7-7700K, Docker 29.8.2, Node.js 클라이언트/앱 v24.21.0/v24.21.0이다. Docker가 보고한 총 메모리는 8329560064 bytes다. 앱 컨테이너의 NanoCpus·CpuQuota·CpuPeriod는 0, 명시적 cpuset은 빈 값이었다. 실제 cgroup `cpu.max`는 `max 100000`, `cpuset.cpus.effective`는 `0-7`로 확인했다. 비교용 CPU 제한은 추가하지 않았다.

## 직접 확인한 수치

아래는 **각 반복의 전체 관찰 구간에서 구한 최대 RTT와 최종 누적 Event Loop Max의 3회 범위**다. 모든 단위는 ms다. Histogram은 프로세스 계측 활성화 이후의 누적값이며 준비 구간도 포함한다. Co-located의 공유 표본은 한 번만 센다.

| 구조 | 부하 | API /health Max RTT 범위 | Worker /metrics Max RTT 범위 | 프로세스별 Event Loop Max 범위 |
|---|---|---:|---:|---|
| co-located | idle | 2.49~6.84 | 5.26~6.66 | shared: 14.02~17.45 |
| co-located | slow | 4.17~8.65 | 5.17~9.43 | shared: 23.12~42.40 |
| co-located | cpu | 1919.25~1994.40 | 1919.70~1995.26 | shared: 2002.78~2009.07 |
| separated | idle | 3.94~6.83 | 4.13~8.33 | api: 24.10~28.02, worker: 12.34~25.28 |
| separated | slow | 3.58~7.87 | 3.32~5.64 | api: 34.34~37.42, worker: 16.66~21.69 |
| separated | cpu | 3.04~18.08 | 1938.25~1984.76 | api: 24.58~37.39, worker: 2003.83~2011.17 |

- slow Handler Monotonic 경과는 1999.80~2001.17ms, cpu는 2000.17~2000.30ms였다. 비슷한 경과 시간에서 CPU 조건의 약 2초 Event Loop Max와 HTTP 지연이 관측됐다.
- Co-located CPU에서는 API·Worker Metrics 모두 큰 지연을 보였다. Separated CPU에서는 Worker Metrics가 1938.25~1984.76ms 지연됐고 API 최대 RTT는 3.04~18.08ms였다. 이 환경·반복에서 직접 관측한 사실이며 일반적인 응답시간 보장은 도출하지 않는다.
- CPU 조건의 Event Loop p99는 약 10.77~13.57ms였다. 드문 큰 차단은 p99만으로 드러나지 않아 Max와 Count를 함께 보존했다. 프로세스별 Max/p99/Mean/Count 전체는 기계적 요약에 남겼다.
- Docker Stats의 CPU 조건 관측 최대는 Co-located 컨테이너 83.59%, Separated API 4.54%·Worker 98.73%였다. 서로 다른 컨테이너/시각의 최대를 합치지 않았다. CPU·메모리 개별 표본은 원자료에 남겼다.

## 측정 유효성과 보존

- 독립 Probe 2,312건을 발송했고 모두 HTTP 200으로 완료했다. 발송 누락·Timeout·네트워크 오류·비정상 HTTP는 각각 0건이다. 요청별 예약/발송/완료 시각·RTT 및 가장 큰 지연 사례는 각 조건의 `requests.jsonl`, `requests.json`, `result.json`에 있다.
- 최대 예약 발송 지연은 9.43ms로 한 주기 100ms 이하였다. 각 slow/cpu 조건에서 API·Worker 각각 Handler Marker 안의 실제 발송을 20건 이상 확인했다. 예정 발송과 실제 발송의 중첩도 별도로 남겼다.
- 12개 Job 모두 DB·기존 GET 경로에서 `succeeded`, `attempt=1`, 예상 payload/result를 확인했다. 단일 Claim·Handler 시작/종료·성공 사건과 Counter가 일치했고 Retry·Recovery·최종 실패는 없었다. Idle에는 새 Job이 없었다.
- 각 조건의 관측 전 표본과 완료 후 추가 표본 증가를 확인했고 Metrics·Docker 자원 수집 결측은 없었다. 프로세스 재시작·예상 외 Worker도 없었다.
- 클라이언트 동일 시작/종료 경계에서 Wall 경과−Monotonic 경과는 -0.50~18.08ms였다. 중간 사건 경계를 포함한 최대 절댓값도 18.08ms였다. 이전 실험 시계 변동의 원인이나 해결을 입증하는 결과는 아니다.
- 모든 조건과 최종 정리가 성공했다. 전용 컨테이너만 제거하고, 실행 전 완료 행 4개를 그대로 보존하며 새 실험 Job 12개도 남겼다. 초기 도구 개발 중 Entrypoint 비교·Streaming Stats 파싱이 실패한 Smoke 자료는 별도 원자료 디렉터리에 보존했다. 본 기본 18회에는 해당 수집 실패가 없다.

## 해석상 한계와 검토 대상

프로세스 간 사건 정렬은 Wall Clock에 의존하며 offset이나 정렬 오차의 상한을 측정하지 않았다. Handler Marker는 호출 직전/반환 직후이며 JSON 로그 처리의 작은 간극이 있다. DB processing 시각~updated 시각은 계산 자체의 정확한 경계가 아니다. HTTP RTT는 독립 클라이언트에서 본 전체 왕복이고 프로세스의 Histogram 지연과 같은 값이 아니다.

계측 Timer도 차단되므로 완료 직후 Snapshot을 최종값으로 취급하지 않았다. 추가 관찰 후 표본 증가를 확인했지만 누적 Histogram의 준비 시간·조건별 전체 노출 길이는 같지 않다. Max/p99/Mean을 차감하여 실행 구간 통계를 만들지 않았다. Docker Stats `--no-stream`의 수 초 수집 주기는 2초 부하의 정확한 CPU 최대·사용 시간 적분을 보장하지 않는다.

프로세스 분리는 공유 CPU 자원을 늘리지 않는다. OS 스케줄링·CPU 경합·JIT·로그/Probe/DB 관측 비용을 분리 통제하지 않았고, 종류 순서·보존 완료 행 누적의 영향도 남아 있다. 이번 환경에서 관측된 API 응답성과 CPU/Timer 차이는 기술적 검토의 근거로 제공하며 보편적 우월성·병목 원인·정밀 Speedup을 확정하지 않는다. 별도 CPU 경합·Scaling·Worker Threads 실험은 실행하지 않았다.

## 도구·회귀 검증

다음 검증이 통과했다. PostgreSQL 전체 테스트는 개발 DB를 기동하지 않고 전용 프로젝트의 PostgreSQL만 시작한 뒤 `DB_HOST=127.0.0.1`, 동적으로 배정된 `DB_PORT`, `DB_NAME=async_job_cpu_event_loop`를 전달해 실행했다. 종료 전후 Job 18개의 전체 행이 동일했고 전용 컨테이너도 제거했다.

```bash
npm run typecheck
node --test experiments/cpu-event-loop/experiment.test.ts experiments/backpressure/experiment.test.ts
npm run test:observability
node --env-file=.env --test experiments/cpu-event-loop/smoke.test.ts
# 위의 전용 DB 접속값을 환경변수로 전달
npm run test:db
git diff --check
```

집계·옵션·Probe 6개, 기본 관측 6개, 실제 Compose Smoke 2개, PostgreSQL 전체 64개가 통과했다. Compose Smoke는 두 구조의 cpu Job 성공·실제 발송과 표본·정상 종료·기존 컨테이너/행 보존, 서비스 로그 및 요약 파일 자체의 저장 실패, SIGTERM 후 자료 정산·정리를 검증했다. 정확한 ms 성능 임계값은 강제하지 않았다.

최종 Compose Smoke 자료는 `artifacts/cpu-event-loop/smoke-1793253425797/` 및 같은 루트의 `save-failure-1793253448789/`, `result-save-failure-1793253462666/`, `interrupt-1793253475096/`다. PostgreSQL 회귀 로그·행 보존 근거는 `artifacts/cpu-event-loop/2026-10-29-verification-final/`에 있다. 첫 전체 회귀에서는 새 Handler Marker를 예상하지 않은 기존 로그 Assertions가 실패해, Handler 실행과 DB 저장 사건을 구분하는 정확한 기대 순서로 수정했다. Counter·소유권·Row Lock·DB 상태 Assertions는 유지했고 재실행 64개가 통과했다.
