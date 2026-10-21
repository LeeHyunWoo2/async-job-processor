export function recordObservation(record: () => void): void {
    try {
        record();
    } catch {
        // 관측 실패가 이미 확정된 DB 상태나 Handler 결과를 바꾸지 않게 한다.
    }
}
