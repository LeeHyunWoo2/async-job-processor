export class RetryableJobError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'RetryableJobError';
    }
}

export function getRetryDelayMs(attempt: number): number {
    return 1_000 * 2 ** (attempt - 1);
}
