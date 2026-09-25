async function retryAsync(operation, options) {
    options = options || {};
    const retryCount = Math.max(0, Number(options.retryCount) || 0);
    const baseDelayMillis = Math.max(0, Number(options.baseDelayMillis) || 0);
    const isRetryable = options.isRetryable || (() => true);
    const onRetry = options.onRetry;

    let attempt = 0;
    while (true) {
        try {
            return await operation(attempt);
        } catch (error) {
            if (attempt >= retryCount || !isRetryable(error)) {
                throw error;
            }

            const retryNumber = attempt + 1;
            const delayMillis = baseDelayMillis * Math.pow(2, attempt);
            if (onRetry) {
                onRetry(error, retryNumber, delayMillis);
            }
            if (delayMillis > 0) {
                await new Promise((resolve) => setTimeout(resolve, delayMillis));
            }
            attempt++;
        }
    }
}

export {
    retryAsync
};
