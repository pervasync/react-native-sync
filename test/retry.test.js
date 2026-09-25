import assert from "node:assert/strict";
import test from "node:test";

import { retryAsync } from "../retry.js";

test("retryAsync retries a transient failure and returns the result", async () => {
    const attempts = [];
    const retries = [];

    const result = await retryAsync(async (attempt) => {
        attempts.push(attempt);
        if (attempt < 2) {
            throw new Error("connection reset");
        }
        return "response";
    }, {
        retryCount: 3,
        baseDelayMillis: 0,
        onRetry: (error, retryNumber, delayMillis) => {
            retries.push([error.message, retryNumber, delayMillis]);
        }
    });

    assert.equal(result, "response");
    assert.deepEqual(attempts, [0, 1, 2]);
    assert.deepEqual(retries, [
        ["connection reset", 1, 0],
        ["connection reset", 2, 0]
    ]);
});

test("retryAsync does not retry a non-retryable failure", async () => {
    let attempts = 0;

    await assert.rejects(retryAsync(async () => {
        attempts++;
        const error = new Error("HTTP 500");
        error.retryable = false;
        throw error;
    }, {
        retryCount: 3,
        isRetryable: (error) => error.retryable !== false
    }), /HTTP 500/);

    assert.equal(attempts, 1);
});

test("retryAsync stops after the configured retry count", async () => {
    let attempts = 0;

    await assert.rejects(retryAsync(async () => {
        attempts++;
        throw new Error("still offline");
    }, {
        retryCount: 2,
        baseDelayMillis: 0
    }), /still offline/);

    assert.equal(attempts, 3, "one initial attempt plus two retries");
});
