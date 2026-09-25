import assert from "node:assert/strict";
import test from "node:test";

import { createSyncCoordinator, runDefinitionAwareSync } from "../sync-control.js";

function deferred() {
    let resolve;
    let reject;
    const promise = new Promise((resolvePromise, rejectPromise) => {
        resolve = resolvePromise;
        reject = rejectPromise;
    });
    return { promise, resolve, reject };
}

test("identical concurrent sync requests share one operation", async () => {
    const syncingStates = [];
    const release = deferred();
    let operationCalls = 0;
    const coordinator = createSyncCoordinator((syncing) => syncingStates.push(syncing));

    const first = coordinator.run("same-request", async () => {
        operationCalls++;
        return release.promise;
    });
    const second = coordinator.run("same-request", () => {
        operationCalls++;
        return "unexpected";
    });

    assert.strictEqual(first, second);
    assert.equal(coordinator.isSyncing(), true);
    assert.equal(operationCalls, 0, "operation starts after the lock is installed");

    await Promise.resolve();
    assert.equal(operationCalls, 1);
    release.resolve({ status: "ok" });

    const [firstResult, secondResult] = await Promise.all([first, second]);
    assert.strictEqual(firstResult, secondResult);
    assert.equal(coordinator.isSyncing(), false);
    assert.deepEqual(syncingStates, [true, false]);
});

test("a different request is rejected while a sync is active", async () => {
    const release = deferred();
    const coordinator = createSyncCoordinator();
    const active = coordinator.run("two-way:all", () => release.promise);

    await assert.rejects(
        coordinator.run("refresh-only:all", () => "unexpected"),
        /different direction or scope/
    );

    release.resolve("done");
    assert.equal(await active, "done");
});

test("the lock is released after a failed operation", async () => {
    const syncingStates = [];
    const coordinator = createSyncCoordinator((syncing) => syncingStates.push(syncing));

    await assert.rejects(
        coordinator.run("request", () => Promise.reject(new Error("network failed"))),
        /network failed/
    );
    assert.equal(coordinator.isSyncing(), false);

    const result = await coordinator.run("request", () => "retry succeeded");
    assert.equal(result, "retry succeeded");
    assert.deepEqual(syncingStates, [true, false, true, false]);
});

test("a definition change runs one necessary follow-up pass", async () => {
    const passResults = [true, false];
    const followUps = [];

    const result = await runDefinitionAwareSync(
        async () => passResults.shift(),
        3,
        (completedPasses) => followUps.push(completedPasses)
    );

    assert.deepEqual(result, {
        hadDefChanges: true,
        pendingDefChanges: false,
        passCount: 2
    });
    assert.deepEqual(followUps, [1]);
});

test("definition refresh is bounded at three passes", async () => {
    let passCalls = 0;
    const result = await runDefinitionAwareSync(async () => {
        passCalls++;
        return true;
    }, 3);

    assert.equal(passCalls, 3);
    assert.deepEqual(result, {
        hadDefChanges: true,
        pendingDefChanges: true,
        passCount: 3
    });
});

test("no definition change completes after the initial pass", async () => {
    let passCalls = 0;
    const result = await runDefinitionAwareSync(async () => {
        passCalls++;
        return false;
    }, 3);

    assert.equal(passCalls, 1);
    assert.deepEqual(result, {
        hadDefChanges: false,
        pendingDefChanges: false,
        passCount: 1
    });
});
