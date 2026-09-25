/**
 * Serialize sync operations that share the module-level agent and transport
 * state. Identical requests share the active promise; a request with a
 * different direction or scope is rejected until the active request finishes.
 */
function createSyncCoordinator(onSyncingChange) {
    let activeSync = null;

    function run(requestKey, operation) {
        if (activeSync) {
            if (activeSync.requestKey == requestKey) {
                return activeSync.promise;
            }
            return Promise.reject(new Error(
                "A sync session is already active with a different direction or scope."
            ));
        }

        if (typeof operation != "function") {
            return Promise.reject(new TypeError("Sync operation must be a function."));
        }

        if (onSyncingChange) {
            onSyncingChange(true);
        }

        // Defer operation startup by one microtask so activeSync is installed
        // before COMPOSING or another synchronous state callback can re-enter.
        const operationPromise = Promise.resolve().then(operation);
        let coordinatedPromise;
        coordinatedPromise = operationPromise.finally(() => {
            if (activeSync && activeSync.promise == coordinatedPromise) {
                activeSync = null;
                if (onSyncingChange) {
                    onSyncingChange(false);
                }
            }
        });

        activeSync = {
            requestKey: requestKey,
            promise: coordinatedPromise
        };
        return coordinatedPromise;
    }

    function isSyncing() {
        return activeSync != null;
    }

    return {
        run,
        isSyncing
    };
}

/**
 * Run the initial sync pass and only the definition-refresh passes that are
 * actually requested. The cap preserves the previous three-pass limit.
 */
async function runDefinitionAwareSync(runPass, maxPasses, onFollowUp) {
    if (!maxPasses || maxPasses < 1) {
        throw new Error("maxPasses must be at least 1.");
    }

    let hadDefChanges = false;
    let pendingDefChanges = false;
    let passCount = 0;

    while (passCount < maxPasses) {
        pendingDefChanges = Boolean(await runPass(passCount));
        passCount++;

        if (!pendingDefChanges) {
            break;
        }

        hadDefChanges = true;
        if (passCount < maxPasses && onFollowUp) {
            onFollowUp(passCount);
        }
    }

    return {
        hadDefChanges,
        pendingDefChanges,
        passCount
    };
}

export {
    createSyncCoordinator,
    runDefinitionAwareSync
};
