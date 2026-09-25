import assert from "node:assert/strict";
import test from "node:test";

import context from "../context.js";
import transport from "../transport.js";

class FakeRealm {
    constructor() {
        this.tables = new Map();
    }

    table(name) {
        if (!this.tables.has(name)) {
            this.tables.set(name, new Map());
        }
        return this.tables.get(name);
    }

    write(operation) {
        operation();
    }

    objects(name) {
        const rows = Array.from(this.table(name).values());
        rows.tableName = name;
        return rows;
    }

    delete(rows) {
        this.tables.set(rows.tableName, new Map());
    }

    create(name, value) {
        this.table(name).set(value.ID, { ...value });
    }

    objectForPrimaryKey(name, id) {
        return this.table(name).get(id);
    }
}

test("transport retries a failed response with the same session, message and body", async () => {
    const originalFetch = globalThis.fetch;
    const originalRealm = context.pvcAdminRealm;
    const originalSettings = { ...context.settings };
    const requests = [];

    context.pvcAdminRealm = new FakeRealm();
    context.settings.syncServerUrl = "https://sync.example.test";
    context.settings.httpRetryCount = 1;
    context.settings.httpRetryDelayMillis = 0;
    context.settings.maxMessageSize = 2000000;

    globalThis.fetch = async (url, options) => {
        requests.push({
            url,
            sessionId: options.headers["session-id"],
            messageId: options.headers["message-id"],
            body: options.body
        });

        if (requests.length == 1) {
            throw new TypeError("connection reset");
        }

        return {
            ok: true,
            status: 200,
            headers: {
                get(name) {
                    if (name == "session-id") {
                        return "session-A";
                    }
                    if (name == "message-id") {
                        return "0";
                    }
                    return null;
                }
            },
            async text() {
                return "response-complete";
            }
        };
    };

    try {
        await transport.openOutputStream("session-A");
        await transport.writeCommand({ name: "PING", value: null });
        await transport.closeOutputStream();
    } finally {
        globalThis.fetch = originalFetch;
        context.pvcAdminRealm = originalRealm;
        context.settings = originalSettings;
    }

    assert.equal(requests.length, 2);
    assert.deepEqual(requests[0], requests[1]);
    assert.equal(requests[0].sessionId, "session-A");
    assert.equal(requests[0].messageId, "0");
});

test("transport retries when reading an interrupted response body", async () => {
    const originalFetch = globalThis.fetch;
    const originalRealm = context.pvcAdminRealm;
    const originalSettings = { ...context.settings };
    const requests = [];

    context.pvcAdminRealm = new FakeRealm();
    context.settings.syncServerUrl = "https://sync.example.test";
    context.settings.httpRetryCount = 1;
    context.settings.httpRetryDelayMillis = 0;
    context.settings.maxMessageSize = 2000000;

    globalThis.fetch = async (url, options) => {
        requests.push({
            url,
            sessionId: options.headers["session-id"],
            messageId: options.headers["message-id"],
            body: options.body
        });

        const requestNumber = requests.length;
        return {
            ok: true,
            status: 200,
            headers: {
                get(name) {
                    if (name == "session-id") {
                        return "session-B";
                    }
                    if (name == "message-id") {
                        return "0";
                    }
                    return null;
                }
            },
            async text() {
                if (requestNumber == 1) {
                    throw new TypeError("response body interrupted");
                }
                return "response-complete";
            }
        };
    };

    try {
        await transport.openOutputStream("session-B");
        await transport.writeCommand({ name: "PING", value: null });
        await transport.closeOutputStream();
    } finally {
        globalThis.fetch = originalFetch;
        context.pvcAdminRealm = originalRealm;
        context.settings = originalSettings;
    }

    assert.equal(requests.length, 2);
    assert.deepEqual(requests[0], requests[1]);
});
