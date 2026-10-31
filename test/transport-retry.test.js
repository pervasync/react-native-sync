import assert from "node:assert/strict";
import test from "node:test";

import context from "../context.js";
import transport from "../transport.js";

for (const status of [502, 503, 504]) {
    test(`transport resumes a later message after HTTP ${status}`, async () => {
        const originalFetch = globalThis.fetch;
        const originalRealm = context.pvcAdminRealm;
        const originalSettings = { ...context.settings };
        const realm = new FakeRealm();
        const requests = [];

        context.pvcAdminRealm = realm;
        context.settings.syncServerUrl = "https://sync.example.test";
        context.settings.httpRetryCount = 1;
        context.settings.httpRetryDelayMillis = 0;
        context.settings.maxMessageSize = 2000000;

        globalThis.fetch = async (url, options) => {
            requests.push({ url, headers: { ...options.headers }, body: options.body });
            if (requests.length == 2) {
                return { ok: false, status };
            }
            assert.ok(requests.length <= 3);
            const body = requests.length == 1
                ? context.settings.morePayload : "response-complete";
            return {
                ok: true,
                status: 200,
                headers: { get: (name) => options.headers[name] },
                async text() { return body; }
            };
        };

        try {
            await transport.openOutputStream("session-gateway");
            await transport.writeCommand({ name: "PING", value: null });
            await transport.closeOutputStream();

            assert.equal(requests.length, 3);
            assert.equal(requests[0].headers["message-id"], "0");
            assert.equal(requests[1].headers["message-id"], "1");
            assert.equal(requests[1].headers["session-id"], "session-gateway");
            assert.equal(requests[1].body, context.settings.morePayload);
            assert.deepEqual(requests[1], requests[2]);
            assert.equal(realm.table("pvc__payload_in").size, 2);
            assert.equal(realm.objectForPrimaryKey("pvc__payload_in", 1).PAYLOAD,
                "response-complete");
        } finally {
            globalThis.fetch = originalFetch;
            context.pvcAdminRealm = originalRealm;
            context.settings = originalSettings;
        }
    });
}

for (const status of [400, 401, 403, 404, 500, 502]) {
    test(`transport bounds retries for persistent HTTP ${status}`, async () => {
        const originalFetch = globalThis.fetch;
        const originalRealm = context.pvcAdminRealm;
        const originalSettings = { ...context.settings };
        const realm = new FakeRealm();
        const requests = [];

        context.pvcAdminRealm = realm;
        context.settings.syncServerUrl = "https://sync.example.test";
        context.settings.httpRetryCount = 2;
        context.settings.httpRetryDelayMillis = 0;
        context.settings.maxMessageSize = 2000000;

        globalThis.fetch = async (url, options) => {
            requests.push({ url, headers: { ...options.headers }, body: options.body });
            return { ok: false, status };
        };

        try {
            await transport.openOutputStream("session-persistent");
            await transport.writeCommand({ name: "PING", value: null });
            await assert.rejects(transport.closeOutputStream(),
                new RegExp("Got non-OK response: " + status));

            assert.equal(requests.length, status == 502 ? 3 : 1);
            for (const request of requests) {
                assert.deepEqual(request, requests[0]);
            }
            assert.equal(realm.table("pvc__payload_in").size, 0);
        } finally {
            globalThis.fetch = originalFetch;
            context.pvcAdminRealm = originalRealm;
            context.settings = originalSettings;
        }
    });
}

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
