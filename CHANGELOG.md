# Changelog

## 1.0.14 — 2026-10-30

- Retry HTTP 502, 503, and 504 responses using the existing bounded backoff.
- Preserve the session ID, message ID, and request body when retrying, including
  continuation requests in a multi-message sync.
- Keep other HTTP status errors non-retryable.
- Add regression coverage for gateway recovery, retry exhaustion, and response
  storage without duplication.

The default remains three retries with delays of one, two, and four seconds.
Configure `httpRetryCount` and `httpRetryDelayMillis` to override these values.
Persistent gateway timeouts still require investigation of proxy limits and
server processing time. The Pervasync protocol version and dependencies are
unchanged.
