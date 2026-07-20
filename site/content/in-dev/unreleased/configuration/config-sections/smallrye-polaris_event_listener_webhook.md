---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
title: smallrye-polaris_event_listener_webhook
build:
  list: never
  render: never
---

Configuration for the webhook event listener.

Delivery is **best-effort and in-process only**: pending and in-flight deliveries are not durable
and are lost on process restart; events that exhaust retries or cannot be accepted into the bounded
queue are dropped. Ordering is not guaranteed. This listener is not a durable audit spool and is
**not** strengthened by enabling `persistence-in-memory-buffer` (that buffer is a separate listener,
not a webhook delivery queue).

HTTP redirects are not followed. When `require-https` is `true` (default), only `https` endpoints
are accepted.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.event-listener.webhook.endpoint` |  | `uri` | HTTP(S) endpoint that receives JSON POSTs. Required when the webhook listener is enabled. |
| `polaris.event-listener.webhook.secret` |  | `string` | Optional HMAC-SHA256 shared secret (`X-Polaris-Signature-256: sha256=<hex>`). Protects authenticity/integrity, not confidentiality. |
| `polaris.event-listener.webhook.headers.`_`<name>`_ |  | `string` | Extra headers (e.g. `Authorization`). Reserved headers (`Content-Type`, `X-Polaris-Signature-256`, `X-Polaris-Event`, `Host`, `Content-Length`) cannot be overridden. |
| `polaris.event-listener.webhook.timeout` | `10s` | `duration` | Timeout for a single HTTP attempt. |
| `polaris.event-listener.webhook.max-attempts` | `3` | `int` | Max attempts per event including the first. |
| `polaris.event-listener.webhook.retry-backoff` | `1s` | `duration` | Base retry delay (exponential growth with full jitter). |
| `polaris.event-listener.webhook.max-retry-backoff` | `1m` | `duration` | Cap on retry delay (also caps `Retry-After`). |
| `polaris.event-listener.webhook.max-concurrent` | `16` | `int` | Max concurrent HTTP deliveries. |
| `polaris.event-listener.webhook.max-pending` | `1000` | `int` | Max events accepted for delivery/retry; excess are dropped. |
| `polaris.event-listener.webhook.require-https` | `true` | `boolean` | When true, only `https` endpoints are allowed. |
