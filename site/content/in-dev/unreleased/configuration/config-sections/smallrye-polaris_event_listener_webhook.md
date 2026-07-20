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

Delivery is best-effort and in-process only: pending and in-flight deliveries  are not durable and are lost on process restart; events that exhaust retries or cannot be  accepted into the bounded queue are dropped. Ordering is not guaranteed. This listener is not a  durable audit spool and is not strengthened by enabling `persistence-in-memory-buffer` (that buffer is a separate listener, not a webhook delivery queue).   

HTTP redirects are not followed; the endpoint must respond directly. When `require-https` is `true` (the default), only `https` endpoints are accepted.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.event-listener.webhook.endpoint` |  | `uri` | HTTP(S) endpoint that receives webhook POSTs with a JSON body. <br><br>A 2xx response marks success. Transient failures (network errors, 408/429/5xx) are retried  within the concurrency limits; permanent client errors (other 4xx) are not retried.   <br><br>Required when the `webhook` event listener is enabled.   <br><br>Configuration property: `polaris.event-listener.webhook.endpoint` |
| `polaris.event-listener.webhook.secret` |  | `string` | Shared secret for HMAC-SHA256 signing. When set, each request includes `X-Polaris-Signature-256: sha256=<hex>` over the raw body. HMAC authenticates and protects  integrity; it does not encrypt the payload — use HTTPS for confidentiality.   <br><br>Configuration property: `polaris.event-listener.webhook.secret` |
| `polaris.event-listener.webhook.headers.`_`<name>`_ |  | `string` | Extra headers on every request (for example `Authorization`). Reserved headers ( `Content-Type` , `X-Polaris-Signature-256`, `X-Polaris-Event`, `Host`, `Content-Length` ) cannot be overridden. Header values may be secrets; protect them accordingly.   <br><br>Configuration property: `polaris.event-listener.webhook.headers."<header-name>"` |
| `polaris.event-listener.webhook.timeout` | `10s` | `duration` | Timeout for a single HTTP attempt. Timed-out attempts count as transient failures.   <br><br>Configuration property: `polaris.event-listener.webhook.timeout` |
| `polaris.event-listener.webhook.max-attempts` | `3` | `int` | Maximum delivery attempts per event including the first try. After this, the event is dropped.   <br><br>Configuration property: `polaris.event-listener.webhook.max-attempts` |
| `polaris.event-listener.webhook.retry-backoff` | `1s` | `duration` | Base delay before the first retry. Subsequent delays grow exponentially with full jitter,  capped by `max-retry-backoff`.   <br><br>Configuration property: `polaris.event-listener.webhook.retry-backoff` |
| `polaris.event-listener.webhook.max-retry-backoff` | `1m` | `duration` | Upper bound on retry delay after exponential growth and jitter. <br><br>Configuration property: `polaris.event-listener.webhook.max-retry-backoff` |
| `polaris.event-listener.webhook.max-concurrent` | `16` | `int` | Maximum concurrent HTTP deliveries (including retries currently executing). Additional work  waits on the concurrency limit while still counting toward `max-pending`.   <br><br>Configuration property: `polaris.event-listener.webhook.max-concurrent` |
| `polaris.event-listener.webhook.max-pending` | `1000` | `int` | Maximum events accepted for delivery or retry at once. When full, new events are dropped.   <br><br>Configuration property: `polaris.event-listener.webhook.max-pending` |
| `polaris.event-listener.webhook.require-https` | `true` | `boolean` | When `true`, only `https` endpoints are allowed (recommended for production).  Set  `false` only for local testing with plain HTTP.   <br><br>Configuration property: `polaris.event-listener.webhook.require-https` |
