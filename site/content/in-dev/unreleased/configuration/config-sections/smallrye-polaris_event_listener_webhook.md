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

Delivery is best-effort and in-process only: `onEvent` POSTs on the  shared event-listener executor. Transient failures retry immediately in that call; events that  exhaust retries are dropped. In-flight work is not durable and is lost on process restart. This  listener is not a durable audit spool and is not strengthened by enabling `persistence-in-memory-buffer` (that buffer is a separate listener, not a webhook delivery queue).   

HTTP redirects are not followed; the endpoint must respond directly. When `require-https` is `true` (the default), only `https` endpoints are accepted.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.event-listener.webhook.endpoint` |  | `uri` | HTTP(S) endpoint that receives webhook POSTs with a JSON body. <br><br>A 2xx response marks success. Transient failures (network errors, 408/425/429/5xx) are  retried immediately on the shared event-listener executor; permanent client errors (other 4xx)  are not retried.   <br><br>Required when the `webhook` event listener is enabled.   <br><br>Configuration property: `polaris.event-listener.webhook.endpoint` |
| `polaris.event-listener.webhook.secret` |  | `string` | Shared secret for HMAC-SHA256 signing. When set, each request includes `Polaris-Signature-256: sha256=<hex>` over the raw body. HMAC authenticates and protects  integrity; it does not encrypt the payload — use HTTPS for confidentiality.   <br><br>Configuration property: `polaris.event-listener.webhook.secret` |
| `polaris.event-listener.webhook.headers.`_`<name>`_ |  | `string` | Extra headers on every request (for example `Authorization`). Reserved headers ( `Content-Type` , `Polaris-Signature-256`, `Polaris-Event`, `Host`, `Content-Length` ) cannot be overridden. Header values may be secrets; protect them accordingly.   <br><br>Configuration property: `polaris.event-listener.webhook.headers."<header-name>"` |
| `polaris.event-listener.webhook.timeout` | `10s` | `duration` | Timeout for a single HTTP attempt. Timed-out attempts count as transient failures.   <br><br>Configuration property: `polaris.event-listener.webhook.timeout` |
| `polaris.event-listener.webhook.require-https` | `true` | `boolean` | When `true`, only `https` endpoints are allowed (recommended for production).  Set  `false` only for local testing with plain HTTP.   <br><br>Configuration property: `polaris.event-listener.webhook.require-https` |
