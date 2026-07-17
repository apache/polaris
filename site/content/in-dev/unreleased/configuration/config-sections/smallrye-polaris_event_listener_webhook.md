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

Configuration interface for the webhook event listener integration. 

Deliveries are performed asynchronously and do not block the event executor; in-flight  requests to a slow or unreachable endpoint are bounded by (`#timeout()`). Delivery is  best-effort: retries are kept in memory only, so pending retries are lost on shutdown, and an  event that exhausts all attempts is dropped (and logged). Deployments that need stronger delivery  guarantees should also enable the `persistence-in-memory-buffer` event listener. HTTP  redirects are not followed; the endpoint must respond directly.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.event-listener.webhook.endpoint` |  | `uri` | Returns the HTTP(S) endpoint that webhook events are delivered to. <br><br>Each event is sent as an HTTP POST request with a JSON body. The endpoint must return a 2xx  status code for the delivery to be considered successful; any other status code (or a  connection error) triggers a retry. HTTPS is strongly recommended because the payload may  contain sensitive audit data.   <br><br>This property is required when the `webhook` event listener is enabled.   <br><br>Configuration property: `polaris.event-listener.webhook.endpoint` |
| `polaris.event-listener.webhook.secret` |  | `string` | Returns the shared secret used to sign webhook payloads. <br><br>When set, each request carries an `X-Polaris-Signature-256` header containing the hex  encoded HMAC-SHA256 of the request body, prefixed with `sha256=`. Receivers should  recompute the HMAC over the raw request body and compare it against this header to verify  authenticity and integrity. If not set, no signature header is sent.   <br><br>Configuration property: `polaris.event-listener.webhook.secret` |
| `polaris.event-listener.webhook.headers.`_`<name>`_ |  | `string` | Returns additional HTTP headers to send with every webhook request. <br><br>This is typically used to authenticate against the receiver, for example  `polaris.event-listener.webhook.headers."Authorization"=Bearer ` or  `polaris.event-listener.webhook.headers."Authorization"=Splunk <hec-token>`. Header values may  contain secrets; protect them like any other Polaris credential.   <br><br>Configuration property: `polaris.event-listener.webhook.headers."<header-name>"` |
| `polaris.event-listener.webhook.timeout` | `10s` | `duration` | Returns the timeout for a single webhook delivery attempt. <br><br>If a delivery attempt does not complete within this duration, it is treated as failed and  the retry schedule applies.   <br><br>Configuration property: `polaris.event-listener.webhook.timeout` |
| `polaris.event-listener.webhook.max-attempts` | `3` | `int` | Returns the maximum number of delivery attempts per event, including the initial attempt. <br><br>When all attempts fail, the event is dropped and an error is logged.   <br><br>Configuration property: `polaris.event-listener.webhook.max-attempts` |
| `polaris.event-listener.webhook.retry-backoff` | `1s` | `duration` | Returns the initial backoff between delivery attempts. <br><br>The backoff doubles after each failed attempt (exponential backoff).   <br><br>Configuration property: `polaris.event-listener.webhook.retry-backoff` |
