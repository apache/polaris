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
title: Telemetry
type: docs
weight: 450
---

## Metrics

Metrics are published using [Micrometer]; they are available from Polaris's management interface
(port 8282 by default) under the path `/q/metrics`. For example, if the server is running on
localhost, the metrics can be accessed via http://localhost:8282/q/metrics.

[Micrometer]: https://quarkus.io/guides/telemetry-micrometer

Metrics can be scraped by Prometheus or any compatible metrics scraping server. See:
[Prometheus](https://prometheus.io) for more information.

Additional tags can be added to the metrics by setting the `polaris.metrics.tags.*` property. Each
tag is a key-value pair, where the key is the tag name and the value is the tag value. For example,
to add a tag `environment=prod` to all metrics, set `polaris.metrics.tags.environment=prod`. Many
tags can be added, such as below:

```properties
polaris.metrics.tags.service=polaris
polaris.metrics.tags.environment=prod
polaris.metrics.tags.region=us-west-2
```

Note that by default Polaris adds one tag: `application=Polaris`. You can override this tag by
setting the `polaris.metrics.tags.application=<new-value>` property.

### Realm ID Tag

Polaris can add the realm ID as a tag to all API and HTTP request metrics. This is disabled by
default to prevent high cardinality issues, but can be enabled by setting the following properties:

```properties
polaris.metrics.realm-id-tag.enable-in-api-metrics=true
polaris.metrics.realm-id-tag.enable-in-http-metrics=true
```

You should be particularly careful when enabling the realm ID tag in HTTP request metrics, as these
metrics typically have a much higher cardinality than API request metrics.

In order to prevent the number of tags from growing indefinitely and causing performance issues or
crashing the server, the number of unique realm IDs in HTTP request metrics is limited to 100 by
default. If the number of unique realm IDs exceeds this value, a warning will be logged and no more
HTTP request metrics will be recorded. This threshold can be changed by setting the
`polaris.metrics.realm-id-tag.http-metrics-max-cardinality` property.

## Traces

Traces are published using [OpenTelemetry].

[OpenTelemetry]: https://quarkus.io/guides/opentelemetry-tracing

By default OpenTelemetry is disabled in Polaris, because there is no reasonable default
for the collector endpoint for all cases.

To enable OpenTelemetry and publish traces for Polaris set `quarkus.otel.sdk.disabled=false`
and configure a valid collector endpoint URL with `http://` or `https://` as the server property
`quarkus.otel.exporter.otlp.traces.endpoint`.

_If these properties are not set, the server will not publish traces._

The collector must talk the OpenTelemetry protocol (OTLP) and the port must be its gRPC port
(by default 4317), e.g. "http://otlp-collector:4317".

By default, Polaris adds a few attributes to the [OpenTelemetry Resource] to identify the server,
and notably:

- `service.name`: set to `Apache Polaris Server (incubating)`;
- `service.version`: set to the Polaris version.

[OpenTelemetry Resource]: https://opentelemetry.io/docs/languages/js/resources/

You can override the default resource attributes or add additional ones by setting the
`quarkus.otel.resource.attributes` property.

This property expects a comma-separated list of key-value pairs, where the key is the attribute name
and the value is the attribute value. For example, to change the service name to `Polaris` and add
an attribute `deployment.environment=dev`, set the following property:

```properties
quarkus.otel.resource.attributes=service.name=Polaris,deployment.environment=dev
```

The alternative syntax below can also be used:

```properties
quarkus.otel.resource.attributes[0]=service.name=Polaris
quarkus.otel.resource.attributes[1]=deployment.environment=dev
```

Finally, two additional span attributes are added to all request parent spans:

- `polaris.request.id`: The unique identifier of the request, if set by the caller through the
  `Polaris-Request-Id` header.
- `polaris.realm`: The unique identifier of the realm. Always set (unless the request failed because
  of a realm resolution error).

### Troubleshooting Traces

If the server is unable to publish traces, check first for a log warning message like the following:

```
SEVERE [io.ope.exp.int.grp.OkHttpGrpcExporter] (OkHttp http://localhost:4317/...) Failed to export spans.
The request could not be executed. Full error message: Failed to connect to localhost/0:0:0:0:0:0:0:1:4317
```

This means that the server is unable to connect to the collector. Check that the collector is
running and that the URL is correct.

## Logging

Polaris relies on [Quarkus](https://quarkus.io/guides/logging) for logging.

By default, logs are written to the console and to a file located in the `./logs` directory. The log
file is rotated daily and compressed. The maximum size of the log file is 10MB, and the maximum
number of backup files is 14.

JSON logging can be enabled by setting the `quarkus.log.console.json` and `quarkus.log.file.json`
properties to `true`. By default, JSON logging is disabled.

The log level can be set for the entire application or for specific packages. The default log level
is `INFO`. To set the log level for the entire application, use the `quarkus.log.level` property.

To set the log level for a specific package, use the `quarkus.log.category."package-name".level`,
where `package-name` is the name of the package. For example, the package `io.smallrye.config` has a
useful logger to help debugging configuration issues; but it needs to be set to the `DEBUG` level.
This can be done by setting the following property:

```properties
quarkus.log.category."io.smallrye.config".level=DEBUG
```

The log message format for both console and file output is highly configurable. The default format
is:

```
%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c{3.}] [%X{requestId},%X{realmId}] [%X{traceId},%X{parentId},%X{spanId},%X{sampled}] (%t) %s%e%n
```

Refer to the [Logging format](https://quarkus.io/guides/logging#logging-format) guide for more
information on placeholders and how to customize the log message format.

### MDC Logging

Polaris uses Mapped Diagnostic Context (MDC) to enrich log messages with additional context. The
following MDC keys are available:

- `requestId`: The unique identifier of the request, if set by the caller through the
  `Polaris-Request-Id` header.
- `realmId`: The unique identifier of the realm. Always set.
- `traceId`: The unique identifier of the trace. Present if tracing is enabled and the message is
  originating from a traced context.
- `parentId`: The unique identifier of the parent span. Present if tracing is enabled and the
  message is originating from a traced context.
- `spanId`: The unique identifier of the span. Present if tracing is enabled and the message is
  originating from a traced context.
- `sampled`: Whether the trace has been sampled. Present if tracing is enabled and the message is
  originating from a traced context.

Other MDC keys can be added by setting the `polaris.log.mdc.*` property. Each property is a
key-value pair, where the key is the MDC key name and the value is the MDC key value. For example,
to add the MDC keys `environment=prod` and `region=us-west-2` to all log messages, set the following
properties:

```properties
polaris.log.mdc.environment=prod
polaris.log.mdc.region=us-west-2
```

MDC context is propagated across threads, including in `TaskExecutor` threads.
