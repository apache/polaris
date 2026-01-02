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
(port 8182 by default) under the path `/q/metrics`. For example, if the server is running on
localhost, the metrics can be accessed via http://localhost:8182/q/metrics.

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
  `X-Request-ID` header.
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

JSON logging can be enabled by setting the `quarkus.log.console.json.enabled` and `quarkus.log.file.json.enabled`
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
  `X-Request-ID` header.
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

## Compute Client Audit Reporting

Polaris supports end-to-end audit correlation between catalog operations, credential vending, and
compute engine metrics reports. This enables organizations to trace data access from the initial
catalog request through to actual S3/GCS/Azure storage access.

### Metrics Reporting Endpoint

Compute engines can report scan and commit metrics to Polaris using the standard Iceberg REST
Catalog metrics endpoint:

```
POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics
```

**Request Body**: A `ReportMetricsRequest` containing either a `ScanReport` or `CommitReport`:

```json
{
  "report-type": "scan-report",
  "table-name": "my_table",
  "snapshot-id": 123456789,
  "schema-id": 0,
  "projected-field-ids": [1, 2, 3],
  "projected-field-names": ["id", "name", "value"],
  "filter": {"type": "always-true"},
  "metrics": {
    "result-data-files": {"unit": "count", "value": 10},
    "total-file-size-bytes": {"unit": "bytes", "value": 1048576}
  },
  "metadata": {
    "trace-id": "abcdef1234567890abcdef1234567890",
    "client-app": "spark-3.5"
  }
}
```

**Response**: `204 No Content` on success.

The `metadata` map in the report can contain a `trace-id` for correlation with other audit events.
This trace ID is extracted and stored in the event's `additional_properties` with a `report.` prefix.

### Trace Correlation

When OpenTelemetry is enabled, Polaris captures the `trace_id` at multiple points:

1. **Catalog Operations**: Events like `loadTable`, `createTable` include the OpenTelemetry trace
   context in their metadata.
2. **Credential Vending**: When AWS STS session tags are enabled, the `trace_id` is included as a
   session tag (`polaris:trace_id`) in the vended credentials. This appears in AWS CloudTrail logs.
3. **Metrics Reports**: When compute engines report scan/commit metrics back to Polaris, the
   `reportMetrics` events capture both the OpenTelemetry trace context from HTTP headers and any
   `trace-id` passed in the report's `metadata` map.

### Enabling Session Tags for AWS

To enable session tags (including trace_id) in AWS STS credentials, set the following feature flag:

```properties
polaris.features."INCLUDE_SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL"=true
```

This adds the following tags to all STS AssumeRole requests:

- `polaris:catalog` - The catalog name
- `polaris:namespace` - The namespace being accessed
- `polaris:table` - The table name
- `polaris:principal` - The authenticated principal
- `polaris:roles` - The activated principal roles
- `polaris:trace_id` - The OpenTelemetry trace ID

These tags appear in AWS CloudTrail logs, enabling correlation with Polaris audit events.

**Note**: Enabling session tags requires the IAM role trust policy to allow the `sts:TagSession`
action. This feature may also reduce credential caching effectiveness since credentials become
specific to each table/namespace/role combination.

### Compute Engine Integration

For end-to-end trace correlation, compute engines should propagate the W3C Trace Context headers
when making requests to Polaris. The standard headers are:

- `traceparent`: Contains the trace ID, parent span ID, and trace flags
- `tracestate`: Optional vendor-specific trace information

#### Apache Spark

Spark can propagate trace context using the OpenTelemetry Java agent. Add the agent to your Spark
submit command:

```bash
spark-submit \
  --conf "spark.driver.extraJavaOptions=-javaagent:/path/to/opentelemetry-javaagent.jar" \
  --conf "spark.executor.extraJavaOptions=-javaagent:/path/to/opentelemetry-javaagent.jar" \
  -Dotel.service.name=spark-app \
  -Dotel.exporter.otlp.endpoint=http://collector:4317 \
  your-application.jar
```

Alternatively, configure the agent via environment variables:

```bash
export OTEL_SERVICE_NAME=spark-app
export OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317
export JAVA_TOOL_OPTIONS="-javaagent:/path/to/opentelemetry-javaagent.jar"
```

#### Trino

Trino supports OpenTelemetry tracing with the following configuration in `config.properties`:

```properties
tracing.enabled=true
tracing.exporter.endpoint=http://collector:4317
```

#### Flink

Flink can be configured with OpenTelemetry using the Java agent:

```bash
-javaagent:/path/to/opentelemetry-javaagent.jar \
-Dotel.service.name=flink-job \
-Dotel.exporter.otlp.endpoint=http://collector:4317
```

### Correlating Audit Events

With trace correlation enabled, you can join events across systems:

1. **Polaris Events**: Query the events table for operations with a specific `trace_id`
2. **CloudTrail Logs**: Filter by the `polaris:trace_id` session tag
3. **Compute Engine Logs**: Search for the same trace ID in engine logs

Example queries to find all Polaris events for a trace:

**PostgreSQL** (using JSON operators):
```sql
SELECT * FROM polaris_schema.events
WHERE additional_properties->>'otel.trace_id' = '<trace-id>'
   OR additional_properties->>'report.trace-id' = '<trace-id>'
ORDER BY timestamp_ms;
```

**H2/Generic SQL** (using LIKE pattern matching):
```sql
SELECT * FROM polaris_schema.events
WHERE additional_properties LIKE '%<trace-id>%'
ORDER BY timestamp_ms;
```

### Metrics Event Data

The `AfterReportMetricsEvent` captures the following data in `additional_properties`:

**For ScanReports:**
- `report_type`: "scan"
- `snapshot_id`: The snapshot ID being scanned
- `schema_id`: The schema ID
- `result_data_files`: Number of data files in the scan result
- `result_delete_files`: Number of delete files in the scan result
- `total_file_size_bytes`: Total size of files scanned
- `scanned_data_manifests`: Number of data manifests scanned
- `skipped_data_manifests`: Number of data manifests skipped
- `report.*`: Any metadata from the report's metadata map (e.g., `report.trace-id`)

**For CommitReports:**
- `report_type`: "commit"
- `snapshot_id`: The new snapshot ID
- `sequence_number`: The sequence number
- `operation`: The operation type (e.g., "append", "overwrite")
- `added_data_files`: Number of data files added
- `removed_data_files`: Number of data files removed
- `added_records`: Number of records added
- `removed_records`: Number of records removed
- `added_file_size_bytes`: Total size of files added
- `removed_file_size_bytes`: Total size of files removed
- `report.*`: Any metadata from the report's metadata map (e.g., `report.trace-id`)

## Links

Visit [Using Polaris with telemetry tools]({{% relref "getting-started/using-polaris/telemetry-tools" %}}) to see sample Polaris config with Prometheus and Jaeger.
