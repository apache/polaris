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
title: Events
type: docs
weight: 460
---

Polaris emits events for catalog, namespace, table, view, policy, principal, task, and other
server-side operations. Events can be delivered to configured event listeners for auditing,
observability, or downstream processing.

Each event has:

- `type`: the event type, such as `BEFORE_CREATE_TABLE` or `AFTER_CREATE_TABLE`.
- `metadata`: event metadata, including the event ID, timestamp, realm ID, user, request ID, and
  OpenTelemetry context.
- `attributes`: typed operation-specific values, such as `catalog_name`, `table_name`, or request
  and response objects.

Built-in event types and categories are defined in the [`PolarisEventType`] class.

Built-in attribute names are defined in the [`EventAttributes`] class. Custom attributes can be
added by event emitters and listeners.

{{< alert note >}}
Custom event attributes are currently not supported in the event filter expressions. Only built-in 
attributes can be used in filter expressions.
{{< /alert >}}

[`PolarisEventType`]: https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/events/PolarisEventType.java

[`EventAttributes`]: https://github.com/apache/polaris/blob/main/runtime/service/src/main/java/org/apache/polaris/service/events/EventAttributes.java

## Configuring Event Listeners

Enable event listeners with `polaris.event-listener.types`. The value is a comma-separated list of
registered event listener identifiers. Polaris includes listener implementations such as
`persistence-in-memory-buffer` and `aws-cloudwatch`.

```properties
polaris.event-listener.types=persistence-in-memory-buffer,aws-cloudwatch
```

By default, a configured listener receives all event types. You can restrict a listener to selected
event types or categories:

```properties
polaris.event-listener.types=aws-cloudwatch
polaris.event-listener.aws-cloudwatch.enabled-event-categories=TABLE,VIEW
polaris.event-listener.aws-cloudwatch.enabled-event-types=AFTER_CREATE_NAMESPACE
```

When both event categories and event types are configured for a listener, the listener receives the
union of both selections.

## Event Filters

Event filters let you route only matching events to selected listeners. Filters are named
independently of listeners, then mapped to one or more listener identifiers.

The only supported filter type is `jakarta-el` which uses the [Jakarta Expression Language](https://jakarta.ee/specifications/expression-language/6.0/jakarta-expression-language-spec-6.0).

The expression predicates can read these beans:

- `type`: the event type.
- `metadata`: the event metadata object, containing the following methods:
  - `id()`: the event ID.
  - `timestamp()`: the event timestamp.
  - `realmId()`: the realm ID.
  - `user()`: the `PolarisPrincipal` instance for the user that triggered the event.
  - `requestId()`: the request ID.
  - `otelContext()`: the OpenTelemetry context.
- `attributes`: the event attribute map.

{{< alert important >}}
Jakarta EL expressions are evaluated in-process and can access sensitive information, e.g. the 
`PolarisPrincipal` instance via `metadata.user()`. It is therefore strongly advised to never source 
filter expressions from untrusted input. 
{{< /alert >}}

Use `==` for equality comparisons. Event attributes can be accessed by name, for example
`attributes.catalog_name`, or with bracket notation, for example `attributes['table_name']`.

Filters are referenced by name from the listener configuration. Each listener declares which named
filters apply to it, in the order they should be evaluated:

```properties
polaris.event-listener.<listener-name>.filters=<filter1>,<filter2>,...
```

Each filter can define:

- `include`: an expression the event must match.
- `exclude`: an expression the event must not match.

At least one of `include` or `exclude` must be configured. An event passes a filter when the
`include` expression is absent or evaluates to `true`, and the `exclude` expression is absent or
evaluates to `false`.

Example:

```properties
polaris.event-listener.types=aws-cloudwatch,persistence-in-memory-buffer
polaris.event-listener.aws-cloudwatch.filters=sales-tables
polaris.event-filter.sales-tables.type=jakarta-el
polaris.event-filter.sales-tables.include=metadata.realmId() == "realm1" && attributes.catalog_name == "prod"
polaris.event-filter.sales-tables.exclude=attributes.table_name.startsWith("tmp_")
```

This example delivers matching events to `aws-cloudwatch` only when they are emitted in `realm1`,
target catalog `prod`, and do not target a table whose name starts with `tmp_`. The
`persistence-in-memory-buffer` listener declares no filters, so it continues to receive all events
selected by its listener configuration.

If multiple filters are listed for a listener, an event must pass all of them before it is
delivered to that listener. Filters are evaluated in the declared order.

Example:

```properties
polaris.event-listener.types=aws-cloudwatch
polaris.event-listener.aws-cloudwatch.filters=realm,catalog
polaris.event-filter.realm.type=jakarta-el
polaris.event-filter.realm.include=metadata.realmId() == "realm1"
polaris.event-filter.catalog.type=jakarta-el
polaris.event-filter.catalog.include=attributes.catalog_name == "prod"
```

With this configuration, `aws-cloudwatch` receives only events that match both the `realm` and
`catalog` filters.

See the [configuration reference]({{% ref "configuration/configuration-reference.md#events" %}})
for the complete list of event listener and event filter properties.
