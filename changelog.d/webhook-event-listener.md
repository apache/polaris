### New Features

- Added a `webhook` event listener (`polaris.event-listener.types=webhook`) that delivers sanitized
  Polaris events as JSON HTTP POSTs to a configurable endpoint (SIEM/audit collectors). Payloads use
  the shared `CloudEventsEventJsonMapper` envelope (CE context at the top, Kafka-parity Polaris
  fields in `data`). Event time is the CloudEvents `time` field. Optional HMAC-SHA256 signing
  (`Polaris-Signature-256`). Delivery POSTs on the shared event-listener executor; transient
  failures retry immediately via SmallRye Fault Tolerance. HTTPS is required by default
  (`require-https`); reserved signature and event-type headers cannot be overridden by custom
  headers.
