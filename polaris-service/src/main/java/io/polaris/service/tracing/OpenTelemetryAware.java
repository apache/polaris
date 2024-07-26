package io.polaris.service.tracing;

import io.opentelemetry.api.OpenTelemetry;

/** Allows setting a configured instance of {@link OpenTelemetry} */
public interface OpenTelemetryAware {
  void setOpenTelemetry(OpenTelemetry openTelemetry);
}
