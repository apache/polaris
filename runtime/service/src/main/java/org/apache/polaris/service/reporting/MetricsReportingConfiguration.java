package org.apache.polaris.service.reporting;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "polaris.metrics.reporting")
public interface MetricsReportingConfiguration {
  @WithDefault("default")
  String type();
}
