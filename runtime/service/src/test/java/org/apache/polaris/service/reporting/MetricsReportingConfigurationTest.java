package org.apache.polaris.service.reporting;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class MetricsReportingConfigurationTest {
  @Inject MetricsReportingConfiguration metricsConfig;

  @Test
  @TestProfile(MetricsReportingConfiguration.DefaultProfile.class)
  void testDefault() {
    String type = metricsConfig.type();
    assertThat(type).isEqualTo("default");
  }

  @Test
  @TestProfile(MetricsReportingConfiguration.LoggingProfile.class)
  void testLogging() {
    assertThat(metricsConfig.type()).isEqualTo("logging");
  }

  public static class DefaultProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.metrics.reporting.type", "default")
          .build();
    }
  }

  public static class LoggingProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.metrics.reporting.type", "logging")
          .build();
    }
  }
}
