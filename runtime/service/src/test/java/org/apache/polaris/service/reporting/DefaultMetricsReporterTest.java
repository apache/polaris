package org.apache.polaris.service.reporting;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.tuples.Functions.TriConsumer;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.polaris.service.catalog.Profiles;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(DefaultMetricsReporterTest.Profile.class)
public class DefaultMetricsReporterTest {

  @Test
  void testLogging() {
    TriConsumer<String, TableIdentifier, MetricsReport> mockConsumer = mock(TriConsumer.class);
    DefaultMetricsReporter reporter = new DefaultMetricsReporter(mockConsumer);
    String warehouse = "testWarehouse";
    TableIdentifier table = TableIdentifier.of("testNamespace", "testTable");
    MetricsReport metricsReport = mock(MetricsReport.class);

    reporter.reportMetric(warehouse, table, metricsReport);

    verify(mockConsumer).accept(warehouse, table, metricsReport);
  }

  public static class Profile extends Profiles.DefaultProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("quarkus.log.category.\"polaris.service.reporting\".level", "INFO")
          .build();
    }
  }
}
