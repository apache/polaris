package org.apache.polaris.test.commons;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.List;
import java.util.Map;

public class OpaIntegrationProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    // Additional config overrides can be added here
    return Map.of();
  }

  @Override
  public List<TestResourceEntry> testResources() {
    return List.of(new TestResourceEntry(OpaTestResource.class, Map.of()));
  }
}
