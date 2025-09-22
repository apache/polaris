package org.apache.polaris.service.it;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.service.config.ServiceProducers;
import org.junit.jupiter.api.Test;

@QuarkusTest
@io.quarkus.test.junit.TestProfile(ServiceProducersIT.InlineConfig.class)
public class ServiceProducersIT {

  public static class InlineConfig implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.implementation", "default");
      config.put("polaris.authorization.opa.url", "http://localhost:8181");
      config.put("polaris.authorization.opa.policy-path", "/v1/data/polaris/allow");
      config.put("polaris.authorization.opa.timeout-ms", "2000");
      return config;
    }
  }

  @Inject ServiceProducers serviceProducers;

  @Test
  void testPolarisAuthorizerProduced() {
    PolarisAuthorizer authorizer = serviceProducers.polarisAuthorizer();
    assertNotNull(authorizer, "PolarisAuthorizer should be produced");
    // Optionally, add more assertions based on expected type/config
  }
}
