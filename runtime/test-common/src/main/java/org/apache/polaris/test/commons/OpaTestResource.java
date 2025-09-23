package org.apache.polaris.test.commons;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class OpaTestResource implements QuarkusTestResourceLifecycleManager {
  private GenericContainer<?> opa;
  private int mappedPort;

  @Override
  public Map<String, String> start() {
    opa =
        new GenericContainer<>(DockerImageName.parse("openpolicyagent/opa:latest"))
            .withExposedPorts(8181);
    opa.start();
    mappedPort = opa.getMappedPort(8181);
    String baseUrl = "http://localhost:" + mappedPort;
    // Load a simple Rego policy
    try {
      URL url = new URL(baseUrl + "/v1/policies/polaris-authz");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("PUT");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "text/plain");
      String rego = "package polaris.authz\n\nallow { input.principal == \"admin\" }";
      try (OutputStream os = conn.getOutputStream()) {
        os.write(rego.getBytes(StandardCharsets.UTF_8));
      }
      conn.getResponseCode();
    } catch (Exception e) {
      throw new RuntimeException("Failed to load OPA policy", e);
    }
    Map<String, String> config = new HashMap<>();
    config.put("polaris.authz.implementation", "opa");
    config.put("polaris.authz.opa.base-url", baseUrl);
    config.put("polaris.authz.opa.package", "polaris/authz");
    return config;
  }

  @Override
  public void stop() {
    if (opa != null) {
      opa.stop();
    }
  }
}
