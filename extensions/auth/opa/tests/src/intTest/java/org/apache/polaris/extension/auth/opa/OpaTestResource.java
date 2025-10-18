/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.extension.auth.opa;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.containerspec.ContainerSpecHelper;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class OpaTestResource implements QuarkusTestResourceLifecycleManager {
  private static GenericContainer<?> opa;
  private int mappedPort;
  private Map<String, String> resourceConfig;

  @Override
  public void init(Map<String, String> initArgs) {
    this.resourceConfig = initArgs;
  }

  @Override
  public Map<String, String> start() {
    try {
      // Reuse container across tests to speed up execution
      if (opa == null || !opa.isRunning()) {
        opa =
            new GenericContainer<>(
                ContainerSpecHelper.containerSpecHelper("opa", OpaTestResource.class)
                    .dockerImageName(null))
                .withExposedPorts(8181)
                .withReuse(true)
                .withCommand("run", "--server", "--addr=0.0.0.0:8181")
                .waitingFor(
                    Wait.forHttp("/health")
                        .forPort(8181)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofSeconds(120)));

        opa.start();
      }

      mappedPort = opa.getMappedPort(8181);
      String containerHost = opa.getHost();
      String baseUrl = "http://" + containerHost + ":" + mappedPort;

      // Load Rego policy into OPA
      loadRegoPolicy(baseUrl, "policy-name", "rego-policy");

      Map<String, String> config = new HashMap<>();
      config.put("polaris.authorization.opa.url", baseUrl);

      return config;

    } catch (Exception e) {
      throw new RuntimeException("Failed to start OPA test resource", e);
    }
  }

  private void loadRegoPolicy(String baseUrl, String policyNameKey, String regoPolicyKey) {
    String policyName = resourceConfig.get(policyNameKey);
    String regoPolicy = resourceConfig.get(regoPolicyKey);

    if (policyName == null) {
      throw new IllegalArgumentException(
          policyNameKey + " parameter is required for OpaTestResource");
    }
    if (regoPolicy == null) {
      throw new IllegalArgumentException(
          regoPolicyKey + " parameter is required for OpaTestResource");
    }

    try {
      URL url = new URL(baseUrl + "/v1/policies/" + policyName);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("PUT");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "text/plain");

      try (OutputStream os = conn.getOutputStream()) {
        os.write(regoPolicy.getBytes(StandardCharsets.UTF_8));
      }

      int code = conn.getResponseCode();
      if (code < 200 || code >= 300) {
        throw new RuntimeException("OPA policy upload failed, HTTP " + code);
      }
    } catch (Exception e) {
      // Surface container logs to help debug on CI
      String logs = "";
      try {
        logs = opa.getLogs();
      } catch (Throwable ignored) {
      }
      throw new RuntimeException("Failed to load OPA policy. Container logs:\n" + logs, e);
    }
  }

  @Override
  public void stop() {
    // Don't stop the container to allow reuse across tests
    // Container will be cleaned up when the JVM exits
  }
}
