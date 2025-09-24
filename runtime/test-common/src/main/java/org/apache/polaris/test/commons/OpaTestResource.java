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
package org.apache.polaris.test.commons;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

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
    // Reuse container across tests to speed up execution
    if (opa == null || !opa.isRunning()) {
      opa =
          new GenericContainer<>(DockerImageName.parse("openpolicyagent/opa:0.63.0"))
              .withExposedPorts(8181)
              .withCommand("run", "--server", "--addr=0.0.0.0:8181")
              .withReuse(true)
              .waitingFor(
                  Wait.forHttp("/health")
                      .forPort(8181)
                      .forStatusCode(200)
                      .withStartupTimeout(Duration.ofSeconds(30)));
      opa.start();
    }

    mappedPort = opa.getMappedPort(8181);
    String baseUrl = "http://localhost:" + mappedPort;

    loadRegoPolicy(baseUrl);

    Map<String, String> config = new HashMap<>();
    config.put("polaris.authorization.opa.url", baseUrl);
    return config;
  }

  private void loadRegoPolicy(String baseUrl) {
    String policyName = resourceConfig.get("policy-name");
    String regoPolicy = resourceConfig.get("rego-policy");

    if (policyName == null) {
      throw new IllegalArgumentException("policy-name parameter is required for OpaTestResource");
    }
    if (regoPolicy == null) {
      throw new IllegalArgumentException("rego-policy parameter is required for OpaTestResource");
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
