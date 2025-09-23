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
