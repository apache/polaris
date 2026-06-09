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
package org.apache.polaris.extension.auth.opa.test;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.polaris.containerspec.ContainerSpecHelper;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupAction;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/** Starts an OPA test server before the external Polaris server process starts. */
public class OpaStartupAction implements PolarisServerStartupAction {
  private static final int OPA_PORT = 8181;
  private static final String POLICY_NAME = "polaris-authz";

  private GenericContainer<?> opa;

  @Override
  @SuppressWarnings("resource")
  public void start(PolarisServerStartupContext context) {
    opa =
        new GenericContainer<>(
                ContainerSpecHelper.containerSpecHelper("opa", OpaStartupAction.class)
                    .dockerImageName(null))
            .withExposedPorts(OPA_PORT)
            .withCommand("run", "--server", "--addr=0.0.0.0:8181")
            .waitingFor(
                Wait.forHttp("/health")
                    .forPort(OPA_PORT)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofSeconds(120)));

    opa.start();

    String baseUrl = "http://" + opa.getHost() + ":" + opa.getMappedPort(OPA_PORT);
    loadRegoPolicy(baseUrl, POLICY_NAME, polarisRegoPolicy());
    context
        .getSystemProperties()
        .put("polaris.authorization.opa.policy-uri", baseUrl + "/v1/data/polaris/authz");
  }

  @Override
  public void close() {
    if (opa != null) {
      opa.stop();
      opa = null;
    }
  }

  private void loadRegoPolicy(String baseUrl, String policyName, String regoPolicy) {
    try {
      URL url = URI.create(baseUrl + "/v1/policies/" + policyName).toURL();
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
      String logs = "";
      try {
        logs = opa.getLogs();
      } catch (Throwable ignored) {
        // ignore logging failures while reporting the original startup failure
      }
      throw new RuntimeException("Failed to load OPA policy. Container logs:\n" + logs, e);
    }
  }

  private String polarisRegoPolicy() {
    return """
        package polaris.authz

        default allow := false

        # Allow root user for all operations
        allow if {
          input.actor.principal == "root"
        }

        # Allow admin user for all operations
        allow if {
          input.actor.principal == "admin"
        }
        """;
  }
}
