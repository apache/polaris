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
package org.apache.polaris.test.opa;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.polaris.containerspec.ContainerSpecHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

public class OpaContainer extends GenericContainer<OpaContainer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpaContainer.class);

  private static final int OPA_PORT = 8181;

  private URI externalUrl;

  @SuppressWarnings("resource")
  public OpaContainer() {
    super(
        ContainerSpecHelper.containerSpecHelper("opa", OpaContainer.class)
            .dockerImageName(null)
            .asCanonicalNameString());
    withExposedPorts(OPA_PORT);
    withCommand("run", "--server", "--addr=0.0.0.0:" + OPA_PORT);
    waitingFor(
        Wait.forHttp("/health")
            .forPort(OPA_PORT)
            .forStatusCode(200)
            .withStartupTimeout(Duration.ofSeconds(120)));
    withLogConsumer(new Slf4jLogConsumer(LOGGER));
  }

  @Override
  public void start() {
    super.start();
    externalUrl = URI.create("http://" + getHost() + ":" + getMappedPort(OPA_PORT) + "/");
  }

  public URI getExternalUrl() {
    return externalUrl;
  }

  public void uploadRegoPolicy(String name, String rego) {
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(getExternalUrl().resolve("v1/policies/" + name))
              .header("Content-Type", "text/plain")
              .PUT(HttpRequest.BodyPublishers.ofString(rego, StandardCharsets.UTF_8))
              .build();
      HttpResponse<Void> response;
      try (HttpClient client =
          HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
        response = client.send(request, HttpResponse.BodyHandlers.discarding());
      }
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new IllegalStateException("OPA policy upload failed, HTTP " + response.statusCode());
      }
    } catch (Exception e) {
      String logs = "";
      try {
        logs = getLogs();
      } catch (Throwable ignored) {
        // ignore logging failures while reporting the original startup failure
      }
      throw new IllegalStateException("Failed to load OPA policy. Container logs:\n" + logs, e);
    }
  }
}
