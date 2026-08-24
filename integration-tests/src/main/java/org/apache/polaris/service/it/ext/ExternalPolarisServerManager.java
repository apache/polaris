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
package org.apache.polaris.service.it.ext;

import java.net.URI;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.ClientPrincipal;
import org.apache.polaris.service.it.env.Server;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * {@link PolarisServerManager} implementation for tests whose Polaris server lifecycle is managed
 * outside JUnit, for example by Gradle.
 */
public class ExternalPolarisServerManager implements PolarisServerManager {

  public static final String HTTP_PORT_PROPERTY = "quarkus.http.test-port";
  public static final String MANAGEMENT_PORT_PROPERTY = "quarkus.management.test-port";
  public static final String HOST_PROPERTY = "polaris.test.server.host";

  @Override
  public Server serverForContext(ExtensionContext context) {
    URI baseUri = formatUri(httpPort());
    Optional<URI> managementUri =
        managementPort().stream().boxed().map(ExternalPolarisServerManager::formatUri).findFirst();
    return new Server() {
      @Override
      public URI baseUri() {
        return baseUri;
      }

      @Override
      public Optional<URI> managementUri() {
        return managementUri;
      }

      @Override
      public ClientPrincipal adminCredentials() {
        return new ClientPrincipal("root", new ClientCredentials("test-admin", "test-secret"));
      }

      @Override
      public void close() {
        // The external process owner is responsible for server lifecycle.
      }
    };
  }

  private static URI formatUri(int port) {
    return URI.create(String.format("http://%s:%d", host(), port));
  }

  private static String host() {
    String host = System.getProperty(HOST_PROPERTY, "localhost");
    return host.isBlank() ? "localhost" : host;
  }

  private static int httpPort() {
    String port = System.getProperty(HTTP_PORT_PROPERTY);
    return portAsInt(port, HTTP_PORT_PROPERTY);
  }

  private static OptionalInt managementPort() {
    String port = System.getProperty(MANAGEMENT_PORT_PROPERTY);
    return port == null || port.isBlank()
        ? OptionalInt.empty()
        : OptionalInt.of(portAsInt(port, MANAGEMENT_PORT_PROPERTY));
  }

  private static int portAsInt(String port, String property) {
    if (port == null || port.isBlank()) {
      throw new IllegalStateException(String.format("System property '%s' must be set", property));
    }
    try {
      int parsed = Integer.parseInt(port);
      if (parsed <= 0 || parsed > 65535) {
        throw new IllegalArgumentException("Port out of range: " + parsed);
      }
      return parsed;
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          String.format("System property '%s' must be a valid TCP port: %s", property, port), e);
    }
  }
}
