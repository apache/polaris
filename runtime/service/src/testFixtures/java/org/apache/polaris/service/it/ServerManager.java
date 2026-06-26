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
package org.apache.polaris.service.it;

import io.quarkus.test.common.http.TestHTTPResourceManager;
import io.quarkus.test.config.ConfigInjector;
import io.quarkus.test.config.ValueRegistryInjector;
import io.quarkus.value.registry.ValueRegistry;
import java.net.URI;
import java.util.Optional;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.ClientPrincipal;
import org.apache.polaris.service.it.env.Server;
import org.apache.polaris.service.it.ext.PolarisServerManager;
import org.junit.jupiter.api.extension.ExtensionContext;

public class ServerManager implements PolarisServerManager {

  private static final ValueRegistry.RuntimeKey<Integer> MANAGEMENT_PORT =
      ValueRegistry.RuntimeKey.intKey("quarkus.management.port");

  @Override
  public Server serverForContext(ExtensionContext context) {
    return new Server() {

      @Override
      public URI baseUri() {
        var registry = ValueRegistryInjector.get(context);
        var config = ConfigInjector.get(context);
        return URI.create(TestHTTPResourceManager.testUrl(registry, config));
      }

      @Override
      public Optional<URI> managementUri() {
        var registry = ValueRegistryInjector.get(context);
        var config = ConfigInjector.get(context);
        // Probe whether the actual port to use has been registered. If not, the management
        // interface is not available, and we are likely in a @QuarkusIntegrationTest.
        int dynamicPort = registry.getOrDefault(MANAGEMENT_PORT, -1);
        return dynamicPort == -1
            ? Optional.empty()
            : Optional.of(URI.create(TestHTTPResourceManager.testManagementUrl(registry, config)));
      }

      @Override
      public ClientPrincipal adminCredentials() {
        // These credentials are injected via env. variables from build scripts.
        // Cf. POLARIS_BOOTSTRAP_CREDENTIALS in build.gradle.kts
        return new ClientPrincipal("root", new ClientCredentials("test-admin", "test-secret"));
      }

      @Override
      public void close() {
        // Nothing to do
      }
    };
  }
}
