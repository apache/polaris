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

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.ClientPrincipal;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.Server;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class PolarisIntegrationTestExtensionTest {

  private static final AtomicInteger managementUriCalls = new AtomicInteger();

  @RegisterExtension
  static final PolarisIntegrationTestExtension extension =
      new PolarisIntegrationTestExtension(
          context ->
              new Server() {
                @Override
                public URI baseUri() {
                  return URI.create("http://localhost:8080");
                }

                @Override
                public Optional<URI> managementUri() {
                  managementUriCalls.incrementAndGet();
                  return Optional.empty();
                }

                @Override
                public ClientPrincipal adminCredentials() {
                  return new ClientPrincipal(
                      "root", new ClientCredentials("test-admin", "test-secret"));
                }

                @Override
                public void close() {
                  // Nothing to do
                }
              });

  @Test
  void resolvesPolarisEndpointsWithoutRetrievingManagementUri(PolarisApiEndpoints endpoints) {
    assertThat(endpoints).isNotNull();
    assertThat(managementUriCalls).hasValue(0);
  }
}
