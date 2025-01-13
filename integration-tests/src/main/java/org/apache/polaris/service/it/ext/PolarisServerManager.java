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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.polaris.service.it.env.PolarisClient.buildObjectMapper;

import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import java.util.ServiceLoader;
import org.apache.polaris.service.it.env.Server;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * This is a plugin interfaces to allow different test execution environments to control how tests
 * access Polaris Servers when they run under {@link PolarisIntegrationTestExtension}.
 *
 * <p>Implementations are loaded via {@link ServiceLoader} assuming there is only one implementation
 * per test runtime class path.
 */
public interface PolarisServerManager {

  /**
   * Returns server connection parameters for the tests under the specified context.
   *
   * <p>Implementations may reuse the same server for multiple contexts (with the same of different
   * {@link Server#realmId() realm IDs}) or create a fresh server for each context. In any case,
   * {@link Server#close()} will be invoked when the context provided as the argument to this call
   * is closed.
   *
   * <p>Note: {@link Server} objects are generally attached to the test {@code class} context, but
   * this is not guaranteed.
   */
  Server serverForContext(ExtensionContext context);

  /** Create a new HTTP client for accessing the server targeted by tests. */
  default Client createClient() {
    return ClientBuilder.newBuilder()
        .readTimeout(5, MINUTES)
        .connectTimeout(1, MINUTES)
        .register(new JacksonJsonProvider(buildObjectMapper()))
        .build();
  }

  /**
   * Transforms the name of an entity that tests need to create. Implementations may prepend of
   * append text to the original name, but they should not alter the original name text or add any
   * characters that have special meaning in Spark SQL, HTTP or Iceberg REST specifications.
   *
   * <p>This method will be called for all top-level entities (catalogs, principal, principal
   * roles), but may not be called for secondary entities (such as catalog roles, namespaces,
   * tables, etc.).
   */
  default String transformEntityName(String name) {
    return name;
  }
}
