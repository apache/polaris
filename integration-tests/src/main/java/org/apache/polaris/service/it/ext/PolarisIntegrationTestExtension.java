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

import static org.apache.polaris.service.it.ext.PolarisServerManagerLoader.polarisServerManager;

import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.ClientPrincipal;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.Server;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.engine.UniqueId;

/**
 * A JUnit test extension that connects {@link PolarisServerManager} with test code by resolving
 * test parameters and managing the lifecycle of {@link Server} objects.
 */
public class PolarisIntegrationTestExtension implements ParameterResolver {
  private static final Namespace NAMESPACE =
      Namespace.create(PolarisIntegrationTestExtension.class);

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Class<?> type = parameterContext.getParameter().getType();
    return type.isAssignableFrom(PolarisApiEndpoints.class)
        || type.isAssignableFrom(ClientPrincipal.class)
        || type.isAssignableFrom(ClientCredentials.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Env env = env(extensionContext);
    Class<?> type = parameterContext.getParameter().getType();
    if (type.isAssignableFrom(PolarisApiEndpoints.class)) {
      return env.endpoints();
    } else if (type.isAssignableFrom(ClientPrincipal.class)) {
      return env.server.adminCredentials();
    } else if (type.isAssignableFrom(ClientCredentials.class)) {
      return env.server.adminCredentials().credentials();
    }
    throw new IllegalStateException("Unable to resolve parameter: " + parameterContext);
  }

  private Env env(ExtensionContext context) {
    ExtensionContext classCtx = classContext(context);
    ExtensionContext.Store store = classCtx.getStore(NAMESPACE);
    return store.getOrComputeIfAbsent(
        Env.class, (key) -> new Env(polarisServerManager().serverForContext(classCtx)), Env.class);
  }

  private ExtensionContext classContext(ExtensionContext context) {
    while (context.getParent().isPresent()) {
      UniqueId id = UniqueId.parse(context.getUniqueId());
      if ("class".equals(id.getLastSegment().getType())) {
        break;
      }

      context = context.getParent().get();
    }

    return context;
  }

  private static class Env implements AutoCloseable {
    private final Server server;
    private final PolarisApiEndpoints endpoints;

    private Env(Server server) {
      this.server = server;
      this.endpoints =
          new PolarisApiEndpoints(server.baseUri(), server.realmId(), server.realmHeaderName());
    }

    PolarisApiEndpoints endpoints() {
      return endpoints;
    }

    @Override
    public void close() throws Exception {
      server.close();
    }
  }
}
