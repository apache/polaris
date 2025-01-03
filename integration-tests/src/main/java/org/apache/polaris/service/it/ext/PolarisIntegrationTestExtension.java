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

import static java.util.concurrent.TimeUnit.*;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.ext.ContextResolver;
import java.util.ServiceLoader;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.polaris.service.it.env.AuthToken;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.PolarisApiClient;
import org.apache.polaris.service.it.env.Server;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.engine.UniqueId;

public class PolarisIntegrationTestExtension implements ParameterResolver {
  private static final Namespace NAMESPACE =
      Namespace.create(PolarisIntegrationTestExtension.class);

  private static final PolarisServerManager manager =
      ServiceLoader.load(PolarisServerManager.class)
          .findFirst()
          .orElseThrow(() -> new IllegalStateException("PolarisServerManager not found"));

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Class<?> type = parameterContext.getParameter().getType();
    return type.isAssignableFrom(PolarisApiClient.class)
        || type.isAssignableFrom(ClientCredentials.class)
        || type.isAssignableFrom(AuthToken.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Env env = env(extensionContext);
    Class<?> type = parameterContext.getParameter().getType();
    if (type.isAssignableFrom(PolarisApiClient.class)) {
      return env.client();
    } else if (type.isAssignableFrom(ClientCredentials.class)) {
      return env.server.adminCredentials();
    } else if (type.isAssignableFrom(AuthToken.class)) {
      return env.server.adminToken();
    }
    throw new IllegalStateException("Unable to resolve parameter: " + parameterContext);
  }

  private Env env(ExtensionContext context) {
    ExtensionContext classCtx = classContext(context);
    ExtensionContext.Store store = classCtx.getStore(NAMESPACE);
    return store.getOrComputeIfAbsent(
        Env.class, (key) -> new Env(manager.realmForContext(classCtx)), Env.class);
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

  private static class Env implements CloseableResource {
    private final Server server;
    private final Client client;

    private Env(Server server) {
      this.server = server;

      ObjectMapper mapper = new ObjectMapper();
      mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      mapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
      RESTSerializers.registerAll(mapper);

      // Note: converting to lambda apparently breaks discovery/injections in the client below
      //noinspection Convert2Lambda
      ContextResolver<ObjectMapper> mapperProvider =
          new ContextResolver<>() {
            @Override
            public ObjectMapper getContext(Class<?> type) {
              return mapper;
            }
          };

      this.client =
          ClientBuilder.newBuilder()
              .readTimeout(5, MINUTES)
              .connectTimeout(1, MINUTES)
              .register(mapperProvider)
              .build();
    }

    PolarisApiClient client() {
      return new PolarisApiClient(client, server.realmId(), server.baseUri());
    }

    @Override
    public void close() throws Throwable {
      server.close();
      client.close();
    }
  }
}
