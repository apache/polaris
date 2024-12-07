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
package org.apache.polaris.service.test;

import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.apache.polaris.service.test.DropwizardTestEnvironmentResolver.findDropwizardExtension;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.auth.TokenUtils;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class PolarisConnectionExtension
    implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private MetaStoreManagerFactory metaStoreManagerFactory;
  private DropwizardAppExtension dropwizardAppExtension;

  public record PolarisToken(String token) {}

  private static PolarisPrincipalSecrets adminSecrets;
  private static String realm;

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    dropwizardAppExtension = findDropwizardExtension(extensionContext);
    if (dropwizardAppExtension == null) {
      return;
    }

    // Generate unique realm using test name for each test since the tests can run in parallel
    realm = extensionContext.getRequiredTestClass().getName().replace('.', '_');
    extensionContext
        .getStore(Namespace.create(extensionContext.getRequiredTestClass()))
        .put(REALM_PROPERTY_KEY, realm);

    try {
      PolarisApplicationConfig config =
          (PolarisApplicationConfig) dropwizardAppExtension.getConfiguration();
      metaStoreManagerFactory = config.getMetaStoreManagerFactory();
      if (!(metaStoreManagerFactory instanceof InMemoryPolarisMetaStoreManagerFactory)) {
        metaStoreManagerFactory.bootstrapRealms(List.of(realm));
      }

      URI testEnvUri = TestEnvironmentExtension.getEnv(extensionContext).baseUri();
      String path = testEnvUri.getPath();
      if (path.isEmpty()) {
        path = "/";
      }

      RealmContext realmContext =
          config
              .getRealmContextResolver()
              .resolveRealmContext(
                  String.format("%s://%s", testEnvUri.getScheme(), testEnvUri.getHost()),
                  "GET",
                  path,
                  Map.of(),
                  Map.of(REALM_PROPERTY_KEY, realm));
      CallContext ctx =
          config
              .getCallContextResolver()
              .resolveCallContext(realmContext, "GET", path, Map.of(), Map.of());
      CallContext.setCurrentContext(ctx);
      PolarisMetaStoreManager metaStoreManager =
          metaStoreManagerFactory.getOrCreateMetaStoreManager(ctx.getRealmContext());
      PolarisMetaStoreManager.EntityResult principal =
          metaStoreManager.readEntityByName(
              ctx.getPolarisCallContext(),
              null,
              PolarisEntityType.PRINCIPAL,
              PolarisEntitySubType.NULL_SUBTYPE,
              PolarisEntityConstants.getRootPrincipalName());

      Map<String, String> propertiesMap = readInternalProperties(principal);
      adminSecrets =
          metaStoreManager
              .loadPrincipalSecrets(ctx.getPolarisCallContext(), propertiesMap.get("client_id"))
              .getPrincipalSecrets();
    } finally {
      CallContext.unsetCurrentContext();
    }
  }

  @Override
  public void afterAll(ExtensionContext context) {
    if (!(metaStoreManagerFactory instanceof InMemoryPolarisMetaStoreManagerFactory)) {
      metaStoreManagerFactory.purgeRealms(List.of(realm));
    }
  }

  public static void createTestDir(String realm) throws IOException {
    // Set up the database location
    Path testDir = Path.of("build/test_data/polaris/" + realm);
    if (Files.exists(testDir)) {
      if (Files.isDirectory(testDir)) {
        Files.walk(testDir)
            .sorted(Comparator.reverseOrder())
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });

      } else {
        Files.delete(testDir);
      }
    }
    Files.createDirectories(testDir);
  }

  static PolarisPrincipalSecrets getAdminSecrets() {
    return adminSecrets;
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext
            .getParameter()
            .getType()
            .equals(PolarisConnectionExtension.PolarisToken.class)
        || parameterContext.getParameter().getType().equals(MetaStoreManagerFactory.class)
        || parameterContext.getParameter().getType().equals(PolarisPrincipalSecrets.class)
        || (parameterContext.getParameter().getType().equals(String.class)
            && parameterContext.getParameter().isAnnotationPresent(PolarisRealm.class));
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    if (parameterContext.getParameter().getType().equals(PolarisToken.class)) {
      try {
        TestEnvironment testEnv = TestEnvironmentExtension.getEnv(extensionContext);
        String token =
            TokenUtils.getTokenFromSecrets(
                testEnv.apiClient(),
                testEnv.baseUri().toString(),
                adminSecrets.getPrincipalClientId(),
                adminSecrets.getMainSecret(),
                realm);
        return new PolarisToken(token);
      } catch (IllegalAccessException e) {
        throw new ParameterResolutionException(e.getMessage());
      }
    } else if (parameterContext.getParameter().getType().equals(String.class)
        && parameterContext.getParameter().isAnnotationPresent(PolarisRealm.class)) {
      return realm;
    } else if (parameterContext.getParameter().getType().equals(PolarisPrincipalSecrets.class)) {
      return adminSecrets;
    } else {
      return metaStoreManagerFactory;
    }
  }

  private static Map<String, String> readInternalProperties(
      PolarisMetaStoreManager.EntityResult principal) {
    try {
      return OBJECT_MAPPER.readValue(
          principal.getEntity().getInternalProperties(),
          new TypeReference<Map<String, String>>() {});
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
