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
package org.apache.polaris.service.auth;

import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.PolarisRealm;
import org.apache.polaris.service.test.TestEnvironment;
import org.apache.polaris.service.test.TestEnvironmentExtension;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({
  DropwizardExtensionsSupport.class,
  PolarisConnectionExtension.class,
  TestEnvironmentExtension.class
})
public class AnonymousAuthIntegrationTest {
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-anonymous.yml"),
          ConfigOverride.config(
              "authenticator.class",
              "org.apache.polaris.service.auth.AnonymousPolarisAuthenticator"),
          ConfigOverride.config("authorizer.factory", AllowAllAuthorizerFactory.class.getName()),
          // Bind to random port to support parallelism
          ConfigOverride.config("server.applicationConnectors[0].port", "0"),
          // Bind to random port to support parallelism
          ConfigOverride.config("server.adminConnectors[0].port", "0"));

  private RESTCatalog restCatalog;

  @BeforeEach
  public void before(TestEnvironment testEnv, @PolarisRealm String realm) {
    String catalogName = "test-no-auth-" + UUID.randomUUID();

    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setProperties(new CatalogProperties("file:///tmp"))
            .setStorageConfigInfo(
                new FileStorageConfigInfo(
                    StorageConfigInfo.StorageTypeEnum.FILE, List.of("file://")))
            .build();

    URI baseUrl = testEnv.baseUri();
    Client client = EXT.client();

    try (Response response =
        client
            .target(String.format("%s/api/management/v1/catalogs", baseUrl))
            .request("application/json")
            .header(REALM_PROPERTY_KEY, realm)
            // Note: the Authorization must still be present, but its value is irrelevant
            .header("Authorization", "Bearer test-token")
            .post(Entity.json(catalog))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    restCatalog = new RESTCatalog();
    // Note: the fake token is not validated by AnonymousPolarisAuthenticator
    restCatalog.initialize(
        "test",
        Map.of(
            "uri",
            String.format("%s/api/catalog", baseUrl),
            "token",
            "test-token",
            "warehouse",
            catalogName,
            "header." + REALM_PROPERTY_KEY,
            realm));
  }

  @Test
  public void testBasicRequest() throws IOException {
    Assertions.assertThatCode(() -> restCatalog.listNamespaces(Namespace.empty()))
        .doesNotThrowAnyException();
  }
}
