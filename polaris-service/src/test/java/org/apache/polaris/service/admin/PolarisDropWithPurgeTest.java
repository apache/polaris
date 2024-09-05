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
package org.apache.polaris.service.admin;

import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith({DropwizardExtensionsSupport.class, PolarisConnectionExtension.class})
public class PolarisDropWithPurgeTest {
    private static final DropwizardAppExtension<PolarisApplicationConfig> BASE_EXT =
        new DropwizardAppExtension<>(
            PolarisApplication.class,
            ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
            // Bind to random port to support parallelism
            ConfigOverride.config("server.applicationConnectors[0].port", "0"),
            ConfigOverride.config("server.adminConnectors[0].port", "0"),
            // Drop with purge disabled at the extension level
            ConfigOverride.config("featureConfiguration.DROP_WITH_PURGE_ENABLED", "true"));

    private static PolarisConnectionExtension.PolarisToken adminToken;
    private static String userToken;
    private static String realm;
    private static String catalog;
    private static String namespace;
    private static final String baseLocation = "file:///tmp/PolarisDropWithPurgeTest";

    @BeforeEach
    public void setup(PolarisConnectionExtension.PolarisToken adminToken) {
        userToken = adminToken.token();
        realm = PolarisConnectionExtension.getTestRealm(PolarisServiceImplIntegrationTest.class);
        catalog = String.format("catalog_%s", UUID.randomUUID().toString());
        CatalogProperties.Builder propertiesBuilder =
            CatalogProperties.builder()
                .setDefaultBaseLocation(String.format("%s/%s", baseLocation, catalog));
        StorageConfigInfo config =
            FileStorageConfigInfo.builder()
                .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
                .build();
        Catalog catalogObject =
            new Catalog(
                Catalog.TypeEnum.INTERNAL,
                catalog,
                propertiesBuilder.build(),
                1725487592064L,
                1725487592064L,
                1,
                config);
        try (Response response =
                 request(BASE_EXT, "management/v1/catalogs")
                     .post(Entity.json(new CreateCatalogRequest(catalogObject)))) {
            if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
                throw new IllegalStateException(
                    "Failed to create catalog: " + response.readEntity(String.class));
            }
        }

        namespace = "ns";
        CreateNamespaceRequest createNamespaceRequest =
            CreateNamespaceRequest.builder()
                .withNamespace(Namespace.of(namespace))
                .build();
        try (Response response =
                 request(BASE_EXT, String.format("catalog/v1/%s/namespaces", catalog))
                     .post(Entity.json(createNamespaceRequest))) {
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(
                    "Failed to create namespace: " + response.readEntity(String.class));
            }
        }
    }

    private String createTable() {
        String name = "table_" + UUID.randomUUID().toString();
        CreateTableRequest createTableRequest =
            CreateTableRequest.builder()
                .withName(name)
                .withSchema(SCHEMA)
                .build();
        String prefix = String.format("catalog/v1/%s/namespaces/%s/tables", catalog, namespace);
        try (Response response = request(BASE_EXT, prefix).post(Entity.json(createTableRequest))) {
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException("Failed to create table: " + name);
            }
            return name;
        }
    }

    private Response dropTable(String name, boolean purge) {
        String prefix = String.format("catalog/v1/%s/namespaces/%s/tables/%s", catalog, namespace, name);

        try (Response response = BASE_EXT.client()
            .target(String.format("http://localhost:%d/api/%s", BASE_EXT.getLocalPort(), prefix))
            .queryParam("purgeRequested", purge) // Add purge flag
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .delete()) { // Send DELETE request

            // Check for success status (204 No Content)
            if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                throw new IllegalStateException("Failed to drop table: " + name);
            }
            return response; // Return response for further handling
        }
    }

    private static Invocation.Builder request(
        DropwizardAppExtension<PolarisApplicationConfig> extension,
        String prefix) {
        return extension
            .client()
            .target(String.format("http://localhost:%d/api/%s", extension.getLocalPort(), prefix))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm);
    }

    @Test
    void testDropTable() {

        // Drop a table without purge
        Assertions
            .assertThat(dropTable(createTable(), false))
            .returns(Response.Status.OK.getStatusCode(), Response::getStatus);


//        assertThat(
//            createCatalog(
//                prefix, "kingdoms", initiallyExternal, Arrays.asList("plants", "animals")))
//            .returns(Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);

    }
}