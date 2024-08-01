/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.polaris.service.admin;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.polaris.core.admin.model.AwsStorageConfigInfo;
import io.polaris.core.admin.model.Catalog;
import io.polaris.core.admin.model.CatalogProperties;
import io.polaris.core.admin.model.CreateCatalogRequest;
import io.polaris.core.admin.model.StorageConfigInfo;
import io.polaris.service.PolarisApplication;
import io.polaris.service.config.PolarisApplicationConfig;
import io.polaris.service.test.PolarisConnectionExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static io.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static io.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith({DropwizardExtensionsSupport.class, PolarisConnectionExtension.class})
public class PolarisOverlappingTableTest {
    private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
        new DropwizardAppExtension<>(
            PolarisApplication.class,
            ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
            // Bind to random port to support parallelism
            ConfigOverride.config("server.applicationConnectors[0].port", "0"),
            ConfigOverride.config("server.adminConnectors[0].port", "0"),
            // Block overlapping table paths globally:
            ConfigOverride.config("featureConfiguration.ENFORCE_GLOBALLY_UNIQUE_TABLE_LOCATIONS", "true"),
            // The value of this parameter is irrelevant because of ENFORCE_GLOBALLY_UNIQUE_TABLE_LOCATIONS
            ConfigOverride.config("featureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP", "true"));
    private static String userToken;
    private static String realm;
    private static String catalog;
    private static Namespace namespace;

    @BeforeAll
    public static void setup(PolarisConnectionExtension.PolarisToken adminToken) {
        userToken = adminToken.token();
        realm = PolarisConnectionExtension.getTestRealm(PolarisServiceImplIntegrationTest.class);
        catalog = String.format("catalog_%s", UUID.randomUUID().toString());
        StorageConfigInfo config =
            AwsStorageConfigInfo.builder()
                .setRoleArn("arn:aws:iam::123456789012:role/my-role")
                .setExternalId("externalId")
                .setUserArn("userArn")
                .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
                .build();
        Catalog catalogObject =
            new Catalog(
                Catalog.TypeEnum.INTERNAL,
                catalog,
                new CatalogProperties(String.format("s3://bucket//%s", catalog)),
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                1,
                config);
        try (Response response = request().post(Entity.json(new CreateCatalogRequest(catalogObject)))) {
            if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
                throw new IllegalStateException("Failed to create catalog");
            }
        }

        namespace = Namespace.of("ns");
        CreateNamespaceRequest createNamespaceRequest =
            CreateNamespaceRequest.builder().withNamespace(namespace).build();
        try (Response response = request().post(Entity.json(createNamespaceRequest))) {
            if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
                throw new IllegalStateException("Failed to create namespace");
            }
        }
    }

    private Response createTable(String location) {
        CreateTableRequest createTableRequest =
            CreateTableRequest
            .builder()
            .withName("table_" + UUID.randomUUID().toString())
            .withLocation(location)
            .withSchema(SCHEMA)
            .build();
        try (Response response = request().post(Entity.json(createTableRequest))) {
            return response;
        }
    }

    private static Invocation.Builder request() {
        return EXT.client()
            .target(String.format("http://localhost:%d/api/management/v1/catalogs", EXT.getLocalPort()))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm);
    }

    @Test
    public void testBasicOverlappingTables() {
        assertThat(createTable("s3://not-in-catalog"))
            .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
}
