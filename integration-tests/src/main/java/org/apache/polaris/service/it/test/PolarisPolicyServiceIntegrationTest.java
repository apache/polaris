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
package org.apache.polaris.service.it.test;

import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;

import com.google.common.collect.ImmutableMap;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.service.it.env.CatalogApi;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IcebergHelper;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.env.PolicyApi;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.types.Policy;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(PolarisIntegrationTestExtension.class)
public class PolarisPolicyServiceIntegrationTest {

  private static final String TEST_ROLE_ARN =
      Optional.ofNullable(System.getenv("INTEGRATION_TEST_ROLE_ARN"))
          .orElse("arn:aws:iam::123456789012:role/my-role");

  private static URI s3BucketBase;
  private static URI externalCatalogBase;

  protected static final String VIEW_QUERY = "select * from ns1.layer1_table";
  private static String principalRoleName;
  private static ClientCredentials adminCredentials;
  private static PrincipalWithCredentials principalCredentials;
  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;
  private static CatalogApi catalogApi;
  private static PolicyApi policyApi;

  private RESTCatalog restCatalog;
  private String currentCatalogName;

  private final String catalogBaseLocation =
      s3BucketBase + "/" + System.getenv("USER") + "/path/to/data";

  private static final String[] DEFAULT_CATALOG_PROPERTIES = {
    "allow.unstructured.table.location", "true",
    "allow.external.table.location", "true"
  };

  @Retention(RetentionPolicy.RUNTIME)
  private @interface CatalogConfig {
    Catalog.TypeEnum value() default Catalog.TypeEnum.INTERNAL;

    String[] properties() default {
      "allow.unstructured.table.location", "true",
      "allow.external.table.location", "true"
    };
  }

  @Retention(RetentionPolicy.RUNTIME)
  private @interface RestCatalogConfig {
    String[] value() default {};
  }

  @BeforeAll
  public static void setup(
      PolarisApiEndpoints apiEndpoints, ClientCredentials credentials, @TempDir Path tempDir) {
    adminCredentials = credentials;
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    managementApi = client.managementApi(credentials);
    String principalName = client.newEntityName("snowman-rest");
    principalRoleName = client.newEntityName("rest-admin");
    principalCredentials = managementApi.createPrincipalWithRole(principalName, principalRoleName);
    catalogApi = client.catalogApi(principalCredentials);
    URI testRootUri = IntegrationTestsHelper.getTemporaryDirectory(tempDir);
    s3BucketBase = testRootUri.resolve("my-bucket");
    externalCatalogBase = testRootUri.resolve("external-catalog");

    policyApi = client.policyApi(principalCredentials);
  }

  @AfterAll
  public static void close() throws Exception {
    client.close();
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    String principalName = "snowman-rest-" + UUID.randomUUID();
    principalRoleName = "rest-admin-" + UUID.randomUUID();
    PrincipalWithCredentials principalCredentials =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);

    catalogApi = client.catalogApi(principalCredentials);

    Method method = testInfo.getTestMethod().orElseThrow();
    currentCatalogName = client.newEntityName(method.getName());
    AwsStorageConfigInfo awsConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn(TEST_ROLE_ARN)
            .setExternalId("externalId")
            .setUserArn("a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
            .build();
    Optional<PolarisPolicyServiceIntegrationTest.CatalogConfig> catalogConfig =
        Optional.ofNullable(
            method.getAnnotation(PolarisPolicyServiceIntegrationTest.CatalogConfig.class));

    CatalogProperties.Builder catalogPropsBuilder = CatalogProperties.builder(catalogBaseLocation);
    String[] properties =
        catalogConfig
            .map(PolarisPolicyServiceIntegrationTest.CatalogConfig::properties)
            .orElse(DEFAULT_CATALOG_PROPERTIES);
    for (int i = 0; i < properties.length; i += 2) {
      catalogPropsBuilder.addProperty(properties[i], properties[i + 1]);
    }
    if (!s3BucketBase.getScheme().equals("file")) {
      catalogPropsBuilder.addProperty(
          CatalogEntity.REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY, "file:");
    }
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(
                catalogConfig
                    .map(PolarisPolicyServiceIntegrationTest.CatalogConfig::value)
                    .orElse(Catalog.TypeEnum.INTERNAL))
            .setName(currentCatalogName)
            .setProperties(catalogPropsBuilder.build())
            .setStorageConfigInfo(
                s3BucketBase.getScheme().equals("file")
                    ? new FileStorageConfigInfo(
                        StorageConfigInfo.StorageTypeEnum.FILE, List.of("file://"))
                    : awsConfigModel)
            .build();

    managementApi.createCatalog(principalRoleName, catalog);

    Optional<PolarisPolicyServiceIntegrationTest.RestCatalogConfig> restCatalogConfig =
        testInfo
            .getTestMethod()
            .flatMap(
                m ->
                    Optional.ofNullable(
                        m.getAnnotation(
                            PolarisPolicyServiceIntegrationTest.RestCatalogConfig.class)));
    ImmutableMap.Builder<String, String> extraPropertiesBuilder = ImmutableMap.builder();
    restCatalogConfig.ifPresent(
        config -> {
          for (int i = 0; i < config.value().length; i += 2) {
            extraPropertiesBuilder.put(config.value()[i], config.value()[i + 1]);
          }
        });

    restCatalog =
        IcebergHelper.restCatalog(
            client,
            endpoints,
            principalCredentials,
            currentCatalogName,
            extraPropertiesBuilder.build());
    CatalogGrant catalogGrant =
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG);
    managementApi.createCatalogRole(currentCatalogName, "catalogrole1");
    managementApi.addGrant(currentCatalogName, "catalogrole1", catalogGrant);
    CatalogRole catalogRole = managementApi.getCatalogRole(currentCatalogName, "catalogrole1");
    managementApi.grantCatalogRoleToPrincipalRole(
        principalRoleName, currentCatalogName, catalogRole);

    policyApi = client.policyApi(principalCredentials);
  }

  @AfterEach
  public void cleanUp() {
    client.cleanUp(adminCredentials);
  }

  @Test
  public void testCreatePolicy() {
    Namespace NS1 = Namespace.of("ns1");
    restCatalog.createNamespace(NS1);
    PolicyIdentifier identifier = new PolicyIdentifier(NS1, "testPolicy");
    String example_content =
        "{\"version\":\"2025-02-03\",\"enable\":true,\"config\":{\"target_file_size_bytes\":134217728,\"compaction_strategy\":\"bin-pack\",\"max-concurrent-file-group-rewrites\":5,\"key1\":\"value1\"}}";
    Policy policy =
        policyApi.createPolicy(
            currentCatalogName,
            identifier,
            PredefinedPolicyTypes.DATA_COMPACTION,
            example_content,
            "test policy");

    Assertions.assertThat(policy).isNotNull();
    Assertions.assertThat(policy.getName()).isEqualTo("testPolicy");
    Assertions.assertThat(policy.getDescription()).isEqualTo("test policy");
    Assertions.assertThat(policy.getPolicyType())
        .isEqualTo(PredefinedPolicyTypes.DATA_COMPACTION.getName());
    Assertions.assertThat(policy.getContent()).isEqualTo(example_content);
    Assertions.assertThat(policy.getInheritable())
        .isEqualTo(PredefinedPolicyTypes.DATA_COMPACTION.isInheritable());
    Assertions.assertThat(policy.getVersion()).isEqualTo(0);

    Policy loadedPolicy = policyApi.loadPolicy(currentCatalogName, identifier);
    Assertions.assertThat(loadedPolicy).isEqualTo(policy);

    policyApi.dropPolicy(currentCatalogName, identifier);
  }

  @Test
  public void testUpdatePolicy() {
    Namespace NS1 = Namespace.of("ns1");
    restCatalog.createNamespace(NS1);
    String example_content =
        "{\"version\":\"2025-02-03\",\"enable\":true,\"config\":{\"target_file_size_bytes\":134217728,\"compaction_strategy\":\"bin-pack\",\"max-concurrent-file-group-rewrites\":5,\"key1\":\"value1\"}}";
    PolicyIdentifier identifier = new PolicyIdentifier(NS1, "testPolicy");
    policyApi.createPolicy(
        currentCatalogName,
        identifier,
        PredefinedPolicyTypes.DATA_COMPACTION,
        example_content,
        "test policy");

    String updatedContent = "{\"enable\":false}";
    String updatedDescription = "updated test policy";
    Policy updatedPolicy =
        policyApi.updatePolicy(
            currentCatalogName, identifier, updatedContent, updatedDescription, 0);

    Assertions.assertThat(updatedPolicy).isNotNull();
    Assertions.assertThat(updatedPolicy.getName()).isEqualTo("testPolicy");
    Assertions.assertThat(updatedPolicy.getDescription()).isEqualTo(updatedDescription);
    Assertions.assertThat(updatedPolicy.getPolicyType())
        .isEqualTo(PredefinedPolicyTypes.DATA_COMPACTION.getName());
    Assertions.assertThat(updatedPolicy.getContent()).isEqualTo(updatedContent);
    Assertions.assertThat(updatedPolicy.getInheritable())
        .isEqualTo(PredefinedPolicyTypes.DATA_COMPACTION.isInheritable());
    Assertions.assertThat(updatedPolicy.getVersion()).isEqualTo(1);

    policyApi.dropPolicy(currentCatalogName, identifier);
  }
}
