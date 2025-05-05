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

import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.GrantResources;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PolicyGrant;
import org.apache.polaris.core.admin.model.PolicyPrivilege;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.core.policy.exceptions.PolicyInUseException;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IcebergHelper;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.env.PolicyApi;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.types.ApplicablePolicy;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.Policy;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(PolarisIntegrationTestExtension.class)
public class PolarisPolicyServiceIntegrationTest {

  private static final String TEST_ROLE_ARN =
      Optional.ofNullable(System.getenv("INTEGRATION_TEST_ROLE_ARN"))
          .orElse("arn:aws:iam::123456789012:role/my-role");

  private static final String CATALOG_ROLE_2 = "catalogrole2";
  private static final String EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT = "{\"enable\":true}";
  private static final Namespace NS1 = Namespace.of("NS1");
  private static final Namespace NS2 = Namespace.of("NS2");
  private static final PolicyIdentifier NS1_P1 = new PolicyIdentifier(NS1, "P1");
  private static final PolicyIdentifier NS1_P2 = new PolicyIdentifier(NS1, "P2");
  private static final PolicyIdentifier NS1_P3 = new PolicyIdentifier(NS1, "P3");
  private static final TableIdentifier NS2_T1 = TableIdentifier.of(NS2, "T1");

  private static URI s3BucketBase;
  private static String principalRoleName;
  private static ClientCredentials adminCredentials;
  private static PrincipalWithCredentials principalCredentials;
  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;
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
    URI testRootUri = IntegrationTestsHelper.getTemporaryDirectory(tempDir);
    s3BucketBase = testRootUri.resolve("my-bucket");

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
    restCatalog.createNamespace(NS1);
    Policy policy =
        policyApi.createPolicy(
            currentCatalogName,
            NS1_P1,
            PredefinedPolicyTypes.DATA_COMPACTION,
            EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT,
            "test policy");

    Assertions.assertThat(policy).isNotNull();
    Assertions.assertThat(policy.getName()).isEqualTo("P1");
    Assertions.assertThat(policy.getDescription()).isEqualTo("test policy");
    Assertions.assertThat(policy.getPolicyType())
        .isEqualTo(PredefinedPolicyTypes.DATA_COMPACTION.getName());
    Assertions.assertThat(policy.getContent()).isEqualTo(EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT);
    Assertions.assertThat(policy.getInheritable())
        .isEqualTo(PredefinedPolicyTypes.DATA_COMPACTION.isInheritable());
    Assertions.assertThat(policy.getVersion()).isEqualTo(0);

    Policy loadedPolicy = policyApi.loadPolicy(currentCatalogName, NS1_P1);
    Assertions.assertThat(loadedPolicy).isEqualTo(policy);

    policyApi.dropPolicy(currentCatalogName, NS1_P1);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        " invalid",
        "invalid ",
        " invalid ",
        "",
        "policy name",
        "policy@name",
        "policy#name",
        "policy$name",
        "policy!name",
        "policy name with space",
        "policy.name",
        "policy,name",
        "policy~name",
        "policy`name",
        "policy;name",
        "policy:name",
        "policy<>name",
        "policy[]name",
        "policy{}name",
        "policy|name",
        "policy\\name",
        "policy/name",
        "policy*name",
        "policy^name",
        "policy%name",
      })
  public void testCreatePolicyWithInvalidName(String policyName) {
    restCatalog.createNamespace(NS1);
    PolicyIdentifier policyIdentifier = new PolicyIdentifier(NS1, policyName);

    String ns = RESTUtil.encodeNamespace(policyIdentifier.getNamespace());
    CreatePolicyRequest request =
        CreatePolicyRequest.builder()
            .setType(PredefinedPolicyTypes.DATA_COMPACTION.getName())
            .setName(policyIdentifier.getName())
            .setDescription("test policy")
            .setContent(EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT)
            .build();
    try (Response res =
        policyApi
            .request(
                "polaris/v1/{cat}/namespaces/{ns}/policies",
                Map.of("cat", currentCatalogName, "ns", ns))
            .post(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class))
          .contains(
              "{\"error\":{\"message\":\"Invalid value: createPolicy.arg2.name: must match \\\"^[A-Za-z0-9\\\\-_]+$\\\"\",\"type\":\"ResteasyReactiveViolationException\",\"code\":400}}");
    }
  }

  @Test
  public void testDropPolicy() {
    restCatalog.createNamespace(NS1);
    policyApi.createPolicy(
        currentCatalogName,
        NS1_P1,
        PredefinedPolicyTypes.DATA_COMPACTION,
        EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT,
        "test policy");

    PolicyAttachmentTarget catalogTarget =
        PolicyAttachmentTarget.builder().setType(PolicyAttachmentTarget.TypeEnum.CATALOG).build();
    policyApi.attachPolicy(currentCatalogName, NS1_P1, catalogTarget, Map.of());

    // dropPolicy should fail because the policy is attached to the catalog
    Assertions.assertThatThrownBy(() -> policyApi.dropPolicy(currentCatalogName, NS1_P1))
        .isInstanceOf(PolicyInUseException.class);

    // with detach-all=true, the policy and the attachment should be dropped
    policyApi.dropPolicy(currentCatalogName, NS1_P1, true);
    Assertions.assertThat(policyApi.listPolicies(currentCatalogName, NS1)).hasSize(0);
    // The policy mapping record should be dropped
    Assertions.assertThat(policyApi.getApplicablePolicies(currentCatalogName, null, null, null))
        .hasSize(0);
  }

  @Test
  public void testUpdatePolicy() {
    restCatalog.createNamespace(NS1);
    policyApi.createPolicy(
        currentCatalogName,
        NS1_P1,
        PredefinedPolicyTypes.DATA_COMPACTION,
        EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT,
        "test policy");

    String updatedContent = "{\"enable\":false}";
    String updatedDescription = "updated test policy";
    Policy updatedPolicy =
        policyApi.updatePolicy(currentCatalogName, NS1_P1, updatedContent, updatedDescription, 0);

    Assertions.assertThat(updatedPolicy).isNotNull();
    Assertions.assertThat(updatedPolicy.getName()).isEqualTo("P1");
    Assertions.assertThat(updatedPolicy.getDescription()).isEqualTo(updatedDescription);
    Assertions.assertThat(updatedPolicy.getPolicyType())
        .isEqualTo(PredefinedPolicyTypes.DATA_COMPACTION.getName());
    Assertions.assertThat(updatedPolicy.getContent()).isEqualTo(updatedContent);
    Assertions.assertThat(updatedPolicy.getInheritable())
        .isEqualTo(PredefinedPolicyTypes.DATA_COMPACTION.isInheritable());
    Assertions.assertThat(updatedPolicy.getVersion()).isEqualTo(1);

    policyApi.dropPolicy(currentCatalogName, NS1_P1);
  }

  @Test
  public void testListPolicies() {
    restCatalog.createNamespace(NS1);
    policyApi.createPolicy(
        currentCatalogName,
        NS1_P1,
        PredefinedPolicyTypes.DATA_COMPACTION,
        EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT,
        "test policy");
    policyApi.createPolicy(
        currentCatalogName,
        NS1_P2,
        PredefinedPolicyTypes.METADATA_COMPACTION,
        EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT,
        "test policy");

    Assertions.assertThat(policyApi.listPolicies(currentCatalogName, NS1))
        .containsExactlyInAnyOrder(NS1_P1, NS1_P2);
    Assertions.assertThat(
            policyApi.listPolicies(currentCatalogName, NS1, PredefinedPolicyTypes.DATA_COMPACTION))
        .containsExactly(NS1_P1);
    Assertions.assertThat(
            policyApi.listPolicies(
                currentCatalogName, NS1, PredefinedPolicyTypes.METADATA_COMPACTION))
        .containsExactly(NS1_P2);

    policyApi.dropPolicy(currentCatalogName, NS1_P1);
    policyApi.dropPolicy(currentCatalogName, NS1_P2);
  }

  @Test
  public void testPolicyMapping() {
    restCatalog.createNamespace(NS1);
    restCatalog.createNamespace(NS2);
    Policy p1 =
        policyApi.createPolicy(
            currentCatalogName,
            NS1_P1,
            PredefinedPolicyTypes.DATA_COMPACTION,
            EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT,
            "test policy");
    Policy p2 =
        policyApi.createPolicy(
            currentCatalogName,
            NS1_P2,
            PredefinedPolicyTypes.METADATA_COMPACTION,
            EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT,
            "test policy");
    Policy p3 =
        policyApi.createPolicy(
            currentCatalogName,
            NS1_P3,
            PredefinedPolicyTypes.ORPHAN_FILE_REMOVAL,
            EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT,
            "test policy");

    restCatalog
        .buildTable(
            NS2_T1, new Schema(Types.NestedField.optional(1, "string", Types.StringType.get())))
        .create();

    PolicyAttachmentTarget catalogTarget =
        PolicyAttachmentTarget.builder().setType(PolicyAttachmentTarget.TypeEnum.CATALOG).build();
    PolicyAttachmentTarget namespaceTarget =
        PolicyAttachmentTarget.builder()
            .setType(PolicyAttachmentTarget.TypeEnum.NAMESPACE)
            .setPath(Arrays.asList(NS2.levels()))
            .build();
    PolicyAttachmentTarget tableTarget =
        PolicyAttachmentTarget.builder()
            .setType(PolicyAttachmentTarget.TypeEnum.TABLE_LIKE)
            .setPath(PolarisCatalogHelpers.tableIdentifierToList(NS2_T1))
            .build();

    policyApi.attachPolicy(currentCatalogName, NS1_P1, catalogTarget, Map.of());
    policyApi.attachPolicy(currentCatalogName, NS1_P2, namespaceTarget, Map.of());
    policyApi.attachPolicy(currentCatalogName, NS1_P3, tableTarget, Map.of());

    List<ApplicablePolicy> applicablePoliciesOnCatalog =
        policyApi.getApplicablePolicies(currentCatalogName, null, null, null);
    Assertions.assertThat(applicablePoliciesOnCatalog)
        .containsExactly(policyToApplicablePolicy(p1, false, NS1));

    List<ApplicablePolicy> applicablePoliciesOnNamespace =
        policyApi.getApplicablePolicies(currentCatalogName, NS2, null, null);
    Assertions.assertThat(applicablePoliciesOnNamespace)
        .containsExactlyInAnyOrder(
            policyToApplicablePolicy(p1, true, NS1), policyToApplicablePolicy(p2, false, NS1));

    List<ApplicablePolicy> applicablePoliciesOnTable =
        policyApi.getApplicablePolicies(currentCatalogName, NS2, NS2_T1.name(), null);
    Assertions.assertThat(applicablePoliciesOnTable)
        .containsExactlyInAnyOrder(
            policyToApplicablePolicy(p1, true, NS1),
            policyToApplicablePolicy(p2, true, NS1),
            policyToApplicablePolicy(p3, false, NS1));

    Assertions.assertThat(
            policyApi.getApplicablePolicies(
                currentCatalogName, NS2, NS2_T1.name(), PredefinedPolicyTypes.METADATA_COMPACTION))
        .containsExactlyInAnyOrder(policyToApplicablePolicy(p2, true, NS1));

    policyApi.detachPolicy(currentCatalogName, NS1_P1, catalogTarget);
    policyApi.detachPolicy(currentCatalogName, NS1_P2, namespaceTarget);
    policyApi.detachPolicy(currentCatalogName, NS1_P3, tableTarget);

    policyApi.dropPolicy(currentCatalogName, NS1_P1);
    policyApi.dropPolicy(currentCatalogName, NS1_P2);
    policyApi.dropPolicy(currentCatalogName, NS1_P3);

    restCatalog.dropTable(NS2_T1);
  }

  @Test
  public void testGrantsOnPolicy() {
    restCatalog.createNamespace(NS1);
    try {
      policyApi.createPolicy(
          currentCatalogName,
          NS1_P1,
          PredefinedPolicyTypes.DATA_COMPACTION,
          EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT,
          "test policy");
      managementApi.createCatalogRole(currentCatalogName, CATALOG_ROLE_2);
      Stream<PolicyGrant> policyGrants =
          Arrays.stream(PolicyPrivilege.values())
              .map(
                  p ->
                      new PolicyGrant(
                          Arrays.asList(NS1.levels()),
                          NS1_P1.getName(),
                          p,
                          GrantResource.TypeEnum.POLICY));
      policyGrants.forEach(g -> managementApi.addGrant(currentCatalogName, CATALOG_ROLE_2, g));

      Assertions.assertThat(managementApi.listGrants(currentCatalogName, CATALOG_ROLE_2))
          .extracting(GrantResources::getGrants)
          .asInstanceOf(InstanceOfAssertFactories.list(GrantResource.class))
          .map(gr -> ((PolicyGrant) gr).getPrivilege())
          .containsExactlyInAnyOrder(PolicyPrivilege.values());

      PolicyGrant policyCreateGrant =
          new PolicyGrant(
              Arrays.asList(NS1.levels()),
              NS1_P1.getName(),
              PolicyPrivilege.POLICY_CREATE,
              GrantResource.TypeEnum.POLICY);
      managementApi.revokeGrant(currentCatalogName, CATALOG_ROLE_2, policyCreateGrant);

      Assertions.assertThat(managementApi.listGrants(currentCatalogName, CATALOG_ROLE_2))
          .extracting(GrantResources::getGrants)
          .asInstanceOf(InstanceOfAssertFactories.list(GrantResource.class))
          .map(gr -> ((PolicyGrant) gr).getPrivilege())
          .doesNotContain(PolicyPrivilege.POLICY_CREATE);
    } finally {
      policyApi.purge(currentCatalogName, NS1);
    }
  }

  @Test
  public void testGrantsOnNonExistingPolicy() {
    restCatalog.createNamespace(NS1);

    try {
      managementApi.createCatalogRole(currentCatalogName, CATALOG_ROLE_2);
      Stream<PolicyGrant> policyGrants =
          Arrays.stream(PolicyPrivilege.values())
              .map(
                  p ->
                      new PolicyGrant(
                          Arrays.asList(NS1.levels()),
                          NS1_P1.getName(),
                          p,
                          GrantResource.TypeEnum.POLICY));
      policyGrants.forEach(
          g -> {
            try (Response response =
                managementApi
                    .request(
                        "v1/catalogs/{cat}/catalog-roles/{role}/grants",
                        Map.of("cat", currentCatalogName, "role", "catalogrole2"))
                    .put(Entity.json(g))) {

              assertThat(response.getStatus()).isEqualTo(NOT_FOUND.getStatusCode());
            }
          });
    } finally {
      policyApi.purge(currentCatalogName, NS1);
    }
  }

  private static ApplicablePolicy policyToApplicablePolicy(
      Policy policy, boolean inherited, Namespace parent) {
    return new ApplicablePolicy(
        policy.getPolicyType(),
        policy.getInheritable(),
        policy.getName(),
        policy.getDescription(),
        policy.getContent(),
        policy.getVersion(),
        inherited,
        Arrays.asList(parent.levels()));
  }
}
