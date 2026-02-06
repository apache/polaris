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

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
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
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.NamespacePrivilege;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.PolicyGrant;
import org.apache.polaris.core.admin.model.PolicyPrivilege;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.TableGrant;
import org.apache.polaris.core.admin.model.TablePrivilege;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.core.policy.exceptions.PolicyInUseException;
import org.apache.polaris.service.it.env.CatalogConfig;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.IcebergHelper;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.ManagementApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.env.PolicyApi;
import org.apache.polaris.service.it.env.RestCatalogConfig;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.types.ApplicablePolicy;
import org.apache.polaris.service.types.AttachPolicyRequest;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.DetachPolicyRequest;
import org.apache.polaris.service.types.Policy;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.apache.polaris.service.types.UpdatePolicyRequest;
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

  private static final String CATALOG_ROLE_1 = "catalogrole1";
  private static final String CATALOG_ROLE_2 = "catalogrole2";
  private static final String EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT = "{\"enable\":true}";
  private static final Namespace NS1 = Namespace.of("NS1");
  private static final Namespace NS2 = Namespace.of("NS2");
  private static final PolicyIdentifier NS1_P1 = new PolicyIdentifier(NS1, "P1");
  private static final PolicyIdentifier NS1_P2 = new PolicyIdentifier(NS1, "P2");
  private static final PolicyIdentifier NS1_P3 = new PolicyIdentifier(NS1, "P3");
  private static final TableIdentifier NS2_T1 = TableIdentifier.of(NS2, "T1");

  private static final String NS1_NAME = RESTUtil.encodeNamespace(NS1);
  private static final String INVALID_NAMESPACE = "INVALID_NAMESPACE";
  private static final String INVALID_POLICY = "INVALID_POLICY";
  private static final String INVALID_TABLE = "INVALID_TABLE";
  private static final String INVALID_NAMESPACE_MSG =
      "Namespace does not exist: " + INVALID_NAMESPACE;

  private static URI s3BucketBase;
  private static String principalRoleName;
  private static String adminToken;
  private static PolarisApiEndpoints endpoints;
  private static PolarisClient client;
  private static ManagementApi managementApi;
  private static PolicyApi policyApi;

  private RESTCatalog restCatalog;
  private String currentCatalogName;

  private final String catalogBaseLocation =
      s3BucketBase + "/" + System.getenv("USER") + "/path/to/data";

  private static final Map<String, String> DEFAULT_CATALOG_PROPERTIES =
      Map.of(
          "polaris.config.allow.unstructured.table.location", "true",
          "polaris.config.allow.external.table.location", "true");

  @BeforeAll
  public static void setup(
      PolarisApiEndpoints apiEndpoints, ClientCredentials credentials, @TempDir Path tempDir) {
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    adminToken = client.obtainToken(credentials);
    managementApi = client.managementApi(adminToken);
    String principalName = client.newEntityName("snowman-rest");
    principalRoleName = client.newEntityName("rest-admin");
    PrincipalWithCredentials principalCredentials =
        managementApi.createPrincipalWithRole(principalName, principalRoleName);
    URI testRootUri = IntegrationTestsHelper.getTemporaryDirectory(tempDir);
    s3BucketBase = testRootUri.resolve("my-bucket");

    String principalToken = client.obtainToken(principalCredentials);
    policyApi = client.policyApi(principalToken);
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
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of("s3://my-old-bucket/path/to/data"))
            .build();

    CatalogProperties.Builder catalogPropsBuilder = CatalogProperties.builder(catalogBaseLocation);

    catalogPropsBuilder.putAll(DEFAULT_CATALOG_PROPERTIES);

    Map<String, String> catalogProperties =
        IntegrationTestsHelper.mergeFromAnnotatedElements(
            testInfo, CatalogConfig.class, CatalogConfig::properties);
    catalogPropsBuilder.putAll(catalogProperties);

    if (!s3BucketBase.getScheme().equals("file")) {
      catalogPropsBuilder.addProperty(
          CatalogEntity.REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY, "file:");
    }

    Catalog.TypeEnum catalogType =
        IntegrationTestsHelper.extractFromAnnotatedElements(
            testInfo, CatalogConfig.class, CatalogConfig::value, Catalog.TypeEnum.INTERNAL);

    Catalog catalog =
        PolarisCatalog.builder()
            .setType(catalogType)
            .setName(currentCatalogName)
            .setProperties(catalogPropsBuilder.build())
            .setStorageConfigInfo(
                s3BucketBase.getScheme().equals("file")
                    ? new FileStorageConfigInfo(
                        StorageConfigInfo.StorageTypeEnum.FILE, List.of("file://"))
                    : awsConfigModel)
            .build();

    managementApi.createCatalog(principalRoleName, catalog);

    Map<String, String> restCatalogProperties =
        IntegrationTestsHelper.mergeFromAnnotatedElements(
            testInfo, RestCatalogConfig.class, RestCatalogConfig::value);

    String principalToken = client.obtainToken(principalCredentials);
    restCatalog =
        IcebergHelper.restCatalog(
            endpoints, currentCatalogName, restCatalogProperties, principalToken);
    CatalogGrant catalogGrant =
        new CatalogGrant(CatalogPrivilege.CATALOG_MANAGE_CONTENT, GrantResource.TypeEnum.CATALOG);
    managementApi.createCatalogRole(currentCatalogName, CATALOG_ROLE_1);
    managementApi.addGrant(currentCatalogName, CATALOG_ROLE_1, catalogGrant);
    CatalogRole catalogRole = managementApi.getCatalogRole(currentCatalogName, CATALOG_ROLE_1);
    managementApi.grantCatalogRoleToPrincipalRole(
        principalRoleName, currentCatalogName, catalogRole);

    policyApi = client.policyApi(principalToken);
  }

  @AfterEach
  public void cleanUp() throws IOException {
    try {
      if (restCatalog != null) {
        restCatalog.close();
      }
    } finally {
      client.cleanUp(adminToken);
    }
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
  public void testCreatePolicyWithNonExistingNamespace() {
    CreatePolicyRequest request =
        CreatePolicyRequest.builder()
            .setType(PredefinedPolicyTypes.DATA_COMPACTION.getName())
            .setName(currentCatalogName)
            .setDescription("test policy")
            .setContent(EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT)
            .build();
    try (Response res =
        policyApi
            .request(
                "polaris/v1/{cat}/namespaces/{ns}/policies",
                Map.of("cat", currentCatalogName, "ns", INVALID_NAMESPACE))
            .post(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class)).contains(INVALID_NAMESPACE_MSG);
    }
  }

  @Test
  public void testAttachPolicyToNonExistingNamespace() {
    restCatalog.createNamespace(NS1);
    policyApi.createPolicy(
        currentCatalogName,
        NS1_P1,
        PredefinedPolicyTypes.DATA_COMPACTION,
        EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT,
        "test policy");

    Namespace invalidNamespace = Namespace.of(INVALID_NAMESPACE);
    var invalidTarget =
        new PolicyAttachmentTarget(
            PolicyAttachmentTarget.TypeEnum.NAMESPACE, List.of(invalidNamespace.levels()));

    AttachPolicyRequest request =
        AttachPolicyRequest.builder().setTarget(invalidTarget).setParameters(Map.of()).build();
    try (Response res =
        policyApi
            .request(
                "polaris/v1/{cat}/namespaces/{ns}/policies/{policy-name}/mappings",
                Map.of("cat", currentCatalogName, "ns", NS1_NAME, "policy-name", NS1_P1.getName()))
            .put(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class)).contains(INVALID_NAMESPACE_MSG);
    }
    policyApi.dropPolicy(currentCatalogName, NS1_P1);
  }

  @Test
  public void testAttachPolicyToNonExistingTable() {
    restCatalog.createNamespace(NS1);
    policyApi.createPolicy(
        currentCatalogName,
        NS1_P1,
        PredefinedPolicyTypes.DATA_COMPACTION,
        EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT,
        "test policy");
    TableIdentifier invalidTable = TableIdentifier.of(NS1, INVALID_TABLE);
    var invalidTarget =
        new PolicyAttachmentTarget(
            PolicyAttachmentTarget.TypeEnum.TABLE_LIKE,
            List.of(invalidTable.toString().split("\\.")));

    AttachPolicyRequest request =
        AttachPolicyRequest.builder().setTarget(invalidTarget).setParameters(Map.of()).build();
    try (Response res =
        policyApi
            .request(
                "polaris/v1/{cat}/namespaces/{ns}/policies/{policy-name}/mappings",
                Map.of("cat", currentCatalogName, "ns", NS1_NAME, "policy-name", NS1_P1.getName()))
            .put(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class))
          .contains("Table or view does not exist: " + NS1_NAME + "." + INVALID_TABLE);
    }
    policyApi.dropPolicy(currentCatalogName, NS1_P1);
  }

  @Test
  public void testDetachPolicyFromNonExistingNamespace() {
    restCatalog.createNamespace(NS1);
    policyApi.createPolicy(
        currentCatalogName,
        NS1_P1,
        PredefinedPolicyTypes.DATA_COMPACTION,
        EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT,
        "test policy");
    Namespace invalidNamespace = Namespace.of(INVALID_NAMESPACE);
    var invalidTarget =
        new PolicyAttachmentTarget(
            PolicyAttachmentTarget.TypeEnum.NAMESPACE, List.of(invalidNamespace.levels()));

    DetachPolicyRequest request = DetachPolicyRequest.builder().setTarget(invalidTarget).build();
    try (Response res =
        policyApi
            .request(
                "polaris/v1/{cat}/namespaces/{ns}/policies/{policy-name}/mappings",
                Map.of("cat", currentCatalogName, "ns", NS1_NAME, "policy-name", NS1_P1.getName()))
            .post(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class)).contains(INVALID_NAMESPACE_MSG);
    }
    policyApi.dropPolicy(currentCatalogName, NS1_P1);
  }

  @Test
  public void testDetachPolicyFromNonExistingTable() {
    restCatalog.createNamespace(NS1);
    policyApi.createPolicy(
        currentCatalogName,
        NS1_P1,
        PredefinedPolicyTypes.DATA_COMPACTION,
        EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT,
        "test policy");
    TableIdentifier invalidTable = TableIdentifier.of(NS1, INVALID_TABLE);
    var invalidTarget =
        new PolicyAttachmentTarget(
            PolicyAttachmentTarget.TypeEnum.TABLE_LIKE,
            List.of(invalidTable.toString().split("\\.")));

    DetachPolicyRequest request = DetachPolicyRequest.builder().setTarget(invalidTarget).build();
    try (Response res =
        policyApi
            .request(
                "polaris/v1/{cat}/namespaces/{ns}/policies/{policy-name}/mappings",
                Map.of("cat", currentCatalogName, "ns", NS1_NAME, "policy-name", NS1_P1.getName()))
            .post(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class))
          .contains("Table or view does not exist: " + NS1_NAME + "." + INVALID_TABLE);
    }
    policyApi.dropPolicy(currentCatalogName, NS1_P1);
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
  public void testDropNonExistingPolicy() {
    restCatalog.createNamespace(NS1);
    try (Response res =
        policyApi
            .request(
                "polaris/v1/{cat}/namespaces/{ns}/policies/{policy}",
                Map.of("cat", currentCatalogName, "ns", NS1_NAME, "policy", INVALID_POLICY))
            .delete()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class))
          .contains(
              "Policy does not exist: class PolicyIdentifier",
              "namespace: " + NS1_NAME,
              "name: " + INVALID_POLICY);
    }
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
  public void testUpdateNonExistingPolicy() {
    restCatalog.createNamespace(NS1);
    UpdatePolicyRequest request =
        UpdatePolicyRequest.builder()
            .setContent("{\"enable\":false}")
            .setDescription("updated test policy")
            .build();
    try (Response res =
        policyApi
            .request(
                "polaris/v1/{cat}/namespaces/{ns}/policies/{policy}",
                Map.of("cat", currentCatalogName, "ns", NS1_NAME, "policy", INVALID_POLICY))
            .put(Entity.json(request))) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class))
          .contains(
              "Policy does not exist: class PolicyIdentifier",
              "namespace: " + NS1_NAME,
              "name: " + INVALID_POLICY);
    }
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
  public void testListPoliciesOnNonExistingNamespace() {
    try (Response res =
        policyApi
            .request(
                "polaris/v1/{cat}/namespaces/{ns}/policies",
                Map.of("cat", currentCatalogName, "ns", INVALID_NAMESPACE))
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class)).contains(INVALID_NAMESPACE_MSG);
    }
  }

  @Test
  public void testGetApplicablePoliciesOnNonExistingNamespace() {
    try (Response res =
        policyApi
            .request(
                "polaris/v1/{cat}/applicable-policies",
                Map.of("cat", currentCatalogName),
                Map.of("namespace", INVALID_NAMESPACE))
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class)).contains(INVALID_NAMESPACE_MSG);
    }
  }

  @Test
  public void testGetApplicablePoliciesOnNonExistingTable() {
    restCatalog.createNamespace(NS1);
    policyApi.createPolicy(
        currentCatalogName,
        NS1_P1,
        PredefinedPolicyTypes.DATA_COMPACTION,
        EXAMPLE_TABLE_MAINTENANCE_POLICY_CONTENT,
        "test policy");
    try (Response res =
        policyApi
            .request(
                "polaris/v1/{cat}/applicable-policies",
                Map.of("cat", currentCatalogName),
                Map.of("namespace", NS1_NAME, "target-name", INVALID_TABLE))
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class))
          .contains("Table does not exist: " + NS1_NAME + "." + INVALID_TABLE);
    }
    policyApi.dropPolicy(currentCatalogName, NS1_P1);
  }

  @Test
  public void testLoadNonExistingPolicy() {
    restCatalog.createNamespace(NS1);
    try (Response res =
        policyApi
            .request(
                "polaris/v1/{cat}/namespaces/{ns}/policies/{policy}",
                Map.of("cat", currentCatalogName, "ns", NS1_NAME, "policy", INVALID_POLICY))
            .get()) {
      Assertions.assertThat(res.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
      Assertions.assertThat(res.readEntity(String.class))
          .contains(
              "Policy does not exist: class PolicyIdentifier",
              "namespace: " + NS1_NAME,
              "name: " + INVALID_POLICY);
    }
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

      PolicyGrant policyReadGrant =
          new PolicyGrant(
              Arrays.asList(NS1.levels()),
              NS1_P1.getName(),
              PolicyPrivilege.POLICY_READ,
              GrantResource.TypeEnum.POLICY);
      managementApi.revokeGrant(currentCatalogName, CATALOG_ROLE_2, policyReadGrant);

      Assertions.assertThat(managementApi.listGrants(currentCatalogName, CATALOG_ROLE_2))
          .extracting(GrantResources::getGrants)
          .asInstanceOf(InstanceOfAssertFactories.list(GrantResource.class))
          .map(gr -> ((PolicyGrant) gr).getPrivilege())
          .doesNotContain(PolicyPrivilege.POLICY_READ);
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

  @Test
  public void testGrantsOnNamespace() {
    restCatalog.createNamespace(NS1);
    try {
      managementApi.createCatalogRole(currentCatalogName, CATALOG_ROLE_2);
      List<NamespacePrivilege> policyPrivilegesOnNamespace =
          List.of(
              NamespacePrivilege.POLICY_LIST,
              NamespacePrivilege.POLICY_CREATE,
              NamespacePrivilege.POLICY_DROP,
              NamespacePrivilege.POLICY_WRITE,
              NamespacePrivilege.POLICY_READ,
              NamespacePrivilege.POLICY_FULL_METADATA,
              NamespacePrivilege.NAMESPACE_ATTACH_POLICY,
              NamespacePrivilege.NAMESPACE_DETACH_POLICY);
      Stream<NamespaceGrant> namespaceGrants =
          policyPrivilegesOnNamespace.stream()
              .map(
                  p ->
                      new NamespaceGrant(
                          Arrays.asList(NS1.levels()), p, GrantResource.TypeEnum.NAMESPACE));
      namespaceGrants.forEach(g -> managementApi.addGrant(currentCatalogName, CATALOG_ROLE_2, g));

      Assertions.assertThat(managementApi.listGrants(currentCatalogName, CATALOG_ROLE_2))
          .extracting(GrantResources::getGrants)
          .asInstanceOf(InstanceOfAssertFactories.list(GrantResource.class))
          .map(gr -> ((NamespaceGrant) gr).getPrivilege())
          .containsExactlyInAnyOrderElementsOf(policyPrivilegesOnNamespace);
    } finally {
      policyApi.purge(currentCatalogName, NS1);
    }
  }

  @Test
  public void testGrantsOnCatalog() {
    managementApi.createCatalogRole(currentCatalogName, CATALOG_ROLE_2);
    List<CatalogPrivilege> policyPrivilegesOnCatalog =
        List.of(
            CatalogPrivilege.POLICY_LIST,
            CatalogPrivilege.POLICY_CREATE,
            CatalogPrivilege.POLICY_DROP,
            CatalogPrivilege.POLICY_WRITE,
            CatalogPrivilege.POLICY_READ,
            CatalogPrivilege.POLICY_FULL_METADATA,
            CatalogPrivilege.CATALOG_ATTACH_POLICY,
            CatalogPrivilege.CATALOG_DETACH_POLICY);
    Stream<CatalogGrant> catalogGrants =
        policyPrivilegesOnCatalog.stream()
            .map(p -> new CatalogGrant(p, GrantResource.TypeEnum.CATALOG));
    catalogGrants.forEach(g -> managementApi.addGrant(currentCatalogName, CATALOG_ROLE_2, g));

    Assertions.assertThat(managementApi.listGrants(currentCatalogName, CATALOG_ROLE_2))
        .extracting(GrantResources::getGrants)
        .asInstanceOf(InstanceOfAssertFactories.list(GrantResource.class))
        .map(gr -> ((CatalogGrant) gr).getPrivilege())
        .containsExactlyInAnyOrderElementsOf(policyPrivilegesOnCatalog);
  }

  @Test
  public void testGrantsOnTable() {
    restCatalog.createNamespace(NS2);
    try {
      managementApi.createCatalogRole(currentCatalogName, CATALOG_ROLE_2);
      restCatalog
          .buildTable(
              NS2_T1, new Schema(Types.NestedField.optional(1, "string", Types.StringType.get())))
          .create();

      List<TablePrivilege> policyPrivilegesOnTable =
          List.of(TablePrivilege.TABLE_ATTACH_POLICY, TablePrivilege.TABLE_DETACH_POLICY);

      Stream<TableGrant> tableGrants =
          policyPrivilegesOnTable.stream()
              .map(
                  p ->
                      new TableGrant(
                          Arrays.asList(NS2.levels()),
                          NS2_T1.name(),
                          p,
                          GrantResource.TypeEnum.TABLE));
      tableGrants.forEach(g -> managementApi.addGrant(currentCatalogName, CATALOG_ROLE_2, g));

      Assertions.assertThat(managementApi.listGrants(currentCatalogName, CATALOG_ROLE_2))
          .extracting(GrantResources::getGrants)
          .asInstanceOf(InstanceOfAssertFactories.list(GrantResource.class))
          .map(gr -> ((TableGrant) gr).getPrivilege())
          .containsExactlyInAnyOrderElementsOf(policyPrivilegesOnTable);
    } finally {
      policyApi.purge(currentCatalogName, NS2);
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
