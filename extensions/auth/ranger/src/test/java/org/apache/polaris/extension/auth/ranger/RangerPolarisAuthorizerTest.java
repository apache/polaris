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

package org.apache.polaris.extension.auth.ranger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.polaris.extension.auth.ranger.RangerTestUtils.createConfig;
import static org.apache.polaris.extension.auth.ranger.RangerTestUtils.createRealmConfig;
import static org.apache.polaris.extension.auth.ranger.RangerTestUtils.createRealmContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PathSegment;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.ranger.authz.embedded.RangerEmbeddedAuthorizer;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerMultiAuthzRequest;
import org.apache.ranger.authz.model.RangerMultiAuthzResult;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DatabindException;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;

public class RangerPolarisAuthorizerTest {
  private static final String RESOURCE_TYPE_NAME_SEP = ":";
  private static final String RESOURCE_ELEMENTS_SEP = "/";

  private final PolarisAuthorizer authorizer;

  public RangerPolarisAuthorizerTest() {
    RangerPolarisAuthorizerFactory factory = new RangerPolarisAuthorizerFactory(createConfig());
    RangerPolarisAuthorizer rangerPolarisAuthorizer = factory.create(createRealmConfig());
    rangerPolarisAuthorizer.setRealmContext(createRealmContext());
    this.authorizer = rangerPolarisAuthorizer;

    assertNotNull(authorizer);
  }

  @Test
  public void testAuthzRoot() throws Exception {
    runTests(authorizer, "/authz_tests/tests_authz_root.json");
  }

  @Test
  public void testAuthzCatalog() throws Exception {
    runTests(authorizer, "/authz_tests/tests_authz_catalog.json");
  }

  @Test
  public void testAuthzPrincipal() throws Exception {
    runTests(authorizer, "/authz_tests/tests_authz_principal.json");
  }

  @Test
  public void testAuthzNamespace() throws Exception {
    runTests(authorizer, "/authz_tests/tests_authz_namespace.json");
  }

  @Test
  public void testAuthzTable() throws Exception {
    runTests(authorizer, "/authz_tests/tests_authz_table.json");
  }

  @Test
  public void testAuthzPolicy() throws Exception {
    runTests(authorizer, "/authz_tests/tests_authz_policy.json");
  }

  @Test
  public void testAuthzUnsupported() throws Exception {
    runTests(authorizer, "/authz_tests/tests_authz_unsupported.json");
  }

  @Test
  void authorizeUsesIntentResolvedTarget() throws Exception {
    RangerEmbeddedAuthorizer embeddedAuthorizer = mock(RangerEmbeddedAuthorizer.class);
    RangerPolarisAuthorizer authorizer =
        new RangerPolarisAuthorizer(embeddedAuthorizer, "dev_polaris", createRealmConfig());
    authorizer.setRealmContext(createRealmContext());
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper tablePath =
        resolvedPath(
            PolarisEntityType.ROOT,
            "root",
            PolarisEntityType.CATALOG,
            "catalog",
            PolarisEntityType.NAMESPACE,
            "ns",
            PolarisEntityType.TABLE_LIKE,
            "table");
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Collections.emptySet());
    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(
                new SingleTargetAuthorizationIntent(
                    PolarisAuthorizableOperation.LOAD_TABLE,
                    PolarisSecurable.of(
                        new PathSegment(PolarisEntityType.CATALOG, "catalog"),
                        new PathSegment(PolarisEntityType.NAMESPACE, "ns"),
                        new PathSegment(PolarisEntityType.TABLE_LIKE, "table")))));

    when(manifest.getResolvedPath(
            ResolvedPathKey.of(List.of("ns", "table"), PolarisEntityType.TABLE_LIKE), true))
        .thenReturn(tablePath);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Collections.emptySet());
    when(embeddedAuthorizer.authorize(
            org.mockito.ArgumentMatchers.any(RangerMultiAuthzRequest.class)))
        .thenReturn(rangerResult(RangerAuthzResult.AccessDecision.ALLOW));

    AuthorizationDecision decision =
        authorizer.authorize(new AuthorizationState(manifest), request);

    assertThat(decision.isAllowed()).isTrue();
    ArgumentCaptor<RangerMultiAuthzRequest> requestCaptor =
        ArgumentCaptor.forClass(RangerMultiAuthzRequest.class);
    verify(embeddedAuthorizer).authorize(requestCaptor.capture());
    RangerMultiAuthzRequest rangerRequest = requestCaptor.getValue();
    assertThat(rangerRequest.getUser().getName()).isEqualTo("alice");
    assertThat(rangerRequest.getAccesses()).hasSize(1);
    assertThat(rangerRequest.getAccesses().get(0).getAction()).isEqualTo("LOAD_TABLE");
    assertThat(rangerRequest.getAccesses().get(0).getResource().getName())
        .isEqualTo("table:POLARIS/catalog/ns/table");
  }

  @Test
  void authorizeReturnsDenyDecisionWhenRangerDenies() throws Exception {
    RangerEmbeddedAuthorizer embeddedAuthorizer = mock(RangerEmbeddedAuthorizer.class);
    RangerPolarisAuthorizer authorizer =
        new RangerPolarisAuthorizer(embeddedAuthorizer, "dev_polaris", createRealmConfig());
    authorizer.setRealmContext(createRealmContext());
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper catalogPath =
        resolvedPath(PolarisEntityType.ROOT, "root", PolarisEntityType.CATALOG, "catalog");
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Collections.emptySet());
    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(
                new SingleTargetAuthorizationIntent(
                    PolarisAuthorizableOperation.GET_CATALOG,
                    PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog")))));

    when(manifest.getResolvedReferenceCatalogEntity(true)).thenReturn(catalogPath);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Collections.emptySet());
    when(embeddedAuthorizer.authorize(
            org.mockito.ArgumentMatchers.any(RangerMultiAuthzRequest.class)))
        .thenReturn(rangerResult(RangerAuthzResult.AccessDecision.DENY));

    AuthorizationDecision decision =
        authorizer.authorize(new AuthorizationState(manifest), request);

    assertThat(decision.isAllowed()).isFalse();
    assertThat(decision.getMessage().orElseThrow())
        .contains("Principal 'alice' is not authorized for op 'GET_CATALOG'");
  }

  private void runTests(PolarisAuthorizer authorizer, String testFilename) throws Exception {
    InputStream inStream = this.getClass().getResourceAsStream(testFilename);
    InputStreamReader reader = new InputStreamReader(inStream, UTF_8);
    TestSuite testSuite = getMapper().readValue(reader, TestSuite.class);

    for (TestData test : testSuite.tests) {
      if (test.result.isAllowed) {
        try {
          authorizer.authorizeOrThrow(
              test.request.principal,
              Collections.emptySet(),
              test.request.authzOp,
              test.request.target,
              test.request.secondary);
        } catch (ForbiddenException excp) {
          fail(
              test.request.principal
                  + " should be allowed to perform "
                  + test.request.authzOp
                  + " on (target: "
                  + test.request.target
                  + ", secondary: "
                  + test.request.secondary
                  + ")",
              excp);
        }
      } else {
        assertThrows(
            ForbiddenException.class,
            () ->
                authorizer.authorizeOrThrow(
                    test.request.principal,
                    Collections.emptySet(),
                    test.request.authzOp,
                    test.request.target,
                    test.request.secondary),
            () ->
                test.request.principal
                    + " should not be allowed to perform "
                    + test.request.authzOp
                    + " on (target: "
                    + test.request.target
                    + ", secondary: "
                    + test.request.secondary
                    + ")");
      }
    }
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class TestSuite {
    public List<TestData> tests;
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class TestData {
    public TestRequest request;
    public TestResult result;
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class TestRequest {
    public PolarisAuthorizableOperation authzOp;
    public PolarisPrincipal principal;
    public PolarisResolvedPathWrapper target;
    public PolarisResolvedPathWrapper secondary;
  }

  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class TestResult {
    public Boolean isAllowed;
  }

  private ObjectMapper getMapper() {
    SimpleModule serDeModule = new SimpleModule("testSerDe");

    serDeModule.addDeserializer(PolarisPrincipal.class, new PolarisPrincipalDeserializer());
    serDeModule.addDeserializer(
        PolarisResolvedPathWrapper.class, new PolarisResolvedPathWrapperDeserializer());

    return JsonMapper.builder().addModule(serDeModule).build();
  }

  private static RangerMultiAuthzResult rangerResult(RangerAuthzResult.AccessDecision decision) {
    RangerAuthzResult accessResult = new RangerAuthzResult();
    accessResult.setDecision(decision);
    RangerMultiAuthzResult result = new RangerMultiAuthzResult();
    result.setDecision(decision);
    result.setAccesses(List.of(accessResult));
    return result;
  }

  private static PolarisResolvedPathWrapper resolvedPath(Object... typeAndNamePairs) {
    List<ResolvedPolarisEntity> entities = new ArrayList<>(typeAndNamePairs.length / 2);
    for (int i = 0; i < typeAndNamePairs.length; i += 2) {
      PolarisEntityType type = (PolarisEntityType) typeAndNamePairs[i];
      String name = (String) typeAndNamePairs[i + 1];
      PolarisEntity entity = new PolarisEntity.Builder().setName(name).setType(type).build();
      entities.add(
          new ResolvedPolarisEntity(entity, Collections.emptyList(), Collections.emptyList()));
    }
    return new PolarisResolvedPathWrapper(entities);
  }

  static class PolarisPrincipalDeserializer extends ValueDeserializer<PolarisPrincipal> {
    @Override
    public PolarisPrincipal deserialize(JsonParser parser, DeserializationContext context) {
      JsonNode root = parser.readValueAs(JsonNode.class);
      JsonNode nameNode = root != null ? root.get("name") : null;

      String name = nameNode != null ? nameNode.asString() : null;

      return PolarisPrincipal.of(name, Collections.emptyMap(), Collections.emptySet());
    }
  }

  static class PolarisResolvedPathWrapperDeserializer
      extends ValueDeserializer<PolarisResolvedPathWrapper> {
    private static final Map<String, PolarisEntityType[]> RESOURCE_PATHS = new HashMap<>();

    static {
      RESOURCE_PATHS.put("ROOT", new PolarisEntityType[] {PolarisEntityType.ROOT});
      RESOURCE_PATHS.put(
          "PRINCIPAL",
          new PolarisEntityType[] {PolarisEntityType.ROOT, PolarisEntityType.PRINCIPAL});
      RESOURCE_PATHS.put(
          "PRINCIPAL_ROLE",
          new PolarisEntityType[] {PolarisEntityType.ROOT, PolarisEntityType.PRINCIPAL_ROLE});
      RESOURCE_PATHS.put(
          "CATALOG", new PolarisEntityType[] {PolarisEntityType.ROOT, PolarisEntityType.CATALOG});
      RESOURCE_PATHS.put(
          "CATALOG_ROLE",
          new PolarisEntityType[] {PolarisEntityType.ROOT, PolarisEntityType.CATALOG_ROLE});
      RESOURCE_PATHS.put(
          "NAMESPACE",
          new PolarisEntityType[] {
            PolarisEntityType.ROOT, PolarisEntityType.CATALOG, PolarisEntityType.NAMESPACE
          });
      RESOURCE_PATHS.put(
          "TABLE_LIKE",
          new PolarisEntityType[] {
            PolarisEntityType.ROOT,
            PolarisEntityType.CATALOG,
            PolarisEntityType.NAMESPACE,
            PolarisEntityType.TABLE_LIKE
          });
      RESOURCE_PATHS.put(
          "TASK", new PolarisEntityType[] {PolarisEntityType.ROOT, PolarisEntityType.TASK});
      RESOURCE_PATHS.put(
          "FILE",
          new PolarisEntityType[] {
            PolarisEntityType.ROOT,
            PolarisEntityType.CATALOG,
            PolarisEntityType.NAMESPACE,
            PolarisEntityType.TABLE_LIKE,
            PolarisEntityType.FILE
          });
      RESOURCE_PATHS.put(
          "POLICY",
          new PolarisEntityType[] {
            PolarisEntityType.ROOT,
            PolarisEntityType.CATALOG,
            PolarisEntityType.NAMESPACE,
            PolarisEntityType.POLICY
          });
    }

    @Override
    public PolarisResolvedPathWrapper deserialize(
        JsonParser parser, DeserializationContext context) {
      String target = parser.getValueAsString();
      String[] typeAndName = target.split(RESOURCE_TYPE_NAME_SEP, 2);

      if (typeAndName.length != 2) {
        throw DatabindException.from(parser, target + ": invalid resource");
      }

      PolarisEntityType[] path = RESOURCE_PATHS.get(typeAndName[0]);

      if (path == null) {
        throw DatabindException.from(parser, target + ": unsupported resource type");
      }

      String[] pathElements = typeAndName[1].split(RESOURCE_ELEMENTS_SEP, path.length);

      if (path.length != pathElements.length) {
        throw DatabindException.from(
            parser,
            target
                + ": incorrect number of path elements. Expected "
                + path.length
                + ", found "
                + pathElements.length);
      }

      List<ResolvedPolarisEntity> entities = new ArrayList<>(path.length);

      for (int i = 0; i < path.length; i++) {
        PolarisEntity entity =
            new PolarisEntity.Builder().setName(pathElements[i]).setType(path[i]).build();

        entities.add(
            new ResolvedPolarisEntity(entity, Collections.emptyList(), Collections.emptyList()));
      }

      return new PolarisResolvedPathWrapper(entities);
    }
  }
}
