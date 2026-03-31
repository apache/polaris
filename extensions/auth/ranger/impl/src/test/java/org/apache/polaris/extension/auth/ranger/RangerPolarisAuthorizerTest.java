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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.junit.jupiter.api.Test;

public class RangerPolarisAuthorizerTest {
  private static final String RESOURCE_TYPE_NAME_SEP = ":";
  private static final String RESOURCE_ELEMENTS_SEP = "/";

  private final PolarisAuthorizer authorizer;

  public RangerPolarisAuthorizerTest() {
    RangerPolarisAuthorizerFactory factory = new RangerPolarisAuthorizerFactory(createConfig());

    factory.initialize();

    authorizer = factory.create(createRealmConfig());

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
    ObjectMapper ret = new ObjectMapper();

    SimpleModule serDeModule = new SimpleModule("testSerDe");

    serDeModule.addDeserializer(PolarisPrincipal.class, new PolarisPrincipalDeserializer());
    serDeModule.addDeserializer(
        PolarisResolvedPathWrapper.class, new PolarisResolvedPathWrapperDeserializer());

    ret.registerModule(serDeModule);

    return ret;
  }

  static class PolarisPrincipalDeserializer extends JsonDeserializer<PolarisPrincipal> {
    @Override
    public PolarisPrincipal deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      ObjectCodec codec = parser.getCodec();
      TreeNode root = codec.readTree(parser);
      JsonNode nameNode = root != null ? (JsonNode) root.get("name") : null;

      String name = nameNode != null ? nameNode.asText() : null;

      return PolarisPrincipal.of(name, Collections.emptyMap(), Collections.emptySet());
    }
  }

  static class PolarisResolvedPathWrapperDeserializer
      extends JsonDeserializer<PolarisResolvedPathWrapper> {
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
    public PolarisResolvedPathWrapper deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      String target = parser.getValueAsString();
      String[] typeAndName = target.split(RESOURCE_TYPE_NAME_SEP, 2);

      if (typeAndName.length != 2) {
        throw new JsonParseException(target + ": invalid resource");
      }

      PolarisEntityType[] path = RESOURCE_PATHS.get(typeAndName[0]);

      if (path == null) {
        throw new JsonParseException(target + ": unsupported resource type");
      }

      String[] pathElements = typeAndName[1].split(RESOURCE_ELEMENTS_SEP, path.length);

      if (path.length != pathElements.length) {
        throw new JsonParseException(
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
