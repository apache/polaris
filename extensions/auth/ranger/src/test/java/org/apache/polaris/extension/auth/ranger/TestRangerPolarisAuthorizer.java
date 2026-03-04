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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.junit.jupiter.api.Test;

public class TestRangerPolarisAuthorizer {
  private static final String RESOURCE_TYPE_NAME_SEP = ":";
  private static final String RESOURCE_ELEMENTS_SEP = "/";

  private final Gson gsonBuilder;
  private final PolarisAuthorizer authorizer;

  public TestRangerPolarisAuthorizer() throws Exception {
    gsonBuilder =
        new GsonBuilder()
            .setDateFormat("yyyyMMdd-HH:mm:ss.SSSZ")
            .setPrettyPrinting()
            .registerTypeAdapter(PolarisPrincipal.class, new PolarisPrincipalDeserializer())
            .registerTypeAdapter(
                PolarisResolvedPathWrapper.class, new PolarisResolvedPathWrapperDeserializer())
            .create();

    RangerPolarisAuthorizerFactory factory =
        new RangerPolarisAuthorizerFactory(createConfig("authz_tests/ranger-plugin.properties"));

    authorizer = factory.create(createRealmConfig());

    assertNotNull(authorizer);
  }

  @Test
  public void testAuthzRoot() {
    runTests(authorizer, "/authz_tests/tests_authz_root.json");
  }

  @Test
  public void testAuthzCatalog() {
    runTests(authorizer, "/authz_tests/tests_authz_catalog.json");
  }

  @Test
  public void testAuthzPrincipal() {
    runTests(authorizer, "/authz_tests/tests_authz_principal.json");
  }

  @Test
  public void testAuthzNamespace() {
    runTests(authorizer, "/authz_tests/tests_authz_namespace.json");
  }

  @Test
  public void testAuthzTable() {
    runTests(authorizer, "/authz_tests/tests_authz_table.json");
  }

  @Test
  public void testAuthzPolicy() {
    runTests(authorizer, "/authz_tests/tests_authz_policy.json");
  }

  @Test
  public void testAuthzUnsupported() {
    runTests(authorizer, "/authz_tests/tests_authz_unsupported.json");
  }

  private void runTests(PolarisAuthorizer authorizer, String testFilename) {
    InputStream inStream = this.getClass().getResourceAsStream(testFilename);
    InputStreamReader reader = new InputStreamReader(inStream, UTF_8);

    TestSuite testSuite = gsonBuilder.fromJson(reader, TestSuite.class);

    for (TestData test : testSuite.tests) {
      try {
        authorizer.authorizeOrThrow(
            test.request.principal,
            Collections.emptySet(),
            test.request.authzOp,
            test.request.target,
            test.request.secondary);

        assertEquals(
            test.result.isAllowed,
            Boolean.TRUE,
            () ->
                test.request.principal
                    + " performed "
                    + test.request.authzOp
                    + " on (target: "
                    + test.request.target
                    + ", secondary: "
                    + test.request.secondary
                    + ")");
      } catch (Throwable t) {
        assertEquals(
            test.result.isAllowed,
            Boolean.FALSE,
            () ->
                test.request.principal
                    + " performed "
                    + test.request.authzOp
                    + " on (target: "
                    + test.request.target
                    + ", secondary: "
                    + test.request.secondary
                    + ")");
      }
    }
  }

  private static class TestSuite {
    List<TestData> tests;
  }

  private static class TestData {
    TestRequest request;
    TestResult result;
  }

  private static class TestRequest {
    PolarisAuthorizableOperation authzOp;
    PolarisPrincipal principal;
    PolarisResolvedPathWrapper target;
    PolarisResolvedPathWrapper secondary;
  }

  private static class TestResult {
    Boolean isAllowed;
  }

  static class PolarisPrincipalDeserializer implements JsonDeserializer<PolarisPrincipal> {
    @Override
    public PolarisPrincipal deserialize(
        JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext)
        throws JsonParseException {
      String name = jsonElement.getAsJsonObject().get("name").getAsString();

      return PolarisPrincipal.of(name, Collections.emptyMap(), Collections.emptySet());
    }
  }

  static class PolarisResolvedPathWrapperDeserializer
      implements JsonDeserializer<PolarisResolvedPathWrapper> {
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
        JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext)
        throws JsonParseException {
      String target = jsonElement.getAsString();
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
