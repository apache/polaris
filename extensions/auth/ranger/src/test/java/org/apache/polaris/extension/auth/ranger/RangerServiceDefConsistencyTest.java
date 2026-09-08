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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.InputStream;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Guards against the operator-facing {@code polaris-ranger-servicedef.json} drifting from the
 * {@code serviceDef} that the authorization tests actually exercise. Every operation covered by
 * {@link RangerPolarisAuthorizerTest} runs against {@code /authz_tests/dev_polaris.json}, so
 * keeping the two identical is what makes those tests representative of the artifact operators
 * register with Ranger Admin.
 */
public class RangerServiceDefConsistencyTest {

  @Test
  public void shippedServiceDefMatchesTestFixture() throws Exception {
    JsonMapper mapper = JsonMapper.builder().build();

    JsonNode shippedServiceDef = readJson(mapper, "/polaris-ranger-servicedef.json");
    JsonNode testFixture = readJson(mapper, "/authz_tests/dev_polaris.json");
    JsonNode testServiceDef = testFixture.get("serviceDef");

    assertNotNull(testServiceDef, "test fixture is missing a serviceDef");
    assertEquals(
        shippedServiceDef,
        testServiceDef,
        "extensions/auth/ranger/src/main/resources/polaris-ranger-servicedef.json must match "
            + "the serviceDef in authz_tests/dev_polaris.json, otherwise the authorization tests "
            + "no longer exercise the artifact operators register with Ranger Admin");
  }

  private static JsonNode readJson(JsonMapper mapper, String resourcePath) throws Exception {
    try (InputStream in = RangerServiceDefConsistencyTest.class.getResourceAsStream(resourcePath)) {
      assertNotNull(in, resourcePath + " not found on classpath");
      return mapper.readTree(in);
    }
  }
}
