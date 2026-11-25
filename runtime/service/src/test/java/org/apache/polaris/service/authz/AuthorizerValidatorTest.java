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
package org.apache.polaris.service.authz;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests {@link AuthorizerValidator}. The invalid realms below are configured but not served, so
 * that startup succeeds; the test validates them explicitly.
 */
@QuarkusTest
@TestProfile(AuthorizerValidatorTest.Profile.class)
public class AuthorizerValidatorTest {

  @Inject AuthorizerValidator validator;

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.authorization.authorizer.realm-without-type.tenant-key",
          "tenant1",
          "polaris.authorization.authorizer.realm-with-unknown-type.type",
          "unknown",
          "polaris.authorization.authorizer.realm-with-unsupported-tenant.type",
          "internal",
          "polaris.authorization.authorizer.realm-with-unsupported-tenant.tenant-key",
          "tenant1",
          "polaris.authorization.authorizer.realm-with-type.type",
          "internal");
    }
  }

  @Test
  void validRealms() {
    // Validates several realms in a row: each one must run in a fresh request context.
    assertThat(validator.validateRealm("realm-with-type")).isEmpty();
    assertThat(validator.validateRealm("unconfigured-realm")).isEmpty();
    assertThat(validator.validateRealm("realm-with-type")).isEmpty();
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "realm-without-type | polaris.authorization.authorizer.realm-without-type.tenant-key is set, but polaris.authorization.authorizer.realm-without-type.type is not",
        "realm-with-unknown-type | unknown authorizer type 'unknown' (polaris.authorization.authorizer.realm-with-unknown-type.type)",
        "realm-with-unsupported-tenant | The internal authorizer does not support tenant keys, but got: tenant1",
      })
  void invalidRealms(String realm, String expectedError) {
    assertThat(validator.validateRealm(realm))
        .hasValueSatisfying(
            error -> assertThat(error).startsWith("realm '" + realm + "'").contains(expectedError));
  }
}
