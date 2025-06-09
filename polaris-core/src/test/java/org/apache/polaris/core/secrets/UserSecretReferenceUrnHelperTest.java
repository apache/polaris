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
package org.apache.polaris.core.secrets;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class UserSecretReferenceUrnHelperTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "urn:polaris-secret:unsafe-in-memory:key1",
        "urn:polaris-secret:unsafe-in-memory:key1:value1",
        "urn:polaris-secret:aws_secrets-manager:my-key_123",
        "urn:polaris-secret:vault:project:env:service:key"
      })
  public void testValidUrns(String validUrn) {
    assertThat(UserSecretReferenceUrnHelper.isValid(validUrn)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "   ",
        "not-a-urn",
        "urn:",
        "urn:polaris-secret:",
        "urn:polaris-secret:type:",
        "wrong:polaris-secret:type:key:",
        "urn:polaris-secret:type with spaces:key",
        "urn:polaris-secret:type@invalid:key",
        "urn:polaris-secret:unsafe-in-memory:key::"
      })
  public void testInvalidUrns(String invalidUrn) {
    assertThat(UserSecretReferenceUrnHelper.isValid(invalidUrn))
        .as("URN should be invalid: %s", invalidUrn)
        .isFalse();
  }

  @Test
  public void tesGetUrnComponents() {
    String urn = "urn:polaris-secret:unsafe-in-memory:key1:value1";
    assertThat(UserSecretReferenceUrnHelper.getSecretManagerType(urn))
        .isEqualTo("unsafe-in-memory");
    assertThat(UserSecretReferenceUrnHelper.getTypeSpecificIdentifier(urn))
        .isEqualTo("key1:value1");
  }

  @Test
  public void testBuildUrn() {
    String urn = UserSecretReferenceUrnHelper.buildUrn("aws-secrets", "my-key");
    assertThat(urn).isEqualTo("urn:polaris-secret:aws-secrets:my-key");

    String urnWithMultipleIdentifiers =
        UserSecretReferenceUrnHelper.buildUrn("vault", "project:service");
    assertThat(urnWithMultipleIdentifiers).isEqualTo("urn:polaris-secret:vault:project:service");

    String urnWithNumbers = UserSecretReferenceUrnHelper.buildUrn("type_123", "456:789");
    assertThat(urnWithNumbers).isEqualTo("urn:polaris-secret:type_123:456:789");
  }
}
