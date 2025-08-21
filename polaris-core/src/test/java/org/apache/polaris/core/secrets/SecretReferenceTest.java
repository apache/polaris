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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class SecretReferenceTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "urn:polaris-secret:unsafe-in-memory:key1",
        "urn:polaris-secret:unsafe-in-memory:key1:value1",
        "urn:polaris-secret:aws_secrets-manager:my-key_123",
        "urn:polaris-secret:vault:project:env:service:key"
      })
  public void testValidUrns(String validUrn) {
    assertThat(new SecretReference(validUrn, null)).isNotNull();
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
    assertThatThrownBy(() -> new SecretReference(invalidUrn, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid secret URN: " + invalidUrn);
  }

  @Test
  public void tesGetUrnComponents() {
    String urn = "urn:polaris-secret:unsafe-in-memory:key1:value1";
    SecretReference reference = new SecretReference(urn, null);

    assertThat(reference.getSecretManagerType()).isEqualTo("unsafe-in-memory");
    assertThat(reference.getTypeSpecificIdentifier()).isEqualTo("key1:value1");
  }

  @Test
  public void testBuildUrn() {
    String urn = SecretReference.buildUrnString("aws-secrets", "my-key");
    assertThat(urn).isEqualTo("urn:polaris-secret:aws-secrets:my-key");

    String urnWithMultipleIdentifiers = SecretReference.buildUrnString("vault", "project:service");
    assertThat(urnWithMultipleIdentifiers).isEqualTo("urn:polaris-secret:vault:project:service");

    String urnWithNumbers = SecretReference.buildUrnString("type_123", "456:789");
    assertThat(urnWithNumbers).isEqualTo("urn:polaris-secret:type_123:456:789");
  }
}
