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
package org.apache.polaris.core.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.polaris.core.admin.model.SigV4AuthenticationParameters;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SigV4AuthenticationParametersDpoTest {

  private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/customer-role";

  static Stream<Arguments> invalidParameters() {
    return Stream.of(
        Arguments.of(null, null, null, null, null, "Either roleArn or storageName"),
        // A blank string is not a value; it is treated the same as absent
        Arguments.of("", "", null, null, null, "Either roleArn or storageName"),
        Arguments.of(ROLE_ARN, "sigv4-catalog", null, null, null, "mutually exclusive"),
        Arguments.of(null, "sigv4-catalog", "a-session", null, null, "role assumption only"),
        Arguments.of(null, "sigv4-catalog", null, "an-external-id", null, "role assumption only"),
        Arguments.of(null, "sigv4-catalog", null, null, "{}", "role assumption only"));
  }

  @ParameterizedTest
  @MethodSource("invalidParameters")
  void validateRejectsUnusableCombination(
      String roleArn,
      String storageName,
      String roleSessionName,
      String externalId,
      String sessionPolicy,
      String expectedMessage) {
    assertThatThrownBy(
            () ->
                SigV4AuthenticationParametersDpo.validateAuthenticationParameters(
                    roleArn, storageName, roleSessionName, externalId, sessionPolicy))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedMessage);
  }

  static Stream<Arguments> validParameters() {
    return Stream.of(
        Arguments.of(ROLE_ARN, null, "a-session", "an-external-id", "{}"),
        Arguments.of(ROLE_ARN, "", null, null, null),
        Arguments.of(null, "sigv4-catalog", null, null, null),
        Arguments.of("", "sigv4-catalog", "", "", ""));
  }

  @ParameterizedTest
  @MethodSource("validParameters")
  void validateAcceptsExactlyOneCredentialSource(
      String roleArn,
      String storageName,
      String roleSessionName,
      String externalId,
      String sessionPolicy) {
    assertThatCode(
            () ->
                SigV4AuthenticationParametersDpo.validateAuthenticationParameters(
                    roleArn, storageName, roleSessionName, externalId, sessionPolicy))
        .doesNotThrowAnyException();
  }

  /**
   * The persistence object is also the deserialization entry point, so it has to read back records
   * written before storageName existed, including ones whose roleArn was persisted empty.
   */
  @Test
  void constructorStaysLenient() {
    SigV4AuthenticationParametersDpo dpo =
        new SigV4AuthenticationParametersDpo("", null, null, "us-west-2", "glue", null);
    assertThat(dpo.usesStorageCredentials()).isFalse();
  }

  @Test
  void storageNameSurvivesSerializationAndModelConversion() {
    SigV4AuthenticationParametersDpo dpo =
        new SigV4AuthenticationParametersDpo(
            null, null, null, "us-west-2", "custom-service", null, "sigv4-catalog");
    IcebergRestConnectionConfigInfoDpo connectionConfig =
        new IcebergRestConnectionConfigInfoDpo(
            "https://catalog.example.com", dpo, null, "remote", Map.of());

    ConnectionConfigInfoDpo roundTripped =
        ConnectionConfigInfoDpo.deserialize(connectionConfig.serialize());

    SigV4AuthenticationParametersDpo restored =
        (SigV4AuthenticationParametersDpo) roundTripped.getAuthenticationParameters();
    assertThat(restored.getStorageName()).isEqualTo("sigv4-catalog");
    assertThat(restored.usesStorageCredentials()).isTrue();
    assertThat(((SigV4AuthenticationParameters) restored.asAuthenticationParametersModel()))
        .extracting(SigV4AuthenticationParameters::getStorageName)
        .isEqualTo("sigv4-catalog");
  }
}
