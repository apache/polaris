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
package org.apache.polaris.core.credentials.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class ConnectionCredentialsTest {

  /**
   * Test that validates Bug #3 fix: Null check for expiration timestamp value prevents
   * NullPointerException.
   */
  @Test
  public void testPutWithNullExpirationTimestamp() {
    ConnectionCredentials.Builder builder = ConnectionCredentials.builder();

    assertThatThrownBy(() -> builder.put(CatalogAccessProperty.EXPIRES_AT_MS, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expiration timestamp value cannot be null")
        .hasMessageContaining("rest.expires-at-ms");
  }

  /**
   * Test that validates Bug #3 fix: NumberFormatException is caught and wrapped in
   * IllegalArgumentException when expiration timestamp contains invalid non-numeric value.
   */
  @Test
  public void testPutWithInvalidExpirationTimestamp() {
    ConnectionCredentials.Builder builder = ConnectionCredentials.builder();

    assertThatThrownBy(() -> builder.put(CatalogAccessProperty.EXPIRES_AT_MS, "not-a-number"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid expiration timestamp value")
        .hasMessageContaining("not-a-number")
        .hasCauseInstanceOf(NumberFormatException.class);
  }

  /**
   * Test that validates Bug #3 fix: NumberFormatException is caught when expiration timestamp
   * contains an empty string.
   */
  @Test
  public void testPutWithEmptyStringExpirationTimestamp() {
    ConnectionCredentials.Builder builder = ConnectionCredentials.builder();

    assertThatThrownBy(() -> builder.put(CatalogAccessProperty.EXPIRES_AT_MS, ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid expiration timestamp value")
        .hasCauseInstanceOf(NumberFormatException.class);
  }

  /**
   * Test that validates Bug #3 fix: NumberFormatException is caught when expiration timestamp
   * contains a decimal value.
   */
  @Test
  public void testPutWithDecimalExpirationTimestamp() {
    ConnectionCredentials.Builder builder = ConnectionCredentials.builder();

    assertThatThrownBy(() -> builder.put(CatalogAccessProperty.EXPIRES_AT_MS, "1234.567"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid expiration timestamp value")
        .hasMessageContaining("1234.567")
        .hasCauseInstanceOf(NumberFormatException.class);
  }

  /**
   * Test that validates Bug #3 fix: NumberFormatException is caught when expiration timestamp
   * contains alphanumeric characters.
   */
  @Test
  public void testPutWithAlphanumericExpirationTimestamp() {
    ConnectionCredentials.Builder builder = ConnectionCredentials.builder();

    assertThatThrownBy(() -> builder.put(CatalogAccessProperty.EXPIRES_AT_MS, "abc123xyz"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid expiration timestamp value")
        .hasMessageContaining("abc123xyz")
        .hasCauseInstanceOf(NumberFormatException.class);
  }

  /**
   * Test that validates Bug #3 fix: NumberFormatException is caught when expiration timestamp
   * is a number that's too large for Long.
   */
  @Test
  public void testPutWithOverflowExpirationTimestamp() {
    ConnectionCredentials.Builder builder = ConnectionCredentials.builder();

    assertThatThrownBy(
            () ->
                builder.put(
                    CatalogAccessProperty.EXPIRES_AT_MS, "999999999999999999999999999999999999"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid expiration timestamp value")
        .hasCauseInstanceOf(NumberFormatException.class);
  }

  /**
   * Test that validates Bug #3 fix: Valid expiration timestamp works correctly after the fix.
   */
  @Test
  public void testPutWithValidExpirationTimestamp() {
    ConnectionCredentials.Builder builder = ConnectionCredentials.builder();

    long expectedMillis = 1234567890000L;
    builder.put(CatalogAccessProperty.EXPIRES_AT_MS, String.valueOf(expectedMillis));

    ConnectionCredentials credentials = builder.build();
    assertThat(credentials.expiresAt())
        .isPresent()
        .hasValue(Instant.ofEpochMilli(expectedMillis));
  }

  /**
   * Test that validates non-expiration properties still work after the fix.
   */
  @Test
  public void testPutWithCredentialProperty() {
    ConnectionCredentials.Builder builder = ConnectionCredentials.builder();

    builder.put(CatalogAccessProperty.BEARER_TOKEN, "my-secret-token");

    ConnectionCredentials credentials = builder.build();
    assertThat(credentials.get(CatalogAccessProperty.BEARER_TOKEN)).isEqualTo("my-secret-token");
  }

  /**
   * Test that validates Bug #3 fix: Negative timestamp values are accepted (valid for dates
   * before epoch).
   */
  @Test
  public void testPutWithNegativeExpirationTimestamp() {
    ConnectionCredentials.Builder builder = ConnectionCredentials.builder();

    long expectedMillis = -1234567890000L;
    builder.put(CatalogAccessProperty.EXPIRES_AT_MS, String.valueOf(expectedMillis));

    ConnectionCredentials credentials = builder.build();
    assertThat(credentials.expiresAt())
        .isPresent()
        .hasValue(Instant.ofEpochMilli(expectedMillis));
  }

  /**
   * Test that validates Bug #3 fix: Zero timestamp value is accepted.
   */
  @Test
  public void testPutWithZeroExpirationTimestamp() {
    ConnectionCredentials.Builder builder = ConnectionCredentials.builder();

    builder.put(CatalogAccessProperty.EXPIRES_AT_MS, "0");

    ConnectionCredentials credentials = builder.build();
    assertThat(credentials.expiresAt()).isPresent().hasValue(Instant.ofEpochMilli(0L));
  }

  /**
   * Test that validates whitespace in timestamp is not handled (should throw exception).
   */
  @Test
  public void testPutWithWhitespaceExpirationTimestamp() {
    ConnectionCredentials.Builder builder = ConnectionCredentials.builder();

    assertThatThrownBy(() -> builder.put(CatalogAccessProperty.EXPIRES_AT_MS, "  12345  "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid expiration timestamp value")
        .hasCauseInstanceOf(NumberFormatException.class);
  }

  /**
   * Test that validates Bug #3 fix works with AWS session token expiration timestamp.
   */
  @Test
  public void testPutWithAWSSessionTokenExpiresAt() {
    ConnectionCredentials.Builder builder = ConnectionCredentials.builder();

    long expectedMillis = 1711234567890L;
    builder.put(CatalogAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS, String.valueOf(expectedMillis));

    ConnectionCredentials credentials = builder.build();
    assertThat(credentials.expiresAt())
        .isPresent()
        .hasValue(Instant.ofEpochMilli(expectedMillis));
  }

  /**
   * Test that validates Bug #3 fix catches invalid AWS session token expiration timestamp.
   */
  @Test
  public void testPutWithInvalidAWSSessionTokenExpiresAt() {
    ConnectionCredentials.Builder builder = ConnectionCredentials.builder();

    assertThatThrownBy(
            () ->
                builder.put(CatalogAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS, "invalid-time"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid expiration timestamp value")
        .hasMessageContaining("invalid-time")
        .hasCauseInstanceOf(NumberFormatException.class);
  }
}
