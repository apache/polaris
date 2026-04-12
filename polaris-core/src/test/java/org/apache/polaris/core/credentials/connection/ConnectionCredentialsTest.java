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

import static org.apache.polaris.core.credentials.connection.CatalogAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS;
import static org.apache.polaris.core.credentials.connection.CatalogAccessProperty.EXPIRES_AT_MS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ConnectionCredentialsTest {

  @Test
  public void testSameExpirationTs() {
    Map<CatalogAccessProperty, String> properties = new HashMap<>();
    Instant expireAt = Instant.ofEpochMilli(1);
    String expireAtStr = String.valueOf(expireAt.toEpochMilli());

    properties.put(AWS_SESSION_TOKEN_EXPIRES_AT_MS, expireAtStr);
    properties.put(EXPIRES_AT_MS, expireAtStr);

    ConnectionCredentials credentials = ConnectionCredentials.of(properties);

    assertThat(credentials.expiresAt()).hasValue(expireAt);
  }

  @Test
  public void testDifferentExpirationTs() {
    Map<CatalogAccessProperty, String> properties = new HashMap<>();
    properties.put(
        AWS_SESSION_TOKEN_EXPIRES_AT_MS, String.valueOf(Instant.ofEpochMilli(1).toEpochMilli()));
    properties.put(EXPIRES_AT_MS, String.valueOf(Instant.ofEpochMilli(2).toEpochMilli()));

    assertThatThrownBy(() -> ConnectionCredentials.of(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Multiple distinct expiration timestamps");
  }

  @Test
  public void testSingleExpirationTs() {
    Instant expireAt = Instant.ofEpochMilli(1);
    ConnectionCredentials credentials =
        ConnectionCredentials.of(
            Map.of(AWS_SESSION_TOKEN_EXPIRES_AT_MS, String.valueOf(expireAt.toEpochMilli())));
    assertThat(credentials.expiresAt()).hasValue(expireAt);
  }
}
