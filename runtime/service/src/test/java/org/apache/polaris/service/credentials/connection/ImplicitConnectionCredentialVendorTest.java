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
package org.apache.polaris.service.credentials.connection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ImplicitAuthenticationParametersDpo;
import org.apache.polaris.core.connection.iceberg.IcebergRestConnectionConfigInfoDpo;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link ImplicitConnectionCredentialVendor}. */
public class ImplicitConnectionCredentialVendorTest {

  private ImplicitConnectionCredentialVendor implicitVendor;

  @BeforeEach
  void setup() {
    implicitVendor = new ImplicitConnectionCredentialVendor();
  }

  @Test
  public void testGetConnectionCredentials() {
    // Setup
    ImplicitAuthenticationParametersDpo authParams = new ImplicitAuthenticationParametersDpo();

    IcebergRestConnectionConfigInfoDpo connectionConfig =
        new IcebergRestConnectionConfigInfoDpo(
            "https://catalog.example.com", authParams, null, "test-catalog");

    // Execute
    ConnectionCredentials credentials = implicitVendor.getConnectionCredentials(connectionConfig);

    // Verify - no credentials are provided, but expiration is set to infinite
    Assertions.assertThat(credentials.credentials()).isEmpty();
    Assertions.assertThat(credentials.expiresAt()).contains(Instant.ofEpochMilli(Long.MAX_VALUE));
  }

  @Test
  public void testGetConnectionCredentialsWithWrongAuthType() {
    // Setup - use a mock with wrong authentication type
    ConnectionConfigInfoDpo mockConfig = mock(ConnectionConfigInfoDpo.class);
    AuthenticationParametersDpo mockAuthParams = mock(AuthenticationParametersDpo.class);

    when(mockConfig.getAuthenticationParameters()).thenReturn(mockAuthParams);

    // Execute & Verify - should fail precondition check
    Assertions.assertThatThrownBy(() -> implicitVendor.getConnectionCredentials(mockConfig))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expected ImplicitAuthenticationParametersDpo");
  }
}
