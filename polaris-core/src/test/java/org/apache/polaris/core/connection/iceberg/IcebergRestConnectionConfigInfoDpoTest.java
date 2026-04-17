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

package org.apache.polaris.core.connection.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCharSequence;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.GcpAuthenticationParametersDpo;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

class IcebergRestConnectionConfigInfoDpoTest {

  @Test
  void testRoundTrip() {
    IcebergRestConnectionConfigInfoDpo dpo = createDpo(Map.of("x", "y"));
    ConnectionConfigInfo dto = dpo.asConnectionConfigInfoModel(null);
    assertThatCharSequence(dpo.toString())
        .isEqualTo(
            ConnectionConfigInfoDpo.fromConnectionConfigInfoModelWithSecrets(dto, Map.of())
                .toString());
  }

  @Test
  void testNullAdditionalHeadersHandledGracefully() {
    IcebergRestConnectionConfigInfoDpo dpo = createDpo(null);
    PolarisCredentialManager mockCredentialManager = mock(PolarisCredentialManager.class);
    ConnectionCredentials mockCredentials = mock(ConnectionCredentials.class);
    String expectedKey = "credential_key";
    String expectedValue = "credential_value";
    when(mockCredentials.credentials()).thenReturn(Map.of(expectedKey, expectedValue));
    when(mockCredentialManager.getConnectionCredentials(dpo)).thenReturn(mockCredentials);
    Map<String, String> properties = dpo.asIcebergCatalogProperties(mockCredentialManager);
    assertThat(properties).containsEntry(expectedKey, expectedValue);
  }

  private static @NonNull IcebergRestConnectionConfigInfoDpo createDpo(
      Map<String, String> additionalHeaders) {
    return new IcebergRestConnectionConfigInfoDpo(
        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
        new GcpAuthenticationParametersDpo(),
        null,
        null,
        additionalHeaders);
  }
}
