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
package org.apache.polaris.extensions.federation.hive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ImplicitAuthenticationParametersDpo;
import org.apache.polaris.core.connection.hive.HiveConnectionConfigInfoDpo;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class HiveFederatedCatalogFactoryTest {

  private static final String WAREHOUSE = "file:///tmp/hive-federation-test-warehouse";
  private static final String HMS_URI = "thrift://127.0.0.1:9083";

  private PolarisCredentialManager polarisCredentialManager;
  private HiveFederatedCatalogFactory factory;

  @BeforeEach
  void setUp() {
    polarisCredentialManager = mock(PolarisCredentialManager.class);
    factory = new HiveFederatedCatalogFactory();
  }

  @ParameterizedTest
  @EnumSource(
      value = AuthenticationType.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = {"IMPLICIT"})
  void createCatalogRejectsNonImplicitAuthentication(AuthenticationType authenticationType) {
    ConnectionConfigInfoDpo connectionConfig = mock(ConnectionConfigInfoDpo.class);
    AuthenticationParametersDpo authenticationParameters = mock(AuthenticationParametersDpo.class);
    when(connectionConfig.getAuthenticationParameters()).thenReturn(authenticationParameters);
    when(authenticationParameters.getAuthenticationTypeCode())
        .thenReturn(authenticationType.getCode());

    assertThatThrownBy(
            () -> factory.createCatalog(connectionConfig, polarisCredentialManager, Map.of()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Hive federation only supports IMPLICIT authentication.");
  }

  @Test
  void createCatalogWithImplicitAuthenticationReturnsHiveCatalog() {
    HiveConnectionConfigInfoDpo connectionConfig =
        new HiveConnectionConfigInfoDpo(
            HMS_URI, new ImplicitAuthenticationParametersDpo(), WAREHOUSE, null);
    when(polarisCredentialManager.getConnectionCredentials(any(ConnectionConfigInfoDpo.class)))
        .thenReturn(new ConnectionCredentials(Map.of(), Optional.empty()));

    Catalog catalog = factory.createCatalog(connectionConfig, polarisCredentialManager, Map.of());

    assertThat(catalog).isNotNull().isInstanceOf(HiveCatalog.class);
  }

  @Test
  void createGenericCatalogThrowsUnsupportedOperationException() {
    ConnectionConfigInfoDpo connectionConfig = mock(ConnectionConfigInfoDpo.class);

    assertThatThrownBy(
            () ->
                factory.createGenericCatalog(connectionConfig, polarisCredentialManager, Map.of()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Generic table federation to this catalog is not supported.");
  }
}
