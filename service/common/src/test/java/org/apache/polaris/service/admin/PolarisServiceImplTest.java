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
package org.apache.polaris.service.admin;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.secrets.UserSecretsManagerFactory;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.config.ReservedProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PolarisServiceImplTest {

  private RealmEntityManagerFactory entityManagerFactory;
  private MetaStoreManagerFactory metaStoreManagerFactory;
  private UserSecretsManagerFactory userSecretsManagerFactory;
  private PolarisAuthorizer polarisAuthorizer;
  private CallContext callContext;
  private ReservedProperties reservedProperties;
  private RealmConfig realmConfig;

  private PolarisServiceImpl polarisService;

  @BeforeEach
  void setUp() {
    entityManagerFactory = Mockito.mock(RealmEntityManagerFactory.class);
    metaStoreManagerFactory = Mockito.mock(MetaStoreManagerFactory.class);
    userSecretsManagerFactory = Mockito.mock(UserSecretsManagerFactory.class);
    polarisAuthorizer = Mockito.mock(PolarisAuthorizer.class);
    callContext = Mockito.mock(CallContext.class);
    reservedProperties = Mockito.mock(ReservedProperties.class);
    realmConfig = Mockito.mock(RealmConfig.class);

    when(callContext.getRealmConfig()).thenReturn(realmConfig);
    when(realmConfig.getConfig(FeatureConfiguration.SUPPORTED_CATALOG_CONNECTION_TYPES))
        .thenReturn(List.of("ICEBERG_REST"));
    when(realmConfig.getConfig(
            FeatureConfiguration.SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES))
        .thenReturn(List.of("OAUTH"));

    polarisService =
        new PolarisServiceImpl(
            entityManagerFactory,
            metaStoreManagerFactory,
            userSecretsManagerFactory,
            polarisAuthorizer,
            callContext,
            reservedProperties);
  }

  @Test
  void testValidateExternalCatalog_InternalCatalog() {
    StorageConfigInfo storageConfig =
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file://tmp"))
            .build();

    PolarisCatalog internalCatalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("test-catalog")
            .setProperties(new CatalogProperties("file://tmp"))
            .setStorageConfigInfo(storageConfig)
            .build();

    assertThatCode(() -> invokeValidateExternalCatalog(polarisService, internalCatalog))
        .doesNotThrowAnyException();
  }

  @Test
  void testValidateExternalCatalog_LegacyExternalCatalog() {
    StorageConfigInfo storageConfig =
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file://tmp"))
            .build();
    ExternalCatalog externalCatalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName("test-catalog")
            .setProperties(new CatalogProperties("file://tmp"))
            .setStorageConfigInfo(storageConfig)
            .setConnectionConfigInfo(null)
            .build();

    assertThatCode(() -> invokeValidateExternalCatalog(polarisService, externalCatalog))
        .doesNotThrowAnyException();
  }

  @Test
  void testValidateExternalCatalog_SupportedExternalCatalog() {
    StorageConfigInfo storageConfig =
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file://tmp"))
            .build();

    ConnectionConfigInfo connectionConfigInfo = Mockito.mock(ConnectionConfigInfo.class);
    AuthenticationParameters authenticationParameters =
        Mockito.mock(AuthenticationParameters.class);
    when(connectionConfigInfo.getConnectionType())
        .thenReturn(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST);
    when(connectionConfigInfo.getAuthenticationParameters()).thenReturn(authenticationParameters);
    when(authenticationParameters.getAuthenticationType())
        .thenReturn(AuthenticationParameters.AuthenticationTypeEnum.OAUTH);

    ExternalCatalog externalCatalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName("test-catalog")
            .setProperties(new CatalogProperties("file://tmp"))
            .setStorageConfigInfo(storageConfig)
            .setConnectionConfigInfo(connectionConfigInfo)
            .build();

    assertThatCode(() -> invokeValidateExternalCatalog(polarisService, externalCatalog))
        .doesNotThrowAnyException();
  }

  @Test
  void testValidateExternalCatalog_UnsupportedConnectionType() {
    StorageConfigInfo storageConfig =
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file://tmp"))
            .build();

    ConnectionConfigInfo connectionConfigInfo = Mockito.mock(ConnectionConfigInfo.class);
    AuthenticationParameters authenticationParameters =
        Mockito.mock(AuthenticationParameters.class);
    when(connectionConfigInfo.getConnectionType())
        .thenReturn(ConnectionConfigInfo.ConnectionTypeEnum.HADOOP);
    when(connectionConfigInfo.getAuthenticationParameters()).thenReturn(authenticationParameters);
    when(authenticationParameters.getAuthenticationType())
        .thenReturn(AuthenticationParameters.AuthenticationTypeEnum.OAUTH);

    ExternalCatalog externalCatalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName("test-catalog")
            .setProperties(new CatalogProperties("file://tmp"))
            .setStorageConfigInfo(storageConfig)
            .setConnectionConfigInfo(connectionConfigInfo)
            .build();

    assertThatThrownBy(() -> invokeValidateExternalCatalog(polarisService, externalCatalog))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Unsupported connection type: HADOOP");
  }

  @Test
  void testValidateExternalCatalog_UnsupportedAuthenticationType() {
    StorageConfigInfo storageConfig =
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file://tmp"))
            .build();

    ConnectionConfigInfo connectionConfigInfo = Mockito.mock(ConnectionConfigInfo.class);
    AuthenticationParameters authenticationParameters =
        Mockito.mock(AuthenticationParameters.class);
    when(connectionConfigInfo.getConnectionType())
        .thenReturn(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST);
    when(connectionConfigInfo.getAuthenticationParameters()).thenReturn(authenticationParameters);
    when(authenticationParameters.getAuthenticationType())
        .thenReturn(AuthenticationParameters.AuthenticationTypeEnum.BEARER);

    ExternalCatalog externalCatalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName("test-catalog")
            .setProperties(new CatalogProperties("file://tmp"))
            .setStorageConfigInfo(storageConfig)
            .setConnectionConfigInfo(connectionConfigInfo)
            .build();

    assertThatThrownBy(() -> invokeValidateExternalCatalog(polarisService, externalCatalog))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Unsupported authentication type: BEARER");
  }

  private void invokeValidateExternalCatalog(PolarisServiceImpl service, Catalog catalog)
      throws Exception {
    Method method =
        PolarisServiceImpl.class.getDeclaredMethod("validateExternalCatalog", Catalog.class);
    method.setAccessible(true);
    try {
      method.invoke(service, catalog);
    } catch (java.lang.reflect.InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      } else {
        throw e;
      }
    }
  }
}
