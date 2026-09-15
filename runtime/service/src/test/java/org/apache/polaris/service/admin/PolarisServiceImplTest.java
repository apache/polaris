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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.service.config.ReservedProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PolarisServiceImplTest {

  private ResolutionManifestFactory resolutionManifestFactory;
  private PolarisMetaStoreManager metaStoreManager;
  private UserSecretsManager userSecretsManager;
  private ServiceIdentityProvider serviceIdentityProvider;
  private PolarisAuthorizer polarisAuthorizer;
  private CallContext callContext;
  private ReservedProperties reservedProperties;
  private RealmConfig realmConfig;

  private PolarisAdminService adminService;
  private PolarisServiceImpl polarisService;

  @BeforeEach
  void setUp() {
    resolutionManifestFactory = Mockito.mock(ResolutionManifestFactory.class);
    metaStoreManager = Mockito.mock(PolarisMetaStoreManager.class);
    userSecretsManager = Mockito.mock(UserSecretsManager.class);
    serviceIdentityProvider = Mockito.mock(ServiceIdentityProvider.class);
    polarisAuthorizer = Mockito.mock(PolarisAuthorizer.class);
    callContext = Mockito.mock(CallContext.class);
    reservedProperties = Mockito.mock(ReservedProperties.class);
    realmConfig = Mockito.mock(RealmConfig.class);
    PolarisPrincipal principal = Mockito.mock(PolarisPrincipal.class);

    when(callContext.getRealmConfig()).thenReturn(realmConfig);
    when(realmConfig.getConfig(FeatureConfiguration.SUPPORTED_CATALOG_CONNECTION_TYPES))
        .thenReturn(List.of("ICEBERG_REST"));
    when(realmConfig.getConfig(
            FeatureConfiguration.SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES))
        .thenReturn(List.of("OAUTH"));

    adminService =
        new PolarisAdminService(
            callContext,
            resolutionManifestFactory,
            metaStoreManager,
            userSecretsManager,
            serviceIdentityProvider,
            principal,
            polarisAuthorizer,
            reservedProperties);
    polarisService =
        new PolarisServiceImpl(
            realmConfig, reservedProperties, adminService, serviceIdentityProvider);
  }

  /**
   * Drives the real resource method: a denied caller gets the 403 before the realm's allowlist is
   * read. A check re-added in front of {@code adminService.createCatalog} fails this.
   */
  @Test
  void deniedCreateCatalogThroughTheResourceNeverConsultsTheAllowlist() {
    PolarisResolutionManifest manifest = Mockito.mock(PolarisResolutionManifest.class);
    when(resolutionManifestFactory.createResolutionManifest(any(), any())).thenReturn(manifest);
    when(polarisAuthorizer.authorize(any(), any()))
        .thenReturn(AuthorizationDecision.deny("denied"));
    when(realmConfig.getConfig(FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES))
        .thenReturn(List.of("S3"));
    when(realmConfig.getConfig(FeatureConfiguration.ALLOW_SETTING_S3_ENDPOINTS)).thenReturn(true);
    when(realmConfig.getConfig(
            FeatureConfiguration.ALLOW_SETTING_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS))
        .thenReturn(true);

    AwsStorageConfigInfo r2 =
        AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
            .setCredentialVendingMechanism("CLOUDFLARE_R2")
            .setEndpoint("https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com")
            .setPathStyleAccess(true)
            .setRegion("auto")
            .setAllowedLocations(List.of("s3://bucket/base/"))
            .build();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName("r2")
            .setProperties(new CatalogProperties("s3://bucket/base/"))
            .setStorageConfigInfo(r2)
            .build();

    assertThatThrownBy(
            () -> polarisService.createCatalog(new CreateCatalogRequest(catalog), null, null))
        .isInstanceOf(ForbiddenException.class)
        .hasMessage("denied");
    verify(realmConfig, never())
        .getConfig(FeatureConfiguration.SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS);
    verify(metaStoreManager, never()).createCatalog(any(), any(), any());
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
      if (cause instanceof Exception exception) {
        throw exception;
      } else {
        throw e;
      }
    }
  }
}
