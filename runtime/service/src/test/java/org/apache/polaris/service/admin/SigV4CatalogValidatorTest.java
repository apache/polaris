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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.OAuthClientCredentialsParameters;
import org.apache.polaris.core.admin.model.SigV4AuthenticationParameters;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.junit.jupiter.api.Test;

class SigV4CatalogValidatorTest {

  @Test
  void constructionDoesNotReadRealmConfiguration() {
    RealmConfig contextualConfig = mock(RealmConfig.class);

    new CatalogConfigValidators(List.of(new SigV4CatalogValidator(contextualConfig)));

    verifyNoInteractions(contextualConfig);
  }

  @Test
  void readsRealmConfigurationOnEveryValidation() {
    RealmConfig contextualConfig = mock(RealmConfig.class);
    when(contextualConfig.getConfig(FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME))
        .thenReturn(true, false);
    CatalogConfigValidators validators =
        new CatalogConfigValidators(List.of(new SigV4CatalogValidator(contextualConfig)));
    Catalog catalog = sigV4Catalog(storageNameAuth("sigv4-catalog"));

    assertThatCode(() -> validators.validate(catalog)).doesNotThrowAnyException();
    assertThatThrownBy(() -> validators.validate(catalog))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key());
  }

  @Test
  void storageNameRequiresResolveByStorageName() {
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME))
        .thenReturn(false);
    SigV4CatalogValidator validator = new SigV4CatalogValidator(realmConfig);

    assertThatThrownBy(() -> validator.validate(sigV4Catalog(storageNameAuth("sigv4-catalog"))))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key());
  }

  @Test
  void storageNamePassesWhenResolveByStorageNameIsEnabled() {
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME))
        .thenReturn(true);
    SigV4CatalogValidator validator = new SigV4CatalogValidator(realmConfig);

    assertThatCode(() -> validator.validate(sigV4Catalog(storageNameAuth("sigv4-catalog"))))
        .doesNotThrowAnyException();
  }

  @Test
  void roleArnDoesNotReadRealmConfiguration() {
    RealmConfig realmConfig = mock(RealmConfig.class);
    SigV4CatalogValidator validator = new SigV4CatalogValidator(realmConfig);

    assertThatCode(() -> validator.validate(sigV4Catalog(roleArnAuth())))
        .doesNotThrowAnyException();
    verifyNoInteractions(realmConfig);
  }

  @Test
  void skipsCatalogsWithoutSigV4StorageName() {
    RealmConfig realmConfig = mock(RealmConfig.class);
    SigV4CatalogValidator validator = new SigV4CatalogValidator(realmConfig);

    assertThatCode(() -> validator.validate(mock(Catalog.class))).doesNotThrowAnyException();
    assertThatCode(() -> validator.validate(oauthCatalog())).doesNotThrowAnyException();
    verifyNoInteractions(realmConfig);
  }

  private static Catalog sigV4Catalog(SigV4AuthenticationParameters auth) {
    return ExternalCatalog.builder()
        .setType(Catalog.TypeEnum.EXTERNAL)
        .setName("external-catalog")
        .setProperties(CatalogProperties.builder("s3://bucket/path/to/data").build())
        .setConnectionConfigInfo(
            IcebergRestConnectionConfigInfo.builder(
                    ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
                .setUri("https://example.com/polaris/api/catalog")
                .setAuthenticationParameters(auth)
                .build())
        .build();
  }

  private static Catalog oauthCatalog() {
    return ExternalCatalog.builder()
        .setType(Catalog.TypeEnum.EXTERNAL)
        .setName("external-catalog")
        .setProperties(CatalogProperties.builder("s3://bucket/path/to/data").build())
        .setConnectionConfigInfo(
            IcebergRestConnectionConfigInfo.builder(
                    ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
                .setUri("https://example.com/polaris/api/catalog")
                .setAuthenticationParameters(
                    OAuthClientCredentialsParameters.builder(
                            AuthenticationParameters.AuthenticationTypeEnum.OAUTH)
                        .setClientId("client-id")
                        .setClientSecret("client-secret")
                        .setScopes(List.of("PRINCIPAL_ROLE:ALL"))
                        .build())
                .build())
        .build();
  }

  private static SigV4AuthenticationParameters storageNameAuth(String storageName) {
    return SigV4AuthenticationParameters.builder()
        .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.SIGV4)
        .setStorageName(storageName)
        .setSigningRegion("us-east-1")
        .build();
  }

  private static SigV4AuthenticationParameters roleArnAuth() {
    return SigV4AuthenticationParameters.builder()
        .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.SIGV4)
        .setRoleArn("arn:aws:iam::123456789012:role/customer-role")
        .setSigningRegion("us-east-1")
        .build();
  }
}
