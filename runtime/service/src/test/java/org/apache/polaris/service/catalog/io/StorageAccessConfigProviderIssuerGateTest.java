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
package org.apache.polaris.service.catalog.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.junit.jupiter.api.Test;

/**
 * Spec 5.3 item 3: the issuer check runs before the {@code SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION}
 * early return, so an early-opt-in CLOUDFLARE_R2 catalog fails here before any FileIO, on the
 * loadTable path and on the task path ({@code TaskFileIOSupplier} calls this first).
 */
class StorageAccessConfigProviderIssuerGateTest {

  private static final RealmContext REALM = () -> "test-realm";
  private static final String R2_ENDPOINT =
      "https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com";

  private final PolarisStorageIntegrationProvider integrationProvider =
      mock(PolarisStorageIntegrationProvider.class);

  private static RealmConfig realmConfig(boolean skipSubscoping, List<String> issuers) {
    Map<String, Object> config =
        Map.of(
            "SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION", skipSubscoping,
            "SUPPORTED_S3_CREDENTIAL_ISSUERS", issuers);
    return new RealmConfigImpl((rc, name) -> config.get(name), REALM);
  }

  private StorageAccessConfigProvider provider(RealmConfig realmConfig) {
    CallContext callContext = mock(CallContext.class);
    when(callContext.getRealmConfig()).thenReturn(realmConfig);
    return new StorageAccessConfigProvider(
        callContext, mock(PolarisPrincipal.class), REALM, integrationProvider);
  }

  private static PolarisResolvedPathWrapper pathTo(
      RealmConfig realmConfig, AwsStorageConfigInfo model) {
    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("c")
            .addProperty(CatalogEntity.DEFAULT_BASE_LOCATION_KEY, "s3://bucket/base/")
            .setStorageConfigurationInfo(realmConfig, model)
            .build();
    return new PolarisResolvedPathWrapper(
        List.of(new ResolvedPolarisEntity(catalog, List.of(), List.of())));
  }

  private static AwsStorageConfigInfo r2() {
    return AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
        .setCredentialIssuer(AwsStorageConfigInfo.CredentialIssuerEnum.CLOUDFLARE_R2)
        .setEndpoint(R2_ENDPOINT)
        .setPathStyleAccess(true)
        .setRegion("auto")
        .setAllowedLocations(List.of("s3://bucket/base/"))
        .build();
  }

  private static AwsStorageConfigInfo sts() {
    return AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
        .setRoleArn("arn:aws:iam::123456789012:role/r")
        .setAllowedLocations(List.of("s3://bucket/base/"))
        .build();
  }

  private static StorageAccessConfig call(
      StorageAccessConfigProvider p, PolarisResolvedPathWrapper path) {
    return p.getStorageAccessConfig(
        TableIdentifier.of("ns", "t"),
        Set.of("s3://bucket/base/ns/t/"),
        Set.of(PolarisStorageActions.READ),
        Optional.empty(),
        path);
  }

  @Test
  void earlyOptInPlusSkipSubscopingFailsBeforeTheEarlyReturn() {
    RealmConfig rc = realmConfig(true, List.of("STS", "CLOUDFLARE_R2"));
    assertThatThrownBy(() -> call(provider(rc), pathTo(rc, r2())))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 credential issuer CLOUDFLARE_R2 is not available in this build");
    verifyNoInteractions(integrationProvider);
  }

  @Test
  void disabledIssuerPlusSkipSubscopingFailsBeforeTheEarlyReturn() {
    RealmConfig rc = realmConfig(true, List.of("STS"));
    assertThatThrownBy(() -> call(provider(rc), pathTo(rc, r2())))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 credential issuer CLOUDFLARE_R2 is not enabled in this realm");
    verifyNoInteractions(integrationProvider);
  }

  @Test
  void stsWithSkipSubscopingBehavesAsUpstream() {
    RealmConfig rc = realmConfig(true, List.of("STS"));
    StorageAccessConfig config = call(provider(rc), pathTo(rc, sts()));
    assertThat(config.supportsCredentialVending()).isFalse();
    assertThat(config.credentials()).isEmpty();
    verifyNoInteractions(integrationProvider);
  }
}
