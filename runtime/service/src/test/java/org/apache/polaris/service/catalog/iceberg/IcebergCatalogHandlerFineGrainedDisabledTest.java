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
package org.apache.polaris.service.catalog.iceberg;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test class specifically for testing fine-grained authorization when the feature is DISABLED. This
 * ensures that fine-grained privileges are properly ignored when the feature flag is off.
 */
@QuarkusTest
@TestProfile(IcebergCatalogHandlerFineGrainedDisabledTest.Profile.class)
public class IcebergCatalogHandlerFineGrainedDisabledTest extends PolarisAuthzTestBase {

  @jakarta.inject.Inject CallContextCatalogFactory callContextCatalogFactory;

  @SuppressWarnings("unchecked")
  private static Instance<org.apache.polaris.core.catalog.ExternalCatalogFactory>
      emptyExternalCatalogFactory() {
    Instance<org.apache.polaris.core.catalog.ExternalCatalogFactory> mock =
        Mockito.mock(Instance.class);
    Mockito.when(mock.select(Mockito.any())).thenReturn(mock);
    Mockito.when(mock.isUnsatisfied()).thenReturn(true);
    return mock;
  }

  private IcebergCatalogHandler newWrapper() {
    PolarisPrincipal authenticatedPrincipal = PolarisPrincipal.of(principalEntity, Set.of());
    return new IcebergCatalogHandler(
        diagServices,
        callContext,
        resolutionManifestFactory,
        metaStoreManager,
        credentialManager,
        securityContext(authenticatedPrincipal),
        callContextCatalogFactory,
        CATALOG_NAME,
        polarisAuthorizer,
        reservedProperties,
        catalogHandlerUtils,
        emptyExternalCatalogFactory(),
        polarisEventListener,
        storageAccessConfigProvider);
  }

  public static class Profile extends PolarisAuthzTestBase.Profile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("polaris.features.\"ENABLE_FINE_GRAINED_UPDATE_TABLE_PRIVILEGES\"", "false")
          .build();
    }
  }

  @Test
  public void testUpdateTableFineGrainedPrivilegesIgnoredWhenFeatureDisabled() {
    // Test that when fine-grained authorization is disabled, fine-grained privileges alone are
    // insufficient
    // This ensures the feature flag properly controls behavior and fine-grained privileges don't
    // "leak through"
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(new MetadataUpdate.AssignUUID(UUID.randomUUID().toString())));

    // With fine-grained authorization disabled, even having the specific fine-grained privilege
    // should be insufficient - the system should require the broader privileges
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege
                .TABLE_ASSIGN_UUID, // This alone should be insufficient when feature disabled
            PolarisPrivilege.TABLE_UPGRADE_FORMAT_VERSION,
            PolarisPrivilege.TABLE_SET_PROPERTIES,
            PolarisPrivilege.TABLE_REMOVE_PROPERTIES,
            PolarisPrivilege.TABLE_ADD_SCHEMA,
            PolarisPrivilege.TABLE_SET_LOCATION,
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_LIST,
            PolarisPrivilege.TABLE_DROP),
        PRINCIPAL_NAME,
        () -> newWrapper().updateTable(TABLE_NS1A_2, request),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }
}
