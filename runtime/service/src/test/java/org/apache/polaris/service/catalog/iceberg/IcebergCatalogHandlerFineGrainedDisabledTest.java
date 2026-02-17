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
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

/**
 * Test class specifically for testing fine-grained authorization when the feature is DISABLED. This
 * ensures that fine-grained privileges are properly ignored when the feature flag is off.
 */
@QuarkusTest
@TestProfile(IcebergCatalogHandlerFineGrainedDisabledTest.Profile.class)
public class IcebergCatalogHandlerFineGrainedDisabledTest extends PolarisAuthzTestBase {

  @Inject IcebergCatalogHandlerFactory icebergCatalogHandlerFactory;

  private IcebergCatalogHandler newHandler() {
    PolarisPrincipal authenticatedPrincipal = PolarisPrincipal.of(principalEntity, Set.of());
    return icebergCatalogHandlerFactory.createHandler(CATALOG_NAME, authenticatedPrincipal);
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

  @TestFactory
  Stream<DynamicNode> testUpdateTableFineGrainedPrivilegesIgnoredWhenFeatureDisabled() {
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
    return authzTestsBuilder("updateTable")
        .action(() -> newHandler().updateTable(TABLE_NS1A_2, request))
        .shouldFailWith(
            PolarisPrivilege
                .TABLE_ASSIGN_UUID) // This alone should be insufficient when feature disabled
        .shouldFailWith(PolarisPrivilege.TABLE_UPGRADE_FORMAT_VERSION)
        .shouldFailWith(PolarisPrivilege.TABLE_SET_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.TABLE_REMOVE_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.TABLE_ADD_SCHEMA)
        .shouldFailWith(PolarisPrivilege.TABLE_SET_LOCATION)
        .shouldFailWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldFailWith(PolarisPrivilege.TABLE_CREATE)
        .shouldFailWith(PolarisPrivilege.TABLE_LIST)
        .shouldFailWith(PolarisPrivilege.TABLE_DROP)
        .createTests();
  }
}
