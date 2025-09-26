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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test class for fine-grained authorization features in IcebergCatalogHandler.
 * This class extends the base authorization test and enables the fine-grained 
 * update table privileges feature flag to test the new authorization behavior.
 * 
 * This class runs:
 * - All tests from the base class (validating they still work with the feature enabled)
 * - Additional fine-grained authorization tests specific to the enabled feature
 * - Disables the "feature disabled" test since it's not applicable here
 */
@QuarkusTest
@TestProfile(IcebergCatalogHandlerFineGrainedAuthzTest.Profile.class)
public class IcebergCatalogHandlerFineGrainedAuthzTest extends IcebergCatalogHandlerAuthzTest {

  public static class Profile extends PolarisAuthzTestBase.Profile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("polaris.features.\"ENABLE_FINE_GRAINED_UPDATE_TABLE_PRIVILEGES\"", "true")
          .build();
    }
  }

  @Test
  public void testUpdateTableWithFineGrainedPrivileges_AssignUuid() {
    // Test that TABLE_ASSIGN_UUID privilege is required for AssignUUID MetadataUpdate
    UpdateTableRequest request = UpdateTableRequest.create(
        TABLE_NS1A_2,
        List.of(), // no requirements
        List.of(new MetadataUpdate.AssignUUID(UUID.randomUUID().toString())));

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_ASSIGN_UUID,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES, // Should also work with broader privilege
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().updateTable(TABLE_NS1A_2, request),
        null /* cleanupAction */);
  }

  @Test
  public void testUpdateTableWithFineGrainedPrivileges_AssignUuidInsufficientPermissions() {
    UpdateTableRequest request = UpdateTableRequest.create(
        TABLE_NS1A_2,
        List.of(), // no requirements  
        List.of(new MetadataUpdate.AssignUUID(UUID.randomUUID().toString())));

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_LIST,
            PolarisPrivilege.TABLE_DROP,
            // Test that other fine-grained privileges don't work
            PolarisPrivilege.TABLE_ADD_SCHEMA,
            PolarisPrivilege.TABLE_SET_LOCATION),
        () -> newWrapper().updateTable(TABLE_NS1A_2, request));
  }

  @Test
  public void testUpdateTableWithFineGrainedPrivileges_UpgradeFormatVersion() {
    // Test that TABLE_UPGRADE_FORMAT_VERSION privilege is required for UpgradeFormatVersion MetadataUpdate
    UpdateTableRequest request = UpdateTableRequest.create(
        TABLE_NS1A_2,
        List.of(), // no requirements
        List.of(new MetadataUpdate.UpgradeFormatVersion(2)));

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_UPGRADE_FORMAT_VERSION,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES, // Should also work with broader privilege
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().updateTable(TABLE_NS1A_2, request),
        null /* cleanupAction */);
  }

  @Test
  public void testUpdateTableWithFineGrainedPrivileges_SetProperties() {
    // Test that TABLE_SET_PROPERTIES privilege is required for SetProperties MetadataUpdate
    UpdateTableRequest request = UpdateTableRequest.create(
        TABLE_NS1A_2,
        List.of(), // no requirements
        List.of(new MetadataUpdate.SetProperties(Map.of("test.property", "test.value"))));

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_SET_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES, // Should also work with broader privilege
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().updateTable(TABLE_NS1A_2, request),
        null /* cleanupAction */);
  }

  @Test
  public void testUpdateTableWithFineGrainedPrivileges_RemoveProperties() {
    // Test that TABLE_REMOVE_PROPERTIES privilege is required for RemoveProperties MetadataUpdate
    UpdateTableRequest request = UpdateTableRequest.create(
        TABLE_NS1A_2,
        List.of(), // no requirements
        List.of(new MetadataUpdate.RemoveProperties(Set.of("property.to.remove"))));

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_REMOVE_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES, // Should also work with broader privilege
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().updateTable(TABLE_NS1A_2, request),
        null /* cleanupAction */);
  }

  @Test
  public void testUpdateTableWithFineGrainedPrivileges_MultipleUpdates() {
    // Test that multiple MetadataUpdate types require multiple specific privileges
    UpdateTableRequest request = UpdateTableRequest.create(
        TABLE_NS1A_2,
        List.of(), // no requirements
        List.of(
            new MetadataUpdate.UpgradeFormatVersion(2),
            new MetadataUpdate.SetProperties(Map.of("test.prop", "test.val"))));

    // Test that having both specific privileges works
    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.TABLE_UPGRADE_FORMAT_VERSION, PolarisPrivilege.TABLE_SET_PROPERTIES),
            Set.of(PolarisPrivilege.TABLE_WRITE_PROPERTIES), // Broader privilege should work
            Set.of(PolarisPrivilege.TABLE_FULL_METADATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> newWrapper().updateTable(TABLE_NS1A_2, request),
        null /* cleanupAction */,
        PRINCIPAL_NAME,
        CATALOG_NAME);
  }

  @Test
  public void testUpdateTableWithFineGrainedPrivileges_MultipleUpdatesInsufficientPermissions() {
    // Test that having only one of the required privileges fails
    UpdateTableRequest request = UpdateTableRequest.create(
        TABLE_NS1A_2,
        List.of(), // no requirements
        List.of(
            new MetadataUpdate.UpgradeFormatVersion(2),
            new MetadataUpdate.SetProperties(Map.of("test.prop", "test.val"))));

    // Test that having only one specific privilege fails (need both)
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_UPGRADE_FORMAT_VERSION, // Only one of the two needed
            PolarisPrivilege.TABLE_SET_PROPERTIES, // Only one of the two needed
            PolarisPrivilege.TABLE_ASSIGN_UUID, // Wrong privilege
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_CREATE),
        () -> newWrapper().updateTable(TABLE_NS1A_2, request));
  }

  /**
   * Override the "feature disabled" test from the parent class since it's not applicable
   * when the fine-grained authorization feature is enabled in this test class.
   */
  @Override
  @Disabled("This test is only applicable when fine-grained authorization is disabled")
  public void testUpdateTableFineGrainedPrivilegesIgnoredWhenFeatureDisabled() {
    // This test is disabled in this class since the feature is enabled here
  }
}
