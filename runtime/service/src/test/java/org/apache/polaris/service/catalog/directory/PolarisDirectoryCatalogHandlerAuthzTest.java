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
package org.apache.polaris.service.catalog.directory;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.catalog.FederatedCatalogFactory;
import org.apache.polaris.core.catalog.LocalCatalogFactory;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.catalog.spi.DirectoryScanService;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

@QuarkusTest
@TestProfile(Profiles.PolarisAuthzBaseProfile.class)
public class PolarisDirectoryCatalogHandlerAuthzTest extends PolarisAuthzTestBase {
  private static final TableIdentifier DIRECTORY = TableIdentifier.of(NS1, "directory");
  private static final String DIRECTORY_LOCATION = "file:///tmp/authz/ns1/directory_data/";

  @Inject PolarisCredentialManager credentialManager;
  @Inject @Any Instance<FederatedCatalogFactory> federatedCatalogFactories;
  @Inject LocalCatalogFactory localCatalogFactory;
  @Inject DirectoryScanService directoryScanService;

  private DirectoryCatalogHandler newWrapper() {
    return newWrapper(
        PolarisPrincipal.of(
            principalEntity.getName(),
            ImmutableAttributeMap.builder()
                .put(PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, principalEntity)
                .put(PolarisPrincipalAttributes.PRINCIPAL_ROLE_ALL_ATTRIBUTE_KEY, true)
                .build(),
            Set.of()));
  }

  private DirectoryCatalogHandler newWrapper(Set<String> activatedPrincipalRoles) {
    return newWrapper(
        PolarisPrincipal.of(
            principalEntity.getName(),
            ImmutableAttributeMap.builder()
                .put(PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, principalEntity)
                .build(),
            activatedPrincipalRoles));
  }

  private DirectoryCatalogHandler newWrapper(PolarisPrincipal authenticatedPrincipal) {
    return ImmutableDirectoryCatalogHandler.builder()
        .catalogName(PolarisAuthzTestBase.CATALOG_NAME)
        .polarisPrincipal(authenticatedPrincipal)
        .callContext(callContext)
        .resolutionManifestFactory(resolutionManifestFactory)
        .metaStoreManager(metaStoreManager)
        .authorizer(polarisAuthorizer)
        .credentialManager(credentialManager)
        .federatedCatalogFactories(federatedCatalogFactories)
        .localCatalogFactory(localCatalogFactory)
        .storageAccessConfigProvider(storageAccessConfigProvider)
        .fileIOFactory(fileIOFactory)
        .directoryScanService(directoryScanService)
        .build();
  }

  private void createDirectory() {
    newWrapper(Set.of(PRINCIPAL_ROLE2))
        .createDirectory(DIRECTORY, DIRECTORY_LOCATION, null, null, null, Map.of());
  }

  @TestFactory
  Stream<DynamicNode> testListDirectoriesPrivileges() {
    return authzTestsBuilder("listDirectories")
        .action(() -> newWrapper().listDirectories(NS1, null, null))
        .shouldPassWith(PolarisPrivilege.TABLE_LIST)
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE)
        .shouldPassWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testCreateDirectoryPrivileges() {
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP));
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_WRITE_DATA));

    return authzTestsBuilder("createDirectory")
        .action(
            () ->
                newWrapper(Set.of(PRINCIPAL_ROLE1))
                    .createDirectory(DIRECTORY, DIRECTORY_LOCATION, null, null, null, Map.of()))
        .cleanupAction(() -> newWrapper(Set.of(PRINCIPAL_ROLE2)).dropDirectory(DIRECTORY))
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testLoadDirectoryPrivileges() {
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_CREATE));
    createDirectory();

    return authzTestsBuilder("loadDirectory")
        .action(() -> newWrapper().loadDirectory(DIRECTORY))
        .shouldPassWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDropDirectoryPrivileges() {
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_CREATE));
    createDirectory();

    return authzTestsBuilder("dropDirectory")
        .action(() -> newWrapper(Set.of(PRINCIPAL_ROLE1)).dropDirectory(DIRECTORY))
        .cleanupAction(this::createDirectory)
        .shouldPassWith(PolarisPrivilege.TABLE_DROP)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .createTests();
  }
}
