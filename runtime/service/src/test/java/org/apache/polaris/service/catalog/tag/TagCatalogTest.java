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
package org.apache.polaris.service.catalog.tag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.tag.TagEntity;
import org.apache.polaris.core.tag.exceptions.NoSuchTagException;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.admin.PolarisAdminServiceTestSupport;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

/**
 * TagCatalog-level tests for behavior that needs direct metastore access or a manager double: the
 * durable target-types representation, corrupt-stored-data handling, and the compare-and-swap
 * conflict mapping.
 */
@QuarkusTest
@TestProfile(Profiles.DefaultProfile.class)
public class TagCatalogTest {

  private static final String CATALOG_NAME = "tag-test-catalog";
  private static final String TAG1 = "t1";

  @Inject ServiceIdentityProvider serviceIdentityProvider;
  @Inject StorageCredentialCache storageCredentialCache;
  @Inject ResolutionManifestFactory resolutionManifestFactory;
  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject UserSecretsManager userSecretsManager;
  @Inject CallContext callContext;
  @Inject RealmConfig realmConfig;
  @Inject PolarisPrincipalHolder polarisPrincipalHolder;

  private PolarisCallContext polarisContext;
  private PolarisPrincipal authenticatedRoot;
  private PolarisEntity catalogEntity;
  private TagCatalog tagCatalog;

  @BeforeAll
  public static void setUpMocks() {
    PolarisStorageIntegrationProviderImpl mock =
        Mockito.mock(PolarisStorageIntegrationProviderImpl.class);
    QuarkusMock.installMockForType(mock, PolarisStorageIntegrationProviderImpl.class);
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    storageCredentialCache.invalidateAll();

    String realmName =
        "realm_%s_%s"
            .formatted(
                testInfo.getTestMethod().map(Method::getName).orElse("test"), System.nanoTime());
    RealmContext realmContext = () -> realmName;
    QuarkusMock.installMockForType(realmContext, RealmContext.class);
    polarisContext = callContext.getPolarisCallContext();

    PrincipalEntity rootPrincipal =
        metaStoreManager.findRootPrincipal(polarisContext).orElseThrow();
    authenticatedRoot =
        PolarisPrincipal.of(
            rootPrincipal.getName(),
            Map.of(
                PolarisPrincipal.PRINCIPAL_ENTITY_ATTRIBUTE_KEY,
                rootPrincipal,
                PolarisPrincipal.PRINCIPAL_ROLE_ALL_ATTRIBUTE_KEY,
                true),
            Set.of());
    polarisPrincipalHolder.set(authenticatedRoot);

    PolarisAuthorizer authorizer = new PolarisAuthorizerImpl(realmConfig);
    PolarisAdminService adminService =
        PolarisAdminServiceTestSupport.newAdminService(
            polarisContext,
            resolutionManifestFactory,
            metaStoreManager,
            userSecretsManager,
            serviceIdentityProvider,
            authenticatedRoot,
            authorizer,
            ReservedProperties.NONE);

    String storageLocation = "s3://my-bucket/path/to/data";
    AwsStorageConfigInfo storageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(storageLocation))
            .build();
    catalogEntity =
        adminService.createCatalog(
            new CreateCatalogRequest(
                new CatalogEntity.Builder()
                    .setName(CATALOG_NAME)
                    .setDefaultBaseLocation(storageLocation)
                    .setStorageConfigurationInfo(realmConfig, storageConfigModel)
                    .build()
                    .asCatalog(serviceIdentityProvider)));

    tagCatalog = new TagCatalog(metaStoreManager, polarisContext, newPassthroughView());
  }

  private PolarisPassthroughResolutionView newPassthroughView() {
    return new PolarisPassthroughResolutionView(
        resolutionManifestFactory, authenticatedRoot, CATALOG_NAME);
  }

  private TagEntity loadStoredTag(String tagName) {
    EntityResult result =
        metaStoreManager.readEntityByName(
            polarisContext,
            List.of(catalogEntity),
            PolarisEntityType.TAG,
            PolarisEntitySubType.NULL_SUBTYPE,
            tagName);
    assertThat(result.isSuccess()).isTrue();
    return TagEntity.of(result.getEntity());
  }

  @Test
  public void testStoredTargetTypesUseWireVocabulary() {
    tagCatalog.createTag(
        TAG1, "comment", List.of("a"), List.of(TargetType.CATALOG, TargetType.TABLE_LIKE));

    // The entity stores the wire strings, not Java constant names.
    assertThat(loadStoredTag(TAG1).getTargetTypes()).containsExactly("catalog", "table-like");
    // And the read side round-trips them back to the enum.
    assertThat(tagCatalog.loadTag(TAG1).getTargetTypes())
        .containsExactlyInAnyOrder(TargetType.CATALOG, TargetType.TABLE_LIKE);
  }

  @Test
  public void testCorruptStoredTargetTypeIsServerSideError() {
    tagCatalog.createTag(TAG1, "comment", List.of("a"), List.of(TargetType.CATALOG));

    // Corrupt the stored representation directly (a Java constant name is not a wire value).
    TagEntity stored = loadStoredTag(TAG1);
    TagEntity corrupted = new TagEntity.Builder(stored).setTargetTypes(List.of("CATALOG")).build();
    assertThat(
            metaStoreManager
                .updateEntityPropertiesIfNotChanged(
                    polarisContext, List.of(catalogEntity), corrupted)
                .getEntity())
        .isNotNull();

    // The read must fail as a server-side condition, not as a client-fault mapping.
    assertThatThrownBy(() -> tagCatalog.loadTag(TAG1))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("target-types");
  }

  @Test
  public void testUpdateTagLosingConcurrentUpdateIsRetryableConflict() {
    tagCatalog.createTag(TAG1, "comment", List.of("a"), List.of(TargetType.CATALOG));

    // Simulate another writer winning the compare-and-swap on the tag entity.
    PolarisMetaStoreManager concurrentlyModified = Mockito.spy(metaStoreManager);
    Mockito.doReturn(
            new EntityResult(
                BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, "simulated"))
        .when(concurrentlyModified)
        .updateEntityPropertiesIfNotChanged(Mockito.any(), Mockito.any(), Mockito.any());
    TagCatalog catalog = new TagCatalog(concurrentlyModified, polarisContext, newPassthroughView());

    assertThatThrownBy(
            () ->
                catalog.updateTag(
                    TAG1,
                    UpdateTagRequest.builder()
                        .setComment("changed")
                        .setCurrentTagVersion(0)
                        .build()))
        .isInstanceOf(CommitConflictException.class);
  }

  @ParameterizedTest
  @EnumSource(
      value = BaseResult.ReturnStatus.class,
      names = {
        "TARGET_ENTITY_CONCURRENTLY_MODIFIED",
        "ENTITY_NOT_FOUND",
        "ENTITY_CANNOT_BE_RESOLVED",
        "CATALOG_PATH_CANNOT_BE_RESOLVED"
      })
  public void testRenameTagLosingConcurrentUpdateIsRetryableConflict(
      BaseResult.ReturnStatus conflictStatus) {
    tagCatalog.createTag(TAG1, "comment", List.of("a"), List.of(TargetType.CATALOG));

    // Simulate the rename losing against a concurrent modification or removal of the tag after
    // it resolved and its version matched.
    PolarisMetaStoreManager concurrentlyModified = Mockito.spy(metaStoreManager);
    Mockito.doReturn(new EntityResult(conflictStatus, "simulated"))
        .when(concurrentlyModified)
        .renameEntity(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    TagCatalog catalog = new TagCatalog(concurrentlyModified, polarisContext, newPassthroughView());

    assertThatThrownBy(
            () ->
                catalog.updateTag(
                    TAG1,
                    UpdateTagRequest.builder().setName("renamed").setCurrentTagVersion(0).build()))
        .isInstanceOf(CommitConflictException.class);
  }

  @ParameterizedTest
  @EnumSource(
      value = BaseResult.ReturnStatus.class,
      names = {"ENTITY_NOT_FOUND", "CATALOG_PATH_CANNOT_BE_RESOLVED"})
  public void testDropTagConcurrentlyRemovedIsNotFound(BaseResult.ReturnStatus missingStatus) {
    tagCatalog.createTag(TAG1, "comment", List.of("a"), List.of(TargetType.CATALOG));

    // Simulate a concurrent request removing the tag (or its catalog path) between this
    // request's resolution and the delete.
    PolarisMetaStoreManager concurrentlyRemoved = Mockito.spy(metaStoreManager);
    Mockito.doReturn(new DropEntityResult(missingStatus, "simulated"))
        .when(concurrentlyRemoved)
        .dropEntityIfExists(
            Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    TagCatalog catalog = new TagCatalog(concurrentlyRemoved, polarisContext, newPassthroughView());

    assertThatThrownBy(() -> catalog.dropTag(TAG1)).isInstanceOf(NoSuchTagException.class);
  }
}
