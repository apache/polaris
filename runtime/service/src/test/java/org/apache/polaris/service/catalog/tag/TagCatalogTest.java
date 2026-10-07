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
import java.util.Set;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
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
import org.apache.polaris.core.tag.exceptions.TagVersionMismatchException;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.admin.PolarisAdminServiceTestSupport;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.apache.polaris.service.idempotency.IdempotencyRequestContext;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.types.Tag;
import org.apache.polaris.service.types.TargetType;
import org.apache.polaris.service.types.UpdateTagRequest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
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
  private static final String TAG2 = "t2";

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
            ImmutableAttributeMap.builder()
                .put(PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, rootPrincipal)
                .put(PolarisPrincipalAttributes.PRINCIPAL_ROLE_ALL_ATTRIBUTE_KEY, true)
                .build(),
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

    tagCatalog =
        new TagCatalog(
            metaStoreManager,
            polarisContext,
            newPassthroughView(),
            IdempotencyRequestContext.DISABLED);
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
        TAG1, "description", List.of("a"), List.of(TargetType.CATALOG, TargetType.VIEW));

    // The entity stores the wire strings.
    assertThat(loadStoredTag(TAG1).getTargetTypes()).containsExactly("CATALOG", "VIEW");
    // And the read side round-trips them back to the enum.
    assertThat(tagCatalog.loadTag(TAG1).getTargetTypes())
        .containsExactlyInAnyOrder(TargetType.CATALOG, TargetType.VIEW);
  }

  @Test
  public void testCorruptStoredTargetTypeIsServerSideError() {
    tagCatalog.createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG));

    // Corrupt the stored representation directly. "table-like" is the retired spelling of a target
    // kind: it is not a value of the current enum, so it can only be stale or corrupt data. Every
    // spelling the enum accepts is a valid wire value now, so a case variant would no longer
    // corrupt.
    TagEntity stored = loadStoredTag(TAG1);
    TagEntity corrupted =
        new TagEntity.Builder(stored).setTargetTypes(List.of("table-like")).build();
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
    tagCatalog.createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG));

    // Simulate another writer winning the compare-and-swap on the tag entity.
    PolarisMetaStoreManager concurrentlyModified = Mockito.spy(metaStoreManager);
    Mockito.doReturn(
            new EntityResult(
                BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, "simulated"))
        .when(concurrentlyModified)
        .updateEntityPropertiesIfNotChanged(Mockito.any(), Mockito.any(), Mockito.any());
    TagCatalog catalog =
        new TagCatalog(
            concurrentlyModified,
            polarisContext,
            newPassthroughView(),
            IdempotencyRequestContext.DISABLED);

    assertThatThrownBy(
            () ->
                catalog.updateTag(
                    TAG1,
                    UpdateTagRequest.builder()
                        .setDescription("changed")
                        .setValues(List.of("a"))
                        .setCurrentTagVersion(currentVersion(TAG1))
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
    tagCatalog.createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG));

    // Simulate the rename losing against a concurrent modification or removal of the tag after
    // it resolved and its version matched.
    PolarisMetaStoreManager concurrentlyModified = Mockito.spy(metaStoreManager);
    Mockito.doReturn(new EntityResult(conflictStatus, "simulated"))
        .when(concurrentlyModified)
        .renameEntity(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    TagCatalog catalog =
        new TagCatalog(
            concurrentlyModified,
            polarisContext,
            newPassthroughView(),
            IdempotencyRequestContext.DISABLED);

    String version = currentVersion(TAG1);
    assertThatThrownBy(() -> catalog.renameTag(TAG1, TAG2, version))
        .isInstanceOf(CommitConflictException.class);
  }

  @ParameterizedTest
  @EnumSource(
      value = BaseResult.ReturnStatus.class,
      names = {"ENTITY_NOT_FOUND", "CATALOG_PATH_CANNOT_BE_RESOLVED"})
  public void testDropTagConcurrentlyRemovedIsNotFound(BaseResult.ReturnStatus missingStatus) {
    tagCatalog.createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG));

    // Simulate a concurrent request removing the tag (or its catalog path) between this
    // request's resolution and the delete.
    PolarisMetaStoreManager concurrentlyRemoved = Mockito.spy(metaStoreManager);
    Mockito.doReturn(new DropEntityResult(missingStatus, "simulated"))
        .when(concurrentlyRemoved)
        .dropEntityIfExists(
            Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    TagCatalog catalog =
        new TagCatalog(
            concurrentlyRemoved,
            polarisContext,
            newPassthroughView(),
            IdempotencyRequestContext.DISABLED);

    assertThatThrownBy(() -> catalog.dropTag(TAG1)).isInstanceOf(NoSuchTagException.class);
  }

  @Test
  public void testUpdateWithStaleTokenIsRejected() {
    String stale =
        tagCatalog
            .createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG))
            .getVersion();
    tagCatalog.updateTag(
        TAG1,
        UpdateTagRequest.builder()
            .setDescription("changed")
            .setValues(List.of("a"))
            .setCurrentTagVersion(stale)
            .build());

    assertThatThrownBy(
            () ->
                tagCatalog.updateTag(
                    TAG1,
                    UpdateTagRequest.builder()
                        .setDescription("changed again")
                        .setValues(List.of("a"))
                        .setCurrentTagVersion(stale)
                        .build()))
        .isInstanceOf(TagVersionMismatchException.class);
  }

  /**
   * A request that would change nothing is still an update: it has to present the current token,
   * and a stale one is rejected rather than quietly succeeding because there was nothing to write.
   */
  @Test
  public void testStaleTokenIsRejectedEvenWhenNothingWouldChange() {
    String stale =
        tagCatalog
            .createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG))
            .getVersion();
    tagCatalog.updateTag(
        TAG1,
        UpdateTagRequest.builder()
            .setDescription("changed")
            .setValues(List.of("a"))
            .setCurrentTagVersion(stale)
            .build());

    assertThatThrownBy(
            () ->
                tagCatalog.updateTag(
                    TAG1,
                    UpdateTagRequest.builder()
                        .setDescription("description")
                        .setValues(List.of("a"))
                        .setCurrentTagVersion(stale)
                        .build()))
        .isInstanceOf(TagVersionMismatchException.class);
  }

  @Test
  public void testMatchingTokenWithNothingToChangeReturnsTheSameTokenAndDoesNotWrite() {
    String version =
        tagCatalog
            .createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG))
            .getVersion();
    PolarisMetaStoreManager writeWatcher = Mockito.spy(metaStoreManager);
    TagCatalog catalog =
        new TagCatalog(
            writeWatcher, polarisContext, newPassthroughView(), IdempotencyRequestContext.DISABLED);

    Tag result =
        catalog.updateTag(
            TAG1,
            UpdateTagRequest.builder()
                .setDescription("description")
                .setValues(List.of("a"))
                .setCurrentTagVersion(version)
                .build());

    assertThat(result.getVersion()).isEqualTo(version);
    Mockito.verify(writeWatcher, Mockito.never())
        .updateEntityPropertiesIfNotChanged(Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.verify(writeWatcher, Mockito.never())
        .renameEntity(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testChangeIssuesANewTokenAndInvalidatesTheOldOne() {
    String first =
        tagCatalog
            .createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG))
            .getVersion();

    Tag updated =
        tagCatalog.updateTag(
            TAG1,
            UpdateTagRequest.builder()
                .setDescription("changed")
                .setValues(List.of("a"))
                .setCurrentTagVersion(first)
                .build());

    assertThat(updated.getVersion()).isNotEmpty().isNotEqualTo(first);
    assertThat(tagCatalog.loadTag(TAG1).getVersion()).isEqualTo(updated.getVersion());
    assertThatThrownBy(
            () ->
                tagCatalog.updateTag(
                    TAG1,
                    UpdateTagRequest.builder()
                        .setDescription("changed twice")
                        .setValues(List.of("a"))
                        .setCurrentTagVersion(first)
                        .build()))
        .isInstanceOf(TagVersionMismatchException.class);
  }

  /**
   * Setting a field back to a previous value is another change, so it does not revive the token
   * that described the definition when it last held that value.
   */
  @Test
  public void testRevertingAFieldDoesNotReviveAnEarlierToken() {
    String original =
        tagCatalog
            .createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG))
            .getVersion();
    String changed =
        tagCatalog
            .updateTag(
                TAG1,
                UpdateTagRequest.builder()
                    .setDescription("other")
                    .setValues(List.of("a"))
                    .setCurrentTagVersion(original)
                    .build())
            .getVersion();
    Tag reverted =
        tagCatalog.updateTag(
            TAG1,
            UpdateTagRequest.builder()
                .setDescription("description")
                .setValues(List.of("a"))
                .setCurrentTagVersion(changed)
                .build());

    assertThat(reverted.getDescription()).isEqualTo("description");
    assertThat(reverted.getVersion()).isNotEqualTo(original).isNotEqualTo(changed);
  }

  /**
   * A token names the definition it was issued for, so the token of a dropped definition cannot
   * authorize an update to a new definition that took over its name.
   */
  @Test
  public void testTokenOfADroppedDefinitionCannotUpdateItsSameNameReplacement() {
    String beforeDrop =
        tagCatalog
            .createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG))
            .getVersion();
    tagCatalog.dropTag(TAG1);
    tagCatalog.createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG));

    assertThatThrownBy(
            () ->
                tagCatalog.updateTag(
                    TAG1,
                    UpdateTagRequest.builder()
                        .setDescription("changed")
                        .setValues(List.of("a"))
                        .setCurrentTagVersion(beforeDrop)
                        .build()))
        .isInstanceOf(TagVersionMismatchException.class);
  }

  @Test
  public void testTokenOfAnotherDefinitionIsRejected() {
    tagCatalog.createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG));
    String otherVersion =
        tagCatalog
            .createTag(TAG2, "description", List.of("a"), List.of(TargetType.CATALOG))
            .getVersion();

    assertThatThrownBy(
            () ->
                tagCatalog.updateTag(
                    TAG1,
                    UpdateTagRequest.builder()
                        .setDescription("changed")
                        .setValues(List.of("a"))
                        .setCurrentTagVersion(otherVersion)
                        .build()))
        .isInstanceOf(TagVersionMismatchException.class);
  }

  /**
   * A non-empty string this server could not have issued is a version mismatch, not a malformed
   * request: only a missing, empty or non-string token is a request-schema failure, and the schema
   * rejects those before the handler runs.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {"not-a-token", "AAAAAAAAAAA", "AAAAAAAAAAAAAAAAAAAAAA", "!!!!!!!!!!!!!!!!"})
  public void testUnusableTokenIsAVersionMismatchNotABadRequest(String token) {
    tagCatalog.createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG));

    assertThatThrownBy(
            () ->
                tagCatalog.updateTag(
                    TAG1,
                    UpdateTagRequest.builder()
                        .setDescription("changed")
                        .setValues(List.of("a"))
                        .setCurrentTagVersion(token)
                        .build()))
        .isInstanceOf(TagVersionMismatchException.class);
  }

  @ParameterizedTest
  @NullAndEmptySource
  public void testAbsentTokenIsABadRequest(String token) {
    tagCatalog.createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG));

    assertThatThrownBy(
            () ->
                tagCatalog.updateTag(
                    TAG1,
                    UpdateTagRequest.builder()
                        .setDescription("changed")
                        .setValues(List.of("a"))
                        .setCurrentTagVersion(token)
                        .build()))
        .isInstanceOf(BadRequestException.class);
  }

  @Test
  public void testRenameWithMatchingTokenIssuesANewToken() {
    String version =
        tagCatalog
            .createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG))
            .getVersion();

    tagCatalog.renameTag(TAG1, TAG2, version);

    Tag renamed = tagCatalog.loadTag(TAG2);
    assertThat(renamed.getName()).isEqualTo(TAG2);
    assertThat(renamed.getVersion()).isNotEmpty().isNotEqualTo(version);
    assertThatThrownBy(() -> tagCatalog.loadTag(TAG1)).isInstanceOf(NoSuchTagException.class);
  }

  /**
   * The rename branch is a separate write path from the property compare-and-swap, so it needs its
   * own proof that a stale token cannot reach the write.
   */
  @Test
  public void testRenameWithStaleTokenIsRejected() {
    String stale =
        tagCatalog
            .createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG))
            .getVersion();
    tagCatalog.updateTag(
        TAG1,
        UpdateTagRequest.builder()
            .setDescription("changed")
            .setValues(List.of("a"))
            .setCurrentTagVersion(stale)
            .build());

    assertThatThrownBy(() -> tagCatalog.renameTag(TAG1, TAG2, stale))
        .isInstanceOf(TagVersionMismatchException.class);
  }

  /**
   * Two mutations racing on one definition with the same token: exactly one lands.
   *
   * <p>{@link #testUpdateTagLosingConcurrentUpdateIsRetryableConflict} and {@link
   * #testRenameTagLosingConcurrentUpdateIsRetryableConflict} show what a compare-and-swap the
   * metastore refuses turns into, and {@link #testRenameWithStaleTokenIsRejected} shows the token
   * is compared before the write. Neither lets a real second writer commit between that comparison
   * and the write. Here the update's token comparison has already passed when a rename of the same
   * definition, carrying the same token, commits underneath it: the rename runs inside the update's
   * metastore write call, before the real write proceeds. The update's write conditions on the
   * entity version the token described, which the rename has moved, so the metastore refuses it and
   * the update reports a conflict. The check and the write are one compare-and-swap; a check
   * followed by a write would have let both through.
   */
  @Test
  public void testTwoMutationsWithTheSameTokenOnOneDefinitionLetExactlyOneThrough() {
    String token =
        tagCatalog
            .createTag(TAG1, "description", List.of("a"), List.of(TargetType.CATALOG))
            .getVersion();
    TagCatalog renamer =
        new TagCatalog(
            metaStoreManager,
            polarisContext,
            newPassthroughView(),
            IdempotencyRequestContext.DISABLED);

    PolarisMetaStoreManager raced = Mockito.spy(metaStoreManager);
    Mockito.doAnswer(
            invocation -> {
              // The update has compared its token by now; the rename commits before its write.
              renamer.renameTag(TAG1, TAG2, token);
              return invocation.callRealMethod();
            })
        .when(raced)
        .updateEntityPropertiesIfNotChanged(Mockito.any(), Mockito.any(), Mockito.any());
    TagCatalog updater =
        new TagCatalog(
            raced, polarisContext, newPassthroughView(), IdempotencyRequestContext.DISABLED);

    assertThatThrownBy(
            () ->
                updater.updateTag(
                    TAG1,
                    UpdateTagRequest.builder()
                        .setDescription("changed")
                        .setValues(List.of("a"))
                        .setCurrentTagVersion(token)
                        .build()))
        .isInstanceOfSatisfying(
            CommitConflictException.class, e -> assertThat(e.httpStatusCode()).isEqualTo(409));
    // The token comparison passed and the write was attempted exactly once; only the
    // compare-and-swap refused it.
    Mockito.verify(raced, Mockito.times(1))
        .updateEntityPropertiesIfNotChanged(Mockito.any(), Mockito.any(), Mockito.any());

    // The final state is the rename's: the name moved, the description did not, the token moved on.
    assertThatThrownBy(() -> tagCatalog.loadTag(TAG1)).isInstanceOf(NoSuchTagException.class);
    Tag winner = tagCatalog.loadTag(TAG2);
    assertThat(winner.getDescription()).isEqualTo("description");
    assertThat(winner.getValues()).containsExactly("a");
    assertThat(winner.getVersion()).isNotEqualTo(token);
  }

  /** The token a client would have read for the named tag, as of now. */
  private String currentVersion(String tagName) {
    return tagCatalog.loadTag(tagName).getVersion();
  }
}
