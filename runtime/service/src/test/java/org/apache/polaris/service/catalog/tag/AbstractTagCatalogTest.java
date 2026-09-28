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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusMock;
import jakarta.inject.Inject;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.LoadTagAssignmentTargetsResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.tag.TagAssignmentRecord;
import org.apache.polaris.core.tag.TagEntity;
import org.apache.polaris.core.tag.exceptions.NoSuchTagException;
import org.apache.polaris.core.tag.exceptions.NoSuchTargetException;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.admin.PolarisAdminServiceTestSupport;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.iceberg.LocalIcebergCatalog;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.listeners.InMemoryEventCollector;
import org.apache.polaris.service.idempotency.IdempotencyRequestContext;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TaskExecutor;
import org.apache.polaris.service.types.ObjectTag;
import org.apache.polaris.service.types.TagAttachmentTarget;
import org.apache.polaris.service.types.TaggedObject;
import org.apache.polaris.service.types.TargetType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/**
 * Traversal-matrix tests for the tag read operations: the per-definition effective walk with its
 * bidirectional target-types rules, the direct view, observational orphan hiding, and the reverse
 * lookup. Column targets need a real metadata read and are exercised end-to-end in
 * PolarisTagServiceIntegrationTest instead.
 */
public abstract class AbstractTagCatalogTest {

  private static final Namespace NS = Namespace.of("ns1");
  private static final Namespace NS_CHILD = Namespace.of("ns1", "ns1a");
  private static final TableIdentifier TABLE = TableIdentifier.of(NS, "table");
  private static final TableIdentifier DEEP_TABLE = TableIdentifier.of(NS_CHILD, "table2");
  private static final String CATALOG_NAME = "polaris-catalog";
  private static final String TEST_ACCESS_KEY = "test_access_key";
  private static final String SECRET_ACCESS_KEY = "secret_access_key";
  private static final String SESSION_TOKEN = "session_token";
  private static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));

  private static final List<TargetType> ALL_KINDS =
      List.of(TargetType.CATALOG, TargetType.NAMESPACE, TargetType.TABLE, TargetType.COLUMN);

  private static final TagAttachmentTarget CATALOG_TARGET =
      TagAttachmentTarget.builder(TargetType.CATALOG).build();

  @Inject ServiceIdentityProvider serviceIdentityProvider;
  @Inject StorageCredentialCache storageCredentialCache;
  @Inject PolarisStorageIntegrationProvider storageIntegrationProvider;
  @Inject PolarisDiagnostics diagServices;
  @Inject ResolverFactory resolverFactory;
  @Inject ResolutionManifestFactory resolutionManifestFactory;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;
  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject UserSecretsManager userSecretsManager;
  @Inject CallContext callContext;
  @Inject RealmConfig realmConfig;
  @Inject StorageAccessConfigProvider storageAccessConfigProvider;
  @Inject FileIOFactory fileIOFactory;
  @Inject PolarisPrincipalHolder polarisPrincipalHolder;

  private TagCatalog tagCatalog;
  private LocalIcebergCatalog icebergCatalog;
  private String realmName;
  private PolarisCallContext polarisContext;
  private PolarisAdminService adminService;
  private PolarisPrincipal authenticatedRoot;
  private PolarisEntity catalogEntity;

  @BeforeAll
  public static void setUpMocks() {
    PolarisStorageIntegrationProviderImpl mock =
        Mockito.mock(PolarisStorageIntegrationProviderImpl.class);
    QuarkusMock.installMockForType(mock, PolarisStorageIntegrationProviderImpl.class);
  }

  protected void bootstrapRealm(String realmName) {}

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void before(TestInfo testInfo) {
    storageCredentialCache.invalidateAll();

    realmName =
        "realm_%s_%s"
            .formatted(
                testInfo.getTestMethod().map(Method::getName).orElse("test"), System.nanoTime());
    bootstrapRealm(realmName);

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
    ReservedProperties reservedProperties = ReservedProperties.NONE;

    adminService =
        PolarisAdminServiceTestSupport.newAdminService(
            polarisContext,
            resolutionManifestFactory,
            metaStoreManager,
            userSecretsManager,
            serviceIdentityProvider,
            authenticatedRoot,
            authorizer,
            reservedProperties);

    String storageLocation = "s3://my-bucket/path/to/data";
    AwsStorageConfigInfo storageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(storageLocation, "s3://externally-owned-bucket"))
            .build();
    catalogEntity =
        adminService.createCatalog(
            new CreateCatalogRequest(
                new CatalogEntity.Builder()
                    .setName(CATALOG_NAME)
                    .setDefaultBaseLocation(storageLocation)
                    .addProperty(
                        FeatureConfiguration.ALLOW_EXTERNAL_METADATA_FILE_LOCATION.catalogConfig(),
                        "true")
                    .addProperty(
                        FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
                        "true")
                    .setStorageConfigurationInfo(realmConfig, storageConfigModel)
                    .build()
                    .asCatalog(serviceIdentityProvider)));

    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            resolutionManifestFactory, authenticatedRoot, CATALOG_NAME);
    TaskExecutor taskExecutor = Mockito.mock();

    StsClient stsClient = Mockito.mock(StsClient.class);
    when(stsClient.assumeRole(isA(AssumeRoleRequest.class)))
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId(TEST_ACCESS_KEY)
                        .secretAccessKey(SECRET_ACCESS_KEY)
                        .sessionToken(SESSION_TOKEN)
                        .build())
                .build());
    AwsStorageConfigurationInfo mockAwsConfig =
        AwsStorageConfigurationInfo.builder()
            .roleARN("arn:aws:iam::012345678901:role/mock")
            .build();
    AwsCredentialsStorageIntegration storageIntegration =
        new AwsCredentialsStorageIntegration(
            (destination) -> stsClient,
            config -> Optional.empty(),
            storageCredentialCache,
            mockAwsConfig,
            callContext.getRealmConfig());
    when(storageIntegrationProvider.getStorageIntegration(Mockito.anyList()))
        .thenReturn(storageIntegration);

    this.tagCatalog =
        new TagCatalog(
            metaStoreManager,
            callContext,
            passthroughView,
            IdempotencyRequestContext.DISABLED,
            storageAccessConfigProvider,
            fileIOFactory,
            realmConfig);
    this.icebergCatalog =
        new LocalIcebergCatalog(
            diagServices,
            resolverFactory,
            metaStoreManager,
            polarisContext,
            passthroughView,
            authenticatedRoot,
            taskExecutor,
            storageAccessConfigProvider,
            fileIOFactory,
            new InMemoryEventCollector(),
            eventMetadataFactory);
    this.icebergCatalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
  }

  @AfterEach
  public void after() throws IOException {
    metaStoreManager.purge(polarisContext);
  }

  private void createTag(String name, List<TargetType> targetTypes) {
    tagCatalog.createTag(name, "test", List.of("v1", "v2"), targetTypes);
  }

  private void assign(String tagName, TagAttachmentTarget target, String value) {
    tagCatalog.assignTag(tagName, target, List.of(value));
  }

  /**
   * The definition as one request resolves it, which is what the handler hands the catalog. A test
   * that resolves it before a drop and calls afterwards is the shape of a request whose definition
   * was replaced under it.
   */
  private TagEntity resolvedTag(String tagName) {
    return TagEntity.of(
        metaStoreManager
            .readEntityByName(
                polarisContext,
                List.of(PolarisEntity.of(catalogEntity)),
                org.apache.polaris.core.entity.PolarisEntityType.TAG,
                org.apache.polaris.core.entity.PolarisEntitySubType.NULL_SUBTYPE,
                tagName)
            .getEntity());
  }

  private Set<ObjectTag> read(TagAttachmentTarget target, boolean effective) {
    return new LinkedHashSet<>(
        tagCatalog
            .getObjectTags(target, effective, PageToken.readEverything(), Integer.MAX_VALUE)
            .items());
  }

  /**
   * A tag read is bounded by the candidates it may consume, not only by the results it may return.
   * The budget is spent on rows read, so a target holding more assignments than the request may
   * consume is refused rather than answered from part of them: an answer built from some of a
   * level's rows is indistinguishable, to the client, from that level's whole set.
   */
  @Test
  public void testDirectReadRefusesATargetPastItsCandidateBudget() {
    createHierarchy();
    createTag("t1", ALL_KINDS);
    createTag("t2", ALL_KINDS);
    createTag("t3", ALL_KINDS);
    assign("t1", tableTarget(TABLE), "v1");
    assign("t2", tableTarget(TABLE), "v1");
    assign("t3", tableTarget(TABLE), "v1");

    assertThatThrownBy(() -> read(tableTarget(TABLE), false, 2))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("more candidate assignments than one request may consume")
        // the limit is named, so a caller knows what it asked past
        .hasMessageContaining("(limit 2)")
        // and the remedy is one that works: the page size does not move this budget
        .hasMessageContaining("pagination=false");
  }

  /**
   * The budget is the total across levels, not a per-level allowance. The level walk itself is
   * correctness-required and at most four deep, so it is never the thing trimmed; what one request
   * may consume is rows, and rows are fungible across levels.
   */
  @Test
  public void testEffectiveReadCountsCandidatesAcrossEveryLevel() {
    createHierarchy();
    createTag("t1", ALL_KINDS);
    createTag("t2", ALL_KINDS);
    createTag("t3", ALL_KINDS);
    assign("t1", CATALOG_TARGET, "v1");
    assign("t2", namespaceTarget(NS), "v1");
    assign("t3", tableTarget(TABLE), "v1");

    // Two rows would pass at any single level; the read is refused because three levels are read.
    assertThatThrownBy(() -> read(tableTarget(TABLE), true, 2))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("(limit 2)");

    // The same read within its budget answers exactly what it answered before the bound existed.
    Set<ObjectTag> tags = read(tableTarget(TABLE), true, 3);
    assertThat(tags).hasSize(3);
    assertThat(tags.stream().map(t -> t.getTag().getName()))
        .containsExactlyInAnyOrder("t1", "t2", "t3");
  }

  private Set<ObjectTag> read(TagAttachmentTarget target, boolean effective, int candidateBudget) {
    return new LinkedHashSet<>(
        tagCatalog
            .getObjectTags(target, effective, PageToken.readEverything(), candidateBudget)
            .items());
  }

  private static Optional<ObjectTag> find(Set<ObjectTag> tags, String tagName) {
    return tags.stream().filter(t -> t.getTag().getName().equals(tagName)).findFirst();
  }

  private static TagAttachmentTarget namespaceTarget(Namespace namespace) {
    return TagAttachmentTarget.builder(TargetType.NAMESPACE)
        .setPath(Arrays.asList(namespace.levels()))
        .build();
  }

  private static TagAttachmentTarget tableTarget(TableIdentifier identifier) {
    List<String> path = new java.util.ArrayList<>(Arrays.asList(identifier.namespace().levels()));
    path.add(identifier.name());
    return TagAttachmentTarget.builder(TargetType.TABLE).setPath(path).build();
  }

  private void createHierarchy() {
    icebergCatalog.createNamespace(NS);
    icebergCatalog.createNamespace(NS_CHILD);
    icebergCatalog.createTable(TABLE, SCHEMA);
    icebergCatalog.createTable(DEEP_TABLE, SCHEMA);
  }

  @Test
  public void testDirectReadPerKindEchoesQueriedTarget() {
    createHierarchy();
    createTag("t1", ALL_KINDS);
    assign("t1", CATALOG_TARGET, "v1");
    assign("t1", namespaceTarget(NS), "v2");
    assign("t1", tableTarget(TABLE), "v1");

    for (TagAttachmentTarget target :
        List.of(CATALOG_TARGET, namespaceTarget(NS), tableTarget(TABLE))) {
      Set<ObjectTag> tags = read(target, false);
      assertThat(tags).hasSize(1);
      ObjectTag tag = tags.iterator().next();
      assertThat(tag.getTag().getName()).isEqualTo("t1");
      assertThat(tag.getApplyMethod()).isEqualTo("DIRECT");
      assertThat(tag.getAssignedAt()).isEqualTo(target);
    }
    assertThat(find(read(CATALOG_TARGET, false), "t1").orElseThrow().getValues())
        .containsExactly("v1");
    assertThat(find(read(namespaceTarget(NS), false), "t1").orElseThrow().getValues())
        .containsExactly("v2");
  }

  @Test
  public void testDirectViewIgnoresParentsEffectiveInherits() {
    createHierarchy();
    createTag("t1", ALL_KINDS);
    assign("t1", CATALOG_TARGET, "v1");

    assertThat(read(tableTarget(TABLE), false)).isEmpty();

    Set<ObjectTag> effective = read(tableTarget(TABLE), true);
    ObjectTag tag = find(effective, "t1").orElseThrow();
    assertThat(tag.getApplyMethod()).isEqualTo("INHERITED");
    assertThat(tag.getAssignedAt()).isEqualTo(CATALOG_TARGET);
    assertThat(tag.getValues()).containsExactly("v1");
  }

  @Test
  public void testEffectiveDestinationFilterAndChildNamespace() {
    createHierarchy();
    createTag("nsonly", List.of(TargetType.NAMESPACE));
    assign("nsonly", namespaceTarget(NS), "v1");

    // the queried target kind is excluded by the definition: omitted entirely
    assertThat(find(read(tableTarget(TABLE), true), "nsonly")).isEmpty();
    // the queried namespace itself: direct
    assertThat(find(read(namespaceTarget(NS), true), "nsonly").orElseThrow().getApplyMethod())
        .isEqualTo("DIRECT");
    // a child namespace inherits when namespace is listed
    ObjectTag child = find(read(namespaceTarget(NS_CHILD), true), "nsonly").orElseThrow();
    assertThat(child.getApplyMethod()).isEqualTo("INHERITED");
    assertThat(child.getAssignedAt()).isEqualTo(namespaceTarget(NS));
  }

  @Test
  public void testExcludedIntermediateKindDoesNotStopWalk() {
    createHierarchy();
    // namespaces are excluded: a catalog assignment must still reach a table below them
    createTag("skipns", List.of(TargetType.CATALOG, TargetType.TABLE));
    assign("skipns", CATALOG_TARGET, "v1");

    ObjectTag deep = find(read(tableTarget(DEEP_TABLE), true), "skipns").orElseThrow();
    assertThat(deep.getApplyMethod()).isEqualTo("INHERITED");
    assertThat(deep.getAssignedAt()).isEqualTo(CATALOG_TARGET);
    // and the excluded intermediate kind itself is omitted as a destination
    assertThat(find(read(namespaceTarget(NS), true), "skipns")).isEmpty();
  }

  @Test
  public void testClosestWinsAndDeepestNamespace() {
    createHierarchy();
    createTag("t1", ALL_KINDS);
    assign("t1", CATALOG_TARGET, "v1");
    assign("t1", namespaceTarget(NS), "v1");
    assign("t1", namespaceTarget(NS_CHILD), "v2");

    // deepest namespace wins for a table below both namespaces
    ObjectTag deep = find(read(tableTarget(DEEP_TABLE), true), "t1").orElseThrow();
    assertThat(deep.getApplyMethod()).isEqualTo("INHERITED");
    assertThat(deep.getAssignedAt()).isEqualTo(namespaceTarget(NS_CHILD));
    assertThat(deep.getValues()).containsExactly("v2");

    // the queried level's own assignment wins over every parent
    ObjectTag own = find(read(namespaceTarget(NS_CHILD), true), "t1").orElseThrow();
    assertThat(own.getApplyMethod()).isEqualTo("DIRECT");
    assertThat(own.getAssignedAt()).isEqualTo(namespaceTarget(NS_CHILD));

    // values from different levels are never combined: exactly one result, one value
    assertThat(read(tableTarget(DEEP_TABLE), true)).hasSize(1);
  }

  @Test
  public void testPerDefinitionIndependentWinners() {
    createHierarchy();
    createTag("near", ALL_KINDS);
    createTag("far", ALL_KINDS);
    assign("near", namespaceTarget(NS), "v1");
    assign("near", tableTarget(TABLE), "v2");
    assign("far", CATALOG_TARGET, "v1");

    Set<ObjectTag> tags = read(tableTarget(TABLE), true);
    assertThat(tags).hasSize(2);
    ObjectTag near = find(tags, "near").orElseThrow();
    assertThat(near.getApplyMethod()).isEqualTo("DIRECT");
    assertThat(near.getAssignedAt()).isEqualTo(tableTarget(TABLE));
    assertThat(near.getValues()).containsExactly("v2");
    ObjectTag far = find(tags, "far").orElseThrow();
    assertThat(far.getApplyMethod()).isEqualTo("INHERITED");
    assertThat(far.getAssignedAt()).isEqualTo(CATALOG_TARGET);
  }

  @Test
  public void testObservationalAtomicityAfterDetachAllDrop() {
    createHierarchy();
    createTag("gone", ALL_KINDS);
    assign("gone", namespaceTarget(NS), "v1");
    assign("gone", tableTarget(TABLE), "v2");

    // resolved before the drop, read after it: the shape of a request whose definition went away
    // under it. The miss is the store's own existence check on the id the request resolved, not a
    // second by-name resolution that would have found nothing -- or, worse, found a successor.
    TagEntity resolvedBeforeTheDrop = resolvedTag("gone");

    tagCatalog.dropTag("gone", true);

    assertThat(find(read(namespaceTarget(NS), false), "gone")).isEmpty();
    assertThat(find(read(tableTarget(TABLE), true), "gone")).isEmpty();
    assertThatThrownBy(
            () ->
                tagCatalog.listObjectsByTag(
                    resolvedBeforeTheDrop,
                    null,
                    PageToken.readEverything(),
                    t -> true,
                    Integer.MAX_VALUE))
        .isInstanceOf(NoSuchTagException.class);
  }

  /**
   * The reverse lookup reads the definition the request resolved, and only that one.
   *
   * <p>A definition dropped with detach-all and recreated under the same name is a different
   * definition holding different assignments. A read that resolved the name once and then resolved
   * it again to fetch the rows would be authorized against the first and answered from the second,
   * and a continuation minted for the first would land in the second's rows -- rows no request of
   * this definition ever returned or refused. Being handed the identity is what makes that
   * impossible: the replacement is reported as a miss, so the caller is never quietly moved onto
   * it.
   */
  @Test
  public void testReverseLookupRefusesWhenTheDefinitionIsReplacedBetweenValidationAndRead() {
    createHierarchy();
    createTag("identitybound", ALL_KINDS);
    assign("identitybound", CATALOG_TARGET, "v1");
    assign("identitybound", namespaceTarget(NS), "v1");

    // what the request resolved before its read
    TagEntity resolvedForTheRequest = resolvedTag("identitybound");
    assertThat(
            tagCatalog
                .listObjectsByTag(
                    resolvedForTheRequest,
                    null,
                    PageToken.readEverything(),
                    t -> true,
                    Integer.MAX_VALUE)
                .items())
        .hasSize(2);

    // the same name, a different definition, with assignments of its own
    tagCatalog.dropTag("identitybound", true);
    createTag("identitybound", ALL_KINDS);
    assign("identitybound", namespaceTarget(NS), "v1");
    TagEntity replacement = resolvedTag("identitybound");
    assertThat(replacement.getId()).isNotEqualTo(resolvedForTheRequest.getId());

    // the read the first request was going to do now finds its definition gone, and does not read
    // the replacement's rows in its place
    assertThatThrownBy(
            () ->
                tagCatalog.listObjectsByTag(
                    resolvedForTheRequest,
                    null,
                    PageToken.readEverything(),
                    t -> true,
                    Integer.MAX_VALUE))
        .isInstanceOf(NoSuchTagException.class);

    // and the replacement, read as itself, returns only its own
    assertThat(
            tagCatalog
                .listObjectsByTag(
                    replacement, null, PageToken.readEverything(), t -> true, Integer.MAX_VALUE)
                .items())
        .hasSize(1);
  }

  @Test
  public void testReverseLookupValueFilterAndPagination() {
    createHierarchy();
    createTag("t1", ALL_KINDS);
    createTag("other", ALL_KINDS);
    assign("t1", CATALOG_TARGET, "v1");
    assign("t1", namespaceTarget(NS), "v2");
    assign("t1", tableTarget(TABLE), "v1");
    assign("other", namespaceTarget(NS), "v1");

    List<TaggedObject> all =
        List.copyOf(
            tagCatalog
                .listObjectsByTag(
                    resolvedTag("t1"),
                    null,
                    PageToken.readEverything(),
                    t -> true,
                    Integer.MAX_VALUE)
                .items());
    assertThat(all).hasSize(3);
    assertThat(all).allMatch(o -> o.getApplyMethod().equals("DIRECT"));
    assertThat(all.stream().map(TaggedObject::getTarget))
        .containsExactlyInAnyOrder(CATALOG_TARGET, namespaceTarget(NS), tableTarget(TABLE));

    // exact, case-sensitive value filter
    assertThat(
            tagCatalog
                .listObjectsByTag(
                    resolvedTag("t1"),
                    "v1",
                    PageToken.readEverything(),
                    t -> true,
                    Integer.MAX_VALUE)
                .items())
        .hasSize(2)
        .allMatch(o -> o.getValues().contains("v1"));
    assertThat(
            tagCatalog
                .listObjectsByTag(
                    resolvedTag("t1"),
                    "V1",
                    PageToken.readEverything(),
                    t -> true,
                    Integer.MAX_VALUE)
                .items())
        .isEmpty();

    // pagination round-trip: page size 1, no duplicates, no misses, terminates
    java.util.List<TaggedObject> paged = new java.util.ArrayList<>();
    PageToken request = PageToken.fromLimit(1);
    int pages = 0;
    while (true) {
      var response =
          tagCatalog.listObjectsByTag(
              resolvedTag("t1"), null, request, t -> true, Integer.MAX_VALUE);
      paged.addAll(response.items());
      pages++;
      assertThat(pages).isLessThan(10);
      if (response.encodedResponseToken() == null) {
        break;
      }
      request =
          PageToken.build(
              response.encodedResponseToken(),
              null,
              0 /* keep the size the token carries */,
              () -> true);
    }
    assertThat(paged).hasSize(3);
    assertThat(paged.stream().map(TaggedObject::getTarget).distinct()).hasSize(3);
  }

  @Test
  public void testReverseLookupHidesOrphanedTargetRow() {
    createHierarchy();
    createTag("t1", ALL_KINDS);
    assign("t1", namespaceTarget(NS), "v1");

    // simulate a row left behind by a failed best-effort cleanup: its target id no longer
    // resolves, so the read must hide it while still returning the live row
    TagEntity tag =
        TagEntity.of(
            metaStoreManager
                .readEntityByName(
                    polarisContext,
                    List.of(PolarisEntity.of(catalogEntity)),
                    org.apache.polaris.core.entity.PolarisEntityType.TAG,
                    org.apache.polaris.core.entity.PolarisEntitySubType.NULL_SUBTYPE,
                    "t1")
                .getEntity());
    TagAssignmentRecord orphan = new TagAssignmentRecord();
    orphan.setTargetCatalogId(catalogEntity.getId());
    orphan.setTargetId(987654321L);
    orphan.setFieldId(0);
    orphan.setTagCatalogId(tag.getCatalogId());
    orphan.setTagId(tag.getId());
    orphan.setValue("v1");
    polarisContext.getMetaStore().writeToTagAssignmentRecords(polarisContext, orphan);

    var objects =
        tagCatalog
            .listObjectsByTag(
                resolvedTag("t1"), null, PageToken.readEverything(), t -> true, Integer.MAX_VALUE)
            .items();
    assertThat(objects).hasSize(1);
    assertThat(objects.iterator().next().getTarget()).isEqualTo(namespaceTarget(NS));
  }

  @Test
  public void testReverseLookupSkipsEntirelyHiddenPageWithoutEmptyingResponse() {
    createHierarchy();
    createTag("t1", ALL_KINDS);

    // orphan's target id sorts before every real, generated entity id (always positive):
    // with page size 1, this row alone fills the first persistence page, and the live
    // assignment below lands on the second
    TagEntity tag =
        TagEntity.of(
            metaStoreManager
                .readEntityByName(
                    polarisContext,
                    List.of(PolarisEntity.of(catalogEntity)),
                    org.apache.polaris.core.entity.PolarisEntityType.TAG,
                    org.apache.polaris.core.entity.PolarisEntitySubType.NULL_SUBTYPE,
                    "t1")
                .getEntity());
    TagAssignmentRecord orphan = new TagAssignmentRecord();
    orphan.setTargetCatalogId(catalogEntity.getId());
    orphan.setTargetId(-1L);
    orphan.setFieldId(0);
    orphan.setTagCatalogId(tag.getCatalogId());
    orphan.setTagId(tag.getId());
    orphan.setValue("v1");
    polarisContext.getMetaStore().writeToTagAssignmentRecords(polarisContext, orphan);

    assign("t1", namespaceTarget(NS), "v1");

    var response =
        tagCatalog.listObjectsByTag(
            resolvedTag("t1"), null, PageToken.fromLimit(1), t -> true, Integer.MAX_VALUE);
    assertThat(response.items())
        .as("a page whose rows are entirely hidden must not surface as empty with a token")
        .isNotEmpty();
    assertThat(response.items().iterator().next().getTarget()).isEqualTo(namespaceTarget(NS));
  }

  @Test
  public void testReadsHideDefinitionSideRaceLeftovers() {
    createHierarchy();
    createTag("live", ALL_KINDS);
    assign("live", namespaceTarget(NS), "v1");
    createTag("ghost", ALL_KINDS);
    assign("ghost", namespaceTarget(NS), "v1");

    // simulate the drop-vs-assign race leftover: the definition row is gone while its assignment
    // row survives; reads must hide the row rather than surface a definition-less assignment
    TagEntity ghost =
        TagEntity.of(
            metaStoreManager
                .readEntityByName(
                    polarisContext,
                    List.of(PolarisEntity.of(catalogEntity)),
                    org.apache.polaris.core.entity.PolarisEntityType.TAG,
                    org.apache.polaris.core.entity.PolarisEntitySubType.NULL_SUBTYPE,
                    "ghost")
                .getEntity());
    polarisContext.getMetaStore().deleteEntity(polarisContext, ghost);

    Set<ObjectTag> direct = read(namespaceTarget(NS), false);
    assertThat(find(direct, "live")).isPresent();
    assertThat(find(direct, "ghost")).isEmpty();
    Set<ObjectTag> effective = read(tableTarget(TABLE), true);
    assertThat(find(effective, "live")).isPresent();
    assertThat(find(effective, "ghost")).isEmpty();

    // a same-name recreate gets a fresh definition id and must not resurrect the leftover row
    createTag("ghost", ALL_KINDS);
    assertThat(find(read(namespaceTarget(NS), false), "ghost")).isEmpty();
    assertThat(
            tagCatalog
                .listObjectsByTag(
                    resolvedTag("ghost"),
                    null,
                    PageToken.readEverything(),
                    t -> true,
                    Integer.MAX_VALUE)
                .items())
        .isEmpty();
  }

  @Test
  public void testEffectiveReadFailsRatherThanDegrades() {
    createHierarchy();
    createTag("t1", ALL_KINDS);
    assign("t1", tableTarget(TABLE), "v1");

    PolarisMetaStoreManager failing = Mockito.spy(metaStoreManager);
    Mockito.doThrow(new RuntimeException("level load failed"))
        .when(failing)
        .loadTagsOnEntities(
            Mockito.any(),
            Mockito.argThat(
                (List<org.apache.polaris.core.tag.PolarisTagAssignmentManager.TargetLevel>
                        levels) ->
                    levels != null
                        && levels.stream()
                            .anyMatch(
                                level ->
                                    level.entity().getTypeCode()
                                        == org.apache.polaris.core.entity.PolarisEntityType.CATALOG
                                            .getCode())),
            Mockito.anyInt());
    TagCatalog failingCatalog =
        new TagCatalog(
            failing,
            callContext,
            new PolarisPassthroughResolutionView(
                resolutionManifestFactory, authenticatedRoot, CATALOG_NAME),
            IdempotencyRequestContext.DISABLED,
            storageAccessConfigProvider,
            fileIOFactory,
            realmConfig);

    // a failed parent level fails the whole effective read: no partial result, no direct fallback
    assertThatThrownBy(
            () ->
                failingCatalog.getObjectTags(
                    tableTarget(TABLE), true, PageToken.readEverything(), Integer.MAX_VALUE))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("level load failed");
  }

  @Test
  public void testGetObjectTagsAnswers404WhenTargetVanishesAfterResolution() {
    createHierarchy();
    createTag("t1", ALL_KINDS);
    assign("t1", tableTarget(TABLE), "v1");

    // simulate the target being purged between resolution and this class's own load:
    // loadTagsOnEntities documents ENTITY_NOT_FOUND as an expected outcome of that race.
    PolarisMetaStoreManager racy = Mockito.spy(metaStoreManager);
    Mockito.doReturn(
            new org.apache.polaris.core.persistence.dao.entity.LoadTagAssignmentsResult(
                org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus
                    .ENTITY_NOT_FOUND,
                null))
        .when(racy)
        .loadTagsOnEntities(
            Mockito.any(),
            Mockito.argThat(
                (List<org.apache.polaris.core.tag.PolarisTagAssignmentManager.TargetLevel>
                        levels) ->
                    levels != null
                        && levels.stream()
                            .anyMatch(
                                level ->
                                    level.entity().getTypeCode()
                                        == org.apache.polaris.core.entity.PolarisEntityType
                                            .TABLE_LIKE
                                            .getCode())),
            Mockito.anyInt());
    TagCatalog racyCatalog =
        new TagCatalog(
            racy,
            callContext,
            new PolarisPassthroughResolutionView(
                resolutionManifestFactory, authenticatedRoot, CATALOG_NAME),
            IdempotencyRequestContext.DISABLED,
            storageAccessConfigProvider,
            fileIOFactory,
            realmConfig);

    assertThatThrownBy(
            () ->
                racyCatalog.getObjectTags(
                    tableTarget(TABLE), false, PageToken.readEverything(), Integer.MAX_VALUE))
        .isInstanceOf(NoSuchTargetException.class);
  }

  /**
   * The NoSQL backend has no schema-version concept: it rejects every tag-assignment read
   * unconditionally, the same way {@code NoSqlMetaStoreManager} rejects every tag-assignment write.
   * Stubbed here rather than run against a real NoSQL-backed Quarkus profile: a NoSQL subclass of
   * this class would also inherit every assignment-based test above, and those would themselves
   * fail against a backend that rejects assignment writes outright.
   */
  @Test
  public void testGetObjectTagsRejectedOnBackendWithoutTagAssignmentSupport() {
    createHierarchy();
    createTag("t1", ALL_KINDS);

    PolarisMetaStoreManager unsupported = Mockito.spy(metaStoreManager);
    Mockito.doReturn(
            new org.apache.polaris.core.persistence.dao.entity.LoadTagAssignmentsResult(
                BaseResult.ReturnStatus.TAG_ASSIGNMENTS_NOT_SUPPORTED,
                "tag assignments are not supported by the NoSQL backend"))
        .when(unsupported)
        .loadTagsOnEntities(Mockito.any(), Mockito.any(), Mockito.anyInt());
    TagCatalog unsupportedCatalog =
        new TagCatalog(
            unsupported,
            callContext,
            new PolarisPassthroughResolutionView(
                resolutionManifestFactory, authenticatedRoot, CATALOG_NAME),
            IdempotencyRequestContext.DISABLED,
            storageAccessConfigProvider,
            fileIOFactory,
            realmConfig);

    // a read is not a write, but a backend that cannot resolve tag assignments at all must reject
    // this the same way it rejects assign/unassign, not silently answer "no tags"
    assertThatThrownBy(
            () ->
                unsupportedCatalog.getObjectTags(
                    tableTarget(TABLE), false, PageToken.readEverything(), Integer.MAX_VALUE))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("tag assignments are not supported by the NoSQL backend")
        // the subject is the object kind whose tags could not be read, not a tag name: the message
        // used to render "Cannot read tags on tag table-like", which named a tag that has no part
        // in this failure
        .hasMessageContaining("Cannot read tags on TABLE")
        .hasMessageNotContaining("on tag table-like");
  }

  /** See {@link #testGetObjectTagsRejectedOnBackendWithoutTagAssignmentSupport}. */
  @Test
  public void testListObjectsByTagRejectedOnBackendWithoutTagAssignmentSupport() {
    createHierarchy();
    createTag("t1", ALL_KINDS);

    PolarisMetaStoreManager unsupported = Mockito.spy(metaStoreManager);
    Mockito.doReturn(
            new LoadTagAssignmentTargetsResult(
                BaseResult.ReturnStatus.TAG_ASSIGNMENTS_NOT_SUPPORTED,
                "tag assignments are not supported by the NoSQL backend"))
        .when(unsupported)
        .loadTargetsOnTag(
            Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    TagCatalog unsupportedCatalog =
        new TagCatalog(
            unsupported,
            callContext,
            new PolarisPassthroughResolutionView(
                resolutionManifestFactory, authenticatedRoot, CATALOG_NAME),
            IdempotencyRequestContext.DISABLED,
            storageAccessConfigProvider,
            fileIOFactory,
            realmConfig);

    assertThatThrownBy(
            () ->
                unsupportedCatalog.listObjectsByTag(
                    resolvedTag("t1"),
                    null,
                    PageToken.readEverything(),
                    t -> true,
                    Integer.MAX_VALUE))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("tag assignments are not supported by the NoSQL backend");
  }

  /**
   * A column row whose table has no current metadata location must fail the lookup, not disappear
   * from it. Being unable to read a table's metadata does not establish that its column is gone, so
   * hiding the row would answer the request as if the assignment did not exist -- and would do it
   * while still handing back a page token, so the caller has no signal at all. Every other failure
   * to load metadata on this path already propagates; this case was the one exception.
   */
  @Test
  public void testReverseLookupFailsWhenATableHasNoReadableMetadata() {
    createHierarchy();
    createTag("t1", ALL_KINDS);
    assign("t1", namespaceTarget(NS), "v1");

    TagEntity tag =
        TagEntity.of(
            metaStoreManager
                .readEntityByName(
                    polarisContext,
                    List.of(PolarisEntity.of(catalogEntity)),
                    org.apache.polaris.core.entity.PolarisEntityType.TAG,
                    org.apache.polaris.core.entity.PolarisEntitySubType.NULL_SUBTYPE,
                    "t1")
                .getEntity());

    // an ICEBERG_TABLE entity that carries no metadata location, plus a column assignment on it
    long tableId = metaStoreManager.generateNewEntityId(polarisContext).getId();
    PolarisBaseEntity noMetadataTable =
        new org.apache.polaris.core.entity.table.IcebergTableLikeEntity.Builder(
                org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_TABLE,
                TableIdentifier.of(NS, "no_metadata"),
                null)
            .setId(tableId)
            .setCatalogId(catalogEntity.getId())
            .setParentId(catalogEntity.getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .build();
    polarisContext.getMetaStore().writeEntity(polarisContext, noMetadataTable, true, null);

    TagAssignmentRecord columnRow = new TagAssignmentRecord();
    columnRow.setTargetCatalogId(catalogEntity.getId());
    columnRow.setTargetId(tableId);
    columnRow.setFieldId(1);
    columnRow.setTagCatalogId(tag.getCatalogId());
    columnRow.setTagId(tag.getId());
    columnRow.setValue("v1");
    polarisContext.getMetaStore().writeToTagAssignmentRecords(polarisContext, columnRow);

    assertThatThrownBy(
            () ->
                tagCatalog.listObjectsByTag(
                    resolvedTag("t1"),
                    null,
                    PageToken.readEverything(),
                    t -> true,
                    Integer.MAX_VALUE))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("no current metadata location");
  }

  /**
   * A whole-object assignment on a generic table is a target the write path accepts, so the reverse
   * lookup has to return it. Building its identifier through the Iceberg subclass instead of the
   * shared table-like accessor threw on the subtype check, which failed the entire page rather than
   * one row: the Iceberg rows sharing that page disappeared behind a 500 too. This puts both kinds
   * on one page and asserts both come back.
   */
  @Test
  public void testReverseLookupReturnsGenericTableRowsAlongsideIcebergRows() {
    createHierarchy();
    createTag("t1", ALL_KINDS);
    assign("t1", tableTarget(TABLE), "v1");

    TagEntity tag =
        TagEntity.of(
            metaStoreManager
                .readEntityByName(
                    polarisContext,
                    List.of(PolarisEntity.of(catalogEntity)),
                    org.apache.polaris.core.entity.PolarisEntityType.TAG,
                    org.apache.polaris.core.entity.PolarisEntitySubType.NULL_SUBTYPE,
                    "t1")
                .getEntity());

    TableIdentifier genericId = TableIdentifier.of(NS, "G1");
    long genericTableId = metaStoreManager.generateNewEntityId(polarisContext).getId();
    PolarisBaseEntity genericTable =
        new org.apache.polaris.core.entity.table.GenericTableEntity.Builder(genericId, "format")
            .setId(genericTableId)
            .setCatalogId(catalogEntity.getId())
            .setParentId(catalogEntity.getId())
            .setCreateTimestamp(System.currentTimeMillis())
            .build();
    polarisContext.getMetaStore().writeEntity(polarisContext, genericTable, true, null);

    TagAssignmentRecord genericRow = new TagAssignmentRecord();
    genericRow.setTargetCatalogId(catalogEntity.getId());
    genericRow.setTargetId(genericTableId);
    genericRow.setFieldId(0);
    genericRow.setTagCatalogId(tag.getCatalogId());
    genericRow.setTagId(tag.getId());
    genericRow.setValue("v1");
    polarisContext.getMetaStore().writeToTagAssignmentRecords(polarisContext, genericRow);

    var objects =
        tagCatalog
            .listObjectsByTag(
                resolvedTag("t1"), null, PageToken.readEverything(), t -> true, Integer.MAX_VALUE)
            .items();
    assertThat(objects).hasSize(2);
    assertThat(objects.stream().map(TaggedObject::getTarget))
        .containsExactlyInAnyOrder(tableTarget(TABLE), tableTarget(genericId));
  }
}
