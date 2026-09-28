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

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.tag.CandidateBudget;
import org.apache.polaris.core.tag.TagAssignmentRecord;
import org.apache.polaris.core.tag.TagEntity;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.types.CreateTagRequest;
import org.apache.polaris.service.types.ListObjectsByTagResponse;
import org.apache.polaris.service.types.ListTagsResponse;
import org.apache.polaris.service.types.TagAttachmentTarget;
import org.apache.polaris.service.types.TagIdentifier;
import org.apache.polaris.service.types.TargetType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * The limit on a full-result listing is a deployment setting, so it takes a realm configured with a
 * small one to be observable at all. That is why these tests live apart from the rest of the tag
 * handler tests rather than beside them.
 */
@QuarkusTest
@TestProfile(TagCatalogHandlerUnpaginatedLimitTest.Profile.class)
public class TagCatalogHandlerUnpaginatedLimitTest extends PolarisAuthzTestBase {

  private static final int LIMIT = 2;

  @Inject StorageAccessConfigProvider storageAccessConfigProvider;
  @Inject FileIOFactory fileIOFactory;

  public static class Profile extends Profiles.PolarisAuthzBaseProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put(
              "polaris.features.\"LIST_PAGINATION_UNPAGINATED_MAX_RESULTS\"", String.valueOf(LIMIT))
          .build();
    }
  }

  @Test
  public void testUnpaginatedListingAtTheLimitReturnsEverything() {
    createTags(LIMIT);

    ListTagsResponse response = handler().listTags(false, null, null);

    assertThat(response.getIdentifiers()).hasSize(LIMIT);
    // A full-result answer is complete, so it carries no continuation for the client to follow.
    assertThat(response.getNextPageToken()).isNull();
  }

  @Test
  public void testUnpaginatedListingOverTheLimitIsRejectedAndNamesTheRemedy() {
    createTags(LIMIT + 1);

    // The server must find out it cannot answer before it answers. Truncating the list or turning
    // it into a page would reach the client as a complete result it has no way to question.
    assertThatThrownBy(() -> handler().listTags(false, null, null))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("removing pagination=false or setting it to true");
  }

  @Test
  public void testPagedListingIsNotSubjectToTheUnpaginatedLimit() {
    createTags(LIMIT + 1);

    // The limit answers "how much will you return in one complete response", so a paged request is
    // not measured against it, however many definitions it walks through.
    Set<String> seen = new HashSet<>();
    String token = "";
    while (token != null) {
      ListTagsResponse page = handler().listTags(true, token, 1);
      page.getIdentifiers().stream().map(TagIdentifier::getName).forEach(seen::add);
      token = page.getNextPageToken();
    }

    assertThat(seen).hasSize(LIMIT + 1);
  }

  /**
   * A result bound is not a work bound. This operation filters candidates by target existence and
   * by the caller's permission, so a definition whose assignments are almost all hidden returns
   * almost nothing while scanning a great deal, and the full-result path has to refuse rather than
   * walk the whole set. The budget is derived from the same limit, so this realm's LIMIT of 2 makes
   * it small enough to reach with rows a test can write.
   */
  @Test
  public void testUnpaginatedReverseLookupRefusesWhenTheCandidateBudgetRunsOut() {
    TagEntity tag = createTagAssignedToTheCatalog("budgettag");
    // Comfortably past the derived budget, and every one of these rows is hidden, so the result
    // stays
    // at the single live row however far the scan goes.
    writeOrphanRows(tag, 45);

    assertThatThrownBy(() -> handler().listObjectsByTag("budgettag", null, false, null, null))
        .isInstanceOf(BadRequestException.class)
        // The remedy has to be a request the contract accepts: pagination=false is what the client
        // sent, so telling it to add a page token would name a combination that is itself a 400.
        .hasMessageContaining("removing pagination=false or setting it to true");
  }

  /**
   * The other half of the same claim: below the budget the identical shape answers normally, so the
   * refusal above is the bound doing its job and not merely "an orphaned row was seen".
   */
  @Test
  public void testUnpaginatedReverseLookupBelowTheCandidateBudgetStillAnswers() {
    TagEntity tag = createTagAssignedToTheCatalog("withinbudgettag");
    writeOrphanRows(tag, 5);

    ListObjectsByTagResponse response =
        handler().listObjectsByTag("withinbudgettag", null, false, null, null);

    assertThat(response.getObjects()).hasSize(1);
    assertThat(response.getObjects().iterator().next().getTarget().getType())
        .isEqualTo(TargetType.CATALOG);
    // A full-result answer is complete, so it carries no continuation.
    assertThat(response.getNextPageToken()).isNull();
  }

  /**
   * A value filter that rejects a row has still examined it, and the in-memory store charges that
   * examination to the request's work budget. Twenty-five rows carrying one value, a request for a
   * value none of them carries, and a page of one, whose budget is twenty: the read examines twenty
   * rows, has five left it may not look at, and refuses. It does not answer an empty page, which
   * would tell the client the definition carries nothing of that value when the read never found
   * out.
   */
  @Test
  public void testPagedReverseLookupRefusesWhenTheVisitBudgetRunsOutOnAValueNobodyUses() {
    TagEntity tag = createTagWithTwoValuesAssignedToTheCatalog("visitbudgettag", "a");
    writeRowsWithValue(tag, 25, "a");

    assertThatThrownBy(() -> handler().listObjectsByTag("visitbudgettag", "b", true, null, 1))
        .isInstanceOf(BadRequestException.class)
        .hasMessage(
            "This request examined more candidate assignments than its work budget allows (limit"
                + " 20) without finding one it could return or resume from; the budget grows with"
                + " the page size, up to the deployment's maximum page size, and a client that sent"
                + " a value filter can instead page without it and apply the filter to the pages it"
                + " receives");
  }

  /**
   * The other boundary: the same rows and the same absent value, but a page of two, whose budget of
   * forty covers every row. The range is scanned to its end within budget and the answer is the
   * ordinary empty page, with no continuation and no error. The refusal above and this answer
   * cannot be confused, because the refusal is never an empty page.
   */
  @Test
  public void testPagedReverseLookupBelowTheVisitBudgetAnswersAnEmptyPage() {
    TagEntity tag = createTagWithTwoValuesAssignedToTheCatalog("withinvisitbudgettag", "a");
    writeRowsWithValue(tag, 25, "a");

    ListObjectsByTagResponse response =
        handler().listObjectsByTag("withinvisitbudgettag", "b", true, null, 2);

    assertThat(response.getObjects()).isEmpty();
    assertThat(response.getNextPageToken()).isNull();
  }

  /** And a value the rows do carry is answered as a page, budget untouched by the question. */
  @Test
  public void testPagedReverseLookupWithAMatchingValueAnswersThePage() {
    TagEntity tag = createTagWithTwoValuesAssignedToTheCatalog("matchingvaluetag", "a");
    writeRowsWithValue(tag, 25, "a");

    ListObjectsByTagResponse response =
        handler().listObjectsByTag("matchingvaluetag", "a", true, null, 1);

    assertThat(response.getObjects()).hasSize(1);
    assertThat(response.getObjects().iterator().next().getTarget().getType())
        .isEqualTo(TargetType.CATALOG);
    assertThat(response.getNextPageToken()).isNotNull();
  }

  /**
   * The budget covers the whole request, not each read the request makes. Ten hidden rows that sort
   * ahead of the one live assignment take ten persistence reads on a page of one: each read
   * examines two rows, the one it hands back and the lookahead that says another exists, and both
   * are charged, so ten reads spend the budget of twenty to zero. Every read charges the same
   * budget; no single read spends more than two. The page is answered empty with a continuation
   * after the last hidden row, because every row handed back was consumed and the live one is still
   * ahead, and the next page, with a budget of its own, returns it. The budget handed to every read
   * is one instance, seen exhausted at the stop.
   */
  @Test
  public void testABudgetStoppedPageResumesAfterTheLastConsumedCandidate() {
    TagEntity tag = createTagAssignedToTheCatalog("crossbatchtag");
    // Below every generated entity id, so these sort ahead of the live catalog row on every store.
    for (int i = 1; i <= 10; i++) {
      writeRow(tag, -i, "public");
    }
    PolarisMetaStoreManager watched = Mockito.spy(metaStoreManager);
    ArgumentCaptor<CandidateBudget> budgets = ArgumentCaptor.forClass(CandidateBudget.class);

    ListObjectsByTagResponse first =
        handler(watched).listObjectsByTag("crossbatchtag", null, true, null, 1);

    assertThat(first.getObjects()).isEmpty();
    assertThat(first.getNextPageToken()).isNotNull();
    Mockito.verify(watched, Mockito.atLeast(2))
        .loadTargetsOnTag(
            Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), budgets.capture());
    assertThat(budgets.getAllValues()).hasSize(10);
    assertThat(budgets.getAllValues().stream().distinct()).hasSize(1);
    assertThat(budgets.getValue().remaining()).isEqualTo(0);

    ListObjectsByTagResponse second =
        handler().listObjectsByTag("crossbatchtag", null, true, first.getNextPageToken(), 1);

    assertThat(second.getObjects()).hasSize(1);
    assertThat(second.getObjects().iterator().next().getTarget().getType())
        .isEqualTo(TargetType.CATALOG);
    assertThat(second.getNextPageToken()).isNull();
  }

  private TagEntity createTagWithTwoValuesAssignedToTheCatalog(String tagName, String value) {
    grantSetupPrivilege(PolarisPrivilege.CATALOG_MANAGE_CONTENT);
    handler()
        .createTag(
            CreateTagRequest.builder()
                .setName(tagName)
                .setValues(List.of("a", "b"))
                .setTargetTypes(List.of(TargetType.CATALOG))
                .build());
    handler()
        .assignTag(
            tagName, TagAttachmentTarget.builder(TargetType.CATALOG).build(), List.of(value));
    return readTag(tagName);
  }

  private TagEntity createTagAssignedToTheCatalog(String tagName) {
    grantSetupPrivilege(PolarisPrivilege.CATALOG_MANAGE_CONTENT);
    handler()
        .createTag(
            CreateTagRequest.builder()
                .setName(tagName)
                .setValues(List.of("public"))
                .setTargetTypes(List.of(TargetType.CATALOG))
                .build());
    handler()
        .assignTag(
            tagName, TagAttachmentTarget.builder(TargetType.CATALOG).build(), List.of("public"));
    return readTag(tagName);
  }

  private TagEntity readTag(String tagName) {
    return TagEntity.of(
        metaStoreManager
            .readEntityByName(
                polarisContext,
                List.of(PolarisEntity.of(catalogEntity)),
                PolarisEntityType.TAG,
                PolarisEntitySubType.NULL_SUBTYPE,
                tagName)
            .getEntity());
  }

  /**
   * Rows with a stored value, on targets that do not resolve, so only the value filter sees them.
   */
  private void writeRowsWithValue(TagEntity tag, int count, String value) {
    for (int i = 0; i < count; i++) {
      writeRow(tag, 900_000_000L + i, value);
    }
  }

  private void writeRow(TagEntity tag, long targetId, String value) {
    TagAssignmentRecord row = new TagAssignmentRecord();
    row.setTargetCatalogId(catalogEntity.getId());
    row.setTargetId(targetId);
    row.setFieldId(0);
    row.setTagCatalogId(tag.getCatalogId());
    row.setTagId(tag.getId());
    row.setValue(value);
    polarisContext.getMetaStore().writeToTagAssignmentRecords(polarisContext, row);
  }

  /**
   * Rows left behind by a failed best-effort cleanup: the target id resolves to nothing, so each is
   * hidden by the read while still costing it a candidate. The ids are far above any generated
   * entity id, so they sort after the live assignment and cannot push it out of the scan.
   */
  private void writeOrphanRows(TagEntity tag, int count) {
    writeRowsWithValue(tag, count, "public");
  }

  private void createTags(int count) {
    grantSetupPrivilege(PolarisPrivilege.CATALOG_MANAGE_CONTENT);
    for (int i = 0; i < count; i++) {
      handler()
          .createTag(
              CreateTagRequest.builder()
                  .setName("tag" + i)
                  .setValues(List.of("public"))
                  .setTargetTypes(List.of(TargetType.CATALOG))
                  .build());
    }
  }

  private void grantSetupPrivilege(PolarisPrivilege privilege) {
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE2, privilege));
  }

  private TagCatalogHandler handler() {
    return handler(metaStoreManager);
  }

  private TagCatalogHandler handler(PolarisMetaStoreManager manager) {
    return ImmutableTagCatalogHandler.builder()
        .catalogName(CATALOG_NAME)
        .polarisPrincipal(
            PolarisPrincipal.of(
                principalEntity.getName(),
                ImmutableAttributeMap.builder()
                    .put(PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, principalEntity)
                    .build(),
                Set.of(PRINCIPAL_ROLE2)))
        .callContext(callContext)
        .resolutionManifestFactory(resolutionManifestFactory)
        .metaStoreManager(manager)
        .authorizer(polarisAuthorizer)
        .storageAccessConfigProvider(storageAccessConfigProvider)
        .fileIOFactory(fileIOFactory)
        .build();
  }
}
