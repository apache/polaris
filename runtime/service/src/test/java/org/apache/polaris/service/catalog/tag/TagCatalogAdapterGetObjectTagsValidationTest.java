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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.UriInfo;
import java.util.Set;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.types.TargetType;
import org.junit.jupiter.api.Test;

/**
 * Pins the getObjectTags query-parameter checks that cannot be answered from the bound arguments at
 * all, only from the raw query string carried by UriInfo: an empty single-value String parameter
 * has already been collapsed to null by the container before the adapter sees it, and only the
 * first value of a repeated parameter survives that same binding.
 */
public class TagCatalogAdapterGetObjectTagsValidationTest {

  private TagCatalogAdapter newAdapter(UriInfo uriInfo) {
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_TAG_STORE)).thenReturn(true);
    PolarisMetaStoreManager metaStoreManager = mock(PolarisMetaStoreManager.class);
    when(metaStoreManager.supportsEntityType(PolarisEntityType.TAG)).thenReturn(true);
    TagCatalogAdapter adapter =
        new TagCatalogAdapter(
            mock(CatalogPrefixParser.class),
            mock(TagCatalogHandlerFactory.class),
            realmConfig,
            metaStoreManager);
    // The adapter takes UriInfo by @Context injection, which no container performs here, so the
    // field is set directly. It is package-private and this test is in the adapter's own package.
    adapter.uriInfo = uriInfo;
    return adapter;
  }

  /**
   * A security context carrying a real principal. Authentication is answered before parameter
   * validation, which is the contract's own order (401 precedes 400), so a context the adapter
   * cannot authenticate would hide every verdict this class is about.
   */
  private static SecurityContext authenticatedSecurityContext() {
    SecurityContext securityContext = mock(SecurityContext.class);
    when(securityContext.getUserPrincipal())
        .thenReturn(
            PolarisPrincipal.of(
                "test-principal", ImmutableAttributeMap.builder().build(), Set.of()));
    return securityContext;
  }

  private UriInfo uriInfoWithRawQueryParams(MultivaluedMap<String, String> rawQueryParams) {
    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getQueryParameters()).thenReturn(rawQueryParams);
    return uriInfo;
  }

  @Test
  public void testEmptyNamespaceCollapsedToNullIsStillRejected() {
    // The bound "namespace" parameter arrives as null (the container's own collapse of "" to
    // null), but the raw query string still shows the key present with an empty value.
    MultivaluedMap<String, String> rawQueryParams = new MultivaluedHashMap<>();
    rawQueryParams.putSingle("namespace", "");
    TagCatalogAdapter adapter = newAdapter(uriInfoWithRawQueryParams(rawQueryParams));

    assertThatThrownBy(
            () ->
                adapter.getObjectTags(
                    "cat",
                    TargetType.NAMESPACE,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    authenticatedSecurityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("namespace");
  }

  @Test
  public void testEmptyTargetNameCollapsedToNullIsStillRejected() {
    MultivaluedMap<String, String> rawQueryParams = new MultivaluedHashMap<>();
    rawQueryParams.putSingle("namespace", "NS1");
    rawQueryParams.putSingle("target-name", "");
    TagCatalogAdapter adapter = newAdapter(uriInfoWithRawQueryParams(rawQueryParams));

    assertThatThrownBy(
            () ->
                adapter.getObjectTags(
                    "cat",
                    TargetType.TABLE,
                    null,
                    "NS1",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    authenticatedSecurityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("target-name");
  }

  @Test
  public void testEmptyColumnCollapsedToNullIsStillRejected() {
    MultivaluedMap<String, String> rawQueryParams = new MultivaluedHashMap<>();
    rawQueryParams.putSingle("namespace", "NS1");
    rawQueryParams.putSingle("target-name", "T1");
    rawQueryParams.putSingle("column", "");
    TagCatalogAdapter adapter = newAdapter(uriInfoWithRawQueryParams(rawQueryParams));

    assertThatThrownBy(
            () ->
                adapter.getObjectTags(
                    "cat",
                    TargetType.COLUMN,
                    null,
                    "NS1",
                    "T1",
                    null,
                    null,
                    null,
                    null,
                    null,
                    authenticatedSecurityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("column");
  }

  @Test
  public void testAbsentNamespaceIsNotConfusedWithPresentButEmpty() {
    // No "namespace" key at all in the raw query string: a genuinely absent parameter must not
    // be rejected the way a present-but-empty one is.
    MultivaluedMap<String, String> rawQueryParams = new MultivaluedHashMap<>();
    TagCatalogAdapter adapter = newAdapter(uriInfoWithRawQueryParams(rawQueryParams));

    // The catalog-level target (no namespace, no target-name, no column) is accepted; it fails
    // later only on principal validation (the mock SecurityContext has no user principal), well
    // past the parameter check this test pins.
    assertThatThrownBy(
            () ->
                adapter.getObjectTags(
                    "cat",
                    TargetType.CATALOG,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    authenticatedSecurityContext()))
        .isNotInstanceOf(BadRequestException.class);
  }

  @Test
  public void testDuplicateNamespaceIsRejected() {
    // Each of these parameters names one thing, so a repeated one is a malformed request rather
    // than a choice for the server to make. The container keeps only the first value, so the raw
    // query string is the one place the repetition still shows.
    MultivaluedMap<String, String> rawQueryParams = new MultivaluedHashMap<>();
    rawQueryParams.addAll("namespace", "sales", "eu");
    TagCatalogAdapter adapter = newAdapter(uriInfoWithRawQueryParams(rawQueryParams));

    assertThatThrownBy(
            () ->
                adapter.getObjectTags(
                    "cat",
                    TargetType.NAMESPACE,
                    null,
                    "sales",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    authenticatedSecurityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("namespace");
  }

  @Test
  public void testDuplicateTargetNameIsRejected() {
    MultivaluedMap<String, String> rawQueryParams = new MultivaluedHashMap<>();
    rawQueryParams.putSingle("namespace", "NS1");
    rawQueryParams.addAll("target-name", "T1", "T2");
    TagCatalogAdapter adapter = newAdapter(uriInfoWithRawQueryParams(rawQueryParams));

    assertThatThrownBy(
            () ->
                adapter.getObjectTags(
                    "cat",
                    TargetType.TABLE,
                    null,
                    "NS1",
                    "T1",
                    null,
                    null,
                    null,
                    null,
                    null,
                    authenticatedSecurityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("target-name");
  }

  @Test
  public void testDuplicateColumnIsRejected() {
    MultivaluedMap<String, String> rawQueryParams = new MultivaluedHashMap<>();
    rawQueryParams.putSingle("namespace", "NS1");
    rawQueryParams.putSingle("target-name", "T1");
    rawQueryParams.addAll("column", "email", "phone");
    TagCatalogAdapter adapter = newAdapter(uriInfoWithRawQueryParams(rawQueryParams));

    assertThatThrownBy(
            () ->
                adapter.getObjectTags(
                    "cat",
                    TargetType.COLUMN,
                    null,
                    "NS1",
                    "T1",
                    "email",
                    null,
                    null,
                    null,
                    null,
                    authenticatedSecurityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("column");
  }

  @Test
  public void testDuplicateViewIsRejected() {
    MultivaluedMap<String, String> rawQueryParams = new MultivaluedHashMap<>();
    rawQueryParams.addAll("view", "direct", "effective");
    TagCatalogAdapter adapter = newAdapter(uriInfoWithRawQueryParams(rawQueryParams));

    assertThatThrownBy(
            () ->
                adapter.getObjectTags(
                    "cat",
                    TargetType.CATALOG,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "direct",
                    null,
                    authenticatedSecurityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("view");
  }

  @Test
  public void testEmptyViewCollapsedToNullIsStillRejected() {
    // "?view=" is present and empty, and the container hands the adapter null for it, which is
    // also what an absent view looks like. Absent means direct; present and empty is malformed,
    // and only the raw query string still tells the two apart.
    MultivaluedMap<String, String> rawQueryParams = new MultivaluedHashMap<>();
    rawQueryParams.putSingle("view", "");
    TagCatalogAdapter adapter = newAdapter(uriInfoWithRawQueryParams(rawQueryParams));

    assertThatThrownBy(
            () ->
                adapter.getObjectTags(
                    "cat",
                    TargetType.CATALOG,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    authenticatedSecurityContext()))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("view");
  }
}
