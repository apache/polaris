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
package org.apache.polaris.service.catalog;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Unit tests for CatalogCaseSensitivityResolver. */
public class CatalogCaseSensitivityResolverTest {

  private PolarisMetaStoreManager metaStoreManager;
  private CallContext callContext;
  private PolarisCallContext polarisCallContext;
  private CatalogCaseSensitivityResolver resolver;

  @BeforeEach
  public void setUp() {
    metaStoreManager = mock(PolarisMetaStoreManager.class);
    callContext = mock(CallContext.class);
    polarisCallContext = mock(PolarisCallContext.class);
    when(callContext.getPolarisCallContext()).thenReturn(polarisCallContext);

    resolver = new CatalogCaseSensitivityResolver(metaStoreManager, callContext);
  }

  @Test
  public void testIsCaseInsensitive_CaseInsensitiveCatalog() {
    // Create a case-insensitive catalog entity
    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setCaseInsensitiveMode(true)
            .build();

    // Mock the metastore to return this catalog
    EntityResult result = mock(EntityResult.class);
    when(result.isSuccess()).thenReturn(true);
    when(result.getEntity()).thenReturn(catalog);

    when(metaStoreManager.readEntityByName(
            polarisCallContext,
            null,
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            "test_catalog"))
        .thenReturn(result);

    // Test
    boolean isCaseInsensitive = resolver.isCaseInsensitive("test_catalog");

    Assertions.assertThat(isCaseInsensitive)
        .as("Case-insensitive catalog should return true")
        .isTrue();
  }

  @Test
  public void testIsCaseInsensitive_CaseSensitiveCatalog() {
    // Create a case-sensitive catalog entity (default)
    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setCaseInsensitiveMode(false)
            .build();

    // Mock the metastore to return this catalog
    EntityResult result = mock(EntityResult.class);
    when(result.isSuccess()).thenReturn(true);
    when(result.getEntity()).thenReturn(catalog);

    when(metaStoreManager.readEntityByName(
            polarisCallContext,
            null,
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            "test_catalog"))
        .thenReturn(result);

    // Test
    boolean isCaseInsensitive = resolver.isCaseInsensitive("test_catalog");

    Assertions.assertThat(isCaseInsensitive)
        .as("Case-sensitive catalog should return false")
        .isFalse();
  }

  @Test
  public void testIsCaseInsensitive_CatalogWithoutExplicitSetting() {
    // Create a catalog without explicit case-insensitive setting (defaults to false)
    CatalogEntity catalog = new CatalogEntity.Builder().setName("test_catalog").build();

    // Mock the metastore to return this catalog
    EntityResult result = mock(EntityResult.class);
    when(result.isSuccess()).thenReturn(true);
    when(result.getEntity()).thenReturn(catalog);

    when(metaStoreManager.readEntityByName(
            polarisCallContext,
            null,
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            "test_catalog"))
        .thenReturn(result);

    // Test
    boolean isCaseInsensitive = resolver.isCaseInsensitive("test_catalog");

    Assertions.assertThat(isCaseInsensitive)
        .as("Catalog without explicit setting should default to case-sensitive (false)")
        .isFalse();
  }

  @Test
  public void testIsCaseInsensitive_CatalogNotFound() {
    // Mock the metastore to return a failed result
    EntityResult result = mock(EntityResult.class);
    when(result.isSuccess()).thenReturn(false);

    when(metaStoreManager.readEntityByName(
            polarisCallContext,
            null,
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            "nonexistent_catalog"))
        .thenReturn(result);

    // Test
    boolean isCaseInsensitive = resolver.isCaseInsensitive("nonexistent_catalog");

    Assertions.assertThat(isCaseInsensitive)
        .as("Non-existent catalog should default to case-sensitive (false)")
        .isFalse();
  }

  @Test
  public void testIsCaseInsensitive_NullEntity() {
    // Mock the metastore to return success but null entity
    EntityResult result = mock(EntityResult.class);
    when(result.isSuccess()).thenReturn(true);
    when(result.getEntity()).thenReturn(null);

    when(metaStoreManager.readEntityByName(
            polarisCallContext,
            null,
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            "test_catalog"))
        .thenReturn(result);

    // Test
    boolean isCaseInsensitive = resolver.isCaseInsensitive("test_catalog");

    Assertions.assertThat(isCaseInsensitive)
        .as("Null entity should default to case-sensitive (false)")
        .isFalse();
  }

  @Test
  public void testIsCaseInsensitive_DroppedCatalog() {
    // Create a dropped catalog entity
    PolarisBaseEntity droppedEntity = mock(PolarisBaseEntity.class);
    when(droppedEntity.getType()).thenReturn(PolarisEntityType.CATALOG);
    when(droppedEntity.getSubType()).thenReturn(PolarisEntitySubType.NULL_SUBTYPE);
    when(droppedEntity.isDropped()).thenReturn(true);

    // Mock the metastore to return this dropped entity
    EntityResult result = mock(EntityResult.class);
    when(result.isSuccess()).thenReturn(true);
    when(result.getEntity()).thenReturn(droppedEntity);

    when(metaStoreManager.readEntityByName(
            polarisCallContext,
            null,
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            "dropped_catalog"))
        .thenReturn(result);

    // Test
    boolean isCaseInsensitive = resolver.isCaseInsensitive("dropped_catalog");

    Assertions.assertThat(isCaseInsensitive)
        .as("Dropped catalog should default to case-sensitive (false)")
        .isFalse();
  }

  @Test
  public void testIsCaseInsensitive_NullCatalogName() {
    // Test with null catalog name
    boolean isCaseInsensitive = resolver.isCaseInsensitive(null);

    Assertions.assertThat(isCaseInsensitive)
        .as("Null catalog name should default to case-sensitive (false)")
        .isFalse();

    // Verify metastore was never called
    Mockito.verifyNoInteractions(metaStoreManager);
  }

  @Test
  public void testIsCaseInsensitive_EmptyCatalogName() {
    // Test with empty catalog name
    boolean isCaseInsensitive = resolver.isCaseInsensitive("");

    Assertions.assertThat(isCaseInsensitive)
        .as("Empty catalog name should default to case-sensitive (false)")
        .isFalse();

    // Verify metastore was never called
    Mockito.verifyNoInteractions(metaStoreManager);
  }

  @Test
  public void testIsCaseInsensitive_MultipleCalls() {
    // Create a case-insensitive catalog entity
    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .setName("test_catalog")
            .setCaseInsensitiveMode(true)
            .build();

    // Mock the metastore to return this catalog
    EntityResult result = mock(EntityResult.class);
    when(result.isSuccess()).thenReturn(true);
    when(result.getEntity()).thenReturn(catalog);

    when(metaStoreManager.readEntityByName(
            polarisCallContext,
            null,
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            "test_catalog"))
        .thenReturn(result);

    // Test multiple calls
    boolean firstCall = resolver.isCaseInsensitive("test_catalog");
    boolean secondCall = resolver.isCaseInsensitive("test_catalog");

    Assertions.assertThat(firstCall)
        .as("First call should return true")
        .isTrue();
    Assertions.assertThat(secondCall)
        .as("Second call should return true")
        .isTrue();

    // Verify metastore was called twice (no caching in the resolver itself)
    Mockito.verify(metaStoreManager, Mockito.times(2))
        .readEntityByName(
            polarisCallContext,
            null,
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            "test_catalog");
  }

  @Test
  public void testIsCaseInsensitive_DifferentCatalogs() {
    // Create two catalogs with different case-sensitivity settings
    CatalogEntity catalog1 =
        new CatalogEntity.Builder()
            .setName("case_insensitive_catalog")
            .setCaseInsensitiveMode(true)
            .build();

    CatalogEntity catalog2 =
        new CatalogEntity.Builder()
            .setName("case_sensitive_catalog")
            .setCaseInsensitiveMode(false)
            .build();

    // Mock the metastore for the first catalog
    EntityResult result1 = mock(EntityResult.class);
    when(result1.isSuccess()).thenReturn(true);
    when(result1.getEntity()).thenReturn(catalog1);

    when(metaStoreManager.readEntityByName(
            polarisCallContext,
            null,
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            "case_insensitive_catalog"))
        .thenReturn(result1);

    // Mock the metastore for the second catalog
    EntityResult result2 = mock(EntityResult.class);
    when(result2.isSuccess()).thenReturn(true);
    when(result2.getEntity()).thenReturn(catalog2);

    when(metaStoreManager.readEntityByName(
            polarisCallContext,
            null,
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            "case_sensitive_catalog"))
        .thenReturn(result2);

    // Test both catalogs
    boolean catalog1Result = resolver.isCaseInsensitive("case_insensitive_catalog");
    boolean catalog2Result = resolver.isCaseInsensitive("case_sensitive_catalog");

    Assertions.assertThat(catalog1Result)
        .as("case_insensitive_catalog should return true")
        .isTrue();
    Assertions.assertThat(catalog2Result)
        .as("case_sensitive_catalog should return false")
        .isFalse();
  }
}
