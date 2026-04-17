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
package org.apache.polaris.service.catalog.common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CatalogUtilsTest {

  private static final String ALLOWED_LOCATION = "s3://my-bucket/path/to/data";
  private static final Namespace NS = Namespace.of("ns");
  private static final TableIdentifier TABLE = TableIdentifier.of(NS, "t1");

  private RealmConfig realmConfig;
  private PolarisResolvedPathWrapper resolvedPathWrapper;

  @BeforeEach
  void setUp() {
    realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(any(PolarisConfiguration.class))).thenReturn(false);
    when(realmConfig.getConfig(any(PolarisConfiguration.class), any(CatalogEntity.class)))
        .thenReturn(false);
    when(realmConfig.getConfig(
            eq(FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION), any(CatalogEntity.class)))
        .thenReturn(true);

    AwsStorageConfigurationInfo storageConfig =
        AwsStorageConfigurationInfo.builder()
            .roleARN("arn:aws:iam::012345678901:role/testrole")
            .addAllowedLocation(ALLOWED_LOCATION)
            .build();

    PolarisEntity catalogEntity =
        new CatalogEntity.Builder()
            .setName("test-catalog")
            .setId(1L)
            .setCatalogId(1L)
            .setParentId(0L)
            .setCreateTimestamp(System.currentTimeMillis())
            .setCatalogType(Catalog.TypeEnum.INTERNAL.name())
            .setInternalProperties(
                Map.of(
                    PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                    storageConfig.serialize(),
                    CatalogEntity.CATALOG_TYPE_PROPERTY,
                    Catalog.TypeEnum.INTERNAL.name()))
            .setProperties(Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, ALLOWED_LOCATION))
            .build();

    PolarisEntity nsEntity =
        new PolarisEntity.Builder()
            .setName("ns")
            .setType(PolarisEntityType.NAMESPACE)
            .setId(2L)
            .setCatalogId(1L)
            .setParentId(1L)
            .setCreateTimestamp(System.currentTimeMillis())
            .build();

    List<ResolvedPolarisEntity> resolvedPath =
        List.of(
            new ResolvedPolarisEntity(catalogEntity, null, null),
            new ResolvedPolarisEntity(nsEntity, null, null));
    resolvedPathWrapper = new PolarisResolvedPathWrapper(resolvedPath);
  }

  @Test
  void testValidateLocationsForTableLike_validLocation() {
    Assertions.assertThatCode(
            () ->
                CatalogUtils.validateLocationsForTableLike(
                    realmConfig, TABLE, Set.of(ALLOWED_LOCATION + "/ns/t1"), resolvedPathWrapper))
        .doesNotThrowAnyException();
  }

  @Test
  void testValidateLocationsForTableLike_locationOutsideAllowed() {
    Assertions.assertThatThrownBy(
            () ->
                CatalogUtils.validateLocationsForTableLike(
                    realmConfig, TABLE, Set.of("s3://other-bucket/path"), resolvedPathWrapper))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void testValidateLocationsForTableLike_fileLocationRejected() {
    Assertions.assertThatThrownBy(
            () ->
                CatalogUtils.validateLocationsForTableLike(
                    realmConfig, TABLE, Set.of("file:///tmp/data"), resolvedPathWrapper))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void testValidateLocationsForTableLike_multipleLocationsOneInvalid() {
    Assertions.assertThatThrownBy(
            () ->
                CatalogUtils.validateLocationsForTableLike(
                    realmConfig,
                    TABLE,
                    Set.of(ALLOWED_LOCATION + "/ns/t1", "s3://unauthorized/path"),
                    resolvedPathWrapper))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void testValidateLocationForTableLike_resolvesViaTableLikePath() {
    PolarisResolutionManifestCatalogView entityView = mock();
    when(entityView.getResolvedPath(
            eq(ResolvedPathKey.ofTableLike(TABLE)), eq(PolarisEntitySubType.ANY_SUBTYPE)))
        .thenReturn(resolvedPathWrapper);

    Assertions.assertThatCode(
            () ->
                CatalogUtils.validateLocationForTableLike(
                    entityView, realmConfig, TABLE, ALLOWED_LOCATION + "/ns/t1"))
        .doesNotThrowAnyException();
  }

  @Test
  void testValidateLocationForTableLike_fallsBackToNamespacePath() {
    PolarisResolutionManifestCatalogView entityView = mock();
    when(entityView.getResolvedPath(
            eq(ResolvedPathKey.ofTableLike(TABLE)), eq(PolarisEntitySubType.ANY_SUBTYPE)))
        .thenReturn(null);
    when(entityView.getResolvedPath(ResolvedPathKey.ofNamespace(NS)))
        .thenReturn(resolvedPathWrapper);

    Assertions.assertThatCode(
            () ->
                CatalogUtils.validateLocationForTableLike(
                    entityView, realmConfig, TABLE, ALLOWED_LOCATION + "/ns/t1"))
        .doesNotThrowAnyException();
  }

  @Test
  void testValidateLocationForTableLike_fallsBackToPassthroughPath() {
    PolarisResolutionManifestCatalogView entityView = mock();
    when(entityView.getResolvedPath(any(), any())).thenReturn(null);
    when(entityView.getResolvedPath(any(ResolvedPathKey.class))).thenReturn(null);
    when(entityView.getPassthroughResolvedPath(ResolvedPathKey.ofNamespace(NS)))
        .thenReturn(resolvedPathWrapper);

    Assertions.assertThatCode(
            () ->
                CatalogUtils.validateLocationForTableLike(
                    entityView, realmConfig, TABLE, ALLOWED_LOCATION + "/ns/t1"))
        .doesNotThrowAnyException();
  }

  @Test
  void testValidateLocationForTableLike_rejectsInvalidLocation() {
    PolarisResolutionManifestCatalogView entityView = mock();
    when(entityView.getResolvedPath(
            eq(ResolvedPathKey.ofTableLike(TABLE)), eq(PolarisEntitySubType.ANY_SUBTYPE)))
        .thenReturn(resolvedPathWrapper);

    Assertions.assertThatThrownBy(
            () ->
                CatalogUtils.validateLocationForTableLike(
                    entityView, realmConfig, TABLE, "s3://unauthorized-bucket/data"))
        .isInstanceOf(ForbiddenException.class);
  }
}
