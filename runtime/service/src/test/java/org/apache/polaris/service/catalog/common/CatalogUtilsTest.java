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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class CatalogUtilsTest {

  @ParameterizedTest
  @ValueSource(strings = {"file:/tmp/test", "http://example.com/table"})
  public void testRejectLocationsWhenFileStorageDisabled(String location) {
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES))
        .thenReturn(List.of("S3"));

    PolarisResolvedPathWrapper resolvedStorageEntity = mock(PolarisResolvedPathWrapper.class);
    when(resolvedStorageEntity.getRawFullPath()).thenReturn(List.of());

    Assertions.assertThatThrownBy(
            () ->
                CatalogUtils.validateLocationsForTableLike(
                    realmConfig,
                    TableIdentifier.of("ns", "tbl"),
                    Set.of(location),
                    resolvedStorageEntity))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("File locations are not allowed");
  }
}
