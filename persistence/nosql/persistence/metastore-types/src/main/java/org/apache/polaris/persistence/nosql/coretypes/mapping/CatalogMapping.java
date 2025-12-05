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

package org.apache.polaris.persistence.nosql.coretypes.mapping;

import static org.apache.polaris.core.entity.CatalogEntity.CATALOG_TYPE_PROPERTY;
import static org.apache.polaris.core.entity.CatalogEntity.DEFAULT_BASE_LOCATION_KEY;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj.CATALOGS_REF_NAME;

import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStatus;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogType;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj;

final class CatalogMapping extends BaseMapping<CatalogObj, CatalogObj.Builder> {
  CatalogMapping() {
    super(CatalogObj.TYPE, CatalogsObj.TYPE, CATALOGS_REF_NAME, PolarisEntityType.CATALOG);
  }

  @Override
  public CatalogObj.Builder newObjBuilder(@Nonnull PolarisEntitySubType subType) {
    return CatalogObj.builder();
  }

  @Override
  void mapToObjTypeSpecific(
      CatalogObj.Builder baseBuilder,
      @Nonnull PolarisBaseEntity entity,
      Optional<PolarisPrincipalSecrets> principalSecrets,
      Map<String, String> properties,
      Map<String, String> internalProperties) {
    super.mapToObjTypeSpecific(
        baseBuilder, entity, principalSecrets, properties, internalProperties);
    var defaultBaseLocation = properties.remove(DEFAULT_BASE_LOCATION_KEY);
    var catalogTypeString = internalProperties.remove(CATALOG_TYPE_PROPERTY);
    if (catalogTypeString == null) {
      catalogTypeString = CatalogType.INTERNAL.name();
    }
    var catalogType =
        switch (catalogTypeString.toUpperCase(Locale.ROOT)) {
          case "INTERNAL" -> CatalogType.INTERNAL;
          case "EXTERNAL" -> CatalogType.EXTERNAL;
          default ->
              throw new IllegalArgumentException("Invalid catalog type " + catalogTypeString);
        };
    baseBuilder
        .catalogType(catalogType)
        .defaultBaseLocation(Optional.ofNullable(defaultBaseLocation))
        .status(CatalogStatus.ACTIVE);
  }

  @Override
  void mapToEntityTypeSpecific(
      CatalogObj o,
      HashMap<String, String> properties,
      HashMap<String, String> internalProperties,
      PolarisEntitySubType subType) {
    super.mapToEntityTypeSpecific(o, properties, internalProperties, subType);
    internalProperties.put(CATALOG_TYPE_PROPERTY, o.catalogType().name());
    o.defaultBaseLocation().ifPresent(v -> properties.put(DEFAULT_BASE_LOCATION_KEY, v));
  }
}
