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

import static org.apache.polaris.core.entity.PolarisEntitySubType.GENERIC_TABLE;
import static org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_TABLE;
import static org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_VIEW;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.METADATA_LOCATION_KEY;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN;

import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.content.GenericTableObj;
import org.apache.polaris.persistence.nosql.coretypes.content.IcebergTableObj;
import org.apache.polaris.persistence.nosql.coretypes.content.IcebergViewObj;
import org.apache.polaris.persistence.nosql.coretypes.content.TableLikeObj;

final class TableLikeMapping<O extends TableLikeObj, B extends TableLikeObj.Builder<O, B>>
    extends BaseCatalogContentMapping<O, B> {
  TableLikeMapping() {
    super(
        TableLikeObj.class,
        Map.of(
            ICEBERG_TABLE, IcebergTableObj.TYPE,
            ICEBERG_VIEW, IcebergViewObj.TYPE,
            GENERIC_TABLE, GenericTableObj.TYPE),
        CatalogStateObj.TYPE,
        CATALOG_STATE_REF_NAME_PATTERN,
        PolarisEntityType.TABLE_LIKE);
  }

  @Override
  public B newObjBuilder(@Nonnull PolarisEntitySubType subType) {
    return cast(
        switch (subType) {
          case ICEBERG_TABLE -> IcebergTableObj.builder();
          case ICEBERG_VIEW -> IcebergViewObj.builder();
          case GENERIC_TABLE -> GenericTableObj.builder();
          default -> throw new IllegalArgumentException("Unknown or invalid subtype");
        });
  }

  @Override
  void mapToObjTypeSpecific(
      B baseBuilder,
      @Nonnull PolarisBaseEntity entity,
      Optional<PolarisPrincipalSecrets> principalSecrets,
      Map<String, String> properties,
      Map<String, String> internalProperties) {
    super.mapToObjTypeSpecific(
        baseBuilder, entity, principalSecrets, properties, internalProperties);
    switch (baseBuilder) {
      case IcebergTableObj.Builder ignored -> {}
      case IcebergViewObj.Builder ignored -> {}
      case GenericTableObj.Builder genericTableObjBuilder ->
          genericTableObjBuilder
              .format(Optional.ofNullable(internalProperties.remove(GenericTableEntity.FORMAT_KEY)))
              .doc(Optional.ofNullable(internalProperties.remove(GenericTableEntity.DOC_KEY)));
      default -> throw new IllegalArgumentException("Unknown or invalid subtype");
    }

    Optional.ofNullable(internalProperties.remove(METADATA_LOCATION_KEY))
        .ifPresent(baseBuilder::metadataLocation);
  }

  @Override
  void mapToEntityTypeSpecific(
      O o,
      HashMap<String, String> properties,
      HashMap<String, String> internalProperties,
      PolarisEntitySubType subType) {
    super.mapToEntityTypeSpecific(o, properties, internalProperties, subType);
    o.metadataLocation().ifPresent(v -> internalProperties.put(METADATA_LOCATION_KEY, v));
    switch (o) {
      case IcebergTableObj ignored -> {}
      case IcebergViewObj ignored -> {}
      case GenericTableObj genericTableObj -> {
        genericTableObj
            .format()
            .ifPresent(v -> internalProperties.put(GenericTableEntity.FORMAT_KEY, v));
        genericTableObj.doc().ifPresent(v -> internalProperties.put(GenericTableEntity.DOC_KEY, v));
      }
      default ->
          throw new IllegalStateException(
              "Cannot map "
                  + o.type().targetClass().getSimpleName()
                  + " ("
                  + o.type().name()
                  + ") to a PolarisEntity");
    }
  }
}
