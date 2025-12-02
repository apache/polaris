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

import static java.lang.String.format;

import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogBaseObj;

/** Base class for mapping objects contained in a catalog. */
abstract class BaseCatalogMapping<O extends CatalogBaseObj, B extends CatalogBaseObj.Builder<O, B>>
    extends BaseMapping<O, B> {
  BaseCatalogMapping(
      @Nonnull ObjType objType,
      @Nonnull ObjType containerObjType,
      @Nonnull String refName,
      @Nonnull PolarisEntityType entityType) {
    super(objType, containerObjType, refName, entityType);
  }

  BaseCatalogMapping(
      @Nonnull Class<? extends CatalogBaseObj> baseObjTypeClass,
      @Nonnull Map<PolarisEntitySubType, ObjType> subTypes,
      @Nonnull ObjType containerObjType,
      @Nonnull String refName,
      @Nonnull PolarisEntityType entityType) {
    super(baseObjTypeClass, subTypes, containerObjType, refName, entityType);
  }

  @Override
  public void checkCatalogId(long catalogId) {
    EntityObjMappings.checkCatalogId(catalogId);
  }

  @Override
  public @Nonnull String refNameForCatalog(long catalogId) {
    checkCatalogId(catalogId);
    return format(refName, catalogId);
  }

  @Override
  public long fixCatalogId(long catalogId) {
    checkCatalogId(catalogId);
    return catalogId;
  }
}
