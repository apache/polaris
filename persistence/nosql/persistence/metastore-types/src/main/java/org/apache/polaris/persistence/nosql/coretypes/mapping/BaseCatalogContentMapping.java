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

import java.util.Map;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogBaseObj;
import org.apache.polaris.persistence.nosql.coretypes.content.ContentObj;
import org.jspecify.annotations.NonNull;

/** Base class for mapping objects that are catalog content, aka tables, views and policies. */
abstract class BaseCatalogContentMapping<
        O extends ContentObj & CatalogBaseObj,
        B extends ContentObj.Builder<O, B> & CatalogBaseObj.Builder<O, B>>
    extends BaseCatalogMapping<O, B> {
  BaseCatalogContentMapping(
      @NonNull ObjType objType,
      @NonNull ObjType containerObjType,
      @NonNull String refName,
      @NonNull PolarisEntityType entityType) {
    super(objType, containerObjType, refName, entityType);
  }

  BaseCatalogContentMapping(
      @NonNull Class<? extends ContentObj> baseObjTypeClass,
      @NonNull Map<PolarisEntitySubType, ObjType> subTypes,
      @NonNull ObjType containerObjType,
      @NonNull String refName,
      @NonNull PolarisEntityType entityType) {
    super(baseObjTypeClass, subTypes, containerObjType, refName, entityType);
  }

  @Override
  public boolean catalogContent() {
    return true;
  }
}
