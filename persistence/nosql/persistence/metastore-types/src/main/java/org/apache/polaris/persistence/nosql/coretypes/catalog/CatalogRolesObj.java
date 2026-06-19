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
package org.apache.polaris.persistence.nosql.coretypes.catalog;

import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

/**
 * Maintains the state of all catalog roles. The current version of this object is maintained via
 * the reference name pattern {@value #CATALOG_ROLES_REF_NAME_PATTERN}, where {@code %d} is to be
 * replaced with the catalog's {@linkplain CatalogObj#stableId() stable ID}.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableCatalogRolesObj.class)
@JsonDeserialize(as = ImmutableCatalogRolesObj.class)
public interface CatalogRolesObj extends ContainerObj {

  String CATALOG_ROLES_REF_NAME_PATTERN = "cat/%d/roles";

  ObjType TYPE = new CatalogRolesObjType();

  /** Mapping of catalog role names to catalog role objects. */
  @Override
  IndexContainer<ObjRef> nameToObjRef();

  // overridden only for posterity, no technical reason
  @Override
  IndexContainer<IndexKey> stableIdToName();

  @Override
  default ObjType type() {
    return TYPE;
  }

  static ImmutableCatalogRolesObj.Builder builder() {
    return ImmutableCatalogRolesObj.builder();
  }

  final class CatalogRolesObjType extends AbstractObjType<CatalogRolesObj> {
    public CatalogRolesObjType() {
      super("cat-rls", "Catalog Roles", CatalogRolesObj.class);
    }
  }

  interface Builder extends ContainerObj.Builder<CatalogRolesObj, Builder> {}
}
