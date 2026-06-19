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
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.acl.GrantsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.RealmGrantsObj;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

/**
 * Maintains the state of all catalog grants. The current version of this object is maintained via
 * the reference name pattern {@value RealmGrantsObj#REALM_GRANTS_REF_NAME}, where {@code %d} is to
 * be replaced with the catalog's {@linkplain CatalogObj#stableId() stable ID}.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableCatalogGrantsObj.class)
@JsonDeserialize(as = ImmutableCatalogGrantsObj.class)
public interface CatalogGrantsObj extends GrantsObj {

  ObjType TYPE = new CatalogGrantsObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  static ImmutableCatalogGrantsObj.Builder builder() {
    return ImmutableCatalogGrantsObj.builder();
  }

  final class CatalogGrantsObjType extends AbstractObjType<CatalogGrantsObj> {
    public CatalogGrantsObjType() {
      super("cat-gts", "Catalog Grants", CatalogGrantsObj.class);
    }
  }

  interface Builder extends GrantsObj.Builder<CatalogGrantsObj, Builder> {}
}
