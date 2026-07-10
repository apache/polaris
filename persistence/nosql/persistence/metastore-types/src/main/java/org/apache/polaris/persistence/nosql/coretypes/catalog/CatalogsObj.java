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
 * Maintains the mapping of all catalogs by name to {@link CatalogObj}s. The current version of this
 * object is maintained via the reference {@value #CATALOGS_REF_NAME}.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableCatalogsObj.class)
@JsonDeserialize(as = ImmutableCatalogsObj.class)
public interface CatalogsObj extends ContainerObj {

  String CATALOGS_REF_NAME = "catalogs";

  ObjType TYPE = new CatalogsObjType();

  /** Mapping of catalog names to catalog objects. */
  @Override
  IndexContainer<ObjRef> nameToObjRef();

  // overridden only for posterity, no technical reason
  @Override
  IndexContainer<IndexKey> stableIdToName();

  @Override
  default ObjType type() {
    return TYPE;
  }

  static ImmutableCatalogsObj.Builder builder() {
    return ImmutableCatalogsObj.builder();
  }

  final class CatalogsObjType extends AbstractObjType<CatalogsObj> {
    public CatalogsObjType() {
      super("cats", "Catalogs", CatalogsObj.class);
    }
  }

  interface Builder extends ContainerObj.Builder<CatalogsObj, Builder> {}
}
