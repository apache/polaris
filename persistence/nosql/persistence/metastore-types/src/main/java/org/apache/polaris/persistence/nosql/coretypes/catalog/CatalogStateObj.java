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

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.changes.Change;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

/**
 * Holds the state of all catalog entities. The current version of this object is maintained via the
 * reference name pattern {@value #CATALOG_STATE_REF_NAME_PATTERN}, where {@code %d} is to be
 * replaced with the catalog's {@linkplain CatalogObj#stableId() stable ID}.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableCatalogStateObj.class)
@JsonDeserialize(as = ImmutableCatalogStateObj.class)
public interface CatalogStateObj extends ContainerObj {

  String CATALOG_STATE_REF_NAME_PATTERN = "cat/%d/heads/main";

  ObjType TYPE = new CatalogStatesObjType();

  /**
   * Mapping of all entity names to catalog entities like namespaces, tables or views, which are
   * accessible from this commit.
   *
   * <p>Think of this index as a {@code Map<}{@link IndexKey IndexKey}{@code ,}{@link ObjRef}{@code
   * >}.
   *
   * <p>This index represents the current state of the catalog without any information about the
   * change(s) that led to this state. Change information is maintained {@linkplain #changes()
   * separately}.
   *
   * <p>Maintaining the {@linkplain #changes() changes} separately has the advantage that the
   * storage needed for this index is not "wasted" with usually unneeded change information.
   */
  @Override
  IndexContainer<ObjRef> nameToObjRef();

  // overridden only for posterity, no technical reason
  @Override
  IndexContainer<IndexKey> stableIdToName();

  /**
   * Contains detailed information about the changes performed in this particular commit.
   *
   * <p>The index here is used to provide literally only the changes, rather a logical
   * representation than an exact 1:1 mapping of a {@link IndexKey} to some change.
   *
   * <p>There is no guarantee that all technically changed keys are mentioned individually in this
   * index. Whether a change is contained depends on functional, not technical requirements.
   *
   * <p>Changes that affect many entities (bulk updates) may also be only recorded once. For
   * example, renaming a namespace with implicit rename of the contained entities might only be
   * mentioned once for the rename of the namespace, not for all contained entities.
   */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<IndexContainer<Change>> changes();

  /**
   * Index of base-locations to {@link ObjBase#stableId() entity-IDs}. There can be multiple IDs for
   * a single location if overlapping locations are allowed.
   */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<IndexContainer<EntityIdSet>> locations();

  @Override
  default ObjType type() {
    return TYPE;
  }

  static ImmutableCatalogStateObj.Builder builder() {
    return ImmutableCatalogStateObj.builder();
  }

  final class CatalogStatesObjType extends AbstractObjType<CatalogStateObj> {
    public CatalogStatesObjType() {
      super("cat-st", "Catalog States", CatalogStateObj.class);
    }
  }

  interface Builder extends ContainerObj.Builder<CatalogStateObj, Builder> {}
}
