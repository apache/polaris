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
package org.apache.polaris.persistence.nosql.metastore;

import static org.apache.polaris.persistence.nosql.api.index.IndexContainer.newUpdatableIndex;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.LongValues.LONG_VALUES_SERIALIZER;
import static org.apache.polaris.persistence.nosql.coretypes.changes.Change.CHANGE_SERIALIZER;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.api.commit.CommitException;
import org.apache.polaris.persistence.nosql.api.commit.CommitRetryable;
import org.apache.polaris.persistence.nosql.api.commit.CommitterState;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;

/**
 * Abstracts common {@link ContainerObj#stableIdToName()} and {@link ContainerObj#nameToObjRef()}
 * handling for committing operations, for catalog related types.
 *
 * @param <RESULT> result of the commiting operation
 */
final class CatalogChangeCommitterWrapper<RESULT>
    implements CommitRetryable<CatalogStateObj, RESULT> {
  private final CatalogChangeCommitter<RESULT> changeCommitter;

  CatalogChangeCommitterWrapper(CatalogChangeCommitter<RESULT> changeCommitter) {
    this.changeCommitter = changeCommitter;
  }

  @Nonnull
  @Override
  public Optional<CatalogStateObj> attempt(
      @Nonnull CommitterState<CatalogStateObj, RESULT> state,
      @Nonnull Supplier<Optional<CatalogStateObj>> refObjSupplier)
      throws CommitException {
    var refObj = refObjSupplier.get();
    var byName =
        refObj
            .map(CatalogStateObj::nameToObjRef)
            .map(c -> c.asUpdatableIndex(state.persistence(), OBJ_REF_SERIALIZER))
            .orElseGet(() -> newUpdatableIndex(state.persistence(), OBJ_REF_SERIALIZER));
    var byId =
        refObj
            .map(CatalogStateObj::stableIdToName)
            .map(c -> c.asUpdatableIndex(state.persistence(), INDEX_KEY_SERIALIZER))
            .orElseGet(() -> newUpdatableIndex(state.persistence(), INDEX_KEY_SERIALIZER));
    var locations =
        refObj
            .flatMap(CatalogStateObj::locations)
            .map(c -> c.asUpdatableIndex(state.persistence(), LONG_VALUES_SERIALIZER))
            .orElseGet(() -> newUpdatableIndex(state.persistence(), LONG_VALUES_SERIALIZER));
    // 'changes' contains the changes for the particular commit
    var changes = newUpdatableIndex(state.persistence(), CHANGE_SERIALIZER);

    var ref = CatalogStateObj.builder();
    refObj.ifPresent(ref::from);

    var r = changeCommitter.change(state, ref, byName, byId, changes, locations);

    if (r instanceof ChangeResult.CommitChange<RESULT>(RESULT result)) {
      ref.changes(changes.toIndexed("idx-changes-", state::writeOrReplace))
          .nameToObjRef(byName.toIndexed("idx-name-", state::writeOrReplace))
          .stableIdToName(byId.toIndexed("idx-id-", state::writeOrReplace))
          .locations(locations.toOptionalIndexed("idx-loc-", state::writeOrReplace));
      return state.commitResult(result, ref, refObj);
    }
    if (r instanceof ChangeResult.NoChange<RESULT>(RESULT result)) {
      return state.noCommit(result);
    }
    throw new IllegalStateException("" + r);
  }
}
