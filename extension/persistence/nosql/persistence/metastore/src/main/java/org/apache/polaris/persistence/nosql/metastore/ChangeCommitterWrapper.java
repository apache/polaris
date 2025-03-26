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
import static org.apache.polaris.persistence.nosql.metastore.TypeMapping.newContainerBuilderForEntityType;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.commit.CommitException;
import org.apache.polaris.persistence.nosql.api.commit.CommitRetryable;
import org.apache.polaris.persistence.nosql.api.commit.CommitterState;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;

/**
 * Abstracts common {@link ContainerObj#stableIdToName()} and {@link ContainerObj#nameToObjRef()}
 * handling for committing operations, for non-catalog related types.
 *
 * @param <REF_OBJ> commited object type
 * @param <B> builder type for {@link REF_OBJ}
 * @param <RESULT> result of the commiting operation
 */
final class ChangeCommitterWrapper<
        REF_OBJ extends ContainerObj, B extends ContainerObj.Builder<REF_OBJ, B>, RESULT>
    implements CommitRetryable<REF_OBJ, RESULT> {
  private final ChangeCommitter<REF_OBJ, RESULT> changeCommitter;
  private final PolarisEntityType entityType;

  ChangeCommitterWrapper(
      ChangeCommitter<REF_OBJ, RESULT> changeCommitter, PolarisEntityType entityType) {
    this.changeCommitter = changeCommitter;
    this.entityType = entityType;
  }

  @Nonnull
  @Override
  public Optional<REF_OBJ> attempt(
      @Nonnull CommitterState<REF_OBJ, RESULT> state,
      @Nonnull Supplier<Optional<REF_OBJ>> refObjSupplier)
      throws CommitException {
    var refObj = refObjSupplier.get();
    var byName =
        refObj
            .map(ContainerObj::nameToObjRef)
            .map(c -> c.asUpdatableIndex(state.persistence(), OBJ_REF_SERIALIZER))
            .orElseGet(() -> newUpdatableIndex(state.persistence(), OBJ_REF_SERIALIZER));
    var byId =
        refObj
            .map(ContainerObj::stableIdToName)
            .map(c -> c.asUpdatableIndex(state.persistence(), INDEX_KEY_SERIALIZER))
            .orElseGet(() -> newUpdatableIndex(state.persistence(), INDEX_KEY_SERIALIZER));
    @SuppressWarnings("unchecked")
    var ref = (B) newContainerBuilderForEntityType(entityType);
    refObj.ifPresent(ref::from);

    var r = changeCommitter.change(state, ref, byName, byId);

    if (r instanceof ChangeResult.CommitChange<RESULT>(RESULT result)) {
      ref.nameToObjRef(byName.toIndexed("idx-name-", state::writeOrReplace))
          .stableIdToName(byId.toIndexed("idx-id-", state::writeOrReplace));
      return state.commitResult(result, ref, refObj);
    }
    if (r instanceof ChangeResult.NoChange<RESULT>(RESULT result)) {
      return state.noCommit(result);
    }
    throw new IllegalStateException("" + r);
  }
}
