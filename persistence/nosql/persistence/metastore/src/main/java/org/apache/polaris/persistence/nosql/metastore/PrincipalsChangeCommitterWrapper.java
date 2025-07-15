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

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.api.commit.CommitException;
import org.apache.polaris.persistence.nosql.api.commit.CommitRetryable;
import org.apache.polaris.persistence.nosql.api.commit.CommitterState;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj;

/**
 * Abstracts common {@link ContainerObj#stableIdToName()} and {@link ContainerObj#nameToObjRef()}
 * handling for committing operations, for principals.
 *
 * @param <RESULT> result of the commiting operation
 */
final class PrincipalsChangeCommitterWrapper<RESULT>
    implements CommitRetryable<PrincipalsObj, RESULT> {
  private final PrincipalsChangeCommitter<RESULT> changeCommitter;

  PrincipalsChangeCommitterWrapper(PrincipalsChangeCommitter<RESULT> changeCommitter) {
    this.changeCommitter = changeCommitter;
  }

  @Nonnull
  @Override
  public Optional<PrincipalsObj> attempt(
      @Nonnull CommitterState<PrincipalsObj, RESULT> state,
      @Nonnull Supplier<Optional<PrincipalsObj>> refObjSupplier)
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
    var byClientId =
        refObj
            .map(PrincipalsObj::byClientId)
            .map(c -> c.asUpdatableIndex(state.persistence(), OBJ_REF_SERIALIZER))
            .orElseGet(() -> newUpdatableIndex(state.persistence(), OBJ_REF_SERIALIZER));

    var ref = PrincipalsObj.builder();
    refObj.ifPresent(ref::from);

    var r = changeCommitter.change(state, ref, byName, byId, byClientId);

    if (r instanceof ChangeResult.CommitChange<RESULT>(RESULT result)) {
      ref.byClientId(byClientId.toIndexed("idx-client-id-", state::writeOrReplace))
          .nameToObjRef(byName.toIndexed("idx-name-", state::writeOrReplace))
          .stableIdToName(byId.toIndexed("idx-id-", state::writeOrReplace));
      return state.commitResult(result, ref, refObj);
    }
    if (r instanceof ChangeResult.NoChange<RESULT>(RESULT result)) {
      return state.noCommit(result);
    }
    throw new IllegalStateException("" + r);
  }
}
