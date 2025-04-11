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
package org.apache.polaris.persistence.bridge;

import static org.apache.polaris.persistence.api.index.IndexContainer.newUpdatableIndex;
import static org.apache.polaris.persistence.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.api.obj.ObjRef.OBJ_REF_SERIALIZER;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.persistence.api.commit.CommitException;
import org.apache.polaris.persistence.api.commit.CommitRetryable;
import org.apache.polaris.persistence.api.commit.CommitterState;
import org.apache.polaris.persistence.coretypes.ContainerObj;

final class ChangeCommitterWrapper<
        REF_OBJ extends ContainerObj, B extends ContainerObj.Builder<REF_OBJ, B>, RESULT>
    implements CommitRetryable<REF_OBJ, RESULT> {
  private final ChangeCommitter<REF_OBJ, RESULT> changeCommitter;
  private final Supplier<B> builderSupplier;

  ChangeCommitterWrapper(
      ChangeCommitter<REF_OBJ, RESULT> changeCommitter, Supplier<B> builderSupplier) {
    this.changeCommitter = changeCommitter;
    this.builderSupplier = builderSupplier;
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
    var ref = builderSupplier.get();
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
