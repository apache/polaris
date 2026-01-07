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
package org.apache.polaris.persistence.nosql.metastore.committers;

import static org.apache.polaris.persistence.nosql.api.index.IndexContainer.newUpdatableIndex;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.commit.CommitException;
import org.apache.polaris.persistence.nosql.api.commit.CommitRetryable;
import org.apache.polaris.persistence.nosql.api.commit.CommitterState;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogRolesObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalRolesObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTasksObj;

/**
 * Abstracts common {@link ContainerObj#stableIdToName()} and {@link ContainerObj#nameToObjRef()}
 * handling for committing operations, for non-catalog related types.
 *
 * @param <REF_OBJ> committed object type
 * @param <B> builder type for {@link REF_OBJ}
 * @param <RESULT> result of the committing operation
 */
public record ChangeCommitterWrapper<
        REF_OBJ extends ContainerObj, B extends ContainerObj.Builder<REF_OBJ, B>, RESULT>(
    ChangeCommitter<REF_OBJ, RESULT> changeCommitter, PolarisEntityType entityType)
    implements CommitRetryable<REF_OBJ, RESULT> {

  @SuppressWarnings("DuplicatedCode")
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

  static ContainerObj.Builder<? extends ContainerObj, ? extends ContainerObj.Builder<?, ?>>
      newContainerBuilderForEntityType(PolarisEntityType entityType) {
    return switch (entityType) {
      case CATALOG -> CatalogsObj.builder();
      case PRINCIPAL -> PrincipalsObj.builder();
      case PRINCIPAL_ROLE -> PrincipalRolesObj.builder();
      case TASK -> ImmediateTasksObj.builder();

      // per catalog
      case CATALOG_ROLE -> CatalogRolesObj.builder();
      case NAMESPACE, TABLE_LIKE, POLICY -> CatalogStateObj.builder();

      default -> throw new IllegalArgumentException("Unsupported entity type: " + entityType);
    };
  }
}
