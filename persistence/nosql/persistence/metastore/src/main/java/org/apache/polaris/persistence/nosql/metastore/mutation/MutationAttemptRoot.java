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

package org.apache.polaris.persistence.nosql.metastore.mutation;

import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS;
import static org.apache.polaris.core.persistence.dao.entity.BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED;
import static org.apache.polaris.persistence.nosql.metastore.mutation.MutationResults.singleEntityResult;

import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.api.commit.CommitterState;
import org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings;
import org.apache.polaris.persistence.nosql.coretypes.realm.RootObj;

public record MutationAttemptRoot(
    CommitterState<RootObj, MutationResults> state,
    Supplier<Optional<RootObj>> refObjSupplier,
    EntityUpdate update) {
  public Optional<RootObj> apply() {
    var entity = update.entity();
    var ref = EntityObjMappings.<RootObj, RootObj.Builder>mapToObj(entity, Optional.empty());
    var refObj = refObjSupplier.get();
    return switch (update.operation()) {
      case CREATE -> {
        if (refObj.isPresent()) {
          yield state.noCommit(singleEntityResult(ENTITY_ALREADY_EXISTS));
        }
        yield state.commitResult(singleEntityResult(entity), ref, refObj);
      }
      case UPDATE -> {
        if (refObj.isPresent()) {
          var rootObj = refObj.get();
          if (entity.getEntityVersion() != rootObj.entityVersion()) {
            yield state.noCommit(singleEntityResult(TARGET_ENTITY_CONCURRENTLY_MODIFIED));
          }
        }
        yield state.commitResult(singleEntityResult(entity), ref, refObj);
      }
      default -> throw new IllegalStateException("Unexpected operation " + update.operation());
    };
  }
}
