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

package org.apache.polaris.service.identity.mutation;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.identity.mutation.EntityMutationEngine;

@ApplicationScoped
public class EntityMutationEngineImpl implements EntityMutationEngine {

  private final Instance<EntityMutator> mutatorInstance;

  @Inject
  public EntityMutationEngineImpl(Instance<EntityMutator> mutatorInstance) {
    this.mutatorInstance = mutatorInstance;
  }

  @Override
  public PolarisBaseEntity applyMutations(PolarisBaseEntity entity) {
    PolarisBaseEntity result = entity;

    Map<String, EntityMutator> mutatorMap =
        mutatorInstance.stream().collect(Collectors.toMap(EntityMutator::id, m -> m));

    // Apply all mutators to the entity
    // TODO: Add a way to control the order of mutators
    for (EntityMutator mutator : mutatorMap.values()) {
      result = mutator.apply(result);
    }

    return result;
  }
}
