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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.identity.mutation.EntityMutationEngine;

@ApplicationScoped
public class EntityMutationEngineImpl implements EntityMutationEngine {

  private final EntityMutationConfiguration config;
  private final Instance<EntityMutator> mutatorInstances;

  @Inject
  public EntityMutationEngineImpl(
      EntityMutationConfiguration config, @Any Instance<EntityMutator> mutatorInstances) {
    this.config = config;
    this.mutatorInstances = mutatorInstances;
  }

  @Override
  public PolarisBaseEntity applyMutations(PolarisBaseEntity entity) {
    PolarisBaseEntity result = entity;

    List<EntityMutator> orderedMutators =
        // config.map(EntityMutationConfiguration::mutators).orElse(List.of()).stream()
        config.mutators().orElse(List.of()).stream()
            .map(
                id -> {
                  Instance<EntityMutator> matched =
                      mutatorInstances.select(Identifier.Literal.of(id));
                  if (matched.isResolvable()) {
                    return matched.get();
                  } else {
                    throw new IllegalStateException("No EntityMutator found for ID: " + id);
                  }
                })
            .toList();

    // Apply all mutators to the entity
    // TODO: Add a way to control the order of mutators
    for (EntityMutator mutator : orderedMutators) {
      result = mutator.apply(result);
    }

    return result;
  }
}
