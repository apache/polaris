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

package org.apache.polaris.service.entity.transformation;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Objects;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.transformation.EntityTransformationEngine;
import org.apache.polaris.core.entity.transformation.EntityTransformer;
import org.apache.polaris.core.entity.transformation.TransformationPoint;

@ApplicationScoped
public class EntityTransformationEngineImpl implements EntityTransformationEngine {

  private final EntityTransformationConfiguration config;
  private final Instance<EntityTransformer> transformerInstances;

  @Inject
  public EntityTransformationEngineImpl(
      EntityTransformationConfiguration config,
      @Any Instance<EntityTransformer> transformerInstances) {
    this.config = config;
    this.transformerInstances = transformerInstances;
  }

  @Override
  public PolarisBaseEntity applyTransformers(
      TransformationPoint transformationPoint, PolarisBaseEntity entity) {
    PolarisBaseEntity result = entity;

    // Collect transformers in configured order, filtering only those applicable to the
    // transformation point
    List<EntityTransformer> orderedtransformers =
        config.transformers().orElse(List.of()).stream()
            .map(
                id -> {
                  // Resolve the transformer instance by ID
                  Instance<EntityTransformer> matched =
                      transformerInstances.select(Identifier.Literal.of(id));
                  if (!matched.isResolvable()) {
                    throw new IllegalStateException("No Entitytransformer found for ID: " + id);
                  }
                  // Filter by TransformationPoint via @AppliesTo
                  Instance<EntityTransformer> filtered =
                      matched.select(AppliesTo.Literal.of(transformationPoint));
                  return filtered.isResolvable() ? filtered.get() : null;
                })
            .filter(Objects::nonNull)
            .toList();

    for (EntityTransformer transformer : orderedtransformers) {
      result = transformer.apply(result);
    }

    return result;
  }
}
