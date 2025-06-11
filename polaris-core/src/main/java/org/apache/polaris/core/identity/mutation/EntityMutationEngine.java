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

package org.apache.polaris.core.identity.mutation;

import org.apache.polaris.core.entity.PolarisBaseEntity;

/**
 * Engine responsible for applying a sequence of {@link EntityMutator} transformations to a {@link
 * PolarisBaseEntity}.
 *
 * <p>This abstraction allows Polaris to customize or enrich entities during runtime or persistence,
 * based on configured or contextual logic (e.g., injecting service identity info, computing derived
 * fields).
 */
public interface EntityMutationEngine {
  /**
   * Applies all registered entity mutators to the provided entity, in order.
   *
   * @param mutationPoint The point in the entity lifecycle where mutations should be applied.
   * @param entity The original Polaris entity to mutate.
   * @return A new transformed copy of the entity of {@link PolarisBaseEntity} after all mutations
   *     are applied.
   */
  PolarisBaseEntity applyMutations(MutationPoint mutationPoint, PolarisBaseEntity entity);
}
