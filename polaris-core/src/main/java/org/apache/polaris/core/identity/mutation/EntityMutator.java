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
 * A transformation hook that mutates a Polaris entity.
 *
 * <p>Implementations of this interface apply custom logic to modify or enrich a {@link
 * PolarisBaseEntity}.
 */
public interface EntityMutator {

  /**
   * Applies the mutation logic to the given entity.
   *
   * @param entity the entity to mutate
   * @return the mutated entity
   */
  PolarisBaseEntity apply(PolarisBaseEntity entity);
}
