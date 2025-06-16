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

package org.apache.polaris.core.entity.transformation;

/**
 * Defines points in the entity lifecycle where {@link EntityTransformer} can be applied.
 *
 * <p>Each transformation point corresponds to a specific hook where transformers may be executed.
 * Transformers can declare which points they support, allowing the engine to invoke only the
 * relevant ones.
 */
public enum TransformationPoint {

  /** Applied before a catalog entity is persisted. */
  CATALOG_PRE_PERSIST(0),
  ;

  private final int id;

  TransformationPoint(int id) {
    this.id = id;
  }

  public int id() {
    return id;
  }
}
