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
package org.apache.polaris.core.auth;

import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Intent-only target reference for authorization decisions.
 *
 * <p>Represents the target name and entity type without any resolved RBAC state. This captures the
 * pure authorization intent of a request. Callers should provide the full name path as {@link
 * #getNameParts()} for hierarchical entities.
 */
@PolarisImmutable
public interface PolarisSecurable {
  static PolarisSecurable of(
      @Nonnull PolarisEntityType entityType, @Nonnull List<String> nameParts) {
    return ImmutablePolarisSecurable.builder().entityType(entityType).nameParts(nameParts).build();
  }

  /** Returns the entity type of the securable. */
  @Nonnull
  PolarisEntityType getEntityType();

  /** Returns the name parts that identify the securable in hierarchical order. */
  @Nonnull
  List<String> getNameParts();

  /** Returns the dot-joined name derived from {@link #getNameParts()}. */
  default @Nonnull String getName() {
    return String.join(".", getNameParts());
  }
}
