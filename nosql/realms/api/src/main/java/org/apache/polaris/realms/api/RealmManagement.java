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
package org.apache.polaris.realms.api;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Low-level realm management functionality.
 *
 * <p>Note: In a CDI container {@link RealmManagement} can be directly injected.
 */
public interface RealmManagement {
  /**
   * Creates a new realm in {@linkplain RealmDefinition.RealmStatus#CREATED created status} with the
   * given realm ID.
   *
   * @throws RealmAlreadyExistsException if a realm with the given ID already exists
   */
  @Nonnull
  RealmDefinition create(@Nonnull String realmId);

  @Nonnull
  Stream<RealmDefinition> list();

  /**
   * Retrieve a realm definition by realm ID.
   *
   * @return the realm definition or {@code null} if the realm does not exist.
   */
  @Nonnull
  Optional<RealmDefinition> get(@Nonnull String realmId);

  /**
   * Updates a realm definition to {@code update}, if the persisted state matches the {@code
   * expected} state, and if the {@linkplain RealmDefinition#status() status} transition is valid. *
   *
   * @throws RealmNotFoundException if a realm with the given ID does not exist
   * @throws RealmExpectedStateMismatchException if the expected state does not match
   * @throws IllegalArgumentException if the transition is not valid.
   */
  @Nonnull
  RealmDefinition update(@Nonnull RealmDefinition expected, @Nonnull RealmDefinition update);

  /**
   * Deletes the given realm.
   *
   * @throws RealmNotFoundException if a realm with the given ID does not exist
   * @throws RealmExpectedStateMismatchException if the expected state does not match
   */
  void delete(@Nonnull RealmDefinition expected);
}
