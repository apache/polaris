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
package org.apache.polaris.persistence.nosql.realms.api;

import com.google.errorprone.annotations.MustBeClosed;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Low-level realm management functionality.
 *
 * <p>Realm IDs must conform to the following constraints:
 *
 * <ul>
 *   <li>Must not start or end with whitespaces.
 *   <li>Must only consist of US-ASCII letters or digits or hyphens ({@code -}) or underscores
 *       ({@code _}).
 *   <li>Must not start with two consecutive colons ({@code ::}).
 *   <li>Must not be empty.
 *   <li>Must not be longer than 128 characters.
 * </ul>
 *
 * <p>Note: In a CDI container {@link RealmManagement} can be directly injected.
 */
public interface RealmManagement {
  /**
   * Creates a new realm in {@linkplain RealmDefinition.RealmStatus#CREATED created status} with the
   * given realm ID.
   *
   * @return the persisted state of the realm definition
   * @throws RealmAlreadyExistsException if a realm with the given ID already exists
   */
  @Nonnull
  RealmDefinition create(@Nonnull String realmId);

  /** Returns a stream of all realm definitions. The returned stream must be closed. */
  @Nonnull
  @MustBeClosed
  Stream<RealmDefinition> list();

  /**
   * Retrieve a realm definition by realm ID.
   *
   * @return the realm definition if it exists.
   */
  @Nonnull
  Optional<RealmDefinition> get(@Nonnull String realmId);

  /**
   * Updates a realm definition to {@code update}, if the persisted state matches the {@code
   * expected} state, and if the {@linkplain RealmDefinition#status() status} transition is valid.
   *
   * @param expected The expected persisted state of the realm definition. This must exactly
   *     represent the persisted realm definition as returned by {@link #create(String)} or {@link
   *     #get(String)} or a prior {@link #update(RealmDefinition, RealmDefinition)}.
   * @param update the new state of the realm definition to be persisted, the {@link
   *     RealmDefinition#created() created} and {@link RealmDefinition#updated() updated} attributes
   *     are solely managed by the implementation.
   * @return the persisted state of the realm definition
   * @throws RealmNotFoundException if a realm with the given ID does not exist
   * @throws RealmExpectedStateMismatchException if the expected state does not match
   * @throws IllegalArgumentException if the transition is not valid.
   */
  @Nonnull
  RealmDefinition update(@Nonnull RealmDefinition expected, @Nonnull RealmDefinition update);

  /**
   * Deletes the given realm.
   *
   * @param expected The expected persisted state of the realm definition. This must exactly
   *     represent the persisted realm definition as returned by {@link #create(String)} or {@link
   *     #get(String)} or {@link #update(RealmDefinition, RealmDefinition)}.
   * @throws RealmNotFoundException if a realm with the given ID does not exist
   * @throws RealmExpectedStateMismatchException if the expected state does not match
   */
  void delete(@Nonnull RealmDefinition expected);
}
