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
package org.apache.polaris.persistence.nosql.realms.spi;

import com.google.errorprone.annotations.MustBeClosed;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.realms.api.RealmAlreadyExistsException;
import org.apache.polaris.persistence.nosql.realms.api.RealmDefinition;
import org.apache.polaris.persistence.nosql.realms.api.RealmManagement;
import org.apache.polaris.persistence.nosql.realms.api.RealmNotFoundException;

/**
 * Interface to be implemented by persistence-specific implementations (NoSQL or metastore manager
 * based).
 *
 * <p>Implementations must not perform any validation of the realm definitions unless explicitly
 * stated below.
 */
public interface RealmStore {
  /** Returns a stream of all realm definitions. The returned stream must be closed. */
  @MustBeClosed
  Stream<RealmDefinition> list();

  /**
   * Returns a realm definition if it exists.
   *
   * <p>Unlike the updating functions, this function does not throw an exception if a realm does not
   * exist.
   */
  Optional<RealmDefinition> get(String realmId);

  /**
   * Deletes a realm definition.
   *
   * @param callback receives the persisted realm definition that is being deleted. If the callback
   *     throws any exception, the delete operation must not be persisted. All thrown exceptions
   *     must be propagated to the caller.
   * @throws RealmNotFoundException if a realm with the given ID does not exist
   */
  void delete(String realmId, Consumer<RealmDefinition> callback);

  /**
   * Updates a realm definition.
   *
   * <p>Implementations update the persisted state of the realm definition. The created timestamp
   * must be carried forwards from the persisted state.
   *
   * <p>{@link RealmManagement} implementations, which call this function, take care of "properly"
   * populating the attributes of the realm definition to persist.
   *
   * @param updater receives the current definition and returns the updated definition. If the
   *     updated throws any exception, the update operation must not be persisted. All thrown
   *     exceptions must be propagated to the caller.
   * @return the persisted realm definition
   * @throws RealmNotFoundException if a realm with the given ID does not exist
   */
  RealmDefinition update(String realmId, Function<RealmDefinition, RealmDefinition> updater);

  /**
   * Create a new realm.
   *
   * <p>{@link RealmManagement} implementations, which call this function, take care of "properly"
   * populating the attributes of the realm definition to persist.
   *
   * @param definition the realm definition of the realm to be created, to be persisted as given.
   * @return the persisted realm definition
   * @throws RealmAlreadyExistsException if the realm already exists
   */
  RealmDefinition create(String realmId, RealmDefinition definition);
}
