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
package org.apache.polaris.realms.spi;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.polaris.realms.api.RealmAlreadyExistsException;
import org.apache.polaris.realms.api.RealmDefinition;
import org.apache.polaris.realms.api.RealmNotFoundException;

public interface RealmStore {
  Stream<RealmDefinition> list();

  Optional<RealmDefinition> get(String realmId);

  /**
   * Deletes a realm definition.
   *
   * @param callback receives the current definition
   * @throws RealmNotFoundException if a realm with the given ID does not exist
   */
  void delete(String realmId, Consumer<RealmDefinition> callback);

  /**
   * Updates a realm definition.
   *
   * @param updater receives the current definition and provides the new definition
   * @throws RealmNotFoundException if a realm with the given ID does not exist
   */
  RealmDefinition update(String realmId, Function<RealmDefinition, RealmDefinition> updater);

  /**
   * Create a new realm.
   *
   * @throws RealmAlreadyExistsException if the realm already exists
   */
  RealmDefinition create(String realmId, RealmDefinition definition);
}
