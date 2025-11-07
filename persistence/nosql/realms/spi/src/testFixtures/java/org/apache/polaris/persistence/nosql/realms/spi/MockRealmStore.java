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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.realms.api.RealmAlreadyExistsException;
import org.apache.polaris.persistence.nosql.realms.api.RealmDefinition;
import org.apache.polaris.persistence.nosql.realms.api.RealmNotFoundException;

public class MockRealmStore implements RealmStore {
  private final Map<String, RealmDefinition> realms = new ConcurrentHashMap<>();

  @Override
  public RealmDefinition create(String realmId, RealmDefinition definition) {
    var ex = realms.putIfAbsent(realmId, definition);
    if (ex != null) {
      throw new RealmAlreadyExistsException(format("A realm with ID '%s' already exists", realmId));
    }
    return definition;
  }

  @Override
  public RealmDefinition update(
      String realmId, Function<RealmDefinition, RealmDefinition> updater) {
    var computed = new AtomicBoolean();
    var updated =
        realms.computeIfPresent(
            realmId,
            (id, current) -> {
              computed.set(true);
              return requireNonNull(updater.apply(current));
            });
    if (!computed.get()) {
      throw new RealmNotFoundException(format("No realm with ID '%s' exists", realmId));
    }
    return updated;
  }

  @Override
  public void delete(String realmId, Consumer<RealmDefinition> callback) {
    var computed = new AtomicBoolean();
    realms.computeIfPresent(
        realmId,
        (id, current) -> {
          computed.set(true);
          callback.accept(current);
          return null;
        });
    if (!computed.get()) {
      throw new RealmNotFoundException(format("No realm with ID '%s' exists", realmId));
    }
  }

  @Override
  public Optional<RealmDefinition> get(String realmId) {
    return Optional.ofNullable(realms.get(realmId));
  }

  @Override
  public Stream<RealmDefinition> list() {
    return realms.values().stream();
  }
}
