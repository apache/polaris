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
package org.apache.polaris.persistence.nosql.api.backend;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.jspecify.annotations.NonNull;

/** Provides "low-level" access to the database-specific backend. */
public interface Backend extends AutoCloseable {
  /** Name of this backend. This value serves as an identifier to select the correct backend. */
  @NonNull String type();

  /**
   * Called to set up the database schema.
   *
   * @return optional, human-readable information
   */
  Optional<String> setupSchema();

  @NonNull Persistence newPersistence(
      Function<Backend, Backend> backendWrapper,
      @NonNull PersistenceParams persistenceParams,
      String realmId,
      MonotonicClock monotonicClock,
      IdGenerator idGenerator);

  /** Whether the implementation supports {@link #deleteRealms(Set)}. */
  boolean supportsRealmDeletion();

  /**
   * Delete the given realms.
   *
   * <p>This function works, if {@link #supportsRealmDeletion()} yields {@code true}.
   *
   * <p>Throws an {@link UnsupportedOperationException}, if {@link #supportsRealmDeletion()} yields
   * {@code false}.
   */
  void deleteRealms(Set<String> realmIds);

  /**
   * Bulk reference deletion, grouped by realm. This functionality is primarily needed for the
   * maintenance service.
   */
  void batchDeleteRefs(Map<String, Set<String>> realmRefs);

  /**
   * Bulk object-part deletion, grouped by realm. This functionality is primarily needed for the
   * maintenance service.
   */
  void batchDeleteObjs(Map<String, Set<PersistId>> realmObjs);

  /** Callback interface for {@link #scanBackend(ReferenceScanCallback, ObjScanCallback)}. */
  @FunctionalInterface
  interface ReferenceScanCallback {
    /**
     * Called for each discovered reference and object-part ("item").
     *
     * @param realmId the realm to which the item belongs
     * @param refName the reference name
     * @param createdAtMicros the timestamp in microseconds since (Unix) epoch at which the item was
     *     created in the database
     */
    void call(@NonNull String realmId, @NonNull String refName, long createdAtMicros);
  }

  /** Callback interface for {@link #scanBackend(ReferenceScanCallback, ObjScanCallback)}. */
  @FunctionalInterface
  interface ObjScanCallback {
    /**
     * Called for each discovered reference and object-part ("item").
     *
     * @param realmId the realm to which the item belongs
     * @param type the object type ID
     * @param id object-part ID
     * @param createdAtMicros the timestamp in microseconds since (Unix) epoch at which the item was
     *     created in the database
     */
    void call(
        @NonNull String realmId, @NonNull String type, @NonNull PersistId id, long createdAtMicros);
  }

  /**
   * Scan the whole backend database and return each discovered reference and object-part via the
   * provided callbacks. This functionality is primarily needed for the maintenance service.
   */
  void scanBackend(
      @NonNull ReferenceScanCallback referenceConsumer, @NonNull ObjScanCallback objConsumer);

  boolean createReference(@NonNull String realmId, @NonNull Reference newRef);

  void createReferences(@NonNull String realmId, @NonNull List<Reference> newRefs);

  boolean updateReference(
      @NonNull String realmId,
      @NonNull Reference updatedRef,
      @NonNull Optional<ObjRef> expectedPointer);

  @NonNull Reference fetchReference(@NonNull String realmId, @NonNull String name);

  @NonNull Map<PersistId, FetchedObj> fetch(@NonNull String realmId, @NonNull Set<PersistId> ids);

  void write(@NonNull String realmId, @NonNull List<WriteObj> writes);

  void delete(@NonNull String realmId, @NonNull Set<PersistId> ids);

  boolean conditionalInsert(
      @NonNull String realmId,
      String objTypeId,
      @NonNull PersistId persistId,
      long createdAtMicros,
      @NonNull String versionToken,
      @NonNull byte[] serializedValue);

  boolean conditionalUpdate(
      @NonNull String realmId,
      String objTypeId,
      @NonNull PersistId persistId,
      long createdAtMicros,
      @NonNull String updateToken,
      @NonNull String expectedToken,
      @NonNull byte[] serializedValue);

  boolean conditionalDelete(
      @NonNull String realmId, @NonNull PersistId persistId, @NonNull String expectedToken);
}
