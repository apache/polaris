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
package org.apache.polaris.persistence.api;

import static com.google.common.base.Preconditions.checkState;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.api.commit.Commits;
import org.apache.polaris.persistence.api.commit.Committer;
import org.apache.polaris.persistence.api.exceptions.ReferenceAlreadyExistsException;
import org.apache.polaris.persistence.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.api.index.Index;
import org.apache.polaris.persistence.api.index.IndexContainer;
import org.apache.polaris.persistence.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.api.index.UpdatableIndex;
import org.apache.polaris.persistence.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.api.obj.Obj;
import org.apache.polaris.persistence.api.obj.ObjRef;
import org.apache.polaris.persistence.api.obj.ObjType;
import org.apache.polaris.persistence.api.ref.Reference;

/**
 * Polaris persistence internal, low-level API interface, scoped to a specific realm.
 *
 * <p>Database specific implementations should extend the abstract {@code BasePersistence} class
 * from the {@code polaris-persistence-impl} module, not directly this interface.
 *
 * <p>Realm-scoped {@link Persistence} instances can be {@link Inject @Inject}ed within {@link
 * RequestScoped @RequestScoped} contexts.
 */
public interface Persistence {
  /**
   * Creates the reference with the given name and {@linkplain Reference#pointer() pointer} value.
   *
   * <p>Reference creation is always a strongly consistent operation.
   *
   * @throws ReferenceAlreadyExistsException if a reference with the same name already exists
   */
  @Nonnull
  Reference createReference(@Nonnull String name, @Nonnull Optional<ObjRef> pointer)
      throws ReferenceAlreadyExistsException;

  /**
   * Convenience function to create a reference with an empty {@linkplain Reference#pointer()
   * pointer}, if it does not already exist.
   */
  default void createReferenceSilent(@Nonnull String name) {
    try {
      fetchReference(name);
    } catch (ReferenceNotFoundException e) {
      try {
        createReference(name, Optional.empty());
      } catch (ReferenceAlreadyExistsException x) {
        // Unlikely that we ever get here (ref does not exist (but then concurrently created)
      }
    }
  }

  /**
   * Convenience function return an existing reference or to create the reference with a supplied
   * {@linkplain Reference#pointer() pointer}, if it does not already exist.
   */
  @Nonnull
  default Reference createOrFetchReference(
      @Nonnull String name, @Nonnull Supplier<Optional<ObjRef>> pointerForCreate) {
    try {
      return fetchReference(name);
    } catch (ReferenceNotFoundException e) {
      try {
        return createReference(name, pointerForCreate.get());
      } catch (ReferenceAlreadyExistsException x) {
        // Unlikely that we ever get here (ref does not exist (but then concurrently created)
        return fetchReference(name);
      }
    }
  }

  /**
   * Updates the {@linkplain Reference#pointer() pointer} to {@code newPointer}, if the reference
   * exists and the current persisted pointer is the same as in {@code reference}.
   *
   * <p>Reference update is always a strongly consistent operation.
   *
   * @param reference the existing reference including the expected pointer
   * @param newPointer the pointer to update the reference to. If the reference has a current
   *     pointer value, both the current and the new pointer must use the same {@link ObjType
   *     ObjType}.
   * @return If the reference was successfully updated, an updated {@link Reference} instances will
   *     be returned.
   * @throws ReferenceNotFoundException if the reference does not exist
   */
  @Nonnull
  Optional<Reference> updateReferencePointer(
      @Nonnull Reference reference, @Nonnull ObjRef newPointer) throws ReferenceNotFoundException;

  /**
   * Fetch the reference with the given name, leveraging the reference cache.
   *
   * @throws ReferenceNotFoundException if the reference does not exist
   * @see #fetchReferenceForUpdate(String)
   * @see #fetchReferenceHead(String, Class)
   */
  @Nonnull
  Reference fetchReference(@Nonnull String name) throws ReferenceNotFoundException;

  /**
   * Fetches the reference with the given name, but will always fetch the most recent state from the
   * backend database.
   *
   * @see #fetchReference(String)
   */
  @Nonnull
  default Reference fetchReferenceForUpdate(@Nonnull String name)
      throws ReferenceNotFoundException {
    return fetchReference(name);
  }

  /**
   * Convenience function to return the {@link Obj} as pointed to from the reference with the given
   * name.
   *
   * @see #fetchReference(String)
   * @see #fetch(ObjRef, Class)
   */
  default <T extends Obj> Optional<T> fetchReferenceHead(
      @Nonnull String name, @Nonnull Class<T> clazz) throws ReferenceNotFoundException {
    var ref = fetchReference(name);
    return ref.pointer()
        .map(
            id -> {
              var head = fetch(id, clazz);
              checkState(head != null, "%s referenced by '%s' does not exist", id, name);
              return head;
            });
  }

  /**
   * Fetch the objects for the given object Ids.
   *
   * <p>Supports assembling object split across multiple rows by {@link #write(Obj, Class)} or
   * {@link #writeMany(Class, Obj[])}.
   *
   * @param id ID of the object to load
   * @param clazz expected {@link Obj} subtype, passing {@code Obj.class} is fine
   * @return loaded object or {@code null} if it does not exist
   * @param <T> returned type, can also be just {@code Obj}
   * @see #fetchMany(Class, ObjRef[])
   */
  @Nullable
  <T extends Obj> T fetch(@Nonnull ObjRef id, @Nonnull Class<T> clazz);

  /**
   * Fetch multiple objects for the given object Ids.
   *
   * <p>Supports assembling object split across multiple rows by {@link #write(Obj, Class)} or
   * {@link #writeMany(Class, Obj[])}.
   *
   * @param <T> returned type, can also be just {@code Obj}
   * @param clazz expected {@link Obj} subtype, passing {@code Obj.class} is fine
   * @param ids ID of the object to load, callers must ensure that the IDs are not duplicated within
   *     the array
   * @return array of the same length as {@code ids} containing the loaded objects, with {@code
   *     null} elements for objects that do not exist
   * @see #fetch(ObjRef, Class)
   */
  @Nonnull
  <T extends Obj> T[] fetchMany(@Nonnull Class<T> clazz, @Nonnull ObjRef... ids);

  /**
   * Persist {@code obj} with eventually consistent guarantees.
   *
   * <p>Supports splitting the serialized representation across multiple rows in the backend
   * database, if the serialized representation does not fit entirely in a single row, limited by
   * {@link #maxSerializedValueSize()}.
   *
   * <p>This function (and {@link #writeMany(Class, Obj[])} are <em>not</em> meant to actually
   * update existing objects with different information, especially not when the size of the
   * serialized object changes the number of splits in the backend database. Note that there is
   * <em>no</em> protection against this scenario.
   *
   * @return {@code obj} with the {@link Obj#createdAtMicros()} and {@link Obj#numParts()} fields
   *     updated
   * @see #writeMany(Class, Obj[])
   */
  @Nonnull
  <T extends Obj> T write(@Nonnull T obj, @Nonnull Class<T> clazz);

  /**
   * Persist multiple {@code objs} with eventually consistent guarantees.
   *
   * <p>See {@link #write(Obj, Class)} for more information.
   *
   * <p>Supports splitting the serialized representation across multiple rows in the backend
   * database, if the serialized representation does not fit entirely in a single row, limited by
   * {@link #maxSerializedValueSize()}.
   *
   * <p>This function and {@link #write(Obj, Class)} are <em>not</em> meant to actually update
   * existing objects with different information, especially not when the size of the serialized
   * object changes the number of splits in the backend database. Note that there is <em>no</em>
   * protection against this scenario.
   *
   * @return {@code objs} with the {@link Obj#createdAtMicros()} and {@link Obj#numParts()} fields
   *     updated, callers must ensure that the IDs are not duplicated within the array. {@code null}
   *     elements in the returned array will appear for {@code null} elements in the {@code objs}
   *     array.
   * @see #write(Obj, Class)
   */
  @SuppressWarnings("unchecked")
  @Nonnull
  <T extends Obj> T[] writeMany(@Nonnull Class<T> clazz, @Nonnull T... objs);

  /**
   * Unconditionally delete the object with the given id.
   *
   * <p>Note that it is generally not advised to actively (or prematurely) delete objects. In
   * general, it is better to just leave the object and let the maintenance service take care or
   * purging it.
   *
   * <p>If the object has been split across multiple database rows, only the number of parts
   * mentioned in {@link ObjRef#numParts()} will be deleted. However, the maintenance service will
   * take care of purging possibly left over parts.
   *
   * @see #deleteMany(ObjRef[])
   */
  void delete(@Nonnull ObjRef id);

  /**
   * Unconditionally delete the objects with the given ids.
   *
   * <p>Note that it is generally not advised to actively (or prematurely) delete objects. In
   * general, it is better to just leave the object and let the maintenance service take care or
   * purging it.
   *
   * <p>If the object has been split across multiple database rows, only the number of parts
   * mentioned in {@link ObjRef#numParts()} will be deleted. However, the maintenance service will
   * take care of purging possibly left over parts.
   *
   * @param ids IDs of objects to delete, callers must ensure that the IDs are not duplicated within
   *     the array
   * @see #delete(ObjRef)
   */
  void deleteMany(@Nonnull ObjRef... ids);

  /**
   * Persist {@code obj} with strong consistent guarantees.
   *
   * <p>Unlike {@linkplain #write(Obj, Class) eventually consistent writes}, a conditional write
   * does not support splitting the serialized representation across multiple rows in the backend
   * database, the serialized representation must fit entirely in a single row, limited by {@link
   * #maxSerializedValueSize()}.
   *
   * @return {@code obj} with the {@link Obj#createdAtMicros()} field updated if and only if no
   *     other object with the same object id existed before, otherwise {@code null}
   */
  @Nullable
  <T extends Obj> T conditionalInsert(@Nonnull T obj, @Nonnull Class<T> clazz);

  /**
   * Update an object with strong consistent guarantees.
   *
   * <p>Unlike {@linkplain #write(Obj, Class) eventually consistent writes}, a conditional write
   * does not support splitting the serialized representation across multiple rows in the backend
   * database, the serialized representation must fit entirely in a single row, limited by {@link
   * #maxSerializedValueSize()}.
   *
   * @param expected the object expected to have the same {@link Obj#versionToken()} as this one
   * @param update the object to be updated to, must have the same {@linkplain Obj#id() id},
   *     {@linkplain Obj#type() type} but a different {@linkplain Obj#versionToken() version token}
   * @return updated state in the database, if successful, otherwise {@code null}
   */
  @Nullable
  <T extends Obj> T conditionalUpdate(
      @Nonnull T expected, @Nonnull T update, @Nonnull Class<T> clazz);

  /**
   * Delete an object with strong consistent guarantees.
   *
   * @param expected the object expected to have the same {@link Obj#versionToken()} as this one
   * @return {@code true} if the object existed with the expected version token and was deleted in
   *     the database, if successful, otherwise {@code false}
   */
  <T extends Obj> boolean conditionalDelete(@Nonnull T expected, Class<T> clazz);

  PersistenceParams params();

  /**
   * Defines the maximum allowed {@linkplain Obj serialized object} size. Serialized representation
   * larger than this value will be split into multiple database rows.
   */
  int maxSerializedValueSize();

  long generateId();

  ObjRef generateObjId(ObjType type);

  /**
   * If the {@linkplain Persistence persistence implementation} is caching, this function returns
   * the object with the ID from the cache, but does not consult the backend.
   *
   * <p>Non-caching implementations default to {@link #fetch(ObjRef, Class)}.
   */
  @Nullable
  <T extends Obj> T getImmediate(@Nonnull ObjRef id, @Nonnull Class<T> clazz);

  Commits commits();

  <REF_OBJ extends BaseCommitObj, RESULT> Committer<REF_OBJ, RESULT> createCommitter(
      @Nonnull String refName,
      @Nonnull Class<REF_OBJ> referencedObjType,
      @Nonnull Class<RESULT> resultType);

  <V> Index<V> buildReadIndex(
      @Nullable IndexContainer<V> indexContainer,
      @Nonnull IndexValueSerializer<V> indexValueSerializer);

  <V> UpdatableIndex<V> buildWriteIndex(
      @Nullable IndexContainer<V> indexContainer,
      @Nonnull IndexValueSerializer<V> indexValueSerializer);

  @Nonnull
  default Duration objAge(@Nonnull Obj obj) {
    return Duration.ofNanos(
        TimeUnit.MICROSECONDS.toNanos(Math.max(currentTimeMicros() - obj.createdAtMicros(), 0L)));
  }

  String realmId();

  MonotonicClock monotonicClock();

  IdGenerator idGenerator();

  /**
   * Convenience for {@link #monotonicClock() monotonicClock().}{@link
   * MonotonicClock#currentTimeMicros()}.
   */
  @SuppressWarnings("resource")
  default long currentTimeMicros() {
    return monotonicClock().currentTimeMicros();
  }

  /**
   * Convenience for {@link #monotonicClock() monotonicClock().}{@link
   * MonotonicClock#currentTimeMillis()}.
   */
  @SuppressWarnings("resource")
  default long currentTimeMillis() {
    return monotonicClock().currentTimeMillis();
  }

  /**
   * Convenience for {@link #monotonicClock() monotonicClock().}{@link
   * MonotonicClock#currentInstant()}.
   */
  @SuppressWarnings("resource")
  default Instant currentInstant() {
    return monotonicClock().currentInstant();
  }
}
