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
package org.apache.polaris.persistence.nosql.impl.commits;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.commit.Commits;
import org.apache.polaris.persistence.nosql.api.commit.Committer;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceAlreadyExistsException;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.index.UpdatableIndex;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

abstract class DelegatingPersistence implements Persistence {
  protected final Persistence delegate;

  protected DelegatingPersistence(Persistence persistence) {
    this.delegate = persistence;
  }

  @NonNull
  @Override
  public Reference createReference(@NonNull String name, @NonNull Optional<ObjRef> pointer)
      throws ReferenceAlreadyExistsException {
    return delegate.createReference(name, pointer);
  }

  @Override
  public void createReferenceSilent(@NonNull String name) {
    delegate.createReferenceSilent(name);
  }

  @Override
  public void createReferencesSilent(Set<String> referenceNames) {
    delegate.createReferencesSilent(referenceNames);
  }

  @NonNull
  @Override
  public Reference fetchOrCreateReference(
      @NonNull String name, @NonNull Supplier<Optional<ObjRef>> pointerForCreate) {
    return delegate.fetchOrCreateReference(name, pointerForCreate);
  }

  @NonNull
  @Override
  public Optional<Reference> updateReferencePointer(
      @NonNull Reference reference, @NonNull ObjRef newPointer) throws ReferenceNotFoundException {
    return delegate.updateReferencePointer(reference, newPointer);
  }

  @NonNull
  @Override
  public Reference fetchReference(@NonNull String name) throws ReferenceNotFoundException {
    return delegate.fetchReference(name);
  }

  @NonNull
  @Override
  public Reference fetchReferenceForUpdate(@NonNull String name) throws ReferenceNotFoundException {
    return delegate.fetchReferenceForUpdate(name);
  }

  @Override
  public <T extends Obj> Optional<T> fetchReferenceHead(
      @NonNull String name, @NonNull Class<T> clazz) throws ReferenceNotFoundException {
    return delegate.fetchReferenceHead(name, clazz);
  }

  @Nullable
  @Override
  public <T extends Obj> T fetch(@NonNull ObjRef id, @NonNull Class<T> clazz) {
    return delegate.fetch(id, clazz);
  }

  @NonNull
  @Override
  public <T extends Obj> T[] fetchMany(@NonNull Class<T> clazz, @NonNull ObjRef... ids) {
    return delegate.fetchMany(clazz, ids);
  }

  @NonNull
  @Override
  public <T extends Obj> T write(@NonNull T obj, @NonNull Class<T> clazz) {
    return delegate.write(obj, clazz);
  }

  @SuppressWarnings("unchecked")
  @NonNull
  @Override
  public <T extends Obj> T[] writeMany(@NonNull Class<T> clazz, @NonNull T... objs) {
    return delegate.writeMany(clazz, objs);
  }

  @Override
  public void delete(@NonNull ObjRef id) {
    delegate.delete(id);
  }

  @Override
  public void deleteMany(@NonNull ObjRef... ids) {
    delegate.deleteMany(ids);
  }

  @Nullable
  @Override
  public <T extends Obj> T conditionalInsert(@NonNull T obj, @NonNull Class<T> clazz) {
    return delegate.conditionalInsert(obj, clazz);
  }

  @Nullable
  @Override
  public <T extends Obj> T conditionalUpdate(
      @NonNull T expected, @NonNull T update, @NonNull Class<T> clazz) {
    return delegate.conditionalUpdate(expected, update, clazz);
  }

  @Override
  public <T extends Obj> boolean conditionalDelete(@NonNull T expected, Class<T> clazz) {
    return delegate.conditionalDelete(expected, clazz);
  }

  @Override
  public PersistenceParams params() {
    return delegate.params();
  }

  @Override
  public int maxSerializedValueSize() {
    return delegate.maxSerializedValueSize();
  }

  @Override
  public long generateId() {
    return delegate.generateId();
  }

  @Override
  public ObjRef generateObjId(ObjType type) {
    return delegate.generateObjId(type);
  }

  @Nullable
  @Override
  public <T extends Obj> T getImmediate(@NonNull ObjRef id, @NonNull Class<T> clazz) {
    return delegate.getImmediate(id, clazz);
  }

  @Override
  public Commits commits() {
    return delegate.commits();
  }

  @Override
  public <REF_OBJ extends BaseCommitObj, RESULT> Committer<REF_OBJ, RESULT> createCommitter(
      @NonNull String refName,
      @NonNull Class<REF_OBJ> referencedObjType,
      @NonNull Class<RESULT> resultType) {
    return delegate.createCommitter(refName, referencedObjType, resultType);
  }

  @Override
  public <V> Index<V> buildReadIndex(
      @Nullable IndexContainer<V> indexContainer,
      @NonNull IndexValueSerializer<V> indexValueSerializer) {
    return delegate.buildReadIndex(indexContainer, indexValueSerializer);
  }

  @Override
  public <V> UpdatableIndex<V> buildWriteIndex(
      @Nullable IndexContainer<V> indexContainer,
      @NonNull IndexValueSerializer<V> indexValueSerializer) {
    return delegate.buildWriteIndex(indexContainer, indexValueSerializer);
  }

  @NonNull
  @Override
  public Duration objAge(@NonNull Obj obj) {
    return delegate.objAge(obj);
  }

  @Override
  public String realmId() {
    return delegate.realmId();
  }

  @Override
  public MonotonicClock monotonicClock() {
    return delegate.monotonicClock();
  }

  @Override
  public IdGenerator idGenerator() {
    return delegate.idGenerator();
  }

  @Override
  public long currentTimeMicros() {
    return delegate.currentTimeMicros();
  }

  @Override
  public long currentTimeMillis() {
    return delegate.currentTimeMillis();
  }

  @Override
  public Instant currentInstant() {
    return delegate.currentInstant();
  }
}
