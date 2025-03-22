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
package org.apache.polaris.persistence.cdi.persistence;

import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.api.Persistence;
import org.apache.polaris.persistence.api.PersistenceParams;
import org.apache.polaris.persistence.api.exceptions.ReferenceAlreadyExistsException;
import org.apache.polaris.persistence.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.api.obj.Obj;
import org.apache.polaris.persistence.api.obj.ObjRef;
import org.apache.polaris.persistence.api.obj.ObjType;
import org.apache.polaris.persistence.api.ref.Reference;
import org.apache.polaris.persistence.base.delegate.PersistenceWithCommitsIndexes;
import org.apache.polaris.realms.id.RealmId;

public abstract class ObservingPersistence implements Persistence, PersistenceWithCommitsIndexes {
  private static final String PREFIX = "polaris.persistence";

  // TODO @MeterTag(key = "realm_id", resolver = PersistenceRealmIdResolver.class)

  abstract Persistence delegate();

  @Nonnull
  @Override
  @WithSpan
  @Counted(PREFIX + ".createReference")
  @Timed(value = PREFIX + ".createReference", histogram = true)
  public Reference createReference(@Nonnull String name, @Nonnull Optional<ObjRef> pointer)
      throws ReferenceAlreadyExistsException {
    return delegate().createReference(name, pointer);
  }

  @Override
  @WithSpan
  @Counted(PREFIX + ".createReferenceSilent")
  @Timed(value = PREFIX + ".createReferenceSilent", histogram = true)
  public void createReferenceSilent(@Nonnull String name) {
    delegate().createReferenceSilent(name);
  }

  @Nonnull
  @Override
  @WithSpan
  @Counted(PREFIX + ".createOrFetchReference")
  @Timed(value = PREFIX + ".createOrFetchReference", histogram = true)
  public Reference createOrFetchReference(
      @Nonnull String name, @Nonnull Supplier<Optional<ObjRef>> pointerForCreate) {
    return delegate().createOrFetchReference(name, pointerForCreate);
  }

  @Nonnull
  @Override
  @WithSpan
  @Counted(PREFIX + ".updateReferencePointer")
  @Timed(value = PREFIX + ".updateReferencePointer", histogram = true)
  public Optional<Reference> updateReferencePointer(
      @Nonnull Reference reference, @Nonnull ObjRef newPointer) throws ReferenceNotFoundException {
    return delegate().updateReferencePointer(reference, newPointer);
  }

  @Nonnull
  @Override
  @WithSpan
  @Counted(PREFIX + ".fetchReference")
  @Timed(value = PREFIX + ".fetchReference", histogram = true)
  public Reference fetchReference(@Nonnull String name) throws ReferenceNotFoundException {
    return delegate().fetchReference(name);
  }

  @Nonnull
  @Override
  @WithSpan
  @Counted(PREFIX + ".fetchReferenceForUpdate")
  @Timed(value = PREFIX + ".fetchReferenceForUpdate", histogram = true)
  public Reference fetchReferenceForUpdate(@Nonnull String name) throws ReferenceNotFoundException {
    return delegate().fetchReferenceForUpdate(name);
  }

  @Override
  @WithSpan
  @Counted(PREFIX + ".fetchReferenceHead")
  @Timed(value = PREFIX + ".fetchReferenceHead", histogram = true)
  public <T extends Obj> Optional<T> fetchReferenceHead(
      @Nonnull String name, @Nonnull Class<T> clazz) throws ReferenceNotFoundException {
    return delegate().fetchReferenceHead(name, clazz);
  }

  @Nullable
  @Override
  @WithSpan
  @Counted(PREFIX + ".fetch")
  @Timed(value = PREFIX + ".fetch", histogram = true)
  public <T extends Obj> T fetch(@Nonnull ObjRef id, @Nonnull Class<T> clazz) {
    return delegate().fetch(id, clazz);
  }

  @Nonnull
  @Override
  @WithSpan
  @Counted(PREFIX + ".fetchMany")
  @Timed(value = PREFIX + ".fetchMany", histogram = true)
  public <T extends Obj> T[] fetchMany(@Nonnull Class<T> clazz, @Nonnull ObjRef... ids) {
    return delegate().fetchMany(clazz, ids);
  }

  @Nonnull
  @Override
  @WithSpan
  @Counted(PREFIX + ".write")
  @Timed(value = PREFIX + ".write", histogram = true)
  public <T extends Obj> T write(@Nonnull T obj, @Nonnull Class<T> clazz) {
    return delegate().write(obj, clazz);
  }

  @SafeVarargs
  @Nonnull
  @Override
  @WithSpan
  @Counted(PREFIX + ".writeMany")
  @Timed(value = PREFIX + ".writeMany", histogram = true)
  public final <T extends Obj> T[] writeMany(@Nonnull Class<T> clazz, @Nonnull T... objs) {
    return delegate().writeMany(clazz, objs);
  }

  @Override
  @WithSpan
  @Counted(PREFIX + ".delete")
  @Timed(value = PREFIX + ".delete", histogram = true)
  public void delete(@Nonnull ObjRef id) {
    delegate().delete(id);
  }

  @Override
  @WithSpan
  @Counted(PREFIX + ".deleteMany")
  @Timed(value = PREFIX + ".deleteMany", histogram = true)
  public void deleteMany(@Nonnull ObjRef... ids) {
    delegate().deleteMany(ids);
  }

  @Nullable
  @Override
  @WithSpan
  @Counted(PREFIX + ".conditionalInsert")
  @Timed(value = PREFIX + ".conditionalInsert", histogram = true)
  public <T extends Obj> T conditionalInsert(@Nonnull T obj, @Nonnull Class<T> clazz) {
    return delegate().conditionalInsert(obj, clazz);
  }

  @Nullable
  @Override
  @WithSpan
  @Counted(PREFIX + ".conditionalUpdate")
  @Timed(value = PREFIX + ".conditionalUpdate", histogram = true)
  public <T extends Obj> T conditionalUpdate(
      @Nonnull T expected, @Nonnull T update, @Nonnull Class<T> clazz) {
    return delegate().conditionalUpdate(expected, update, clazz);
  }

  @Override
  @WithSpan
  @Counted(PREFIX + ".conditionalDelete")
  @Timed(value = PREFIX + ".conditionalDelete", histogram = true)
  public <T extends Obj> boolean conditionalDelete(@Nonnull T expected, Class<T> clazz) {
    return delegate().conditionalDelete(expected, clazz);
  }

  // simple getter, not traced/observed
  @Override
  public PersistenceParams params() {
    return delegate().params();
  }

  // simple getter, not traced/observed
  @Override
  public int maxSerializedValueSize() {
    return delegate().maxSerializedValueSize();
  }

  // fast operation, not traced/observed
  @Override
  public long generateId() {
    return delegate().generateId();
  }

  // fast operation, not traced/observed
  @Override
  public ObjRef generateObjId(ObjType type) {
    return delegate().generateObjId(type);
  }

  // fast operation, not traced/observed
  @Nullable
  @Override
  public <T extends Obj> T getImmediate(@Nonnull ObjRef id, @Nonnull Class<T> clazz) {
    return delegate().getImmediate(id, clazz);
  }

  @Override
  public IdGenerator idGenerator() {
    return delegate().idGenerator();
  }

  @Override
  public MonotonicClock monotonicClock() {
    return delegate().monotonicClock();
  }

  @Override
  public RealmId realmId() {
    return delegate().realmId();
  }

  @Override
  public String toString() {
    return delegate().toString() + ", observing";
  }
}
