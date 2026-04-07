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
package org.apache.polaris.persistence.nosql.maintenance.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyIterator;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import com.google.common.collect.AbstractIterator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
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
import org.apache.polaris.persistence.nosql.maintenance.spi.ObjTypeRetainedIdentifier;
import org.apache.polaris.persistence.nosql.maintenance.spi.RetainedCollector;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** {@link RetainedCollector} implementation, per realm. */
final class RetainedCollectorImpl implements Persistence, RetainedCollector {
  private final Persistence persistence;
  private final AllRetained allRetained;
  private final String realmId;
  private final Map<String, List<ObjTypeRetainedIdentifier>> objTypeRetainedIdentifiers;

  private final Set<Long> currentNesting = new HashSet<>();

  RetainedCollectorImpl(
      Persistence persistence,
      AllRetained allRetained,
      Map<String, List<ObjTypeRetainedIdentifier>> objTypeRetainedIdentifiers) {
    this.persistence = persistence;
    this.allRetained = allRetained;
    this.realmId = persistence.realmId();
    this.objTypeRetainedIdentifiers = objTypeRetainedIdentifiers;
  }

  @NonNull
  @Override
  public String realm() {
    return realmId;
  }

  @NonNull
  @Override
  public Persistence realmPersistence() {
    return this;
  }

  @Override
  public void retainObject(@NonNull ObjRef objRef) {
    if (!currentNesting.add(objRef.id())) {
      return;
    }
    try {
      allRetained.addRetainedObj(realmId, objRef.id());

      var otIdents = objTypeRetainedIdentifiers.get(objRef.type());
      if (otIdents != null) {
        for (var otIdent : otIdents) {
          otIdent.identifyRelatedObj(this, objRef);
        }
      }
    } finally {
      currentNesting.remove(objRef.id());
    }
  }

  @Override
  public void retainReference(@NonNull String name) {
    allRetained.addRetainedRef(realmId, name);
  }

  // Persistence delegate

  @NonNull
  @Override
  public Reference createReference(@NonNull String name, @NonNull Optional<ObjRef> pointer)
      throws ReferenceAlreadyExistsException {
    retainReference(name);
    pointer.ifPresent(this::retainObject);
    return persistence.createReference(name, pointer);
  }

  @Override
  public void createReferenceSilent(@NonNull String name) {
    retainReference(name);
    persistence.createReferenceSilent(name);
  }

  @Override
  public void createReferencesSilent(Set<String> referenceNames) {
    referenceNames.forEach(this::retainReference);
    persistence.createReferencesSilent(referenceNames);
  }

  @NonNull
  @Override
  public Reference fetchOrCreateReference(
      @NonNull String name, @NonNull Supplier<Optional<ObjRef>> pointerForCreate) {
    try {
      return fetchReference(name);
    } catch (ReferenceNotFoundException e) {
      try {
        var objRef = pointerForCreate.get();
        objRef.ifPresent(this::retainObject);
        return createReference(name, objRef);
      } catch (ReferenceAlreadyExistsException x) {
        // Unlikely that we ever get here (ref does not exist (but then concurrently created)
        return fetchReference(name);
      }
    }
  }

  @NonNull
  @Override
  public Optional<Reference> updateReferencePointer(
      @NonNull Reference reference, @NonNull ObjRef newPointer) throws ReferenceNotFoundException {
    retainReference(reference.name());
    retainObject(newPointer);
    return persistence.updateReferencePointer(reference, newPointer);
  }

  @NonNull
  @Override
  public Reference fetchReference(@NonNull String name) throws ReferenceNotFoundException {
    retainReference(name);
    var ref = persistence.fetchReference(name);
    ref.pointer().ifPresent(this::retainObject);
    return ref;
  }

  @NonNull
  @Override
  public Reference fetchReferenceForUpdate(@NonNull String name) throws ReferenceNotFoundException {
    retainReference(name);
    var ref = persistence.fetchReferenceForUpdate(name);
    ref.pointer().ifPresent(this::retainObject);
    return ref;
  }

  @Override
  public <T extends Obj> Optional<T> fetchReferenceHead(
      @NonNull String name, @NonNull Class<T> clazz) throws ReferenceNotFoundException {
    retainReference(name);
    var ref = persistence.fetchReferenceHead(name, clazz);
    ref.ifPresent(head -> retainObject(objRef(head)));
    return ref;
  }

  @Nullable
  @Override
  public <T extends Obj> T fetch(@NonNull ObjRef id, @NonNull Class<T> clazz) {
    retainObject(id);
    return persistence.fetch(id, clazz);
  }

  @NonNull
  @Override
  public <T extends Obj> T[] fetchMany(@NonNull Class<T> clazz, @NonNull ObjRef... ids) {
    for (var id : ids) {
      if (id != null) {
        retainObject(id);
      }
    }
    return persistence.fetchMany(clazz, ids);
  }

  @NonNull
  @Override
  public <T extends Obj> T write(@NonNull T obj, @NonNull Class<T> clazz) {
    retainObject(objRef(obj));
    return persistence.write(obj, clazz);
  }

  @SafeVarargs
  @NonNull
  @Override
  public final <T extends Obj> T[] writeMany(@NonNull Class<T> clazz, @NonNull T... objs) {
    for (var obj : objs) {
      if (obj != null) {
        retainObject(objRef(obj));
      }
    }
    return persistence.writeMany(clazz, objs);
  }

  @Override
  public void delete(@NonNull ObjRef id) {
    persistence.delete(id);
  }

  @Override
  public void deleteMany(@NonNull ObjRef... ids) {
    persistence.deleteMany(ids);
  }

  @Nullable
  @Override
  public <T extends Obj> T conditionalInsert(@NonNull T obj, @NonNull Class<T> clazz) {
    retainObject(objRef(obj));
    return persistence.conditionalInsert(obj, clazz);
  }

  @Nullable
  @Override
  public <T extends Obj> T conditionalUpdate(
      @NonNull T expected, @NonNull T update, @NonNull Class<T> clazz) {
    retainObject(objRef(update));
    return persistence.conditionalUpdate(expected, update, clazz);
  }

  @Override
  public <T extends Obj> boolean conditionalDelete(@NonNull T expected, Class<T> clazz) {
    retainObject(objRef(expected));
    return persistence.conditionalDelete(expected, clazz);
  }

  @Override
  public PersistenceParams params() {
    return persistence.params();
  }

  @Override
  public int maxSerializedValueSize() {
    return persistence.maxSerializedValueSize();
  }

  @Override
  public long generateId() {
    return persistence.generateId();
  }

  @Override
  public ObjRef generateObjId(ObjType type) {
    return persistence.generateObjId(type);
  }

  @Nullable
  @Override
  public <T extends Obj> T getImmediate(@NonNull ObjRef id, @NonNull Class<T> clazz) {
    retainObject(id);
    return persistence.getImmediate(id, clazz);
  }

  @Override
  public String realmId() {
    return persistence.realmId();
  }

  @Override
  public MonotonicClock monotonicClock() {
    return persistence.monotonicClock();
  }

  @Override
  public IdGenerator idGenerator() {
    return persistence.idGenerator();
  }

  @Override
  public <V> UpdatableIndex<V> buildWriteIndex(
      @Nullable IndexContainer<V> indexContainer,
      @NonNull IndexValueSerializer<V> indexValueSerializer) {
    return persistence.buildWriteIndex(indexContainer, indexValueSerializer);
  }

  @Override
  public <V> Index<V> buildReadIndex(
      @Nullable IndexContainer<V> indexContainer,
      @NonNull IndexValueSerializer<V> indexValueSerializer) {
    return persistence.buildReadIndex(indexContainer, indexValueSerializer);
  }

  @Override
  public <REF_OBJ extends BaseCommitObj, RESULT> Committer<REF_OBJ, RESULT> createCommitter(
      @NonNull String refName,
      @NonNull Class<REF_OBJ> referencedObjType,
      @NonNull Class<RESULT> resultType) {
    throw new UnsupportedOperationException(
        "Committing operations not supported during retained-objects identification");
  }

  @Override
  public Commits commits() {
    return new Commits() {
      @Override
      public <C extends BaseCommitObj> Iterator<C> commitLog(
          String refName, OptionalLong offset, Class<C> clazz) {
        checkArgument(
            offset.isEmpty(), "Commit offset must be empty during retained-objects identification");

        var ref = fetchReference(refName);

        return ref.pointer()
            .map(
                head ->
                    (Iterator<C>)
                        new AbstractIterator<C>() {
                          private ObjRef next = head;

                          @Override
                          protected C computeNext() {
                            if (next == null) {
                              return endOfData();
                            }
                            var r = fetch(next, clazz);
                            if (r == null) {
                              return endOfData();
                            }
                            next = r.directParent().orElse(null);
                            return r;
                          }
                        })
            .orElse(emptyIterator());
      }

      @Override
      public <C extends BaseCommitObj> Iterator<C> commitLogReversed(
          String refName, long offset, Class<C> clazz) {
        throw new UnsupportedOperationException(
            "Reversed commit scanning not supported during retained-objects identification");
      }
    };
  }
}
