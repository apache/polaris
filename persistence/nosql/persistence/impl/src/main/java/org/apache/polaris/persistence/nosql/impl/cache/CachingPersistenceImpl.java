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
package org.apache.polaris.persistence.nosql.impl.cache;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.polaris.persistence.nosql.api.cache.CacheBackend.NON_EXISTENT_REFERENCE_SENTINEL;
import static org.apache.polaris.persistence.nosql.api.cache.CacheBackend.NOT_FOUND_OBJ_SENTINEL;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.lang.reflect.Array;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.cache.CacheBackend;
import org.apache.polaris.persistence.nosql.api.commit.Commits;
import org.apache.polaris.persistence.nosql.api.commit.Committer;
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
import org.apache.polaris.persistence.nosql.impl.commits.CommitFactory;
import org.apache.polaris.persistence.nosql.impl.indexes.IndexesProvider;

class CachingPersistenceImpl implements Persistence {

  private final String realmId;
  final Persistence delegate;
  final CacheBackend backend;

  CachingPersistenceImpl(Persistence delegate, CacheBackend backend) {
    this.delegate = delegate;
    this.backend = backend;
    this.realmId = delegate.realmId();
  }

  @Nullable
  @Override
  public <T extends Obj> T getImmediate(@Nonnull ObjRef id, @Nonnull Class<T> clazz) {
    var numParts = id.numParts();
    checkArgument(numParts >= 0, "partNum of %s must not be negative", id);
    @SuppressWarnings("unchecked")
    var o = (T) backend.get(realmId, id);
    if (o != null && o != NOT_FOUND_OBJ_SENTINEL) {
      return o;
    }
    return null;
  }

  @Nullable
  @Override
  public <T extends Obj> T fetch(@Nonnull ObjRef id, @Nonnull Class<T> clazz) {
    var numParts = id.numParts();
    checkArgument(numParts >= 0, "partNum of %s must not be negative", id);
    var o = backend.get(realmId, id);
    if (o != null) {
      if (o != NOT_FOUND_OBJ_SENTINEL) {
        return checkCast(o, clazz);
      }
      return null;
    }

    var f = delegate.fetch(id, clazz);
    if (f == null) {
      backend.putNegative(realmId, id);
    } else {
      backend.putLocal(realmId, f);
    }
    return f;
  }

  @Nonnull
  @Override
  public <T extends Obj> T[] fetchMany(@Nonnull Class<T> clazz, @Nonnull ObjRef... ids) {
    @SuppressWarnings("unchecked")
    var r = (T[]) Array.newInstance(clazz, ids.length);

    var backendIds = fetchObjsPre(ids, r, clazz);

    if (backendIds == null) {
      return r;
    }

    var backendResult = delegate.fetchMany(clazz, backendIds);
    return fetchObjsPost(backendIds, backendResult, r);
  }

  private <T extends Obj> ObjRef[] fetchObjsPre(ObjRef[] ids, T[] r, Class<T> clazz) {
    ObjRef[] backendIds = null;
    for (var i = 0; i < ids.length; i++) {
      var id = ids[i];
      if (id == null) {
        continue;
      }
      var numParts = id.numParts();
      checkArgument(numParts >= 0, "partNum of %s must not be negative", id);
      var o = backend.get(realmId, id);
      if (o != null) {
        if (o != NOT_FOUND_OBJ_SENTINEL) {
          r[i] = checkCast(o, clazz);
        }
      } else {
        if (backendIds == null) {
          backendIds = new ObjRef[ids.length];
        }
        backendIds[i] = id;
      }
    }
    return backendIds;
  }

  private <T extends Obj> T checkCast(Obj o, Class<T> clazz) {
    var type = o.type();
    var typeClass = type.targetClass();
    checkArgument(
        clazz.isAssignableFrom(typeClass),
        "Mismatch between persisted object type '%s' (%s) and deserialized %s. "
            + "The object ID %s is possibly already used by another object. "
            + "If the deserialized type is a GenericObj, ensure that the artifact providing the corresponding ObjType implementation is present and is present in META-INF/services/%s",
        type.id(),
        typeClass,
        clazz,
        o.id(),
        ObjType.class.getName());
    return clazz.cast(o);
  }

  private <T extends Obj> T[] fetchObjsPost(ObjRef[] backendIds, T[] backendResult, T[] r) {
    for (var i = 0; i < backendResult.length; i++) {
      var id = backendIds[i];
      if (id != null) {
        var o = backendResult[i];
        if (o != null) {
          r[i] = o;
          backend.putLocal(realmId, o);
        } else {
          backend.putNegative(realmId, id);
        }
      }
    }
    return r;
  }

  @Nonnull
  @Override
  public <T extends Obj> T write(@Nonnull T obj, @Nonnull Class<T> clazz) {
    obj = delegate.write(obj, clazz);
    backend.put(realmId, obj);
    return obj;
  }

  @SafeVarargs
  @Nonnull
  @Override
  public final <T extends Obj> T[] writeMany(@Nonnull Class<T> clazz, @Nonnull T... objs) {
    var written = delegate.writeMany(clazz, objs);
    for (var w : written) {
      if (w != null) {
        backend.put(realmId, w);
      }
    }
    return written;
  }

  @Override
  public void delete(@Nonnull ObjRef id) {
    try {
      delegate.delete(id);
    } finally {
      backend.remove(realmId, id);
    }
  }

  @Override
  public void deleteMany(@Nonnull ObjRef... ids) {
    try {
      delegate.deleteMany(ids);
    } finally {
      for (var id : ids) {
        if (id != null) {
          backend.remove(realmId, id);
        }
      }
    }
  }

  @Nullable
  @Override
  public <T extends Obj> T conditionalInsert(@Nonnull T obj, @Nonnull Class<T> clazz) {
    var r = delegate.conditionalInsert(obj, clazz);
    if (r != null) {
      backend.put(realmId, obj);
    } else {
      backend.remove(realmId, objRef(obj));
    }
    return r;
  }

  @Nullable
  @Override
  public <T extends Obj> T conditionalUpdate(
      @Nonnull T expected, @Nonnull T update, @Nonnull Class<T> clazz) {
    var r = delegate.conditionalUpdate(expected, update, clazz);
    if (r != null) {
      backend.put(realmId, r);
    } else {
      backend.remove(realmId, objRef(expected));
    }
    return r;
  }

  @Override
  public <T extends Obj> boolean conditionalDelete(@Nonnull T expected, Class<T> clazz) {
    try {
      return delegate.conditionalDelete(expected, clazz);
    } finally {
      backend.remove(realmId, objRef(expected));
    }
  }

  // plain delegates...

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

  @Override
  public Commits commits() {
    return CommitFactory.newCommits(this);
  }

  @Override
  public <REF_OBJ extends BaseCommitObj, RESULT> Committer<REF_OBJ, RESULT> createCommitter(
      @Nonnull String refName,
      @Nonnull Class<REF_OBJ> referencedObjType,
      @Nonnull Class<RESULT> resultType) {
    return CommitFactory.newCommitter(this, refName, referencedObjType, resultType);
  }

  @Override
  public <V> Index<V> buildReadIndex(
      @Nullable IndexContainer<V> indexContainer,
      @Nonnull IndexValueSerializer<V> indexValueSerializer) {
    return IndexesProvider.buildReadIndex(indexContainer, this, indexValueSerializer);
  }

  @Override
  public <V> UpdatableIndex<V> buildWriteIndex(
      @Nullable IndexContainer<V> indexContainer,
      @Nonnull IndexValueSerializer<V> indexValueSerializer) {
    return IndexesProvider.buildWriteIndex(indexContainer, this, indexValueSerializer);
  }

  // References

  @Nonnull
  @Override
  public Reference createReference(@Nonnull String name, @Nonnull Optional<ObjRef> pointer) {
    Reference r = null;
    try {
      return r = delegate.createReference(name, pointer);
    } finally {
      if (r != null) {
        backend.putReference(realmId, r);
      } else {
        backend.removeReference(realmId, name);
      }
    }
  }

  @Override
  public void createReferencesSilent(Set<String> referenceNames) {
    delegate.createReferencesSilent(referenceNames);
    referenceNames.forEach(n -> backend.removeReference(realmId, n));
  }

  @Override
  @Nonnull
  public Optional<Reference> updateReferencePointer(
      @Nonnull Reference reference, @Nonnull ObjRef newPointer) {
    Optional<Reference> r = Optional.empty();
    try {
      r = delegate.updateReferencePointer(reference, newPointer);
    } finally {
      if (r.isPresent()) {
        backend.putReference(realmId, r.get());
      } else {
        backend.removeReference(realmId, reference.name());
      }
    }
    return r;
  }

  @Override
  @Nonnull
  public Reference fetchReference(@Nonnull String name) {
    return fetchReferenceInternal(name, false);
  }

  @Override
  @Nonnull
  public Reference fetchReferenceForUpdate(@Nonnull String name) {
    return fetchReferenceInternal(name, true);
  }

  private Reference fetchReferenceInternal(@Nonnull String name, boolean bypassCache) {
    Reference r = null;
    if (!bypassCache) {
      r = backend.getReference(realmId, name);
      if (r == NON_EXISTENT_REFERENCE_SENTINEL) {
        throw new ReferenceNotFoundException(name);
      }
    }

    if (r == null) {
      try {
        r = delegate.fetchReferenceForUpdate(name);
        backend.putReferenceLocal(realmId, r);
      } catch (ReferenceNotFoundException e) {
        backend.putReferenceNegative(realmId, name);
        throw e;
      }
    }
    return r;
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
  public String toString() {
    return delegate.toString() + ", caching";
  }
}
