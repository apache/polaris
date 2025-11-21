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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.commit.CommitException;
import org.apache.polaris.persistence.nosql.api.commit.CommitRetryable;
import org.apache.polaris.persistence.nosql.api.commit.CommitterState;
import org.apache.polaris.persistence.nosql.api.commit.RetryTimeoutException;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.apache.polaris.persistence.nosql.impl.commits.retry.RetryLoop;
import org.apache.polaris.persistence.nosql.impl.commits.retry.RetryStatsConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommitterImpl<REF_OBJ extends BaseCommitObj, RESULT>
    implements CommitterWithStats<REF_OBJ, RESULT> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CommitterImpl.class);

  /**
   * For testing purposes, add a random sleep within the given bound in milliseconds before each
   * commit attempt's reference bump attempt. This value can be useful when debugging concurrency
   * issues.
   */
  private static final int RANDOM_SLEEP_BOUND =
      Integer.getInteger("x-polaris.persistence.committer.random.sleep-bound", 0);

  private final Persistence persistence;
  private final String refName;
  private final Class<REF_OBJ> referenceType;
  private final Class<RESULT> resultType;
  private boolean synchronizingLocally;

  private static final Object NO_RESULT_SENTINEL = new Object() {};

  @SuppressWarnings("unchecked")
  private RESULT noResultSentinel() {
    return (RESULT) NO_RESULT_SENTINEL;
  }

  CommitterImpl(
      Persistence persistence,
      String refName,
      Class<REF_OBJ> referenceType,
      Class<RESULT> resultType) {
    this.persistence = persistence;
    this.refName = refName;
    this.referenceType = referenceType;
    this.resultType = resultType;
  }

  @Override
  public CommitterWithStats<REF_OBJ, RESULT> synchronizingLocally() {
    this.synchronizingLocally = true;
    return this;
  }

  @Override
  public Optional<RESULT> commit(CommitRetryable<REF_OBJ, RESULT> commitRetryable)
      throws CommitException, RetryTimeoutException {
    return commit(commitRetryable, null);
  }

  @Override
  public Optional<RESULT> commit(
      CommitRetryable<REF_OBJ, RESULT> commitRetryable, RetryStatsConsumer retryStatsConsumer)
      throws CommitException, RetryTimeoutException {
    var committerState = new CommitterStateImpl<REF_OBJ, RESULT>(persistence);
    LOGGER.debug("commit start");

    var sync =
        synchronizingLocally
            ? ExclusiveCommitSynchronizer.forKey(persistence.realmId(), refName)
            : CommitSynchronizer.NON_SYNCHRONIZING;

    try {
      var retryConfig = persistence.params().retryConfig();
      var loop = RetryLoop.<RESULT>newRetryLoop(retryConfig, persistence.monotonicClock());
      if (retryStatsConsumer != null) {
        loop.setRetryStatsConsumer(retryStatsConsumer);
      }
      var result =
          loop.retryLoop(
              nanosRemaining -> {
                try {
                  sync.before(nanosRemaining);
                  return commitAttempt(committerState, commitRetryable);
                } finally {
                  sync.after();
                }
              });
      if (result == noResultSentinel()) {
        LOGGER.debug("commit() yielding no result");
        return Optional.ofNullable(committerState.result);
      }
      LOGGER.debug("commit() yielding result");
      return Optional.of(result);
    } catch (RetryTimeoutException | RuntimeException e) {
      LOGGER.debug("commit() failed");
      committerState.deleteIds.addAll(committerState.allPersistedIds);
      throw e;
    } finally {
      committerState.deleteIds.removeAll(committerState.mustNotDelete);
      if (!committerState.deleteIds.isEmpty()) {
        LOGGER.debug("commit() deleting {}", committerState.deleteIds);
        persistence.deleteMany(committerState.deleteIds.toArray(new ObjRef[0]));
      }
    }
  }

  static final class CommitterStateImpl<REF_OBJ extends BaseCommitObj, RESULT>
      implements CommitterState<REF_OBJ, RESULT> {
    final Persistence persistence;
    private Persistence delegate;
    final Map<ObjRef, Obj> forAttempt = new LinkedHashMap<>();
    final Set<ObjRef> allPersistedIds = new HashSet<>();
    final Set<ObjRef> idsUsed = new HashSet<>();
    final Set<ObjRef> mustNotDelete = new HashSet<>();
    final Set<ObjRef> deleteIds = new HashSet<>();
    final Map<Object, Obj> objs = new HashMap<>();
    boolean noCommit;
    RESULT result;

    CommitterStateImpl(Persistence persistence) {
      this.persistence = persistence;
    }

    @Override
    public Optional<REF_OBJ> noCommit(@Nonnull RESULT result) {
      noCommit = true;
      this.result = result;
      return Optional.empty();
    }

    @Override
    public Optional<REF_OBJ> noCommit() {
      noCommit = true;
      this.result = null;
      return Optional.empty();
    }

    @Override
    public Persistence persistence() {
      var delegate = this.delegate;
      if (delegate == null) {
        delegate =
            this.delegate =
                new DelegatingPersistence(persistence) {
                  @Nullable
                  @Override
                  public <T extends Obj> T fetch(@Nonnull ObjRef id, @Nonnull Class<T> clazz) {
                    T obj = getWrittenById(id, clazz);
                    if (obj == null) {
                      obj = persistence.fetch(id, clazz);
                      if (obj != null) {
                        mustNotDelete.add(id);
                      }
                    }
                    return obj;
                  }

                  @Nonnull
                  @Override
                  public <T extends Obj> T[] fetchMany(
                      @Nonnull Class<T> clazz, @Nonnull ObjRef... ids) {
                    @SuppressWarnings("unchecked")
                    var r = (T[]) Array.newInstance(clazz, ids.length);
                    var persistenceIds = Arrays.copyOf(ids, ids.length);

                    var left = 0;
                    for (int i = 0; i < persistenceIds.length; i++) {
                      var id = persistenceIds[i];
                      if (id != null) {
                        var obj = getWrittenById(id, clazz);
                        if (obj != null) {
                          r[i] = obj;
                          persistenceIds[i] = null;
                        } else {
                          left++;
                        }
                      }
                    }

                    if (left > 0) {
                      var fromPersistence = persistence.fetchMany(clazz, persistenceIds);
                      for (int i = 0; i < fromPersistence.length; i++) {
                        var obj = fromPersistence[i];
                        if (obj != null) {
                          r[i] = obj;
                          mustNotDelete.add(ids[i]);
                        }
                      }
                    }

                    return r;
                  }
                };
      }
      return delegate;
    }

    @Override
    public <B extends BaseCommitObj.Builder<REF_OBJ, B>> Optional<REF_OBJ> commitResult(
        @Nonnull RESULT result, @Nonnull B refObjBuilder, @Nonnull Optional<REF_OBJ> refObj) {
      long[] tail;
      if (refObj.isPresent()) {
        var r = refObj.get();
        refObjBuilder.seq(r.seq() + 1);
        var t = r.tail();
        var max = persistence.params().referencePreviousHeadCount();
        if (t.length < max) {
          tail = new long[t.length + 1];
          System.arraycopy(t, 0, tail, 1, t.length);
        } else {
          tail = new long[max];
          System.arraycopy(t, 0, tail, 1, max - 1);
        }
        tail[0] = r.id();
      } else {
        tail = new long[0];
        refObjBuilder.seq(1L);
      }
      this.result = requireNonNull(result);
      var id = persistence.generateId();
      return Optional.of(refObjBuilder.id(id).tail(tail).build());
    }

    @Override
    public Obj getWrittenByKey(@Nonnull Object key) {
      return objs.get(key);
    }

    @Override
    public <C extends Obj> C getWrittenById(ObjRef id, Class<C> clazz) {
      @SuppressWarnings("unchecked")
      var r = (C) forAttempt.get(id);
      return r;
    }

    @Override
    public <O extends Obj> O writeIfNew(
        @Nonnull Object key, @Nonnull O obj, @Nonnull Class<O> type) {
      var objId = objRef(obj);
      checkState(
          !mustNotDelete.contains(objId),
          "Object ID '%s' is forbidden, because it is used by a fetched object",
          objId);
      return type.cast(
          objs.computeIfAbsent(
              key,
              k -> {
                // Check state _before_ mutating it
                checkState(
                    !idsUsed.contains(objId),
                    "Object ID '%s' to be persisted has already been used. "
                        + "This is a bug in the calling code.",
                    objId);
                idsUsed.add(objId);
                forAttempt.put(objId, obj);
                return obj;
              }));
    }

    @Override
    public void writeIntent(@Nonnull Object key, @Nonnull Obj obj) {
      var objId = objRef(obj);
      checkState(
          !mustNotDelete.contains(objId),
          "Object ID '%s' is forbidden, because it is used by a fetched object",
          objId);

      // Check state _before_ mutating it
      checkState(
          !idsUsed.contains(objId),
          "Object ID '%s' to be persisted has already been used. "
              + "This is a bug in the calling code.",
          objId);
      checkState(
          objs.putIfAbsent(key, obj) == null, "The object-key '%s' has already been used", key);
      idsUsed.add(objId);
      forAttempt.put(objId, obj);
    }

    @Override
    public <O extends Obj> O writeOrReplace(
        @Nonnull Object key, @Nonnull O obj, @Nonnull Class<O> type) {
      var objId = objRef(obj);
      LOGGER.debug("writeOrReplace '{}' {}", key, objId);
      checkState(
          !mustNotDelete.contains(objId),
          "Object ID '%s' is forbidden, because it is used by a fetched object",
          objId);

      return type.cast(
          objs.compute(
              key,
              (k, ex) -> {
                if (ex != null) {
                  var exId = objRef(ex);
                  // Fail if the ID of the new object is not equal to the ID of the existing object
                  // AND if the ID of the existing object is already scheduled for deletion.
                  var sameId = exId.equals(objId);

                  if (sameId) {
                    checkState(idsUsed.contains(exId));

                    if (forAttempt.get(objId) == ex) {
                      LOGGER.debug("writeOrReplace - same, not yet persisted ID");
                      forAttempt.put(objId, obj);
                      return obj;
                    }

                    throw new IllegalStateException(
                        "Object with the same ID has already been persisted, cannot replace it, key = '"
                            + k
                            + "', objId = "
                            + objId);
                  } else {
                    for (var existing : forAttempt.entrySet()) {
                      var exObj = existing.getValue();
                      if (exObj == ex) {
                        LOGGER.debug("writeOrReplace - same, not yet persisted object");
                        // If there's an object _pending_ to be persisted from the _current_
                        // attempt, remove it
                        checkState(exId.equals(objRef(ex)));
                        checkState(exId.equals(objRef(exObj)));
                        forAttempt.remove(existing.getKey());
                        idsUsed.remove(exId);
                        break;
                      }
                    }
                    LOGGER.debug("writeOrReplace - replacing");
                    deleteIds.add(exId);
                    idsUsed.add(exId);
                    forAttempt.put(objId, obj);
                    idsUsed.add(objId);
                    return obj;
                  }
                }

                // New 'key'
                LOGGER.debug("writeOrReplace - new key");
                checkState(
                    !idsUsed.contains(objId),
                    "Object ID '%s' to be persisted has already been used. "
                        + "This is a bug in the calling code.",
                    objId);
                idsUsed.add(objId);
                forAttempt.put(objId, obj);
                return obj;
              }));
    }
  }

  private void randomDelay() {
    if (RANDOM_SLEEP_BOUND == 0) {
      return;
    }

    var i = ThreadLocalRandom.current().nextInt(RANDOM_SLEEP_BOUND);
    if (i > 0) {
      try {
        Thread.sleep(i);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  Optional<RESULT> commitAttempt(
      CommitterState<REF_OBJ, RESULT> stateApi, CommitRetryable<REF_OBJ, RESULT> commitRetryable)
      throws CommitException {
    LOGGER.debug("commitAttempt");

    var state = (CommitterStateImpl<REF_OBJ, RESULT>) stateApi;

    var referenceHolder = new Reference[1];
    var refObjHolder = new Optional[1];

    var refObjSupplier =
        (Supplier<Optional<REF_OBJ>>)
            () -> {
              var reference = persistence.fetchReferenceForUpdate(refName);
              var refObj = reference.pointer().map(id -> persistence.fetch(id, referenceType));
              refObjHolder[0] = refObj;
              referenceHolder[0] = reference;
              LOGGER.debug(
                  "Referenced object {} for commit attempt for reference '{}'", refObj, refName);
              return refObj;
            };

    var attemptResult = commitRetryable.attempt(state, refObjSupplier);
    if (state.noCommit) {
      LOGGER.debug("Commit-retryable instructs to not commit");
      return Optional.of(noResultSentinel());
    }
    if (attemptResult.isEmpty()) {
      LOGGER.debug("Commit-retryable yields no result");
      return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    var refObj = (Optional<REF_OBJ>) refObjHolder[0];
    var reference = referenceHolder[0];
    checkState(reference != null, "CommitRetryable must call the provided refObj supplier");

    var refObjId = refObj.map(ObjRef::objRef);
    refObjId.ifPresent(state.mustNotDelete::add);

    var resultObj = attemptResult.get();
    checkState(
        referenceType.isInstance(resultObj),
        "Result object is not an instance of %s",
        referenceType);
    var resultObjRef = objRef(resultObj);
    if (refObjId.isPresent() && refObjId.orElseThrow().equals(resultObjRef)) {
      checkState(
          state.forAttempt.isEmpty(),
          "CommitRetryable.attempt() returned the current reference's pointer, in this case it must not attempt to persist any objects");
      checkState(
          resultObj.equals(refObj.orElseThrow()),
          "CommitRetryable.attempt() must not modify the returned object when using the same ID");

      LOGGER.debug("Commit yields no change, not committing");

      if (state.result != null) {
        return Optional.of(state.result);
      }
      if (resultType.isAssignableFrom(referenceType)) {
        return attemptResult.map(resultType::cast);
      }
      throw new IllegalStateException(
          "CommitRetryable.attempt() did not set a result via CommitterState.commitResult and the result type "
              + resultType.getName()
              + " cannot be casted to the reference obj type "
              + referenceType.getName());
    }

    state.forAttempt.put(resultObjRef, resultObj);
    var objs = state.forAttempt.values().toArray(new Obj[0]);
    state.forAttempt.clear();
    var persisted = persistence.writeMany(Obj.class, objs);
    // exclude the resultObj's ID here, handled below
    for (int i = 0; i < persisted.length - 1; i++) {
      state.allPersistedIds.add(objRef(persisted[i]));
    }
    @SuppressWarnings("unchecked")
    var persistedResultObj = (REF_OBJ) persisted[persisted.length - 1];

    // For testing purposes only
    randomDelay();

    var newReference = persistence.updateReferencePointer(reference, resultObjRef);
    if (newReference.isEmpty()) {
      state.deleteIds.add(resultObjRef);
      LOGGER.debug(
          "Unsuccessful commit attempt (will retry, if possible) from {} to {}",
          reference,
          resultObjRef);
    } else {
      state.allPersistedIds.add(resultObjRef);
    }
    return newReference.map(
        newRef -> {
          LOGGER.debug("Successfully commited change from {} to {}", reference, newRef);
          if (state.result != null) {
            return state.result;
          }
          if (resultType.isAssignableFrom(referenceType)) {
            return resultType.cast(persistedResultObj);
          }
          throw new IllegalStateException(
              "CommitRetryable.attempt() did not set a non-null result via CommitterState.commitResult and the result type "
                  + resultType.getName()
                  + " cannot be casted to the reference obj type "
                  + referenceType.getName());
        });
  }
}
