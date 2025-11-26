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
package org.apache.polaris.persistence.nosql.maintenance.spi;

import static org.apache.polaris.persistence.nosql.api.Realms.SYSTEM_REALM_ID;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;

import jakarta.annotation.Nonnull;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.polaris.maintenance.cel.CelReferenceContinuePredicate;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.commit.Commits;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexStripe;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.Reference;

public interface RetainedCollector {
  /** ID of the realm being processed. */
  @Nonnull
  String realm();

  default boolean isSystemRealm() {
    return SYSTEM_REALM_ID.equals(realm());
  }

  /**
   * {@link Persistence Persistence} configured for the current {@linkplain #realm() realm}.
   *
   * <p>References and objects that are read or written via this {@link Persistence} are
   * automatically retained.
   *
   * <p>The returned {@link Persistence Persistence} bypasses the cache to avoid polluting the
   * production cache with accesses from the maintenance service.
   *
   * <p>If the reference name(s) and {@linkplain ObjRef object IDs} are known in advance, it is more
   * efficient to just call the {@link #retainReference(String)}/{@link #retainObject(ObjRef)}
   * functions, because those will not access the backend database.
   */
  @Nonnull
  Persistence realmPersistence();

  /**
   * Instruct the maintenance service to retain the reference with the given name.
   *
   * <p>References that are fetched via {@link #realmPersistence()} are automatically marked to be
   * retained.
   */
  void retainReference(@Nonnull String name);

  /**
   * Instruct the maintenance service to retain the reference with the given object ID.
   *
   * <p>Objects that are fetched via {@link #realmPersistence()} are automatically marked to be
   * retained.
   */
  void retainObject(@Nonnull ObjRef objRef);

  default <V> void indexRetain(IndexContainer<V> indexContainer) {
    indexContainer.stripes().stream().map(IndexStripe::segment).forEach(this::retainObject);
  }

  /**
   * Same as {@link #refRetain(String, Class, Predicate, Consumer, ProgressListener)}, without a
   * {@link ProgressListener}.
   */
  default <O extends BaseCommitObj> void refRetain(
      String ref, Class<O> clazz, Predicate<O> continuePredicate, Consumer<O> retainedObjConsumer) {
    refRetain(ref, clazz, continuePredicate, retainedObjConsumer, new ProgressListener<>() {});
  }

  interface ProgressListener<O extends BaseCommitObj> {
    default void onCommit(O obj, long commit) {}

    default void onIndexEntry(long inCommit, long total) {}

    default void cut() {}

    default void finished() {}
  }

  /**
   * Functionality to identify the objects in a {@link Reference} to retain by {@linkplain
   * Commits#commitLog(String, OptionalLong, Class) walking} the commit log.
   *
   * <p>For flexibility, consider using {@link CelReferenceContinuePredicate}.
   *
   * @param ref reference name, automatically marked as to-be-retained
   * @param clazz type of the {@linkplain Reference#pointer() referenced objects}
   * @param continuePredicate predicate to test whether to continue processing the reference
   * @param retainedObjConsumer called for every retained object
   * @param <O> type of the {@linkplain Reference#pointer() referenced objects}
   */
  default <O extends BaseCommitObj> void refRetain(
      String ref,
      Class<O> clazz,
      Predicate<O> continuePredicate,
      Consumer<O> retainedObjConsumer,
      ProgressListener<O> progressListener) {
    var persistence = realmPersistence();
    var commits = persistence.commits();
    var numCommits = 0L;
    for (var iter = commits.commitLog(ref, OptionalLong.empty(), clazz); iter.hasNext(); ) {
      var obj = iter.next();
      retainedObjConsumer.accept(obj);

      progressListener.onCommit(obj, ++numCommits);

      // WARNING! The "stop" predicate must happen AFTER all referenced objects have been retained,
      // doing the test before the loop over the above index would lead to the inconsistent state!
      if (!continuePredicate.test(obj)) {
        progressListener.cut();
        break;
      }
    }
    progressListener.finished();
  }

  /**
   * Same as {@link #refRetainIndexToSingleObj(String, Class, Predicate, Function, ProgressListener,
   * Consumer)}, but without a {@link ProgressListener}.
   */
  default <O extends BaseCommitObj> void refRetainIndexToSingleObj(
      String ref,
      Class<O> clazz,
      Predicate<O> continuePredicate,
      Function<O, IndexContainer<ObjRef>> indexToObjIdFromRetainedObj,
      Consumer<O> retainedObjConsumer) {
    refRetainIndexToSingleObj(
        ref,
        clazz,
        continuePredicate,
        indexToObjIdFromRetainedObj,
        new ProgressListener<>() {
          @Override
          public void onCommit(O obj, long commit) {
            retainedObjConsumer.accept(obj);
          }
        },
        x -> {});
  }

  default <O extends BaseCommitObj> void refRetainIndexToSingleObj(
      String ref,
      Class<O> clazz,
      Predicate<O> continuePredicate,
      Function<O, IndexContainer<ObjRef>> indexToObjIdFromRetainedObj) {
    refRetainIndexToSingleObj(ref, clazz, continuePredicate, indexToObjIdFromRetainedObj, x -> {});
  }

  /**
   * Similar to {@link #refRetain(String, Class, Predicate, Consumer)}, with convenience to iterate
   * over an {@link IndexContainer} having {@link ObjRef} index-element values to mark those as
   * to-be-retained.
   *
   * <p>For flexibility, consider using {@link CelReferenceContinuePredicate}.
   *
   * @param ref reference name
   * @param clazz type of the {@linkplain Reference#pointer() referenced objects}
   * @param continuePredicate predicate to test whether to continue processing the reference
   * @param indexToObjIdFromRetainedObj function to extract the {@link IndexContainer} from objects
   * @param <O> type of the {@linkplain Reference#pointer() referenced objects}
   */
  default <O extends BaseCommitObj> void refRetainIndexToSingleObj(
      String ref,
      Class<O> clazz,
      Predicate<O> continuePredicate,
      Function<O, IndexContainer<ObjRef>> indexToObjIdFromRetainedObj,
      ProgressListener<O> progressListener,
      Consumer<ObjRef> indexedObjRefConsumer) {
    var total = new AtomicLong();
    refRetain(
        ref,
        clazz,
        continuePredicate,
        obj -> {
          var elem = 0L;
          var t = total.get();

          // TODO we can save a lot of time when there is a (long) history to retain to skip
          //  inspecting already seen index segments (as a performance optimization.), but that
          //  requires some changes to the index APIs.

          for (var entry :
              indexToObjIdFromRetainedObj
                  .apply(obj)
                  .indexForRead(realmPersistence(), OBJ_REF_SERIALIZER)) {
            ObjRef indexedObjRef = entry.getValue();
            retainObject(indexedObjRef);
            // ^ is for persistence.fetch(principalEntry.getValue(), PrincipalObj.class);
            ++elem;
            progressListener.onIndexEntry(elem, t + elem);

            indexedObjRefConsumer.accept(indexedObjRef);
          }
          total.addAndGet(elem);
        },
        progressListener);
  }

  /*

     Consumer<O> retainedObjConsumer,
  */
}
