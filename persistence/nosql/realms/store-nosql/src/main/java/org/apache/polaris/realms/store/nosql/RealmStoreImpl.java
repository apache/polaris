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
package org.apache.polaris.realms.store.nosql;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.apache.polaris.persistence.nosql.api.Realms.SYSTEM_REALM_ID;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.realms.store.nosql.RealmsStateObj.REALMS_REF_NAME;

import com.google.common.collect.Streams;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.SystemPersistence;
import org.apache.polaris.persistence.nosql.api.commit.Committer;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.realms.api.RealmAlreadyExistsException;
import org.apache.polaris.realms.api.RealmDefinition;
import org.apache.polaris.realms.api.RealmNotFoundException;
import org.apache.polaris.realms.spi.RealmStore;

@ApplicationScoped
class RealmStoreImpl implements RealmStore {
  private final Persistence systemPersistence;
  private final Committer<RealmsStateObj, RealmObj> committer;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  RealmStoreImpl(@Nonnull @SystemPersistence Persistence systemPersistence) {
    checkArgument(
        SYSTEM_REALM_ID.equals(systemPersistence.realmId()),
        "Realms management must happen in the %s realm",
        SYSTEM_REALM_ID);

    systemPersistence.createReferenceSilent(REALMS_REF_NAME);

    this.systemPersistence = systemPersistence;

    this.committer =
        systemPersistence.createCommitter(REALMS_REF_NAME, RealmsStateObj.class, RealmObj.class);
  }

  @Override
  public Stream<RealmDefinition> list() {
    var realmsIndex =
        systemPersistence
            .fetchReferenceHead(REALMS_REF_NAME, RealmsStateObj.class)
            .map(realms -> realms.realmIndex().indexForRead(systemPersistence, OBJ_REF_SERIALIZER));
    return realmsIndex
        .map(
            entries ->
                Streams.stream(entries)
                    .map(
                        e -> {
                          var realmObj = systemPersistence.fetch(e.getValue(), RealmObj.class);
                          if (realmObj == null) {
                            return null;
                          }
                          var realmId = e.getKey().toString();
                          return objToDefinition(realmId, realmObj);
                        })
                    .filter(Objects::nonNull))
        .orElse(Stream.empty());
  }

  @Override
  public Optional<RealmDefinition> get(String realmId) {
    return systemPersistence
        .fetchReferenceHead(REALMS_REF_NAME, RealmsStateObj.class)
        .map(realms -> realmFromState(realms, realmId));
  }

  @Override
  public RealmDefinition create(String realmId, RealmDefinition definition) {
    var realm =
        committer.commitRuntimeException(
            (state, refObjSupplier) -> {
              var refObj = refObjSupplier.get();
              var current = refObj.orElse(null);

              var key = IndexKey.key(realmId);

              var index =
                  current != null
                      ? current.realmIndex().asUpdatableIndex(systemPersistence, OBJ_REF_SERIALIZER)
                      : IndexContainer.newUpdatableIndex(systemPersistence, OBJ_REF_SERIALIZER);
              if (index.contains(key)) {
                throw new RealmAlreadyExistsException(
                    format("A realm with ID '%s' already exists", realmId));
              }

              var obj =
                  state.writeIfNew(
                      "realm",
                      RealmObj.builder()
                          .created(definition.created())
                          .updated(definition.updated())
                          .id(systemPersistence.generateId())
                          .status(definition.status())
                          .properties(definition.properties())
                          .build(),
                      RealmObj.class);

              index.put(key, objRef(obj));

              var newRealms =
                  RealmsStateObj.builder()
                      .realmIndex(index.toIndexed("idx-", state::writeOrReplace));

              return state.commitResult(obj, newRealms, refObj);
            });

    return objToDefinition(realmId, realm.orElseThrow());
  }

  @Override
  public RealmDefinition update(
      String realmId, Function<RealmDefinition, RealmDefinition> updater) {
    var realm =
        committer.commitRuntimeException(
            (state, refObjSupplier) -> {
              var refObj = refObjSupplier.get();
              var current = refObj.orElse(null);
              if (current == null) {
                throw new RealmNotFoundException(format("No realm with ID '%s' exists", realmId));
              }

              var key = IndexKey.key(realmId);

              var index =
                  current.realmIndex().asUpdatableIndex(systemPersistence, OBJ_REF_SERIALIZER);
              var currentObjId = index.get(key);
              if (currentObjId == null) {
                throw new RealmNotFoundException(format("No realm with ID '%s' exists", realmId));
              }

              var currentObj = systemPersistence.fetch(currentObjId, RealmObj.class);
              if (currentObj == null) {
                throw new RealmNotFoundException(
                    format("RealmObj for realm with ID '%s' does not exist", realmId));
              }

              var update = updater.apply(objToDefinition(realmId, currentObj));

              var obj =
                  state.writeIfNew(
                      "realm",
                      RealmObj.builder()
                          .created(currentObj.created())
                          .updated(update.updated())
                          .id(systemPersistence.generateId())
                          .status(update.status())
                          .properties(update.properties())
                          .build(),
                      RealmObj.class);

              index.put(key, objRef(obj));

              var newRealms =
                  RealmsStateObj.builder()
                      .realmIndex(index.toIndexed("idx-", state::writeOrReplace));

              return state.commitResult(obj, newRealms, refObj);
            });

    return objToDefinition(realmId, realm.orElseThrow());
  }

  @Override
  public void delete(String realmId, Consumer<RealmDefinition> callback) {
    committer.commitRuntimeException(
        (state, refObjSupplier) -> {
          var refObj = refObjSupplier.get();
          var current = refObj.orElse(null);
          if (current == null) {
            throw new RealmNotFoundException(format("No realm with ID '%s' exists", realmId));
          }

          var key = IndexKey.key(realmId);

          var index = current.realmIndex().asUpdatableIndex(systemPersistence, OBJ_REF_SERIALIZER);
          var currentObjId = index.get(key);
          if (currentObjId == null) {
            throw new RealmNotFoundException(format("No realm with ID '%s' exists", realmId));
          }

          var currentObj = systemPersistence.fetch(currentObjId, RealmObj.class);
          if (currentObj == null) {
            throw new RealmNotFoundException(
                format("RealmObj for realm with ID '%s' does not exist", realmId));
          }

          callback.accept(objToDefinition(realmId, currentObj));

          index.remove(key);

          var newRealms =
              RealmsStateObj.builder().realmIndex(index.toIndexed("idx-", state::writeOrReplace));

          return state.commitResult(currentObj, newRealms, refObj);
        });
  }

  private RealmDefinition realmFromState(RealmsStateObj realms, String realmId) {
    var index = realms.realmIndex().indexForRead(systemPersistence, OBJ_REF_SERIALIZER);
    var realmDefId = index.get(IndexKey.key(realmId));
    if (realmDefId == null) {
      return null;
    }
    var obj = systemPersistence.fetch(realmDefId, RealmObj.class);
    checkState(obj != null, "No realm definition object for realm ID '%s'", realmId);
    return objToDefinition(realmId, obj);
  }

  private static RealmDefinition objToDefinition(String realmId, RealmObj obj) {
    return RealmDefinition.builder()
        .id(realmId)
        .created(obj.created())
        .updated(obj.updated())
        .status(obj.status())
        .properties(obj.properties())
        .build();
  }
}
