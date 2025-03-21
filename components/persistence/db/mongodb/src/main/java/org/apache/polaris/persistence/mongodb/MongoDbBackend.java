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
package org.apache.polaris.persistence.mongodb;

import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static org.apache.polaris.persistence.api.backend.PersistId.persistId;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_OBJ_CREATED_AT;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_OBJ_ID;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_OBJ_PART;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_OBJ_TYPE;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_REALM;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_REF_CREATED_AT;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_REF_NAME;
import static org.apache.polaris.persistence.base.util.Identifiers.TABLE_OBJS;
import static org.apache.polaris.persistence.base.util.Identifiers.TABLE_REFS;
import static org.apache.polaris.persistence.mongodb.MongoDbPersistence.ID_PROPERTY_NAME;
import static org.apache.polaris.persistence.mongodb.MongoDbPersistence.unhandledException;

import com.mongodb.CursorType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.api.Persistence;
import org.apache.polaris.persistence.api.PersistenceParams;
import org.apache.polaris.persistence.api.backend.Backend;
import org.apache.polaris.persistence.api.backend.PersistId;
import org.apache.polaris.realms.id.RealmId;
import org.bson.Document;
import org.bson.conversions.Bson;

final class MongoDbBackend implements Backend {
  private final MongoDbBackendConfig config;
  private final MongoClient client;
  private MongoCollection<Document> refs;
  private MongoCollection<Document> objs;

  MongoDbBackend(MongoDbBackendConfig config) {
    this.config = config;
    this.client = config.client();
  }

  @Nonnull
  MongoCollection<Document> refs() {
    return refs;
  }

  @Nonnull
  MongoCollection<Document> objs() {
    return objs;
  }

  private synchronized void initialize() {
    if (refs == null) {
      String databaseName = config.databaseName();
      MongoDatabase database =
          client.getDatabase(Objects.requireNonNull(databaseName, "Database name must be set"));

      refs = database.getCollection(TABLE_REFS);
      objs = database.getCollection(TABLE_OBJS);
    }
  }

  @Override
  @Nonnull
  public String name() {
    return MongoDbBackendFactory.NAME;
  }

  @Nonnull
  @Override
  public Persistence newPersistence(
      @Nonnull PersistenceParams persistenceParams,
      RealmId realmId,
      MonotonicClock monotonicClock,
      IdGenerator idGenerator) {
    initialize();
    return new MongoDbPersistence(this, persistenceParams, realmId, monotonicClock, idGenerator);
  }

  @Override
  public Optional<String> setupSchema() {
    initialize();
    return Optional.of("database name: " + config.databaseName());
  }

  @Override
  public void deleteRealms(Set<RealmId> realmIds) {
    if (realmIds.isEmpty()) {
      return;
    }

    try {
      Bson realmIdFilter =
          in(
              ID_PROPERTY_NAME + "." + COL_REALM,
              realmIds.stream().map(RealmId::id).collect(Collectors.toSet()));
      var failed = new ArrayList<String>();
      for (var coll : List.of(refs, objs)) {
        var res = coll.deleteMany(realmIdFilter);
        var ack = res.wasAcknowledged();
        var deleted = res.getDeletedCount();
        if (!ack) {
          failed.add(
              "realms deletion of collection "
                  + coll
                  + " was not acknowledged, reported "
                  + deleted
                  + " documents");
        }
      }
      if (!failed.isEmpty()) {
        throw new RuntimeException(String.join(", ", failed));
      }
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public boolean supportsRealmDeletion() {
    return true;
  }

  @Override
  public void batchDeleteRefs(Map<RealmId, Set<String>> realmRefs) {
    var idDocs =
        realmRefs.entrySet().stream()
            .flatMap(
                e -> {
                  var realmId = e.getKey().id();
                  return e.getValue().stream()
                      .map(
                          ref -> {
                            var idDoc = new Document();
                            idDoc.put(COL_REALM, realmId);
                            idDoc.put(COL_REF_NAME, ref);
                            return idDoc;
                          });
                })
            .toList();
    if (idDocs.isEmpty()) {
      return;
    }
    try {
      refs.deleteMany(in(ID_PROPERTY_NAME, idDocs));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public void batchDeleteObjs(Map<RealmId, Set<PersistId>> realmObjs) {
    var idDocs =
        realmObjs.entrySet().stream()
            .flatMap(
                e -> {
                  var realmId = e.getKey().id();
                  return e.getValue().stream()
                      .map(
                          id -> {
                            var idDoc = new Document();
                            idDoc.put(COL_REALM, realmId);
                            idDoc.put(COL_OBJ_ID, id.id());
                            idDoc.put(COL_OBJ_PART, id.part());
                            return idDoc;
                          });
                })
            .toList();
    if (idDocs.isEmpty()) {
      return;
    }
    try {
      objs.deleteMany(in(ID_PROPERTY_NAME, idDocs));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public void scanBackend(
      @Nonnull ReferenceScanCallback referenceConsumer, @Nonnull ObjScanCallback objConsumer) {
    try (var cursor =
        refs.find()
            .batchSize(50)
            .cursorType(CursorType.NonTailable)
            .projection(
                fields(include(ID_PROPERTY_NAME, COL_REALM, COL_REF_NAME, COL_REF_CREATED_AT)))
            .cursor()) {
      while (cursor.hasNext()) {
        var d = cursor.next();
        var id = d.get(ID_PROPERTY_NAME, Document.class);
        var realm = id.getString(COL_REALM);
        var ref = id.getString(COL_REF_NAME);
        var cr = d.getLong(COL_REF_CREATED_AT);
        referenceConsumer.call(RealmId.newRealmId(realm), ref, cr);
      }
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
    try (var cursor =
        objs.find()
            .batchSize(50)
            .cursorType(CursorType.NonTailable)
            .projection(
                fields(
                    include(
                        ID_PROPERTY_NAME,
                        COL_OBJ_TYPE,
                        COL_REALM,
                        COL_OBJ_ID,
                        COL_OBJ_PART,
                        COL_OBJ_CREATED_AT)))
            .cursor()) {
      while (cursor.hasNext()) {
        var d = cursor.next();
        var t = d.getString(COL_OBJ_TYPE);
        var id = d.get(ID_PROPERTY_NAME, Document.class);
        var realm = id.getString(COL_REALM);
        var i = id.getLong(COL_OBJ_ID);
        var p = id.getInteger(COL_OBJ_PART);
        var cr = d.getLong(COL_OBJ_CREATED_AT);
        objConsumer.call(RealmId.newRealmId(realm), t, persistId(i, p), cr);
      }
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public synchronized void close() {
    if (config.closeClient()) {
      client.close();
    }
  }
}
