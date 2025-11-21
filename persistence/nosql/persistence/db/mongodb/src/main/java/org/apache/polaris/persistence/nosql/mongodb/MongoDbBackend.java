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
package org.apache.polaris.persistence.nosql.mongodb;

import static com.mongodb.ErrorCategory.DUPLICATE_KEY;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Updates.set;
import static java.util.stream.Collectors.toList;
import static org.apache.polaris.persistence.nosql.api.backend.PersistId.persistId;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_CREATED_AT;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_ID;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_PART;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_REAL_PART_NUM;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_TYPE;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_VALUE;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_VERSION;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REALM;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_CREATED_AT;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_NAME;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_POINTER;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_PREVIOUS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_OBJS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_REFS;
import static org.apache.polaris.persistence.nosql.impl.PersistenceImplementation.deserialize;
import static org.apache.polaris.persistence.nosql.impl.PersistenceImplementation.serialize;

import com.google.common.collect.Maps;
import com.mongodb.CursorType;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteError;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.FetchedObj;
import org.apache.polaris.persistence.nosql.api.backend.PersistId;
import org.apache.polaris.persistence.nosql.api.backend.WriteObj;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.nosql.api.exceptions.UnknownOperationResultException;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.ImmutableReference;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.apache.polaris.persistence.nosql.impl.PersistenceImplementation;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

final class MongoDbBackend implements Backend {

  static final String ID_PROPERTY_NAME = "_id";

  private static final ReplaceOptions WRITE_OPTIONS;

  static {
    ReplaceOptions options = new ReplaceOptions();
    options.upsert(true);
    WRITE_OPTIONS = options;
  }

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
  public String type() {
    return MongoDbBackendFactory.NAME;
  }

  @Override
  public boolean supportsRealmDeletion() {
    return config.allowPrefixDeletion();
  }

  @Override
  public synchronized void close() {
    if (config.closeClient()) {
      client.close();
    }
  }

  @Nonnull
  @Override
  public Persistence newPersistence(
      Function<Backend, Backend> backendWrapper,
      @Nonnull PersistenceParams persistenceParams,
      String realmId,
      MonotonicClock monotonicClock,
      IdGenerator idGenerator) {
    initialize();
    return new PersistenceImplementation(
        backendWrapper.apply(this), persistenceParams, realmId, monotonicClock, idGenerator);
  }

  @Override
  public Optional<String> setupSchema() {
    initialize();
    return Optional.of("database name: " + config.databaseName());
  }

  @Override
  public void deleteRealms(Set<String> realmIds) {
    if (realmIds.isEmpty()) {
      return;
    }

    try {
      Bson realmIdFilter = in(ID_PROPERTY_NAME + "." + COL_REALM, new HashSet<>(realmIds));
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
  public void batchDeleteRefs(Map<String, Set<String>> realmRefs) {
    var idDocs =
        realmRefs.entrySet().stream()
            .flatMap(
                e -> {
                  var realmId = e.getKey();
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
  public void batchDeleteObjs(Map<String, Set<PersistId>> realmObjs) {
    var idDocs =
        realmObjs.entrySet().stream()
            .flatMap(
                e -> {
                  var realmId = e.getKey();
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
        var realmId = id.getString(COL_REALM);
        var ref = id.getString(COL_REF_NAME);
        var cr = d.getLong(COL_REF_CREATED_AT);
        referenceConsumer.call(realmId, ref, cr);
      }
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
    try (var cursor =
        objs.find()
            .batchSize(50)
            .cursorType(CursorType.NonTailable)
            .projection(fields(include(ID_PROPERTY_NAME, COL_OBJ_TYPE, COL_OBJ_CREATED_AT)))
            .cursor()) {
      while (cursor.hasNext()) {
        var d = cursor.next();
        var t = d.getString(COL_OBJ_TYPE);
        var id = d.get(ID_PROPERTY_NAME, Document.class);
        var realmId = id.getString(COL_REALM);
        var i = id.getLong(COL_OBJ_ID);
        var p = id.getInteger(COL_OBJ_PART);
        var cr = d.getLong(COL_OBJ_CREATED_AT);
        objConsumer.call(realmId, t, persistId(i, p), cr);
      }
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public boolean createReference(@Nonnull String realmId, @Nonnull Reference newRef) {
    var doc = newReferenceDoc(realmId, newRef);
    try {
      refs().insertOne(doc);
    } catch (MongoWriteException e) {
      if (e.getError().getCategory() == DUPLICATE_KEY) {
        return false;
      }
      throw unhandledException(e);
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
    return true;
  }

  private Document newReferenceDoc(@Nonnull String realmId, @Nonnull Reference newRef) {
    var doc = new Document();
    doc.put(ID_PROPERTY_NAME, idRefDoc(realmId, newRef));
    doc.put(COL_REF_POINTER, serialize(newRef.pointer()));
    doc.put(COL_REF_CREATED_AT, newRef.createdAtMicros());
    byte[] previous = serialize(newRef.previousPointers());
    if (previous != null) {
      doc.put(COL_REF_PREVIOUS, new Binary(previous));
    }
    return doc;
  }

  @Override
  public void createReferences(@Nonnull String realmId, @Nonnull List<Reference> newRefs) {
    if (newRefs.isEmpty()) {
      return;
    }
    var docs = newRefs.stream().map(r -> newReferenceDoc(realmId, r)).toList();
    try {
      refs().insertMany(docs);
    } catch (MongoBulkWriteException e) {
      if (e.getWriteErrors().stream().anyMatch(we -> we.getCategory() != DUPLICATE_KEY)) {
        throw unhandledException(e);
      }
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public boolean updateReference(
      @Nonnull String realmId,
      @Nonnull Reference updatedRef,
      @Nonnull Optional<ObjRef> expectedPointer) {
    var updates = new ArrayList<Bson>();
    updates.add(set(COL_REF_POINTER, serialize(updatedRef.pointer())));
    var previous = serialize(updatedRef.previousPointers());
    if (previous != null) {
      updates.add(set(COL_REF_PREVIOUS, new Binary(previous)));
    }

    UpdateResult result;
    try {

      var filters = new ArrayList<Bson>(5);
      filters.add(eq(ID_PROPERTY_NAME, idRefDoc(realmId, updatedRef)));
      filters.add(eq(COL_REF_POINTER, serialize(expectedPointer)));
      filters.add(eq(COL_REF_CREATED_AT, updatedRef.createdAtMicros()));
      var condition = and(filters);

      result = refs().updateOne(condition, updates);
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }

    if (result.getModifiedCount() == 1) {
      return true;
    }
    if (result.getModifiedCount() == 0 && result.getMatchedCount() == 1) {
      // not updated
      return false;
    }

    fetchReference(realmId, updatedRef.name());
    return false;
  }

  @Override
  @Nonnull
  public Reference fetchReference(@Nonnull String realmId, @Nonnull String name) {
    FindIterable<Document> result;
    try {
      result = refs().find(eq(ID_PROPERTY_NAME, idRefDoc(realmId, name)));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }

    var doc = result.first();
    if (doc == null) {
      throw new ReferenceNotFoundException(name);
    }

    return ImmutableReference.builder()
        .name(name)
        .pointer(
            Optional.ofNullable(
                deserialize(doc.get(COL_REF_POINTER, Binary.class).getData(), ObjRef.class)))
        .createdAtMicros(doc.getLong(COL_REF_CREATED_AT))
        .previousPointers(
            deserialize(doc.get(COL_REF_PREVIOUS, Binary.class).getData(), long[].class))
        .build();
  }

  @Override
  @Nonnull
  public Map<PersistId, FetchedObj> fetch(@Nonnull String realmId, @Nonnull Set<PersistId> ids) {
    var list = ids.stream().map(id -> idObjDoc(realmId, id)).toList();

    var r = Maps.<PersistId, FetchedObj>newHashMapWithExpectedSize(ids.size());

    if (!list.isEmpty()) {
      FindIterable<Document> result;
      try {
        result = objs().find(in(ID_PROPERTY_NAME, list));
      } catch (RuntimeException e) {
        throw unhandledException(e);
      }
      for (var doc : result) {
        var obj = docToFetched(doc);
        var id = docToPersistId(doc);
        r.put(id, obj);
      }
    }

    return r;
  }

  @Override
  public void write(@Nonnull String realmId, @Nonnull List<WriteObj> writes) {
    var docs = new ArrayList<WriteModel<Document>>(writes.size());
    for (WriteObj write : writes) {
      var idDoc = idFetchedDoc(realmId, write);
      docs.add(
          new ReplaceOneModel<>(
              eq(ID_PROPERTY_NAME, idDoc),
              objToDoc(
                  idDoc,
                  write.type(),
                  write.serialized(),
                  write.createdAtMicros(),
                  null,
                  write.partNum()),
              WRITE_OPTIONS));
    }

    List<WriteModel<Document>> updates = new ArrayList<>(docs);
    if (!updates.isEmpty()) {
      BulkWriteResult res;
      try {
        res = objs().bulkWrite(updates);
      } catch (RuntimeException e) {
        throw unhandledException(e);
      }
      if (!res.wasAcknowledged()) {
        throw new RuntimeException("Upsert not acknowledged");
      }
    }
  }

  @Override
  public void delete(@Nonnull String realmId, @Nonnull Set<PersistId> ids) {
    var list = ids.stream().map(id -> idObjDoc(realmId, id)).collect(toList());
    if (list.isEmpty()) {
      return;
    }
    try {
      objs().deleteMany(in(ID_PROPERTY_NAME, list));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public boolean conditionalInsert(
      @Nonnull String realmId,
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String versionToken,
      @Nonnull byte[] serializedValue) {
    try {
      var idDoc = idObjDoc(realmId, persistId);
      var doc = objToDoc(idDoc, objTypeId, serializedValue, createdAtMicros, versionToken, 1);
      objs().insertOne(doc);
      return true;
    } catch (MongoWriteException e) {
      if (e.getError().getCategory() == DUPLICATE_KEY) {
        return false;
      }
      throw handleMongoWriteException(e);
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public boolean conditionalUpdate(
      @Nonnull String realmId,
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String updateToken,
      @Nonnull String expectedToken,
      @Nonnull byte[] serializedValue) {
    var idDoc = idObjDoc(realmId, persistId);
    var doc = objToDoc(idDoc, objTypeId, serializedValue, createdAtMicros, updateToken, 1);

    try {
      var options = new FindOneAndReplaceOptions().returnDocument(ReturnDocument.BEFORE);
      var updateResult =
          objs()
              .findOneAndReplace(
                  and(eq(ID_PROPERTY_NAME, idDoc), eq(COL_OBJ_VERSION, expectedToken)),
                  doc,
                  options);
      return updateResult != null;
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public boolean conditionalDelete(
      @Nonnull String realmId, @Nonnull PersistId persistId, @Nonnull String expectedToken) {
    var idDoc = idObjDoc(realmId, persistId);

    try {
      return objs()
              .findOneAndDelete(
                  and(eq(ID_PROPERTY_NAME, idDoc), eq(COL_OBJ_VERSION, expectedToken)))
          != null;
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  private FetchedObj docToFetched(Document doc) {
    if (doc == null) {
      return null;
    }
    var bin = doc.get(COL_OBJ_VALUE, Binary.class);
    var objTypeId = doc.getString(COL_OBJ_TYPE);
    var versionToken = doc.getString(COL_OBJ_VERSION);
    var createdAtMicros = doc.getLong(COL_OBJ_CREATED_AT);
    var realPartNum = doc.getInteger(COL_OBJ_REAL_PART_NUM);
    return new FetchedObj(objTypeId, createdAtMicros, versionToken, bin.getData(), realPartNum);
  }

  private PersistId docToPersistId(Document doc) {
    var idDoc = doc.get(ID_PROPERTY_NAME, Document.class);
    var id = idDoc.getLong(COL_OBJ_ID);
    var part = idDoc.getInteger(COL_OBJ_PART);
    return persistId(id, part);
  }

  private Document idDocBase(String realmId) {
    Document idDoc = new Document();
    idDoc.put(COL_REALM, realmId);
    return idDoc;
  }

  private Document idRefDoc(String realmId, Reference ref) {
    return idRefDoc(realmId, ref.name());
  }

  private Document idRefDoc(String realmId, String name) {
    Document idDoc = idDocBase(realmId);
    idDoc.put(COL_REF_NAME, name);
    return idDoc;
  }

  private Document idObjDoc(String realmId, PersistId id) {
    Document idDoc = idDocBase(realmId);
    idDoc.put(COL_OBJ_ID, id.id());
    idDoc.put(COL_OBJ_PART, id.part());
    return idDoc;
  }

  private Document idFetchedDoc(String realmId, WriteObj id) {
    Document idDoc = idDocBase(realmId);
    idDoc.put(COL_OBJ_ID, id.id());
    idDoc.put(COL_OBJ_PART, id.part());
    return idDoc;
  }

  private Document objToDoc(
      @Nonnull Document idDoc,
      @Nonnull String objTypeId,
      @Nonnull byte[] serialized,
      long createdAtMicros,
      String versionToken,
      int partNum) {
    var doc = new Document();
    doc.put(ID_PROPERTY_NAME, idDoc);
    doc.put(COL_OBJ_TYPE, objTypeId);
    doc.put(COL_OBJ_CREATED_AT, createdAtMicros);
    doc.put(COL_OBJ_VALUE, new Binary(serialized));
    if (versionToken != null) {
      doc.put(COL_OBJ_VERSION, versionToken);
    }
    doc.put(COL_OBJ_REAL_PART_NUM, partNum);
    return doc;
  }

  static RuntimeException unhandledException(RuntimeException e) {
    if (e instanceof MongoInterruptedException
        || e instanceof MongoTimeoutException
        || e instanceof MongoServerUnavailableException
        || e instanceof MongoSocketReadTimeoutException
        || e instanceof MongoExecutionTimeoutException) {
      return new UnknownOperationResultException(e);
    }
    if (e instanceof MongoWriteException mongoWriteException) {
      return handleMongoWriteException(mongoWriteException);
    }
    if (e instanceof MongoBulkWriteException specific) {
      for (BulkWriteError error : specific.getWriteErrors()) {
        switch (error.getCategory()) {
          case EXECUTION_TIMEOUT:
          case UNCATEGORIZED:
            return new UnknownOperationResultException(e);
          default:
            break;
        }
      }
    }
    return e;
  }

  static RuntimeException handleMongoWriteException(MongoWriteException e) {
    return handleMongoWriteError(e, e.getError());
  }

  static RuntimeException handleMongoWriteError(MongoException e, WriteError error) {
    return switch (error.getCategory()) {
      case EXECUTION_TIMEOUT, UNCATEGORIZED -> new UnknownOperationResultException(e);
      default -> e;
    };
  }
}
