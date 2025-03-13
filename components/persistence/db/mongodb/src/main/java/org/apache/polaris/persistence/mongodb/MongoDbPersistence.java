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

import static com.mongodb.ErrorCategory.DUPLICATE_KEY;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Updates.set;
import static java.util.stream.Collectors.toList;
import static org.apache.polaris.persistence.api.backend.PersistId.persistId;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_OBJ_CREATED_AT;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_OBJ_ID;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_OBJ_PART;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_OBJ_REAL_PART_NUM;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_OBJ_TYPE;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_OBJ_VALUE;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_OBJ_VERSION;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_REALM;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_REF_CREATED_AT;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_REF_NAME;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_REF_POINTER;
import static org.apache.polaris.persistence.base.util.Identifiers.COL_REF_PREVIOUS;

import com.google.common.collect.Maps;
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
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.api.PersistenceParams;
import org.apache.polaris.persistence.api.backend.PersistId;
import org.apache.polaris.persistence.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.api.exceptions.UnknownOperationResultException;
import org.apache.polaris.persistence.api.obj.ObjRef;
import org.apache.polaris.persistence.api.ref.ImmutableReference;
import org.apache.polaris.persistence.api.ref.Reference;
import org.apache.polaris.persistence.base.AbstractPersistence;
import org.apache.polaris.persistence.base.FetchedObj;
import org.apache.polaris.persistence.base.WriteObj;
import org.apache.polaris.realms.id.RealmId;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

final class MongoDbPersistence extends AbstractPersistence {
  private final MongoDbBackend backend;

  static final String ID_PROPERTY_NAME = "_id";

  private static final ReplaceOptions WRITE_OPTIONS;

  static {
    ReplaceOptions options = new ReplaceOptions();
    options.upsert(true);
    WRITE_OPTIONS = options;
  }

  MongoDbPersistence(
      MongoDbBackend backend,
      PersistenceParams params,
      RealmId realmId,
      MonotonicClock monotonicClock,
      IdGenerator idGenerator) {
    super(params, realmId, monotonicClock, idGenerator);
    this.backend = backend;
  }

  @Override
  protected boolean doCreateReference(@Nonnull Reference newRef) {
    var doc = new Document();
    doc.put(ID_PROPERTY_NAME, idRefDoc(newRef));
    doc.put(COL_REF_POINTER, serialize(newRef.pointer()));
    doc.put(COL_REF_CREATED_AT, newRef.createdAtMicros());
    byte[] previous = serialize(newRef.previousPointers());
    if (previous != null) {
      doc.put(COL_REF_PREVIOUS, new Binary(previous));
    }
    try {
      backend.refs().insertOne(doc);
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

  @Override
  protected boolean doUpdateReference(
      @Nonnull Reference updatedRef, @Nonnull Optional<ObjRef> expectedPointer) {
    var updates = new ArrayList<Bson>();
    updates.add(set(COL_REF_POINTER, serialize(updatedRef.pointer())));
    var previous = serialize(updatedRef.previousPointers());
    if (previous != null) {
      updates.add(set(COL_REF_PREVIOUS, new Binary(previous)));
    }

    UpdateResult result;
    try {

      var filters = new ArrayList<Bson>(5);
      filters.add(eq(ID_PROPERTY_NAME, idRefDoc(updatedRef)));
      filters.add(eq(COL_REF_POINTER, serialize(expectedPointer)));
      filters.add(eq(COL_REF_CREATED_AT, updatedRef.createdAtMicros()));
      var condition = and(filters);

      result = backend.refs().updateOne(condition, updates);
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

    fetchReference(updatedRef.name());
    return false;
  }

  @Override
  @Nonnull
  protected Reference doFetchReference(@Nonnull String name) {
    FindIterable<Document> result;
    try {
      result = backend.refs().find(eq(ID_PROPERTY_NAME, idRefDoc(name)));
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
  protected Map<PersistId, FetchedObj> doFetch(@Nonnull Set<PersistId> ids) {
    var list = ids.stream().map(this::idObjDoc).toList();

    var r = Maps.<PersistId, FetchedObj>newHashMapWithExpectedSize(ids.size());

    if (!list.isEmpty()) {
      FindIterable<Document> result;
      try {
        result = backend.objs().find(in(ID_PROPERTY_NAME, list));
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
  protected void doWrite(@Nonnull List<WriteObj> writes) {
    var docs = new ArrayList<WriteModel<Document>>(writes.size());
    for (WriteObj write : writes) {
      var idDoc = idFetchedDoc(write);
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
        res = backend.objs().bulkWrite(updates);
      } catch (RuntimeException e) {
        throw unhandledException(e);
      }
      if (!res.wasAcknowledged()) {
        throw new RuntimeException("Upsert not acknowledged");
      }
    }
  }

  @Override
  protected void doDelete(@Nonnull Set<PersistId> ids) {
    var list = ids.stream().map(this::idObjDoc).collect(toList());
    if (list.isEmpty()) {
      return;
    }
    try {
      backend.objs().deleteMany(in(ID_PROPERTY_NAME, list));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  protected boolean doConditionalInsert(
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String versionToken,
      @Nonnull byte[] serializedValue) {
    try {
      var idDoc = idObjDoc(persistId);
      var doc = objToDoc(idDoc, objTypeId, serializedValue, createdAtMicros, versionToken, 1);
      backend.objs().insertOne(doc);
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
  protected boolean doConditionalUpdate(
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String updateToken,
      @Nonnull String expectedToken,
      @Nonnull byte[] serializedValue) {
    var idDoc = idObjDoc(persistId);
    var doc = objToDoc(idDoc, objTypeId, serializedValue, createdAtMicros, updateToken, 1);

    try {
      var options = new FindOneAndReplaceOptions().returnDocument(ReturnDocument.BEFORE);
      var updateResult =
          backend
              .objs()
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
  protected boolean doConditionalDelete(
      @Nonnull PersistId persistId, @Nonnull String expectedToken) {
    var idDoc = idObjDoc(persistId);

    try {
      return backend
              .objs()
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

  private Document idDocBase() {
    Document idDoc = new Document();
    idDoc.put(COL_REALM, realmId().id());
    return idDoc;
  }

  private Document idRefDoc(Reference ref) {
    return idRefDoc(ref.name());
  }

  private Document idRefDoc(String name) {
    Document idDoc = idDocBase();
    idDoc.put(COL_REF_NAME, name);
    return idDoc;
  }

  private Document idObjDoc(PersistId id) {
    Document idDoc = idDocBase();
    idDoc.put(COL_OBJ_ID, id.id());
    idDoc.put(COL_OBJ_PART, id.part());
    return idDoc;
  }

  private Document idFetchedDoc(WriteObj id) {
    Document idDoc = idDocBase();
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
    if (e instanceof MongoWriteException) {
      return handleMongoWriteException((MongoWriteException) e);
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
