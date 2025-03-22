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
package org.apache.polaris.persistence.base;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.apache.polaris.persistence.api.backend.PersistId.persistId;
import static org.apache.polaris.persistence.api.backend.PersistId.persistIdPart0;
import static org.apache.polaris.persistence.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.api.obj.ObjSerializationHelper.contextualReader;
import static org.apache.polaris.persistence.api.obj.ObjTypes.objTypeById;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.api.PersistenceParams;
import org.apache.polaris.persistence.api.backend.PersistId;
import org.apache.polaris.persistence.api.exceptions.ReferenceAlreadyExistsException;
import org.apache.polaris.persistence.api.obj.Obj;
import org.apache.polaris.persistence.api.obj.ObjRef;
import org.apache.polaris.persistence.api.obj.ObjType;
import org.apache.polaris.persistence.api.ref.ImmutableReference;
import org.apache.polaris.persistence.api.ref.Reference;
import org.apache.polaris.persistence.base.delegate.PersistenceWithCommitsIndexes;
import org.apache.polaris.realms.id.RealmId;

/**
 * Base implementation that every database specific implementation is encouraged to extend.
 *
 * <p>This class centralizes {@link Obj} de-serialization and parameter validations.
 */
public abstract class AbstractPersistence implements PersistenceWithCommitsIndexes {
  private static final ObjectMapper SMILE_MAPPER =
      new SmileMapper()
          .findAndRegisterModules()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  private static final ObjectWriter OBJ_WRITER =
      SMILE_MAPPER.writer().withView(Obj.StorageView.class);

  protected final PersistenceParams params;
  protected final RealmId realmId;
  protected final MonotonicClock monotonicClock;
  protected final IdGenerator idGenerator;
  protected final int maxSerializedValueSize;

  protected AbstractPersistence(
      PersistenceParams params,
      RealmId realmId,
      MonotonicClock monotonicClock,
      IdGenerator idGenerator) {
    this.params = params;
    this.realmId = realmId;
    this.monotonicClock = monotonicClock;
    this.idGenerator = idGenerator;
    this.maxSerializedValueSize = 350 * 1024;
  }

  @Override
  public IdGenerator idGenerator() {
    return idGenerator;
  }

  @Override
  public MonotonicClock monotonicClock() {
    return monotonicClock;
  }

  @Override
  public RealmId realmId() {
    return realmId;
  }

  @Override
  public PersistenceParams params() {
    return params;
  }

  @Override
  public int maxSerializedValueSize() {
    return maxSerializedValueSize;
  }

  @Override
  public long generateId() {
    return idGenerator().generateId();
  }

  @Override
  public ObjRef generateObjId(ObjType type) {
    return objRef(type, generateId());
  }

  @Nonnull
  @Override
  public final Reference createReference(@Nonnull String name, @Nonnull Optional<ObjRef> pointer) {
    var newRef =
        ImmutableReference.builder()
            .createdAtMicros(currentTimeMicros())
            .name(name)
            .pointer(pointer)
            .previousPointers()
            .build();
    if (!doCreateReference(newRef)) {
      throw new ReferenceAlreadyExistsException(name);
    }
    return newRef;
  }

  protected abstract boolean doCreateReference(@Nonnull Reference newRef);

  @Override
  @Nonnull
  public final Optional<Reference> updateReferencePointer(
      @Nonnull Reference reference, @Nonnull ObjRef newPointer) {
    var current = reference.pointer();
    checkArgument(
        !newPointer.equals(current.orElse(null)),
        "New pointer must not be equal to the expected pointer.");
    checkArgument(
        current.isEmpty() || current.get().type().equals(newPointer.type()),
        "New pointer must use the same ObjType as the current pointer.");

    var sizeLimit = params.referencePreviousHeadCount();
    var newPrevious = new long[sizeLimit];
    var newPreviousIdx = 0;
    if (current.isPresent()) {
      newPrevious[newPreviousIdx++] = current.get().id();
    }
    for (var previousPointer : reference.previousPointers()) {
      newPrevious[newPreviousIdx++] = previousPointer;
      if (newPreviousIdx == sizeLimit) {
        break;
      }
    }
    if (newPreviousIdx < sizeLimit) {
      newPrevious = Arrays.copyOf(newPrevious, newPreviousIdx);
    }

    var updatedRef =
        ImmutableReference.builder()
            .from(reference)
            .pointer(newPointer)
            .previousPointers(newPrevious)
            .build();

    return doUpdateReference(updatedRef, current) ? Optional.of(updatedRef) : Optional.empty();
  }

  protected abstract boolean doUpdateReference(
      @Nonnull Reference updatedRef, @Nonnull Optional<ObjRef> expectedPointer);

  @Nonnull
  @Override
  public final Reference fetchReference(@Nonnull String name) {
    return doFetchReference(name);
  }

  @Nonnull
  protected abstract Reference doFetchReference(@Nonnull String name);

  @Nullable
  @Override
  public <T extends Obj> T getImmediate(@Nonnull ObjRef id, @Nonnull Class<T> clazz) {
    return fetch(id, clazz);
  }

  @Nullable
  @Override
  public final <T extends Obj> T fetch(@Nonnull ObjRef id, @Nonnull Class<T> clazz) {
    return fetchMany(clazz, id)[0];
  }

  @Nonnull
  @Override
  public final <T extends Obj> T[] fetchMany(@Nonnull Class<T> clazz, @Nonnull ObjRef... ids) {
    var fetchIds = new HashSet<PersistId>();
    for (ObjRef id : ids) {
      if (id == null) {
        continue;
      }
      var numParts = id.numParts();
      if (numParts == 0) {
        numParts = 1;
      }
      checkArgument(numParts > 0, "partNum of %s must be greater than 0", id);
      for (var p = 0; p < numParts; p++) {
        fetchIds.add(persistId(id.id(), p));
      }
    }

    var fetched = doFetch(fetchIds);

    @SuppressWarnings("unchecked")
    var r = (T[]) Array.newInstance(clazz, ids.length);

    for (var i = 0; i < ids.length; i++) {
      var id = ids[i];
      if (id == null) {
        continue;
      }

      var f = fetched.get(persistId(id.id(), 0));
      if (f == null) {
        continue;
      }

      var numParts = f.realNumParts();
      if (numParts > fetched.size()) {
        // The value of ObjId.numParts() is inconsistent with the real number of parts.
        // There are more parts that need to be fetched.
        fetchIds.clear();
        for (var p = fetched.size(); p < numParts; p++) {
          fetchIds.add(persistId(id.id(), p));
        }
        fetched.putAll(doFetch(fetchIds));
      }
      var fetchedObjTypeId = f.type();
      try (var in =
          numParts == 1
              ? new ByteArrayInputStream(f.serialized())
              : new MultiByteArrayInputStream(
                  IntStream.range(0, numParts)
                      .mapToObj(
                          p -> {
                            var part = fetched.get(persistId(id.id(), p));
                            checkState(
                                part != null,
                                "Part #%s or %s of object %s does not exist in the database",
                                p,
                                numParts,
                                id);
                            checkState(
                                fetchedObjTypeId.equals(part.type()),
                                "Object type mismatch, expected '%s', got '%s'",
                                part.type(),
                                fetchedObjTypeId);
                            return part.serialized();
                          })
                      .toList())) {
        r[i] =
            deserializeObj(
                fetchedObjTypeId,
                id.id(),
                numParts,
                in,
                f.versionToken(),
                f.createdAtMicros(),
                clazz);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    return r;
  }

  @Nonnull
  protected abstract Map<PersistId, FetchedObj> doFetch(@Nonnull Set<PersistId> ids);

  @Nonnull
  @Override
  public final <T extends Obj> T write(@Nonnull T obj, @Nonnull Class<T> clazz) {
    checkArgument(obj.versionToken() == null, "'obj' must have a null 'versionToken'");

    var createdAtMicros = currentTimeMicros();

    var serializedValue = serializeObj(obj);
    var serializedSize = serializedValue.length;
    var numParts = (serializedSize / maxSerializedValueSize) + 1;
    var writes = new ArrayList<WriteObj>(numParts + 1);
    writeAddWriteObjs(numParts, writes, obj, createdAtMicros, serializedValue, serializedSize);

    doWrite(writes);

    @SuppressWarnings("unchecked")
    var r = (T) obj.withCreatedAtMicros(createdAtMicros).withNumParts(numParts);

    return r;
  }

  @Nonnull
  @Override
  public final <T extends Obj> T[] writeMany(@Nonnull Class<T> clazz, @Nonnull T[] objs) {

    var numObjs = objs.length;

    @SuppressWarnings("unchecked")
    var r = (T[]) Array.newInstance(clazz, numObjs);

    if (numObjs > 0) {
      var writes = new ArrayList<WriteObj>(numObjs);

      var createdAtMicros = currentTimeMicros();
      for (var i = 0; i < numObjs; i++) {
        var obj = objs[i];
        if (obj != null) {
          checkArgument(obj.versionToken() == null, "'obj' must have a null 'versionToken'");

          var serializedValue = serializeObj(obj);
          var serializedSize = serializedValue.length;
          var numParts = (serializedSize / maxSerializedValueSize) + 1;
          writeAddWriteObjs(
              numParts, writes, obj, createdAtMicros, serializedValue, serializedSize);

          @SuppressWarnings("unchecked")
          var u = (T) obj.withCreatedAtMicros(createdAtMicros).withNumParts(numParts);
          r[i] = u;
        }
      }

      doWrite(writes);
    }

    return r;
  }

  private <T extends Obj> void writeAddWriteObjs(
      int numParts,
      ArrayList<WriteObj> writes,
      T obj,
      long createdAtMicros,
      byte[] serializedValue,
      int serializedSize) {
    if (numParts == 1) {
      writes.add(
          new WriteObj(obj.type().id(), obj.id(), 0, createdAtMicros, serializedValue, numParts));
    } else {
      for (int p = 0; p < numParts; p++) {
        var off = p * maxSerializedValueSize;
        var remain = serializedSize - off;
        var len = Math.min(remain, maxSerializedValueSize);
        var part = new byte[len];
        System.arraycopy(serializedValue, off, part, 0, len);
        writes.add(new WriteObj(obj.type().id(), obj.id(), p, createdAtMicros, part, numParts));
      }
    }
  }

  protected abstract void doWrite(@Nonnull List<WriteObj> writes);

  @Override
  public final void delete(@Nonnull ObjRef id) {
    deleteMany(id);
  }

  @Override
  public final void deleteMany(@Nonnull ObjRef... ids) {
    var deleteIds = new HashSet<PersistId>();
    for (ObjRef id : ids) {
      if (id == null) {
        continue;
      }
      var numParts = id.numParts();
      if (numParts == 0) {
        numParts = 1;
      }
      checkArgument(numParts > 0, "partNum of %s must be greater than 0", id);
      var type = id.type();
      for (var p = 0; p < numParts; p++) {
        deleteIds.add(persistId(id.id(), p));
      }
    }

    if (!deleteIds.isEmpty()) {
      doDelete(deleteIds);
    }
  }

  protected abstract void doDelete(@Nonnull Set<PersistId> ids);

  @Nullable
  @Override
  public final <T extends Obj> T conditionalInsert(@Nonnull T obj, @Nonnull Class<T> clazz) {
    var versionToken = obj.versionToken();
    checkArgument(versionToken != null, "'obj' must have a non-null 'versionToken'");
    checkArgument(obj.numParts() == 1, "'obj' must have 'numParts' == 1");

    var objId = objRef(obj);
    var serializedValue = serializeObj(obj);
    var serializedSize = serializedValue.length;
    checkArgument(
        serializedSize <= maxSerializedValueSize(),
        "Length of serialized value %s of object %s must not exceed maximum allowed size %s",
        serializedSize,
        maxSerializedValueSize(),
        objId);

    var createdAtMicros = currentTimeMicros();

    @SuppressWarnings("unchecked")
    var r = (T) obj.withCreatedAtMicros(createdAtMicros).withNumParts(1);

    return doConditionalInsert(
            obj.type().id(), persistIdPart0(obj), createdAtMicros, versionToken, serializedValue)
        ? r
        : null;
  }

  protected abstract boolean doConditionalInsert(
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String versionToken,
      @Nonnull byte[] serializedValue);

  @Nullable
  @Override
  public final <T extends Obj> T conditionalUpdate(
      @Nonnull T expected, @Nonnull T update, @Nonnull Class<T> clazz) {
    checkArgument(
        expected.type().equals(update.type()) && expected.id() == update.id(),
        "Obj ids between 'expected' and 'update' do not match");
    var expectedToken = expected.versionToken();
    var updateToken = update.versionToken();
    checkArgument(
        expectedToken != null && updateToken != null,
        "Both 'expected' and 'update' must have a non-null 'versionToken'");
    checkArgument(
        !expectedToken.equals(updateToken),
        "'versionToken' of 'expected' and 'update' must not be equal");
    checkArgument(expected.numParts() == 1, "'expected' must have 'partNum' == 1");
    checkArgument(
        update.numParts() == 0 || update.numParts() == 1, "'update' must have 'partNum' == 0 or 1");

    var serializedValue = serializeObj(update);
    var serializedSize = serializedValue.length;
    checkArgument(
        serializedSize <= maxSerializedValueSize(),
        "Length of serialized value %s of object %s must not exceed maximum allowed size %s",
        serializedSize,
        maxSerializedValueSize(),
        update);

    var createdAtMicros = currentTimeMicros();

    if (doConditionalUpdate(
        update.type().id(),
        persistIdPart0(update),
        createdAtMicros,
        updateToken,
        expectedToken,
        serializedValue)) {
      @SuppressWarnings("unchecked")
      var r = (T) update.withCreatedAtMicros(createdAtMicros).withNumParts(1);
      return r;
    }
    return null;
  }

  protected abstract boolean doConditionalUpdate(
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String updateToken,
      @Nonnull String expectedToken,
      @Nonnull byte[] serializedValue);

  @Override
  public final <T extends Obj> boolean conditionalDelete(@Nonnull T expected, Class<T> clazz) {
    var expectedToken = expected.versionToken();
    checkArgument(expectedToken != null, "'obj' must have a non-null 'versionToken'");
    checkArgument(expected.numParts() == 1, "'expected' must have 'partNum' == 1");
    return doConditionalDelete(persistIdPart0(expected), expectedToken);
  }

  protected abstract boolean doConditionalDelete(
      @Nonnull PersistId persistId, @Nonnull String expectedToken);

  public static <T> T deserialize(byte[] binary, @Nonnull Class<T> clazz) {
    if (binary == null) {
      return null;
    }
    try {
      return SMILE_MAPPER.readValue(binary, clazz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected static <T> T deserialize(ByteBuffer binary, @Nonnull Class<T> clazz) {
    if (binary == null) {
      return null;
    }
    try {
      return SMILE_MAPPER.readValue(new ByteBufferBackedInputStream(binary), clazz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] serialize(Object o) {
    try {
      return SMILE_MAPPER.writeValueAsBytes(o);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  protected static byte[] serializeObj(Obj o) {
    try {
      // OBJ_WRITES uses the Jackson view mechanism to exclude the
      // type,id,createdAtMicros,versionToken attributes from being
      // serialized by Jackson here.
      return OBJ_WRITER.writeValueAsBytes(o);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  protected static <T> T deserializeObj(
      String type,
      long id,
      int partNum,
      InputStream in,
      String versionToken,
      long createdAtMicros,
      @Nonnull Class<T> clazz)
      throws IOException {
    var objType = objTypeById(type);
    var typeClass = objType.targetClass();
    checkArgument(
        clazz.isAssignableFrom(typeClass),
        "Mismatch between persisted object type '%s' (%s) and deserialized %s. "
            + "The object ID is possibly already used by another object. "
            + "If the deserialized type is a GenericObj, ensure that the artifact providing the corresponding ObjType implementation is present and is present in META-INF/services/%s",
        type,
        typeClass,
        clazz,
        ObjType.class.getName());

    var obj =
        contextualReader(SMILE_MAPPER, objType, id, partNum, versionToken, createdAtMicros)
            .readValue(in, typeClass);
    @SuppressWarnings("unchecked")
    var r = (T) obj;
    return r;
  }

  @Override
  public String toString() {
    return format("Persistence for realm '%s'", realmId().id());
  }
}
