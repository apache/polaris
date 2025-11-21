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
package org.apache.polaris.persistence.nosql.api.obj;

import static org.apache.polaris.persistence.nosql.api.obj.ObjSerializationHelper.OBJ_CREATED_AT_KEY;
import static org.apache.polaris.persistence.nosql.api.obj.ObjSerializationHelper.OBJ_ID_KEY;
import static org.apache.polaris.persistence.nosql.api.obj.ObjSerializationHelper.OBJ_NUM_PARTS_KEY;
import static org.apache.polaris.persistence.nosql.api.obj.ObjSerializationHelper.OBJ_TYPE_KEY;
import static org.apache.polaris.persistence.nosql.api.obj.ObjSerializationHelper.OBJ_VERSION_TOKEN;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.immutables.value.Value;

public interface Obj {

  // Note on the JsonView annotations used here:
  //
  // BasePersistence.serializeObj() does not serialize the attributes type, id, createdAtMicros,
  // versionToken, because those are either part of the key in the database or available via
  // distinct database columns/attributes, so it is unnecessary to serialize those again.

  @JsonView(ObjSerializeAll.class)
  @JacksonInject(OBJ_TYPE_KEY)
  ObjType type();

  @JsonView(ObjSerializeAll.class)
  @JacksonInject(OBJ_ID_KEY)
  long id();

  /**
   * Indicates the number of parts of which the object is split in the backing database. This value
   * is available after the object has been {@linkplain Persistence#write(Obj, Class) written}.
   */
  @JsonView(ObjSerializeAll.class)
  @JacksonInject(OBJ_NUM_PARTS_KEY)
  @Value.Default
  default int numParts() {
    return 1;
  }

  /**
   * Contains the timestamp in microseconds since (Unix) epoch when the object was last written,
   * only intended for repository cleanup mechanisms.
   *
   * <p>The value of this attribute is generated exclusively by the {@link Persistence}
   * implementations.
   *
   * <p>This attribute is <em>not</em> consistent when using a caching {@link Persistence}.
   */
  @JsonView(ObjSerializeAll.class)
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  @JacksonInject(OBJ_CREATED_AT_KEY)
  @Value.Default
  @Value.Auxiliary
  default long createdAtMicros() {
    return 0L;
  }

  /**
   * Opaque token used for objects when persisted using conditional {@linkplain
   * Persistence#conditionalInsert(Obj, Class) inserts}, {@linkplain
   * Persistence#conditionalUpdate(Obj, Obj, Class) updates} or {@linkplain
   * Persistence#conditionalDelete(Obj, Class) deletes}.
   *
   * <p>This value must be {@code null} for non-conditional operations and must be non-{@code null}
   * when used for conditional operations.
   */
  @JsonView(ObjSerializeAll.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JacksonInject(OBJ_VERSION_TOKEN)
  @Nullable
  String versionToken();

  @SuppressWarnings("NullableProblems")
  @Nonnull
  Obj withCreatedAtMicros(long createdAt);

  @SuppressWarnings("NullableProblems")
  @Nonnull
  Obj withNumParts(int numParts);

  class ObjSerializeAll {
    private ObjSerializeAll() {}
  }

  /** The Jackson view used when {@link Obj}s are serialized to be persisted. */
  class StorageView {
    private StorageView() {}
  }

  interface Builder<O extends Obj, B extends Builder<O, B>> {
    @CanIgnoreReturnValue
    B versionToken(@Nullable String versionToken);

    @CanIgnoreReturnValue
    B id(long id);

    @CanIgnoreReturnValue
    B numParts(int numParts);

    O build();
  }
}
