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

import com.fasterxml.jackson.annotation.JacksonInject;
import java.io.Serial;
import org.jspecify.annotations.NonNull;
import tools.jackson.databind.BeanProperty;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.InjectableValues;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;

public final class ObjSerializationHelper {
  private ObjSerializationHelper() {}

  /**
   * The key used to store the injectable {@code long id} instance, representing the id of the
   * object being deserialized. Meant to be used in methods and constructor parameters annotated
   * with {@link JacksonInject}.
   */
  static final String OBJ_ID_KEY = "polaris.persistence.ObjId";

  static final String OBJ_NUM_PARTS_KEY = "polaris.persistence.ObjNumParts";

  static final String OBJ_TYPE_KEY = "polaris.persistence.ObjType";

  static final String OBJ_VERSION_TOKEN = "polaris.persistence.ObjVersion";

  static final String OBJ_CREATED_AT_KEY = "polaris.persistence.ObjCreatedAt";

  /** Reusable typed readers scoped to one mapper. */
  public static ClassValue<ObjectReader> readersFor(@NonNull ObjectMapper mapper) {
    return new ClassValue<>() {
      @Override
      protected ObjectReader computeValue(@NonNull Class<?> type) {
        return mapper.readerFor(type);
      }
    };
  }

  /**
   * Returns an {@link ObjectReader} for the given target {@link ObjType} using the key {@value
   * #OBJ_TYPE_KEY}, with the given {@code long id} injectable under the key {@value #OBJ_ID_KEY},
   * version token using the key {@value #OBJ_VERSION_TOKEN}, {@code createdAtMicros} timestamp
   * using the key {@value #OBJ_CREATED_AT_KEY}, {@code numParts} using the key {@value
   * #OBJ_NUM_PARTS_KEY}.
   */
  public static ObjectReader contextualReader(
      @NonNull ObjectMapper mapper,
      @NonNull ObjType objType,
      long id,
      int numParts,
      String objVersionToken,
      long objCreatedAtMicros) {
    return contextualReader(
        mapper.readerFor(objType.targetClass()),
        objType,
        id,
        numParts,
        objVersionToken,
        objCreatedAtMicros);
  }

  /** Adds per-object metadata to a reusable typed reader. */
  public static ObjectReader contextualReader(
      @NonNull ObjectReader reader,
      @NonNull ObjType objType,
      long id,
      int numParts,
      String objVersionToken,
      long objCreatedAtMicros) {
    return reader.with(new ObjValues(objType, id, numParts, objVersionToken, objCreatedAtMicros));
  }

  private static final class ObjValues extends InjectableValues.Base {
    @Serial private static final long serialVersionUID = 1L;

    private final ObjType type;
    private final long id;
    private final int numParts;
    private final String versionToken;
    private final long createdAtMicros;

    private ObjValues(
        ObjType type, long id, int numParts, String versionToken, long createdAtMicros) {
      this.type = type;
      this.id = id;
      this.numParts = numParts;
      this.versionToken = versionToken;
      this.createdAtMicros = createdAtMicros;
    }

    @Override
    public ObjValues snapshot() {
      return this;
    }

    @Override
    public Object findInjectableValue(
        DeserializationContext ctxt,
        Object valueId,
        BeanProperty property,
        Object bean,
        Boolean optional,
        Boolean useInput) {
      var key = _validateKey(ctxt, valueId, property, bean);
      return switch (key) {
        case OBJ_TYPE_KEY -> type;
        case OBJ_ID_KEY -> id;
        case OBJ_NUM_PARTS_KEY -> numParts;
        case OBJ_VERSION_TOKEN -> versionToken;
        case OBJ_CREATED_AT_KEY -> createdAtMicros;
        default -> _handleMissingValue(ctxt, key, property, bean, optional, useInput);
      };
    }
  }
}
