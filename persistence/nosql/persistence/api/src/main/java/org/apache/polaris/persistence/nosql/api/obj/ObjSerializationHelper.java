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
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import jakarta.annotation.Nonnull;

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

  /**
   * Returns an {@link ObjectReader} for the given target {@link ObjType} using the key {@value
   * #OBJ_TYPE_KEY}, with the given {@code long id} injectable under the key {@value #OBJ_ID_KEY},
   * version token using the key {@value #OBJ_VERSION_TOKEN}, {@code createdAtMicros} timestamp
   * using the key {@value #OBJ_CREATED_AT_KEY}, {@code numParts} using the key {@value
   * #OBJ_NUM_PARTS_KEY}.
   */
  public static ObjectReader contextualReader(
      @Nonnull ObjectMapper mapper,
      @Nonnull ObjType objType,
      long id,
      int numParts,
      String objVersionToken,
      long objCreatedAtMicros) {
    InjectableValues values =
        new InjectableValues.Std()
            .addValue(OBJ_TYPE_KEY, objType)
            .addValue(OBJ_ID_KEY, id)
            .addValue(OBJ_NUM_PARTS_KEY, numParts)
            .addValue(OBJ_VERSION_TOKEN, objVersionToken)
            .addValue(OBJ_CREATED_AT_KEY, objCreatedAtMicros);
    return mapper.reader(values).forType(objType.targetClass());
  }
}
