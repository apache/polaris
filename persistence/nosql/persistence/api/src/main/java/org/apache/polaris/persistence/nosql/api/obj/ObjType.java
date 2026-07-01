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

import static org.apache.polaris.persistence.nosql.api.obj.ObjTypes.objTypeById;

import java.util.function.LongSupplier;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = ObjType.ObjTypeSerializer.class)
@JsonDeserialize(using = ObjType.ObjTypeDeserializer.class)
public interface ObjType {
  /** Human-readable name. */
  String name();

  /** Must be unique among all registered object types. */
  String id();

  /** The target class that objects of this type should be serialized from and deserialized to. */
  Class<? extends Obj> targetClass();

  /**
   * Allows an object type to define how long a particular object instance can be cached.
   *
   * <p>{@value #CACHE_UNLIMITED}, which is the default implementation, defines that an object
   * instance can be cached forever.
   *
   * <p>{@value #NOT_CACHED} defines that an object instance must never be cached.
   *
   * <p>A positive value defines the timestamp in "microseconds since epoch" when the cached object
   * can be evicted
   */
  default long cachedObjectExpiresAtMicros(Obj obj, LongSupplier clockMicros) {
    return CACHE_UNLIMITED;
  }

  /**
   * Allows an object type to define how long the fact of a non-existing object instance can be
   * cached.
   *
   * <p>{@value #CACHE_UNLIMITED} defines that an object instance can be cached forever.
   *
   * <p>{@value #NOT_CACHED}, which is the default implementation, defines that an object instance
   * must never be cached.
   *
   * <p>A positive value defines the timestamp in "microseconds since epoch" when the negative-cache
   * sentinel can be evicted
   */
  default long negativeCacheExpiresAtMicros(LongSupplier clockMicros) {
    return NOT_CACHED;
  }

  long CACHE_UNLIMITED = -1L;
  long NOT_CACHED = 0L;

  class ObjTypeSerializer extends ValueSerializer<ObjType> {
    @Override
    public void serialize(
        ObjType value, JsonGenerator gen, SerializationContext serializationContext) {
      gen.writeString(value.id());
    }
  }

  class ObjTypeDeserializer extends ValueDeserializer<ObjType> {
    @Override
    public ObjType deserialize(JsonParser p, DeserializationContext ctxt) {
      return objTypeById(p.getString());
    }
  }
}
