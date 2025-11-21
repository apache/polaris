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
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import java.util.Map;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

@PolarisImmutable
@Value.Style(jdkOnly = true)
@VisibleForTesting
public abstract class GenericObj implements Obj {

  @Override
  @JsonIgnore
  public abstract ObjType type();

  @Override
  @JsonIgnore
  public abstract long id();

  @Override
  @JsonIgnore
  public abstract int numParts();

  @Override
  @JsonIgnore
  @Value.Auxiliary
  public abstract long createdAtMicros();

  @Override
  @JsonIgnore
  @Nullable
  public abstract String versionToken();

  @JsonAnyGetter
  @AllowNulls
  public abstract Map<String, Object> attributes();

  @JsonCreator
  static GenericObj create(
      @JacksonInject(OBJ_TYPE_KEY) ObjType objType,
      @JacksonInject(OBJ_ID_KEY) long id,
      @JacksonInject(OBJ_NUM_PARTS_KEY) int numParts,
      @JacksonInject(OBJ_VERSION_TOKEN) String versionToken,
      @JacksonInject(OBJ_CREATED_AT_KEY) long createdAtMicros,
      @JsonAnySetter Map<String, Object> attributes) {
    ImmutableGenericObj.Builder builder =
        ImmutableGenericObj.builder()
            .type(objType)
            .id(id)
            .numParts(numParts)
            .createdAtMicros(createdAtMicros);
    if (versionToken != null) {
      builder.versionToken(versionToken);
    }
    attributes.forEach(
        (k, v) -> {
          if (!"type".equals(k)
              && !"id".equals(k)
              && !"numParts".equals(k)
              && !"createdAtMicros".equals(k)
              && !"versionToken".equals(k)) {
            builder.putAttributes(k, v);
          }
        });
    return builder.build();
  }

  @Documented
  @Target({ElementType.FIELD, ElementType.METHOD})
  @interface AllowNulls {}
}
