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
package org.apache.polaris.persistence.nosql.api.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(jdkOnly = true)
@JsonSerialize(as = ImmutableCacheInvalidations.class)
@JsonDeserialize(as = ImmutableCacheInvalidations.class)
public interface CacheInvalidations {
  @Value.Parameter(order = 1)
  List<CacheInvalidation> invalidations();

  static CacheInvalidations cacheInvalidations(List<CacheInvalidation> invalidations) {
    return ImmutableCacheInvalidations.of(invalidations);
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "t")
  @JsonSubTypes({
    @JsonSubTypes.Type(
        value = CacheInvalidationEvictObj.class,
        name = CacheInvalidationEvictObj.TYPE),
    @JsonSubTypes.Type(
        value = CacheInvalidationEvictReference.class,
        name = CacheInvalidationEvictReference.TYPE),
  })
  interface CacheInvalidation {
    String type();
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableCacheInvalidationEvictObj.class)
  @JsonDeserialize(as = ImmutableCacheInvalidationEvictObj.class)
  @JsonTypeName(value = CacheInvalidationEvictObj.TYPE)
  interface CacheInvalidationEvictObj extends CacheInvalidation {
    String TYPE = "obj";

    @Override
    default String type() {
      return TYPE;
    }

    @JsonProperty("r")
    @Value.Parameter(order = 1)
    String realmId();

    @Value.Parameter(order = 2)
    ObjRef id();

    static CacheInvalidationEvictObj cacheInvalidationEvictObj(String realmId, ObjRef id) {
      return ImmutableCacheInvalidationEvictObj.of(realmId, id);
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableCacheInvalidationEvictReference.class)
  @JsonDeserialize(as = ImmutableCacheInvalidationEvictReference.class)
  @JsonTypeName(value = CacheInvalidationEvictReference.TYPE)
  interface CacheInvalidationEvictReference extends CacheInvalidation {
    String TYPE = "ref";

    @Override
    default String type() {
      return TYPE;
    }

    @JsonProperty("r")
    @Value.Parameter(order = 1)
    String realmId();

    @Value.Parameter(order = 2)
    String ref();

    static CacheInvalidationEvictReference cacheInvalidationEvictReference(
        String realmId, String refName) {
      return ImmutableCacheInvalidationEvictReference.of(realmId, refName);
    }
  }
}
