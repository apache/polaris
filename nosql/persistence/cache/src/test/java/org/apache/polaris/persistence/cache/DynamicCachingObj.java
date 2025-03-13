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
package org.apache.polaris.persistence.cache;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.function.LongSupplier;
import org.apache.polaris.persistence.api.obj.AbstractObjType;
import org.apache.polaris.persistence.api.obj.Obj;
import org.apache.polaris.persistence.api.obj.ObjType;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableDynamicCachingObj.class)
@JsonDeserialize(as = ImmutableDynamicCachingObj.class)
interface DynamicCachingObj extends Obj {
  ObjType TYPE = new DynamicCachingObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  long thatExpireTimestamp();

  final class DynamicCachingObjType extends AbstractObjType<DynamicCachingObj> {
    public DynamicCachingObjType() {
      super("dyn-cache", "dynamic caching", DynamicCachingObj.class);
    }

    @Override
    public long cachedObjectExpiresAtMicros(Obj obj, LongSupplier clockMicros) {
      return ((DynamicCachingObj) obj).thatExpireTimestamp() + clockMicros.getAsLong();
    }
  }
}
