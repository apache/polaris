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
import org.apache.polaris.persistence.api.obj.AbstractObjType;
import org.apache.polaris.persistence.api.obj.Obj;
import org.apache.polaris.persistence.api.obj.ObjType;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableDefaultCachingObj.class)
@JsonDeserialize(as = ImmutableDefaultCachingObj.class)
interface DefaultCachingObj extends Obj {
  ObjType TYPE = new DefaultCachingObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  String value();

  final class DefaultCachingObjType extends AbstractObjType<DefaultCachingObj> {
    public DefaultCachingObjType() {
      super("test-default-caching", "default caching", DefaultCachingObj.class);
    }
  }
}
