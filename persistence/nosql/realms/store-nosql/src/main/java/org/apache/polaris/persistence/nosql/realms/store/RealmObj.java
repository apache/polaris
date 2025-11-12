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
package org.apache.polaris.persistence.nosql.realms.store;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.Map;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.realms.api.RealmDefinition;

/** Represents the persisted state of a {@link RealmDefinition}. */
@PolarisImmutable
@JsonSerialize(as = ImmutableRealmObj.class)
@JsonDeserialize(as = ImmutableRealmObj.class)
public interface RealmObj extends Obj {
  ObjType TYPE = new RealmObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  Instant created();

  Instant updated();

  RealmDefinition.RealmStatus status();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> properties();

  static ImmutableRealmObj.Builder builder() {
    return ImmutableRealmObj.builder();
  }

  final class RealmObjType extends AbstractObjType<RealmObj> {
    public RealmObjType() {
      super("rlm", "Realm", RealmObj.class);
    }
  }
}
