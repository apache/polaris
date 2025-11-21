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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;

/** Represents the persisted and system-wide consistent state of all realms. */
@PolarisImmutable
@JsonSerialize(as = ImmutableRealmsStateObj.class)
@JsonDeserialize(as = ImmutableRealmsStateObj.class)
public interface RealmsStateObj extends BaseCommitObj {
  ObjType TYPE = new RealmStateObjType();
  String REALMS_REF_NAME = "realms";

  @Override
  default ObjType type() {
    return TYPE;
  }

  /**
   * Index of all realms by ID (via {@link IndexKey#key(String)}) to the {@link ObjRef}s referencing
   * {@link RealmObj}s.
   */
  IndexContainer<ObjRef> realmIndex();

  static ImmutableRealmsStateObj.Builder builder() {
    return ImmutableRealmsStateObj.builder();
  }

  final class RealmStateObjType extends AbstractObjType<RealmsStateObj> {
    public RealmStateObjType() {
      super("realm-state", "Realms State", RealmsStateObj.class);
    }
  }

  interface Builder extends BaseCommitObj.Builder<RealmsStateObj, Builder> {}
}
