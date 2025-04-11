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
package org.apache.polaris.realms.store;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.api.index.IndexContainer;
import org.apache.polaris.persistence.api.obj.AbstractObjType;
import org.apache.polaris.persistence.api.obj.BaseCommitObj;
import org.apache.polaris.persistence.api.obj.ObjRef;
import org.apache.polaris.persistence.api.obj.ObjType;

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
