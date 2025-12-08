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
package org.apache.polaris.persistence.nosql.coretypes.realm;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.acl.GrantsObj;

/**
 * Maintains the state of all realm grants. The current version of this object is maintained via the
 * reference name {@value #REALM_GRANTS_REF_NAME}.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableRealmGrantsObj.class)
@JsonDeserialize(as = ImmutableRealmGrantsObj.class)
public interface RealmGrantsObj extends GrantsObj {

  String REALM_GRANTS_REF_NAME = "grants";

  ObjType TYPE = new RealmGrantsObjType();

  @Override
  default ObjType type() {
    return TYPE;
  }

  static ImmutableRealmGrantsObj.Builder builder() {
    return ImmutableRealmGrantsObj.builder();
  }

  final class RealmGrantsObjType extends AbstractObjType<RealmGrantsObj> {
    public RealmGrantsObjType() {
      super("gts", "Realm Grants", RealmGrantsObj.class);
    }
  }

  interface Builder extends GrantsObj.Builder<RealmGrantsObj, Builder> {}
}
