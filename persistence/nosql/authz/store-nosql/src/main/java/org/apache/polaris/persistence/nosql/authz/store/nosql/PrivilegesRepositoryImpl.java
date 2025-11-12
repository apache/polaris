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
package org.apache.polaris.persistence.nosql.authz.store.nosql;

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.authz.store.nosql.PrivilegesMappingObj.PRIVILEGES_MAPPING_REF_NAME;

import jakarta.annotation.Nonnull;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.SystemPersistence;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.authz.spi.PrivilegesMapping;
import org.apache.polaris.persistence.nosql.authz.spi.PrivilegesRepository;

@ApplicationScoped
class PrivilegesRepositoryImpl implements PrivilegesRepository {
  private final Persistence persistence;
  private ObjRef privilegesMappingObjRef;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  PrivilegesRepositoryImpl(@SystemPersistence Persistence persistence) {
    this.persistence = persistence;
  }

  @PostConstruct
  void init() {
    privilegesMappingObjRef =
        persistence
            .fetchOrCreateReference(
                PRIVILEGES_MAPPING_REF_NAME,
                () -> Optional.of(objRef(PrivilegesMappingObj.TYPE, persistence.generateId())))
            .pointer()
            .orElseThrow();
  }

  @Override
  public boolean updatePrivilegesMapping(
      @Nonnull PrivilegesMapping expectedState, @Nonnull PrivilegesMapping newState) {
    var existing =
        Optional.ofNullable(persistence.fetch(privilegesMappingObjRef, PrivilegesMappingObj.class));

    if (!existing
        .map(PrivilegesMappingObj::privilegesMapping)
        .orElse(PrivilegesMapping.EMPTY)
        .equals(expectedState)) {
      return false;
    }
    if (expectedState.equals(newState)) {
      return true;
    }

    var newObj =
        PrivilegesMappingObj.builder()
            .id(privilegesMappingObjRef.id())
            .versionToken("" + persistence.generateId())
            .privilegesMapping(newState)
            .build();

    return existing
        .map(
            privilegesMappingObj ->
                persistence.conditionalUpdate(
                        privilegesMappingObj, newObj, PrivilegesMappingObj.class)
                    != null)
        .orElseGet(() -> persistence.conditionalInsert(newObj, PrivilegesMappingObj.class) != null);
  }

  @Override
  @Nonnull
  public PrivilegesMapping fetchPrivilegesMapping() {
    return Optional.ofNullable(
            persistence.fetch(privilegesMappingObjRef, PrivilegesMappingObj.class))
        .map(PrivilegesMappingObj::privilegesMapping)
        .orElse(PrivilegesMapping.EMPTY);
  }
}
