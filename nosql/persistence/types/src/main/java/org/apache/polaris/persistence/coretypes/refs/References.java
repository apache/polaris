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
package org.apache.polaris.persistence.coretypes.refs;

import static java.lang.String.format;

import java.util.List;
import java.util.stream.Stream;
import org.apache.polaris.persistence.coretypes.catalog.CatalogGrantsObj;
import org.apache.polaris.persistence.coretypes.catalog.CatalogRolesObj;
import org.apache.polaris.persistence.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.coretypes.catalog.CatalogsObj;
import org.apache.polaris.persistence.coretypes.principals.PrincipalRolesObj;
import org.apache.polaris.persistence.coretypes.principals.PrincipalsObj;
import org.apache.polaris.persistence.coretypes.realm.ImmediateTasksObj;
import org.apache.polaris.persistence.coretypes.realm.PolicyMappingsObj;
import org.apache.polaris.persistence.coretypes.realm.RealmGrantsObj;
import org.apache.polaris.persistence.coretypes.realm.RootObj;

public final class References {
  private References() {}

  private static final List<String> REALM_REFERENCE_NAMES =
      List.of(
          RootObj.ROOT_REF_NAME,
          CatalogsObj.CATALOGS_REF_NAME,
          PrincipalsObj.PRINCIPALS_REF_NAME,
          PrincipalRolesObj.PRINCIPAL_ROLES_REF_NAME,
          RealmGrantsObj.REALM_GRANTS_REF_NAME,
          ImmediateTasksObj.IMMEDIATE_TASKS_REF_NAME,
          PolicyMappingsObj.POLICY_MAPPINGS_REF_NAME);

  private static final List<String> CATALOG_REFERENCE_PATTERNS =
      List.of(
          CatalogRolesObj.CATALOG_ROLES_REF_NAME_PATTERN,
          CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN,
          CatalogGrantsObj.CATALOG_GRANTS_REF_NAME_PATTERN);

  public static Stream<String> realmReferenceNames() {
    return REALM_REFERENCE_NAMES.stream();
  }

  public static Stream<String> catalogReferenceNames(long catalogStableId) {
    return CATALOG_REFERENCE_PATTERNS.stream()
        .map(refNamePattern -> perCatalogReferenceName(refNamePattern, catalogStableId));
  }

  public static String perCatalogReferenceName(String refNamePattern, long catalogStableId) {
    return format(refNamePattern, catalogStableId);
  }
}
