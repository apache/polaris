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
package org.apache.polaris.persistence.nosql.coretypes.refs;

import static java.lang.String.format;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogRolesObj.CATALOG_ROLES_REF_NAME_PATTERN;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj.CATALOGS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalRolesObj.PRINCIPAL_ROLES_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj.PRINCIPALS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.realm.ImmediateTasksObj.IMMEDIATE_TASKS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.realm.PolicyMappingsObj.POLICY_MAPPINGS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.realm.RealmGrantsObj.REALM_GRANTS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.realm.RootObj.ROOT_REF_NAME;

import java.util.Set;
import java.util.stream.Collectors;

public final class References {
  private References() {}

  private static final Set<String> REALM_REFERENCE_NAMES =
      Set.of(
          ROOT_REF_NAME,
          CATALOGS_REF_NAME,
          PRINCIPALS_REF_NAME,
          PRINCIPAL_ROLES_REF_NAME,
          REALM_GRANTS_REF_NAME,
          IMMEDIATE_TASKS_REF_NAME,
          POLICY_MAPPINGS_REF_NAME);

  private static final Set<String> CATALOG_REFERENCE_PATTERNS =
      Set.of(CATALOG_ROLES_REF_NAME_PATTERN, CATALOG_STATE_REF_NAME_PATTERN);

  public static Set<String> realmReferenceNames() {
    return REALM_REFERENCE_NAMES;
  }

  public static Set<String> catalogReferenceNames(long catalogStableId) {
    return CATALOG_REFERENCE_PATTERNS.stream()
        .map(refNamePattern -> format(refNamePattern, catalogStableId))
        .collect(Collectors.toSet());
  }
}
