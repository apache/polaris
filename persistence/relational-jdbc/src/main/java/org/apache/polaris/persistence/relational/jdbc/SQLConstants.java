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

package org.apache.polaris.persistence.relational.jdbc;

import java.util.stream.Collectors;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEntity;
import org.apache.polaris.persistence.relational.jdbc.models.ModelGrantRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelPolicyMappingRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelPrincipalAuthenticationData;

public class SQLConstants {

  public static final String SCHEMA = "POLARIS_SCHEMA";

  public static final String ENTITY_LOOKUP_BY_CATALOG_ID_ID =
      "SELECT "
          + String.join(", ", ModelEntity.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelEntity.TABLE_NAME
          + " WHERE catalog_id = ? AND id = ? AND realm_id = ?";

  public static final String ENTITY_LOOKUP_BY_CATALOG_ID_ID_TYPE_CODE =
      "SELECT "
          + String.join(", ", ModelEntity.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelEntity.TABLE_NAME
          + " WHERE catalog_id = ? AND id = ? AND type_code = ? AND realm_id = ?";

  public static final String ENTITY_INSERT_QUERY =
      "INSERT INTO "
          + SCHEMA
          + "."
          + ModelEntity.TABLE_NAME
          + "("
          + String.join(", ", ModelEntity.ALL_COLUMNS)
          + ") VALUES ("
          + ModelEntity.ALL_COLUMNS.stream().map(c -> "?").collect(Collectors.joining(", "))
          + " )";

  public static final String ENTITY_DELETE_QUERY =
      "DELETE FROM "
          + SCHEMA
          + "."
          + ModelEntity.TABLE_NAME
          + " WHERE "
          + String.join(" = ? AND ", ModelEntity.PK_COLUMNS)
          + " = ?";

  public static final String ENTITY_LOOKUP_BY_NAME_QUERY =
      "SELECT "
          + String.join(", ", ModelEntity.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelEntity.TABLE_NAME
          + " WHERE catalog_id = ? AND parent_id = ? AND type_code = ? "
          + "AND name = ? AND realm_id = ?";

  public static final String ENTITY_LOOKUP_BY_PARENT_ID_QUERY =
      "SELECT "
          + String.join(", ", ModelEntity.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelEntity.TABLE_NAME
          + " WHERE catalog_id = ? AND parent_id = ? "
          + "AND realm_id = ?";

  public static final String ENTITY_LOOKUP_BY_PARENT_ID_WITH_TYPE_CODE_QUERY =
      "SELECT "
          + String.join(", ", ModelEntity.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelEntity.TABLE_NAME
          + " WHERE catalog_id = ? AND parent_id = ? AND type_code = ? "
          + "AND realm_id = ?";

  public static final String ENTITY_LIST_QUERY =
      "SELECT "
          + String.join(", ", ModelEntity.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelEntity.TABLE_NAME
          + " WHERE catalog_id = ? AND parent_id = ? AND type_code = ? AND realm_id = ?";

  public static final String ENTITY_LIST_BY_CATALOG_ID_AND_ID_QUERY =
      "SELECT "
          + String.join(", ", ModelEntity.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelEntity.TABLE_NAME
          + " WHERE (catalog_id, id) IN (%s) AND realm_id = ?";

  public static final String ENTITY_LOOKUP_FOR_GRANT_RECORD_VERSION_QUERY =
      "SELECT "
          + String.join(", ", ModelEntity.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelEntity.TABLE_NAME
          + " WHERE catalog_id = ? AND id = ? AND realm_id = ? ";

  public static final String ENTITY_UPDATE_QUERY =
      "UPDATE "
          + SCHEMA
          + "."
          + ModelEntity.TABLE_NAME
          + " SET "
          + ModelEntity.ALL_COLUMNS.stream().map(c -> c + " = ?").collect(Collectors.joining(", "))
          + " WHERE catalog_id = ? AND id = ? AND entity_version = ? AND realm_id = ? ";

  public static final String ENTITY_DELETE_ALL_QUERY =
      "DELETE FROM " + SCHEMA + "." + ModelEntity.TABLE_NAME + " WHERE realm_id = ?";

  public static final String GRANT_RECORD_INSERT_QUERY =
      "INSERT INTO "
          + SCHEMA
          + "."
          + ModelGrantRecord.TABLE_NAME
          + "("
          + String.join(", ", ModelGrantRecord.ALL_COLUMNS)
          + ") VALUES ("
          + ModelGrantRecord.ALL_COLUMNS.stream().map(c -> "?").collect(Collectors.joining(", "))
          + " )";

  public static final String GRANT_RECORD_LOOKUP_BY_PK_QUERY =
      "SELECT "
          + String.join(", ", ModelGrantRecord.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelGrantRecord.TABLE_NAME
          + " WHERE securable_catalog_id = ? AND securable_id = ? AND grantee_catalog_id = ? AND privilege_code = ? AND realm_id = ?";

  public static final String GRANT_RECORD_LOOKUP_BY_SECURABLE_QUERY =
      "SELECT "
          + String.join(", ", ModelGrantRecord.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelGrantRecord.TABLE_NAME
          + " WHERE securable_catalog_id = ? AND securable_id = ? AND realm_id = ? ";

  public static final String GRANT_RECORD_LOOKUP_BY_GRANTEE_QUERY =
      "SELECT "
          + String.join(", ", ModelGrantRecord.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelGrantRecord.TABLE_NAME
          + " WHERE grantee_catalog_id = ? AND grantee_id = ? AND realm_id = ? ";

  public static final String GRANT_RECORD_DELETE_QUERY_FOR_ENTITY =
      "DELETE FROM "
          + SCHEMA
          + "."
          + ModelGrantRecord.TABLE_NAME
          + " WHERE ((grantee_id = ? AND grantee_catalog_id = ?) OR (securable_id = ? AND securable_catalog_id = ?)) AND realm_id = ?";

  public static final String GRANT_RECORD_DELETE_QUERY =
      "DELETE FROM "
          + SCHEMA
          + "."
          + ModelGrantRecord.TABLE_NAME
          + " WHERE "
          + String.join(" = ? AND ", ModelGrantRecord.PK_COLUMNS)
          + " = ?";

  public static final String GRANT_RECORD_DELETE_ALL_QUERY =
      "DELETE FROM " + SCHEMA + "." + ModelGrantRecord.TABLE_NAME + " WHERE realm_id = ?";

  public static final String PRINCIPAL_AUTHENTICATION_DATA_INSERT_QUERY =
      "INSERT INTO "
          + SCHEMA
          + "."
          + ModelPrincipalAuthenticationData.TABLE_NAME
          + "("
          + String.join(", ", ModelPrincipalAuthenticationData.ALL_COLUMNS)
          + ") VALUES ("
          + ModelPrincipalAuthenticationData.ALL_COLUMNS.stream()
              .map(c -> "?")
              .collect(Collectors.joining(", "))
          + " )";

  public static final String PRINCIPAL_AUTHENTICATION_DATA_LOOKUP_BY_PRIMARY_KET_QUERY =
      "SELECT "
          + String.join(", ", ModelPrincipalAuthenticationData.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelPrincipalAuthenticationData.TABLE_NAME
          + " WHERE "
          + String.join(" = ? AND ", ModelPrincipalAuthenticationData.PK_COLUMNS)
          + " = ?";

  public static final String PRINCIPAL_AUTHENTICATION_DATA_UPDATE_QUERY =
      "UPDATE "
          + SCHEMA
          + "."
          + ModelPrincipalAuthenticationData.TABLE_NAME
          + " SET "
          + ModelPrincipalAuthenticationData.ALL_COLUMNS.stream()
              .map(c -> c + " = ?")
              .collect(Collectors.joining(", "))
          + " WHERE "
          + String.join(" = ? AND ", ModelPrincipalAuthenticationData.PK_COLUMNS)
          + " = ?";

  public static final String PRINCIPAL_AUTHENTICATION_DELETE_QUERY =
      "DELETE FROM "
          + SCHEMA
          + "."
          + ModelPrincipalAuthenticationData.TABLE_NAME
          + " WHERE realm_id = ? AND principal_client_id = ? AND principal_id = ? ";

  public static final String PRINCIPAL_AUTHENTICATION_DATA_DELETE_ALL_QUERY =
      "DELETE FROM "
          + SCHEMA
          + "."
          + ModelPrincipalAuthenticationData.TABLE_NAME
          + " WHERE realm_id = ?";

  public static final String POLICY_MAPPING_INSERT_QUERY =
      "INSERT INTO "
          + SCHEMA
          + "."
          + ModelPolicyMappingRecord.TABLE_NAME
          + "("
          + String.join(", ", ModelPolicyMappingRecord.ALL_COLUMNS)
          + ") VALUES ("
          + ModelPolicyMappingRecord.ALL_COLUMNS.stream()
              .map(c -> "?")
              .collect(Collectors.joining(", "))
          + " )";

  public static final String POLICY_MAPPING_UPDATE_QUERY =
      "UPDATE "
          + SCHEMA
          + "."
          + ModelPolicyMappingRecord.TABLE_NAME
          + " SET "
          + ModelPolicyMappingRecord.ALL_COLUMNS.stream()
              .map(c -> c + " = ?")
              .collect(Collectors.joining(", "))
          + " WHERE "
          + String.join(" = ? AND ", ModelPolicyMappingRecord.PK_COLUMNS)
          + " = ?";

  public static final String POLICY_MAPPING_RECORD_LOOKUP_BY_PRIMARY_KEY_QUERY =
      "SELECT "
          + String.join(", ", ModelPolicyMappingRecord.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelPolicyMappingRecord.TABLE_NAME
          + " WHERE "
          + String.join(" = ? AND ", ModelPolicyMappingRecord.PK_COLUMNS)
          + " = ?";

  public static final String POLICY_MAPPING_RECORD_LOOKUP_BY_TYPE_QUERY =
      "SELECT "
          + String.join(", ", ModelPolicyMappingRecord.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelPolicyMappingRecord.TABLE_NAME
          + " WHERE target_catalog_id = ? AND target_id = ? AND policy_type_code = ? AND realm_id = ?";

  public static final String POLICY_MAPPING_RECORD_LOOKUP_ON_TARGET_QUERY =
      "SELECT "
          + String.join(", ", ModelPolicyMappingRecord.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelPolicyMappingRecord.TABLE_NAME
          + " WHERE target_catalog_id = ? AND target_id = ? AND realm_id = ?";

  public static final String POLICY_MAPPING_RECORD_LOOKUP_TARGETS_ON_POLICY_QUERY =
      "SELECT "
          + String.join(", ", ModelPolicyMappingRecord.ALL_COLUMNS)
          + " FROM "
          + SCHEMA
          + "."
          + ModelPolicyMappingRecord.TABLE_NAME
          + " WHERE policy_type_code = ? AND policy_catalog_id = ? AND policy_id = ? AND realm_id = ?";

  public static final String POLICY_MAPPING_RECORD_DELETE_ALL_QUERY =
      "DELETE FROM " + SCHEMA + "." + ModelPolicyMappingRecord.TABLE_NAME + " WHERE realm_id = ?";

  public static final String POLICY_MAPPING_RECORD_DELETE_QUERY =
      "DELETE FROM "
          + SCHEMA
          + "."
          + ModelPolicyMappingRecord.TABLE_NAME
          + " WHERE "
          + String.join(" = ? AND ", ModelPolicyMappingRecord.ALL_COLUMNS)
          + " = ?";

  public static final String POLICY_MAPPING_RECORD_DELETE_ENTITY_QUERY =
      "DELETE FROM "
          + SCHEMA
          + "."
          + ModelPolicyMappingRecord.TABLE_NAME
          + " WHERE policy_type_code = ? AND policy_catalog_id = ? AND policy_id = ? AND realm_id = ?";

  public static final String POLICY_MAPPING_RECORD_DELETE_ENTITY_BY_ID_QUERY =
      "DELETE FROM "
          + SCHEMA
          + "."
          + ModelPolicyMappingRecord.TABLE_NAME
          + " WHERE  target_catalog_id = ? AND target_id = ? AND realm_id = ?";
}
