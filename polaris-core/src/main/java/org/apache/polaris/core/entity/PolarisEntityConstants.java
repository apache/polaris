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
package org.apache.polaris.core.entity;

public class PolarisEntityConstants {

  public static final String ENTITY_BASE_LOCATION = "location";
  // the key for the client_id property associated with a principal
  private static final String CLIENT_ID_PROPERTY_NAME = "client_id";

  // id of the root entity
  private static final long ROOT_ENTITY_ID = 0L;

  // special 0 value to represent a NULL value. For example the catalog id is null for a top-level
  // entity like a catalog
  private static final long NULL_ID = 0L;

  // the name of the single root container representing an entire realm
  private static final String ROOT_CONTAINER_NAME = "root_container";

  // the name of the catalog/root admin role
  private static final String ADMIN_CATALOG_ROLE_NAME = "catalog_admin";

  // the name of the root principal we create at bootstrap time
  private static final String ROOT_PRINCIPAL_NAME = "root";

  // the name of the principal role we create to manage the entire Polaris service
  private static final String ADMIN_PRINCIPAL_ROLE_NAME = "service_admin";

  // 24 hours retention before purging. This should be a config
  private static final long RETENTION_TIME_IN_MS = 24 * 3600_000;

  private static final String STORAGE_CONFIGURATION_INFO_PROPERTY_NAME =
      "storage_configuration_info";

  private static final String STORAGE_INTEGRATION_IDENTIFIER_PROPERTY_NAME =
      "storage_integration_identifier";

  private static final String PRINCIPAL_TYPE_NAME = "principal_type_name";

  public static final String PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE =
      "CREDENTIAL_ROTATION_REQUIRED";

  public static final String MAINTENANCE_PREFIX = "polaris.maintenance.";

  /**
   * Name format of storage integration for polaris entity: {@code
   * POLARIS_<catalog_id>_<entity_id>}. This name format gives us flexibility to switch to use
   * integration name in the future if we want.
   */
  public static final String POLARIS_STORAGE_INT_NAME_FORMAT = "POLARIS_%s_%s";

  public static long getRootEntityId() {
    return ROOT_ENTITY_ID;
  }

  public static long getNullId() {
    return NULL_ID;
  }

  public static String getRootContainerName() {
    return ROOT_CONTAINER_NAME;
  }

  public static String getNameOfCatalogAdminRole() {
    return ADMIN_CATALOG_ROLE_NAME;
  }

  public static String getRootPrincipalName() {
    return ROOT_PRINCIPAL_NAME;
  }

  public static String getNameOfPrincipalServiceAdminRole() {
    return ADMIN_PRINCIPAL_ROLE_NAME;
  }

  public static long getRetentionTimeInMs() {
    return RETENTION_TIME_IN_MS;
  }

  public static String getClientIdPropertyName() {
    return CLIENT_ID_PROPERTY_NAME;
  }

  public static String getStorageIntegrationIdentifierPropertyName() {
    return STORAGE_INTEGRATION_IDENTIFIER_PROPERTY_NAME;
  }

  public static String getStorageConfigInfoPropertyName() {
    return STORAGE_CONFIGURATION_INFO_PROPERTY_NAME;
  }

  public static String getPolarisStorageIntegrationNameFormat() {
    return POLARIS_STORAGE_INT_NAME_FORMAT;
  }

  public static String getPrincipalTypeName() {
    return PRINCIPAL_TYPE_NAME;
  }
}
