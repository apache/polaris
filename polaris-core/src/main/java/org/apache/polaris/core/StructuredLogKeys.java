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
package org.apache.polaris.core;

/**
 * Constants for structured log key names used in SLF4J {@code addKeyValue()} calls throughout the
 * codebase. Centralizing these keys prevents typos, enables IDE autocomplete and
 * find-all-references, and provides a single source of truth for log key names used in search and
 * alerting.
 */
public final class StructuredLogKeys {
  private StructuredLogKeys() {}

  // --- Table-related keys ---
  public static final String TABLE_IDENTIFIER = "tableIdentifier";
  public static final String TABLE_ID = "tableId";
  public static final String TABLE_NAME = "tableName";
  public static final String TABLE_ENTITY = "tableEntity";
  public static final String TABLE_LOCATION = "tableLocation";
  public static final String TABLE_LOCATIONS = "tableLocations";

  // --- View-related keys ---
  public static final String VIEW_IDENTIFIER = "viewIdentifier";

  // --- Namespace-related keys ---
  public static final String NAMESPACE = "namespace";

  // --- Catalog-related keys ---
  public static final String CATALOG = "catalog";
  public static final String CATALOG_NAME = "catalogName";

  // --- Metadata keys ---
  public static final String METADATA_LOCATION = "metadataLocation";
  public static final String METADATA_FILES = "metadataFiles";

  // --- File and storage keys ---
  public static final String FILE = "file";
  public static final String FILE_PATH = "filePath";
  public static final String FILES = "files";
  public static final String BASE_FILE = "baseFile";
  public static final String BATCH_FILES = "batchFiles";
  public static final String MANIFEST_FILE = "manifestFile";
  public static final String MISSING_FILES = "missingFiles";
  public static final String LOCATION = "location";
  public static final String LOCATIONS = "locations";
  public static final String READ_LOCATIONS = "readLocations";
  public static final String WRITE_LOCATIONS = "writeLocations";
  public static final String LIST_LOCATIONS = "listLocations";

  // --- Credential and storage config keys ---
  public static final String CREDENTIALS = "credentials";
  public static final String CREDENTIAL_KEYS = "credentialKeys";
  public static final String EXTRA_PROPERTIES = "extraProperties";
  public static final String STORAGE_CONFIG = "storageConfig";
  public static final String STORAGE_ACCOUNT = "storageAccount";
  public static final String CONTAINER = "container";
  public static final String ACCESS_BOUNDARY = "accessBoundary";
  public static final String ALLOWED_LIST_ACTION = "allowedListAction";
  public static final String ENDPOINT = "endpoint";
  public static final String SECRET_REFERENCE = "secretReference";

  // --- Principal and auth keys ---
  public static final String PRINCIPAL = "principal";
  public static final String PRINCIPAL_NAME = "principalName";
  public static final String ROLE = "role";
  public static final String ROLES = "roles";

  // --- Task-related keys ---
  public static final String TASK_ENTITY_ID = "taskEntityId";
  public static final String TASK_ENTITY_NAME = "taskEntityName";
  public static final String TASK_NAME = "taskName";
  public static final String TASK_TYPE = "taskType";
  public static final String TASK_COUNT = "taskCount";
  public static final String HANDLER_CLASS = "handlerClass";
  public static final String ATTEMPT = "attempt";
  public static final String CONFIGURED_BATCH_SIZE = "configuredBatchSize";
  public static final String EFFECTIVE_BATCH_SIZE = "effectiveBatchSize";

  // --- Error keys ---
  public static final String ERROR = "error";
  public static final String ERR_MSG = "errMsg";
  public static final String STACK_TRACE = "stackTrace";

  // --- Misc keys ---
  public static final String TYPE = "type";
  public static final String NOTIFICATION = "notification";
  public static final String REMOTE_URL = "remoteUrl";
  public static final String JSON = "json";
}
