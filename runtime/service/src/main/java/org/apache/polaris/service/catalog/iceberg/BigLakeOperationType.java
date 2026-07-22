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
package org.apache.polaris.service.catalog.iceberg;

import com.google.common.base.Strings;
import java.net.URI;
import java.util.Locale;

enum BigLakeOperationType {
  CONFIG,
  LIST_NAMESPACES,
  LOAD_NAMESPACE,
  CREATE_NAMESPACE,
  UPDATE_NAMESPACE,
  DELETE_NAMESPACE,
  LIST_TABLES,
  LOAD_TABLE,
  TABLE_EXISTS,
  CREATE_TABLE,
  UPDATE_TABLE,
  DELETE_TABLE,
  RENAME_TABLE,
  REGISTER_TABLE,
  LOAD_CREDENTIALS,
  REPORT_METRICS,
  COMMIT_TRANSACTION,
  LIST_VIEWS,
  LOAD_VIEW,
  VIEW_EXISTS,
  CREATE_VIEW,
  UPDATE_VIEW,
  DELETE_VIEW,
  RENAME_VIEW,
  REGISTER_VIEW,
  UNKNOWN;

  static BigLakeOperationType from(String httpMethod, String path) {
    String normalizedMethod = httpMethod == null ? "" : httpMethod.toUpperCase(Locale.ROOT);
    String normalizedPath = normalizePath(path);

    if (normalizedPath.endsWith("/v1/config")) {
      return CONFIG;
    }
    if (normalizedPath.endsWith("/credentials")) {
      return LOAD_CREDENTIALS;
    }
    if (normalizedPath.endsWith("/metrics")) {
      return REPORT_METRICS;
    }
    if (normalizedPath.endsWith("/commit-transaction")) {
      return COMMIT_TRANSACTION;
    }
    if (normalizedPath.endsWith("/rename")) {
      return normalizedPath.contains("/views/") ? RENAME_VIEW : RENAME_TABLE;
    }
    if (normalizedPath.endsWith("/register")) {
      return normalizedPath.contains("/views/") ? REGISTER_VIEW : REGISTER_TABLE;
    }
    if (normalizedPath.contains("/views")) {
      return switch (normalizedMethod) {
        case "GET" -> normalizedPath.endsWith("/views") ? LIST_VIEWS : LOAD_VIEW;
        case "HEAD" -> VIEW_EXISTS;
        case "POST" -> normalizedPath.endsWith("/views") ? CREATE_VIEW : UPDATE_VIEW;
        case "DELETE" -> DELETE_VIEW;
        default -> UNKNOWN;
      };
    }
    if (normalizedPath.contains("/tables")) {
      return switch (normalizedMethod) {
        case "GET" -> normalizedPath.endsWith("/tables") ? LIST_TABLES : LOAD_TABLE;
        case "HEAD" -> TABLE_EXISTS;
        case "POST" -> normalizedPath.endsWith("/tables") ? CREATE_TABLE : UPDATE_TABLE;
        case "DELETE" -> DELETE_TABLE;
        default -> UNKNOWN;
      };
    }
    if (normalizedPath.contains("/namespaces")) {
      return switch (normalizedMethod) {
        case "GET" -> normalizedPath.endsWith("/namespaces") ? LIST_NAMESPACES : LOAD_NAMESPACE;
        case "POST" ->
            normalizedPath.endsWith("/properties") ? UPDATE_NAMESPACE : CREATE_NAMESPACE;
        case "DELETE" -> DELETE_NAMESPACE;
        default -> UNKNOWN;
      };
    }
    return UNKNOWN;
  }

  private static String normalizePath(String path) {
    if (Strings.isNullOrEmpty(path)) {
      return "";
    }

    try {
      URI uri = URI.create(path);
      if (uri.getPath() != null) {
        return uri.getPath();
      }
    } catch (IllegalArgumentException e) {
      // Fall through to the raw path string.
    }

    return path;
  }
}
