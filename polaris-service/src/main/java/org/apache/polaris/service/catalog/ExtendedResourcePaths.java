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
package org.apache.polaris.service.catalog;

import org.apache.iceberg.rest.ResourcePaths;

// TODO: Replace with Iceberg's ResourcePaths after integrating Iceberg 1.7.0
// Related PR: https://github.com/apache/iceberg/pull/10929#issuecomment-2418591566
public class ExtendedResourcePaths extends ResourcePaths {
  public static final String V1_NAMESPACES = "/v1/{prefix}/namespaces";
  public static final String V1_NAMESPACE = "/v1/{prefix}/namespaces/{namespace}";
  public static final String V1_NAMESPACE_PROPERTIES =
      "/v1/{prefix}/namespaces/{namespace}/properties";
  public static final String V1_TABLES = "/v1/{prefix}/namespaces/{namespace}/tables";
  public static final String V1_TABLE = "/v1/{prefix}/namespaces/{namespace}/tables/{table}";
  public static final String V1_TABLE_REGISTER = "/v1/{prefix}/namespaces/{namespace}/register";
  public static final String V1_TABLE_METRICS =
      "/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics";
  public static final String V1_TABLE_RENAME = "/v1/{prefix}/tables/rename";
  public static final String V1_TRANSACTIONS_COMMIT = "/v1/{prefix}/transactions/commit";
  public static final String V1_VIEWS = "/v1/{prefix}/namespaces/{namespace}/views";
  public static final String V1_VIEW = "/v1/{prefix}/namespaces/{namespace}/views/{view}";
  public static final String V1_VIEW_RENAME = "/v1/{prefix}/views/rename";

  public ExtendedResourcePaths(String prefix) {
    super(prefix);
  }
}
