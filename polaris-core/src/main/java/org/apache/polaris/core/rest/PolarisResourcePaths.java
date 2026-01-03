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
package org.apache.polaris.core.rest;

import com.google.common.base.Joiner;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;

public class PolarisResourcePaths {
  private static final Joiner SLASH = Joiner.on("/").skipNulls();
  public static final String PREFIX = "prefix";

  /**
   * The "api/" path segment is the first path segment of all Polaris and Iceberg REST API paths. It
   * is not included in the constants below, as it is considered implicit.
   */
  public static final String API_PATH_SEGMENT = "api";

  // Generic Table endpoints
  public static final String V1_GENERIC_TABLES =
      "polaris/v1/{prefix}/namespaces/{namespace}/generic-tables";
  public static final String V1_GENERIC_TABLE =
      "polaris/v1/{prefix}/namespaces/{namespace}/generic-tables/{generic-table}";

  // Policy Store endpoints
  public static final String V1_POLICIES = "/polaris/v1/{prefix}/namespaces/{namespace}/policies";
  public static final String V1_POLICY =
      "/polaris/v1/{prefix}/namespaces/{namespace}/policies/{policy-name}";
  public static final String V1_POLICY_MAPPINGS =
      "/polaris/v1/{prefix}/namespaces/{namespace}/policies/{policy-name}/mappings";
  public static final String V1_APPLICABLE_POLICIES = "/polaris/v1/{prefix}/applicable-policies";

  private final String prefix;

  public PolarisResourcePaths(String prefix) {
    this.prefix = prefix;
  }

  public static PolarisResourcePaths forCatalogProperties(Map<String, String> properties) {
    return new PolarisResourcePaths(properties.get(PREFIX));
  }

  public String genericTables(Namespace ns) {
    return SLASH.join(
        "polaris", "v1", prefix, "namespaces", RESTUtil.encodeNamespace(ns), "generic-tables");
  }

  public String credentialsPath(TableIdentifier ident) {
    return SLASH.join(
        "v1",
        prefix,
        "namespaces",
        RESTUtil.encodeNamespace(ident.namespace()),
        "tables",
        RESTUtil.encodeString(ident.name()),
        "credentials");
  }

  public String genericTable(TableIdentifier ident) {
    return SLASH.join(
        "polaris",
        "v1",
        prefix,
        "namespaces",
        RESTUtil.encodeNamespace(ident.namespace()),
        "generic-tables",
        RESTUtil.encodeString(ident.name()));
  }

  public String s3RemoteSigning(TableIdentifier ident) {
    return SLASH.join(
        "s3-sign",
        "v1",
        prefix,
        "namespaces",
        RESTUtil.encodeNamespace(ident.namespace()),
        "tables",
        RESTUtil.encodeString(ident.name()));
  }
}
