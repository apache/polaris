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

  // Generic Table endpoints
  public static final String V1_GENERIC_TABLES =
      "polaris/v1/{prefix}/namespaces/{namespace}/generic-tables";
  public static final String V1_GENERIC_TABLE =
      "polaris/v1/{prefix}/namespaces/{namespace}/generic-tables/{generic-table}";

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
}
