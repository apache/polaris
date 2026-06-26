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
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.RESTUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolarisResourcePaths {

  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisResourcePaths.class);

  private static final Joiner SLASH = Joiner.on("/").skipNulls();
  public static final String PREFIX = "prefix";

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

  // Semantic Model endpoints
  public static final String V1_SEMANTIC_MODELS =
      "/polaris/v1/{prefix}/namespaces/{namespace}/semantic-models";
  public static final String V1_SEMANTIC_MODEL =
      "/polaris/v1/{prefix}/namespaces/{namespace}/semantic-models/{semantic-model-name}";

  private final String prefix;
  private final String namespaceSeparatorEncoded;

  public PolarisResourcePaths(String prefix, String namespaceSeparatorEncoded) {
    this.prefix = prefix;
    this.namespaceSeparatorEncoded = namespaceSeparatorEncoded;
  }

  public static PolarisResourcePaths forCatalogProperties(Map<String, String> properties) {
    String namespaceSeparatorEncoded =
        properties.getOrDefault(
            RESTCatalogProperties.NAMESPACE_SEPARATOR,
            RESTCatalogProperties.NAMESPACE_SEPARATOR_DEFAULT);
    if (!namespaceSeparatorEncoded.equals(RESTCatalogProperties.NAMESPACE_SEPARATOR_DEFAULT)) {
      LOGGER.warn("Using non-default namespace separator '{}'", namespaceSeparatorEncoded);
    }
    return new PolarisResourcePaths(properties.get(PREFIX), namespaceSeparatorEncoded);
  }

  public String genericTables(Namespace ns) {
    return SLASH.join(
        "polaris",
        "v1",
        prefix,
        "namespaces",
        // FIXME use RESTUtil.encodeNamespaceAsPathSegment(), see
        // https://github.com/apache/iceberg/pull/15989
        RESTUtil.encodeNamespace(ns, namespaceSeparatorEncoded),
        "generic-tables");
  }

  public String credentialsPath(TableIdentifier ident) {
    return SLASH.join(
        "v1",
        prefix,
        "namespaces",
        // FIXME use RESTUtil.encodeNamespaceAsPathSegment(), see
        // https://github.com/apache/iceberg/pull/15989
        RESTUtil.encodeNamespace(ident.namespace(), namespaceSeparatorEncoded),
        "tables",
        // FIXME use RESTUtil.encodePathSegment(), see https://github.com/apache/iceberg/pull/15989
        RESTUtil.encodeString(ident.name()),
        "credentials");
  }

  public String genericTable(TableIdentifier ident) {
    return SLASH.join(
        "polaris",
        "v1",
        prefix,
        "namespaces",
        // FIXME use RESTUtil.encodeNamespaceAsPathSegment(), see
        // https://github.com/apache/iceberg/pull/15989
        RESTUtil.encodeNamespace(ident.namespace(), namespaceSeparatorEncoded),
        "generic-tables",
        // FIXME use RESTUtil.encodePathSegment(), see https://github.com/apache/iceberg/pull/15989
        RESTUtil.encodeString(ident.name()));
  }
}
