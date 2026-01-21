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

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.catalog.identifier.IdentifierNormalizer;
import org.apache.polaris.service.catalog.identifier.IdentifierNormalizerFactory;

/**
 * Service for parsing and normalizing identifiers from REST requests. Handles case-normalization
 * based on catalog configuration, providing a single point of normalization at the adapter layer.
 */
@RequestScoped
public class IdentifierParser {

  private final CatalogCaseSensitivityResolver caseSensitivityResolver;
  private final IdentifierNormalizerFactory normalizerFactory;
  private final CatalogPrefixParser prefixParser;

  @Inject
  public IdentifierParser(
      CatalogCaseSensitivityResolver caseSensitivityResolver,
      IdentifierNormalizerFactory normalizerFactory,
      CatalogPrefixParser prefixParser) {
    this.caseSensitivityResolver = caseSensitivityResolver;
    this.normalizerFactory = normalizerFactory;
    this.prefixParser = prefixParser;
  }

  /**
   * Parse and normalize a namespace from REST request.
   *
   * @param realmContext the realm context
   * @param prefix the catalog prefix from the REST API
   * @param namespace the URL-encoded namespace string
   * @return normalized Namespace
   */
  public Namespace parseNamespace(RealmContext realmContext, String prefix, String namespace) {
    Namespace ns =
        RESTUtil.decodeNamespace(URLEncoder.encode(namespace, Charset.defaultCharset()));

    IdentifierNormalizer normalizer = getNormalizer(realmContext, prefix);
    return normalizer.normalizeNamespace(ns);
  }

  /**
   * Parse and normalize a table identifier from REST request.
   *
   * @param realmContext the realm context
   * @param prefix the catalog prefix from the REST API
   * @param namespace the URL-encoded namespace string
   * @param table the URL-encoded table name
   * @return normalized TableIdentifier
   */
  public TableIdentifier parseTableIdentifier(
      RealmContext realmContext, String prefix, String namespace, String table) {
    Namespace ns = parseNamespace(realmContext, prefix, namespace);
    String decodedTable = RESTUtil.decodeString(table);

    IdentifierNormalizer normalizer = getNormalizer(realmContext, prefix);
    String normalizedTable = normalizer.normalizeName(decodedTable);

    return TableIdentifier.of(ns, normalizedTable);
  }

  /**
   * Parse and decode a namespace without normalization. Used for cases where the raw decoded
   * namespace is needed.
   *
   * @param namespace the URL-encoded namespace string
   * @return decoded Namespace (not normalized)
   */
  public Namespace decodeNamespace(String namespace) {
    return RESTUtil.decodeNamespace(URLEncoder.encode(namespace, Charset.defaultCharset()));
  }

  /**
   * Get the appropriate normalizer for the catalog.
   *
   * @param realmContext the realm context
   * @param prefix the catalog prefix from the REST API
   * @return the appropriate IdentifierNormalizer for the catalog
   */
  public IdentifierNormalizer getNormalizer(RealmContext realmContext, String prefix) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    boolean isCaseInsensitive = caseSensitivityResolver.isCaseInsensitive(catalogName);

    if (isCaseInsensitive) {
      return normalizerFactory.getCaseInsensitiveNormalizer();
    }
    return normalizerFactory.getDefaultNormalizer();
  }

  /**
   * Check if the catalog identified by the prefix is case-insensitive.
   *
   * @param realmContext the realm context
   * @param prefix the catalog prefix from the REST API
   * @return true if the catalog is case-insensitive
   */
  public boolean isCaseInsensitive(RealmContext realmContext, String prefix) {
    String catalogName = prefixParser.prefixToCatalogName(realmContext, prefix);
    return caseSensitivityResolver.isCaseInsensitive(catalogName);
  }

  /**
   * Normalize a TableIdentifier based on catalog configuration. Used for request body identifiers.
   *
   * @param realmContext the realm context
   * @param prefix the catalog prefix from the REST API
   * @param identifier the TableIdentifier to normalize
   * @return normalized TableIdentifier
   */
  public TableIdentifier normalizeTableIdentifier(
      RealmContext realmContext, String prefix, TableIdentifier identifier) {
    if (identifier == null) {
      return null;
    }
    IdentifierNormalizer normalizer = getNormalizer(realmContext, prefix);
    return normalizer.normalizeTableIdentifier(identifier);
  }

  /**
   * Normalize a Namespace based on catalog configuration. Used for request body namespaces.
   *
   * @param realmContext the realm context
   * @param prefix the catalog prefix from the REST API
   * @param namespace the Namespace to normalize
   * @return normalized Namespace
   */
  public Namespace normalizeNamespace(
      RealmContext realmContext, String prefix, Namespace namespace) {
    if (namespace == null) {
      return null;
    }
    IdentifierNormalizer normalizer = getNormalizer(realmContext, prefix);
    return normalizer.normalizeNamespace(namespace);
  }
}
