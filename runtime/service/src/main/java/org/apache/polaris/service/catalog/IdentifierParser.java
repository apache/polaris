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

import java.net.URLEncoder;
import java.nio.charset.Charset;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.service.catalog.identifier.IdentifierNormalizer;

/**
 * Parses and normalizes Iceberg identifiers (namespaces, tables, views) according to a provided
 * normalizer strategy.
 *
 * <p>This class is stateless except for the normalizer it was created with. All parsing logic
 * delegates to the normalizer for actual case transformation.
 *
 * <p><b>Note</b>: Instances should be created via {@link IdentifierParserFactory} to ensure the
 * correct normalizer is used based on catalog case-sensitivity settings.
 *
 * <pre>
 * // Usage in adapters:
 * PolarisResolutionManifest manifest = resolutionManifestFactory.createResolutionManifest(...);
 * manifest.resolveAll();
 *
 * IdentifierParser parser = identifierParserFactory.createParser(manifest);
 * Namespace ns = parser.parseNamespace(namespace);
 * TableIdentifier tableId = parser.parseTableIdentifier(namespace, tableName);
 * </pre>
 */
public class IdentifierParser {

  private final IdentifierNormalizer normalizer;

  /**
   * Package-private constructor. Use {@link IdentifierParserFactory} to create instances.
   *
   * @param normalizer The normalizer strategy to use for identifier normalization
   */
  IdentifierParser(IdentifierNormalizer normalizer) {
    this.normalizer = normalizer;
  }

  /**
   * Parse a namespace string into a normalized Namespace object.
   *
   * <p>The namespace string is URL-decoded (Iceberg REST spec uses URL encoding for namespace
   * separators) and then normalized according to the catalog's case-sensitivity setting.
   *
   * @param namespace Namespace string (may be encoded with unit separator \u001F for multi-level
   *     namespaces)
   * @return Parsed and normalized Namespace
   */
  public Namespace parseNamespace(String namespace) {
    // Decode the namespace (Iceberg REST uses URL encoding)
    Namespace ns =
        RESTUtil.decodeNamespace(URLEncoder.encode(namespace, Charset.defaultCharset()));

    // Normalize according to catalog case-sensitivity
    return normalizer.normalizeNamespace(ns);
  }

  /**
   * Parse namespace and table name into a normalized TableIdentifier.
   *
   * @param namespace Namespace string
   * @param tableName Table name string (URL-encoded)
   * @return Parsed and normalized TableIdentifier
   */
  public TableIdentifier parseTableIdentifier(String namespace, String tableName) {
    Namespace ns = parseNamespace(namespace);
    String decodedTable = RESTUtil.decodeString(tableName);
    String normalizedTable = normalizer.normalizeName(decodedTable);
    return TableIdentifier.of(ns, normalizedTable);
  }

  /**
   * Normalize a pre-parsed TableIdentifier.
   *
   * <p>Used for identifiers in request bodies that are already parsed but need normalization.
   *
   * @param identifier TableIdentifier to normalize
   * @return Normalized TableIdentifier (or null if input is null)
   */
  public TableIdentifier normalizeTableIdentifier(TableIdentifier identifier) {
    if (identifier == null) {
      return null;
    }
    return normalizer.normalizeTableIdentifier(identifier);
  }

  /**
   * Normalize a pre-parsed Namespace.
   *
   * <p>Used for namespaces in request bodies that are already parsed but need normalization.
   *
   * @param namespace Namespace to normalize
   * @return Normalized Namespace (or null if input is null)
   */
  public Namespace normalizeNamespace(Namespace namespace) {
    if (namespace == null) {
      return null;
    }
    return normalizer.normalizeNamespace(namespace);
  }

  /**
   * Normalize a single name (table, view, policy, etc.).
   *
   * @param name Name to normalize
   * @return Normalized name
   */
  public String normalizeName(String name) {
    return normalizer.normalizeName(name);
  }

  /**
   * Check if this parser performs case normalization.
   *
   * @return true if normalizer transforms case (lowercase), false if case-preserving
   */
  public boolean isCaseNormalizing() {
    return normalizer.isCaseNormalizing();
  }

  /**
   * Get the underlying normalizer. Provided for advanced use cases where direct normalizer access
   * is needed.
   *
   * @return The IdentifierNormalizer used by this parser
   */
  public IdentifierNormalizer getNormalizer() {
    return normalizer;
  }

  /**
   * Parse and decode a namespace without normalization. Used for cases where the raw decoded
   * namespace is needed without any case transformation.
   *
   * <p><b>Note</b>: This method bypasses normalization and should only be used when you explicitly
   * need the raw decoded form.
   *
   * @param namespace the URL-encoded namespace string
   * @return decoded Namespace (not normalized)
   */
  public Namespace decodeNamespace(String namespace) {
    return RESTUtil.decodeNamespace(URLEncoder.encode(namespace, Charset.defaultCharset()));
  }
}
