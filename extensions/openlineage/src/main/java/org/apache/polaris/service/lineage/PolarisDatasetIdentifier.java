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
package org.apache.polaris.service.lineage;

import com.google.common.base.Splitter;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides whether an OpenLineage dataset names a Polaris entity or is external to Polaris, which is
 * the security boundary of lineage ingest: a Polaris-native dataset must resolve and be authorized
 * or it is dropped, whereas an external one is recorded with no privilege check of its own. A
 * dataset is Polaris-native if its dataset namespace is mapped to a catalog by {@link
 * FeatureConfiguration#LINEAGE_NAMESPACE_CATALOGS}, in which case the name is {@code
 * <namespace-levels>.<table>} within it, or failing that if the namespace looks like a Polaris
 * catalog endpoint and the name carries the catalog itself as {@code
 * <catalog>.<namespace-levels>.<table>}. The namespace decides, never the name's shape: a dotted
 * name under an unrelated namespace such as {@code kafka://broker} stays external, since Kafka
 * topics and BigQuery tables are routinely dotted and are not Polaris entities. A name that claims
 * a catalog either way but cannot form a valid table path is reported {@link
 * Identification.Kind#UNADDRESSABLE_POLARIS_CLAIM} and dropped rather than downgraded to external,
 * because recording it as external would write an unauthenticated near-copy of a real entity into
 * the graph beside the real one. No method here throws: a malformed namespace or name still has to
 * produce a classification, since the alternative is a 500 on an event sent in good faith.
 */
public final class PolarisDatasetIdentifier {

  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisDatasetIdentifier.class);

  /** Dataset names are dotted paths. Empty segments are preserved so they can be rejected. */
  private static final Splitter NAME_SPLITTER = Splitter.on('.');

  /** A table path needs at least one namespace level plus the table name. */
  private static final int MIN_TABLE_PATH_SEGMENTS = 2;

  /**
   * An unmapped dataset namespace gives no catalog, so the name has to supply one: a table path
   * plus a leading catalog segment. Anything shorter is ambiguous with the {@code schema.table}
   * convention almost every non-Polaris system uses, and Polaris has no tables directly under a
   * catalog anyway.
   */
  private static final int MIN_QUALIFIED_NAME_SEGMENTS = MIN_TABLE_PATH_SEGMENTS + 1;

  /**
   * The path a default Polaris deployment serves the Iceberg REST catalog from. Deployments may map
   * another path to the catalog root, which is why this is only a fallback.
   */
  private static final String DEFAULT_CATALOG_PATH = "/api/catalog";

  private static final char NULL_BYTE = '\0';

  private final Map<String, String> namespaceCatalogs;

  /**
   * @param namespaceCatalogs OpenLineage dataset namespace to Polaris catalog name
   */
  public PolarisDatasetIdentifier(Map<String, String> namespaceCatalogs) {
    this.namespaceCatalogs = Map.copyOf(namespaceCatalogs);
  }

  /**
   * Builds an identifier from the realm's {@link FeatureConfiguration#LINEAGE_NAMESPACE_CATALOGS}.
   *
   * <p>Tolerates a null config value so a realm that has never set the key — and a test double that
   * does not stub it — behaves like the documented empty default rather than failing the request.
   */
  public static PolarisDatasetIdentifier fromRealmConfig(RealmConfig realmConfig) {
    return new PolarisDatasetIdentifier(
        parseNamespaceCatalogs(
            realmConfig.getConfig(FeatureConfiguration.LINEAGE_NAMESPACE_CATALOGS)));
  }

  /**
   * Parses the configured {@code "<dataset-namespace>=<catalog>"} entries into a lookup map.
   *
   * <p>Only the first {@code '='} separates the two, because a dataset namespace is a URI and may
   * contain {@code '='} in a query string. An entry that is malformed or has a blank side is
   * skipped with a warning instead of failing the realm's configuration, and the first entry for a
   * namespace wins so a later duplicate cannot redirect it to a different catalog.
   */
  static Map<String, String> parseNamespaceCatalogs(List<String> entries) {
    if (entries == null || entries.isEmpty()) {
      return Map.of();
    }
    Map<String, String> parsed = new LinkedHashMap<>();
    for (String entry : entries) {
      if (entry == null) {
        continue;
      }
      int separator = entry.indexOf('=');
      if (separator < 0) {
        LOGGER.warn(
            "Ignoring malformed {} entry (expected \"<namespace>=<catalog>\"): {}",
            FeatureConfiguration.LINEAGE_NAMESPACE_CATALOGS.key(),
            entry);
        continue;
      }
      String namespace = entry.substring(0, separator);
      String catalog = entry.substring(separator + 1);
      if (isBlank(namespace) || isBlank(catalog)) {
        LOGGER.warn(
            "Ignoring {} entry with a blank namespace or catalog: {}",
            FeatureConfiguration.LINEAGE_NAMESPACE_CATALOGS.key(),
            entry);
        continue;
      }
      String previous = parsed.putIfAbsent(namespace, catalog);
      if (previous != null && !previous.equals(catalog)) {
        LOGGER.warn(
            "Ignoring duplicate {} entry for namespace {}: already mapped to catalog {}",
            FeatureConfiguration.LINEAGE_NAMESPACE_CATALOGS.key(),
            namespace,
            previous);
      }
    }
    return parsed;
  }

  /**
   * Classifies the dataset that an OpenLineage event named by {@code datasetNamespace} and {@code
   * name}. Never throws.
   *
   * <p>{@code datasetNamespace} is the OpenLineage dataset namespace — typically a URI such as
   * {@code http://polaris:8181/api/catalog} or {@code kafka://broker}. It is unrelated to a Polaris
   * namespace; the Polaris namespace, if any, comes from {@code name}.
   */
  public Identification identify(String datasetNamespace, String name) {
    List<String> segments = split(name);

    String mappedCatalog =
        datasetNamespace == null ? null : namespaceCatalogs.get(datasetNamespace);
    if (mappedCatalog != null) {
      return identityWithin(mappedCatalog, segments);
    }

    if (!isPolarisCatalogUri(datasetNamespace) || segments.size() < MIN_QUALIFIED_NAME_SEGMENTS) {
      return Identification.external();
    }
    return identityWithin(segments.get(0), segments.subList(1, segments.size()));
  }

  /**
   * Whether {@code datasetNamespace} looks like a Polaris Iceberg REST catalog endpoint.
   *
   * <p>A heuristic, and deliberately only a fallback for an unmapped namespace: a deployment may
   * serve the catalog root from any path, so the URI alone cannot prove the endpoint is Polaris.
   * {@link FeatureConfiguration#LINEAGE_NAMESPACE_CATALOGS} is the authoritative mechanism; this
   * just spares a default deployment from having to configure it.
   */
  private static boolean isPolarisCatalogUri(String datasetNamespace) {
    if (isBlank(datasetNamespace)) {
      return false;
    }
    URI uri;
    try {
      uri = URI.create(datasetNamespace.trim());
    } catch (IllegalArgumentException e) {
      return false;
    }
    String scheme = uri.getScheme();
    if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
      return false;
    }
    String path = uri.getPath();
    if (path == null) {
      return false;
    }
    while (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    return path.endsWith(DEFAULT_CATALOG_PATH);
  }

  /**
   * Builds the identity for {@code pathSegments} — {@code [namespace-levels..., table]} — inside
   * {@code catalog}, or reports the claim unaddressable.
   */
  private static Identification identityWithin(String catalog, List<String> pathSegments) {
    if (!isAddressableSegment(catalog) || pathSegments.size() < MIN_TABLE_PATH_SEGMENTS) {
      return Identification.unaddressablePolarisClaim();
    }
    for (String segment : pathSegments) {
      if (!isAddressableSegment(segment)) {
        return Identification.unaddressablePolarisClaim();
      }
    }
    int tableIndex = pathSegments.size() - 1;
    try {
      Namespace tableNamespace =
          Namespace.of(pathSegments.subList(0, tableIndex).toArray(String[]::new));
      TableIdentifier table = TableIdentifier.of(tableNamespace, pathSegments.get(tableIndex));
      return Identification.polaris(new PolarisDatasetIdentity(catalog, table));
    } catch (RuntimeException e) {
      LOGGER.debug("Dataset name is not an addressable Polaris table path: {}", pathSegments, e);
      return Identification.unaddressablePolarisClaim();
    }
  }

  private static List<String> split(String name) {
    return name == null ? List.of() : NAME_SPLITTER.splitToList(name);
  }

  /**
   * A path segment can address a Polaris entity only if it is non-blank and free of the null byte.
   *
   * <p>Blank is rejected rather than passed to the resolver because no Polaris entity has a blank
   * name, so resolving it is a guaranteed miss; rejecting it up front also keeps {@code "a..b"}
   * from being read as the two-segment {@code "a.b"} by any later change to the splitting. The null
   * byte is rejected because Iceberg's {@code Namespace.of} throws on it.
   */
  private static boolean isAddressableSegment(String segment) {
    return !isBlank(segment) && segment.indexOf(NULL_BYTE) < 0;
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  /**
   * The outcome of classifying one dataset. {@link #identity()} is non-null exactly when {@link
   * #kind()} is {@link Kind#POLARIS}.
   */
  public record Identification(Kind kind, PolarisDatasetIdentity identity) {

    public enum Kind {
      /** Names an addressable Polaris table. Must resolve and be authorized, or be dropped. */
      POLARIS,

      /**
       * Claims a Polaris catalog but cannot be turned into a valid Polaris table path. Dropped:
       * downgrading it to external would write an unauthenticated near-miss of a real entity into
       * the lineage graph.
       */
      UNADDRESSABLE_POLARIS_CLAIM,

      /** Derives no Polaris identity. Recorded as-is, with no privilege check of its own. */
      EXTERNAL
    }

    private static final Identification EXTERNAL = new Identification(Kind.EXTERNAL, null);
    private static final Identification UNADDRESSABLE =
        new Identification(Kind.UNADDRESSABLE_POLARIS_CLAIM, null);

    static Identification external() {
      return EXTERNAL;
    }

    static Identification unaddressablePolarisClaim() {
      return UNADDRESSABLE;
    }

    static Identification polaris(PolarisDatasetIdentity identity) {
      return new Identification(Kind.POLARIS, identity);
    }
  }
}
