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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.service.lineage.LineageDatasetKey.CatalogFacet;
import org.apache.polaris.service.lineage.LineageDatasetKey.Symlink;

/**
 * Reads the Polaris table an OpenLineage dataset names out of the event, which is the first step of
 * lineage ingest authorization.
 *
 * <p>The catalog and the table are taken from what the producer says, never inferred from the shape
 * of the dataset's own {@code namespace} or {@code name}: those are routinely a storage location or
 * a dotted name such as a Kafka topic or a BigQuery table, and carry no catalog. For an Iceberg
 * REST catalog, the OpenLineage Spark integration (1.53.0) emits two facets that do:
 *
 * <ul>
 *   <li>a {@code catalog} facet with {@code framework} {@code iceberg}, {@code type} {@code rest},
 *       {@code metadataUri} the catalog's configured {@code uri} and {@code warehouseUri} its
 *       configured {@code warehouse}, which for Polaris is the catalog name;
 *   <li>a {@code TABLE} symlink whose namespace is that same {@code uri} and whose name is Spark's
 *       {@code Identifier.toString()}: the namespace levels and the table joined by dots, each part
 *       backtick-quoted when it needs it, for example {@code ns.tbl} or {@code `my-ns`.tbl}.
 * </ul>
 *
 * <p>A dataset is a {@link Kind#POLARIS_CANDIDATE candidate} when the event gives both. If the
 * event does not give a catalog, the dataset is {@link Kind#EXTERNAL}. If it gives a catalog but no
 * single usable table path, it is an {@link Kind#UNADDRESSABLE_POLARIS_CLAIM unaddressable claim}:
 * it claims a catalog but cannot be turned into a table, so it must not be downgraded to external,
 * which would write an unauthenticated near-copy of a real entity into the lineage graph next to
 * the real one.
 *
 * <p>This class does not decide whether the catalog is this server's. The host in {@code
 * metadataUri} is deliberately not part of the identity: Polaris cannot tell from an event which
 * server a URI belongs to, so the authorization pass resolves the named catalog in the realm and
 * treats a dataset as Polaris's only if it exists there. A same-named catalog, namespace and table
 * on another server is therefore treated as this server's, which also covers mirrored and federated
 * tables, and the caller still has to hold the privilege for it.
 *
 * <p>The symlinks and the facet can only add a check or a drop, never remove one. No method here
 * throws: a malformed facet or name still has to produce a classification, since the alternative is
 * a 500 on an event sent in good faith.
 */
public final class PolarisDatasetIdentifier {

  private static final String ICEBERG_FRAMEWORK = "iceberg";
  private static final String REST_CATALOG_TYPE = "rest";
  private static final String TABLE_SYMLINK_TYPE = "TABLE";

  /** A table path needs at least one namespace level plus the table name. */
  private static final int MIN_TABLE_PATH_SEGMENTS = 2;

  private static final char NULL_BYTE = '\0';
  private static final char QUOTE = '`';
  private static final char SEPARATOR = '.';

  private PolarisDatasetIdentifier() {}

  /** Classifies the dataset that {@code key} names. Never throws. */
  public static Identification identify(LineageDatasetKey key) {
    CatalogFacet facet = key.catalog();
    if (facet == null
        || !isIcebergRestCatalog(facet)
        || !isAddressableSegment(facet.warehouseUri())) {
      return Identification.external();
    }
    String catalog = facet.warehouseUri();

    Set<TableIdentifier> tables = new LinkedHashSet<>();
    boolean unusable = false;
    for (Symlink symlink : key.symlinks()) {
      // Only a table symlink on this catalog's own endpoint names the table in this catalog. Any
      // other symlink, such as a Hive name, is another catalog's name for the same data.
      if (!TABLE_SYMLINK_TYPE.equalsIgnoreCase(symlink.type())
          || facet.metadataUri() == null
          || !facet.metadataUri().equals(symlink.namespace())) {
        continue;
      }
      TableIdentifier table = parseTable(symlink.name());
      if (table == null) {
        unusable = true;
      } else {
        tables.add(table);
      }
    }
    return unusable || tables.size() != 1
        ? Identification.unaddressablePolarisClaim(catalog)
        : Identification.polarisCandidate(catalog, tables.iterator().next());
  }

  private static boolean isIcebergRestCatalog(CatalogFacet facet) {
    return ICEBERG_FRAMEWORK.equalsIgnoreCase(facet.framework())
        && REST_CATALOG_TYPE.equalsIgnoreCase(facet.type());
  }

  /** The table named by Spark's quoted {@code ns.tbl} form, or {@code null} if it is unusable. */
  private static TableIdentifier parseTable(String qualifiedName) {
    List<String> parts = splitQuoted(qualifiedName);
    if (parts == null || parts.size() < MIN_TABLE_PATH_SEGMENTS) {
      return null;
    }
    for (String part : parts) {
      if (!isAddressableSegment(part)) {
        return null;
      }
    }
    int tableIndex = parts.size() - 1;
    try {
      return TableIdentifier.of(
          Namespace.of(parts.subList(0, tableIndex).toArray(String[]::new)), parts.get(tableIndex));
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * Splits {@code name} on the dots that are not inside a backtick-quoted part, removing the quotes
   * and un-doubling any backtick inside one. Returns {@code null} for a name that is not in that
   * form: an unterminated quote, text after a closing quote, or a quote inside an unquoted part.
   * Empty parts are kept so the caller can reject them.
   */
  private static List<String> splitQuoted(String name) {
    if (name == null) {
      return null;
    }
    List<String> parts = new ArrayList<>();
    StringBuilder part = new StringBuilder();
    boolean quoted = false;
    boolean closed = false;
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (quoted) {
        if (c != QUOTE) {
          part.append(c);
        } else if (i + 1 < name.length() && name.charAt(i + 1) == QUOTE) {
          part.append(QUOTE);
          i++;
        } else {
          quoted = false;
          closed = true;
        }
      } else if (c == SEPARATOR) {
        parts.add(part.toString());
        part.setLength(0);
        closed = false;
      } else if (closed || (c == QUOTE && part.length() > 0)) {
        return null;
      } else if (c == QUOTE) {
        quoted = true;
      } else {
        part.append(c);
      }
    }
    if (quoted) {
      return null;
    }
    parts.add(part.toString());
    return parts;
  }

  /**
   * A path segment can address a Polaris entity only if it is non-blank and free of the null byte.
   *
   * <p>Blank is rejected rather than passed to the resolver because no Polaris entity has a blank
   * name, so resolving it is a guaranteed miss. The null byte is rejected because Iceberg's {@code
   * Namespace.of} throws on it.
   */
  private static boolean isAddressableSegment(String segment) {
    return segment != null && !segment.isBlank() && segment.indexOf(NULL_BYTE) < 0;
  }

  /**
   * The outcome of classifying one dataset. {@link #catalog()} is non-null unless {@link #kind()}
   * is {@link Kind#EXTERNAL}, and {@link #table()} is non-null exactly when it is {@link
   * Kind#POLARIS_CANDIDATE}.
   */
  public record Identification(Kind kind, String catalog, TableIdentifier table) {

    public enum Kind {
      /**
       * Names a catalog and an addressable table. Whether the catalog is this server's is decided
       * by resolving it; if it is, the table must resolve and be authorized, or be dropped.
       */
      POLARIS_CANDIDATE,

      /**
       * Claims a catalog but cannot be turned into a valid table path. If the catalog is this
       * server's, it is dropped: downgrading it to external would write an unauthenticated
       * near-miss of a real entity into the lineage graph.
       */
      UNADDRESSABLE_POLARIS_CLAIM,

      /** Names no catalog. Recorded as-is, with no privilege check of its own. */
      EXTERNAL
    }

    private static final Identification EXTERNAL = new Identification(Kind.EXTERNAL, null, null);

    static Identification external() {
      return EXTERNAL;
    }

    static Identification unaddressablePolarisClaim(String catalog) {
      return new Identification(Kind.UNADDRESSABLE_POLARIS_CLAIM, catalog, null);
    }

    static Identification polarisCandidate(String catalog, TableIdentifier table) {
      return new Identification(Kind.POLARIS_CANDIDATE, catalog, table);
    }
  }
}
