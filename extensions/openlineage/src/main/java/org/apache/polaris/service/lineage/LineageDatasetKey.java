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

import io.openlineage.server.OpenLineage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Identifies one dataset occurrence inside an OpenLineage event: the role the dataset plays in that
 * event, the {@code namespace}/{@code name} pair that names it, and the two dataset facets that say
 * which catalog table it is.
 *
 * <p>The role is part of the key because one table may legitimately appear as both an input and an
 * output of a single event — a {@code MERGE INTO} reads and writes the same table — and the two
 * occurrences require different privileges. Deduplicating across roles would silently keep only one
 * of the two requirements.
 *
 * <p>The {@code catalog} facet and the {@code symlinks} facet are carried because the dataset's own
 * {@code namespace}/{@code name} is not the table's identity. For an Iceberg REST catalog, the
 * OpenLineage Spark integration names the dataset by its storage location (for example {@code
 * s3://bucket} and {@code wh/ns/tbl}); the catalog it was loaded from is in the {@code catalog}
 * facet, and the table's name inside that catalog is a {@code TABLE} symlink.
 *
 * <p>All values are carried exactly as they arrived, including case. Polaris entity names are
 * case-sensitive, so folding case here would let a dataset authorize against one entity while
 * addressing another.
 *
 * <p>Every field may be {@code null} or empty: OpenLineage does not guarantee that any of them is
 * populated, and an event that omits them must be classified rather than rejected with an
 * exception.
 */
public record LineageDatasetKey(
    Role role, String namespace, String name, CatalogFacet catalog, List<Symlink> symlinks) {

  /** The part a dataset plays in the event that carried it. */
  public enum Role {
    /** A dataset the event's job read from. */
    INPUT,

    /** A dataset the event's job wrote to. */
    OUTPUT,

    /**
     * The single dataset carried by a {@code DatasetEvent}, which has no input/output role at all.
     */
    STANDALONE
  }

  /**
   * The {@code catalog} dataset facet: the catalog a table was loaded from. For an Iceberg REST
   * catalog the OpenLineage Spark integration sets {@code metadataUri} to the catalog's configured
   * {@code uri} and {@code warehouseUri} to its {@code warehouse} property, which for Polaris is
   * the catalog name.
   */
  public record CatalogFacet(
      String framework, String type, String metadataUri, String warehouseUri) {}

  /** One entry of the {@code symlinks} dataset facet: another name the same dataset goes by. */
  public record Symlink(String namespace, String name, String type) {}

  public LineageDatasetKey {
    symlinks = symlinks == null ? List.of() : List.copyOf(symlinks);
  }

  /**
   * A key with no facets, for a dataset that is known only by its {@code namespace}/{@code name}.
   */
  public LineageDatasetKey(Role role, String namespace, String name) {
    this(role, namespace, name, null, List.of());
  }

  /**
   * Builds a key for {@code dataset} in {@code role}. A {@code null} dataset yields a key with null
   * namespace and name and no facets, which the identity predicate classifies as external.
   */
  public static LineageDatasetKey of(Role role, OpenLineage.Dataset dataset) {
    if (dataset == null) {
      return new LineageDatasetKey(role, null, null);
    }
    Map<String, OpenLineage.DatasetFacet> facets =
        dataset.getFacets() == null ? null : dataset.getFacets().getAdditionalProperties();
    return new LineageDatasetKey(
        role, dataset.getNamespace(), dataset.getName(), catalogFacet(facets), symlinks(facets));
  }

  private static CatalogFacet catalogFacet(Map<String, OpenLineage.DatasetFacet> facets) {
    Map<String, Object> catalog = properties(facets, "catalog");
    if (catalog == null) {
      return null;
    }
    return new CatalogFacet(
        text(catalog.get("framework")),
        text(catalog.get("type")),
        text(catalog.get("metadataUri")),
        text(catalog.get("warehouseUri")));
  }

  private static List<Symlink> symlinks(Map<String, OpenLineage.DatasetFacet> facets) {
    Map<String, Object> symlinks = properties(facets, "symlinks");
    if (symlinks == null || !(symlinks.get("identifiers") instanceof List<?> identifiers)) {
      return List.of();
    }
    List<Symlink> result = new ArrayList<>();
    for (Object identifier : identifiers) {
      if (identifier instanceof Map<?, ?> fields) {
        result.add(
            new Symlink(
                text(fields.get("namespace")), text(fields.get("name")), text(fields.get("type"))));
      }
    }
    return result;
  }

  /** The free-form fields of the facet called {@code facetName}, or {@code null} if absent. */
  private static Map<String, Object> properties(
      Map<String, OpenLineage.DatasetFacet> facets, String facetName) {
    OpenLineage.DatasetFacet facet = facets == null ? null : facets.get(facetName);
    return facet == null ? null : facet.getAdditionalProperties();
  }

  private static String text(Object value) {
    return value instanceof String string ? string : null;
  }
}
