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

import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRefType;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Atomic promotion of a branch across several tables with nothing but the Iceberg REST multi-table
 * commit: create the branch on every table in one commit, write to it, then move every table's main
 * to its branch head in one commit that asserts main hasn't moved.
 */
public class BranchPromotionRecipeTest {
  private static final String CATALOG = "lake";
  private static final String NAMESPACE = "sales";
  private static final List<String> TABLES = List.of("orders", "items", "customers");
  private static final String BRANCH = "backfill";

  private String catalogLocation;
  private TestServices services;
  private long nextSnapshotId = 1000;

  @BeforeEach
  void setUp(@TempDir Path tempDir) {
    catalogLocation = tempDir.toAbsolutePath().toUri().toString().replaceAll("/$", "");
    services =
        TestServices.builder()
            .config(
                Map.of(
                    "ALLOW_INSECURE_STORAGE_TYPES",
                    "true",
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("FILE")))
            .build();
    createCatalogNamespaceAndTables();
  }

  @Test
  void promotesEveryTableAtOnce() {
    List<Long> bases = commitToAll("main", null);
    createBranchOnAll(bases);
    List<Long> heads = commitToAll(BRANCH, bases);

    promote(bases, heads);

    for (int i = 0; i < TABLES.size(); i++) {
      TableMetadata metadata = load(TABLES.get(i));
      assertThat(metadata.ref("main").snapshotId()).isEqualTo(heads.get(i));
    }
  }

  @Test
  void promotesNothingWhenOneTableMovedOnMain() {
    List<Long> bases = commitToAll("main", null);
    createBranchOnAll(bases);
    List<Long> heads = commitToAll(BRANCH, bases);

    // Someone else writes to main of one table after the branch was created.
    String moved = TABLES.get(1);
    long concurrent = nextSnapshotId++;
    commit(
        List.of(
            UpdateTableRequest.create(
                TableIdentifier.of(NAMESPACE, moved),
                List.of(),
                List.of(
                    new MetadataUpdate.AddSnapshot(snapshot(moved, concurrent, bases.get(1))),
                    setRef("main", concurrent)))));

    assertThatThrownBy(() -> promote(bases, heads)).isNotNull();

    assertThat(load(TABLES.get(0)).ref("main").snapshotId()).isEqualTo(bases.get(0));
    assertThat(load(moved).ref("main").snapshotId()).isEqualTo(concurrent);
    assertThat(load(TABLES.get(2)).ref("main").snapshotId()).isEqualTo(bases.get(2));
  }

  private void promote(List<Long> bases, List<Long> heads) {
    List<UpdateTableRequest> changes = new ArrayList<>();
    for (int i = 0; i < TABLES.size(); i++) {
      changes.add(
          UpdateTableRequest.create(
              TableIdentifier.of(NAMESPACE, TABLES.get(i)),
              List.of(new UpdateRequirement.AssertRefSnapshotID("main", bases.get(i))),
              List.of(setRef("main", heads.get(i)))));
    }
    commit(changes);
  }

  private void createBranchOnAll(List<Long> bases) {
    List<UpdateTableRequest> changes = new ArrayList<>();
    for (int i = 0; i < TABLES.size(); i++) {
      changes.add(
          UpdateTableRequest.create(
              TableIdentifier.of(NAMESPACE, TABLES.get(i)),
              List.of(),
              List.of(setRef(BRANCH, bases.get(i)))));
    }
    commit(changes);
  }

  /** Adds one snapshot to each table and points {@code ref} at it. Returns the snapshot ids. */
  private List<Long> commitToAll(String ref, List<Long> parents) {
    List<Long> ids = new ArrayList<>();
    List<UpdateTableRequest> changes = new ArrayList<>();
    for (int i = 0; i < TABLES.size(); i++) {
      long id = nextSnapshotId++;
      ids.add(id);
      changes.add(
          UpdateTableRequest.create(
              TableIdentifier.of(NAMESPACE, TABLES.get(i)),
              List.of(),
              List.of(
                  new MetadataUpdate.AddSnapshot(
                      snapshot(TABLES.get(i), id, parents == null ? null : parents.get(i))),
                  setRef(ref, id))));
    }
    commit(changes);
    return ids;
  }

  private static MetadataUpdate setRef(String name, long snapshotId) {
    return new MetadataUpdate.SetSnapshotRef(
        name, snapshotId, SnapshotRefType.BRANCH, null, null, null);
  }

  private Snapshot snapshot(String table, long id, Long parent) {
    String json =
        "{\"snapshot-id\":"
            + id
            + (parent == null ? "" : ",\"parent-snapshot-id\":" + parent)
            + ",\"sequence-number\":"
            + (id - 999)
            + ",\"timestamp-ms\":"
            + (System.currentTimeMillis() + id)
            + ",\"summary\":{\"operation\":\"append\"},\"manifest-list\":\""
            + catalogLocation
            + "/"
            + CATALOG
            + "/"
            + NAMESPACE
            + "/"
            + table
            + "/metadata/snap-"
            + id
            + ".avro\",\"schema-id\":0}";
    return SnapshotParser.fromJson(json);
  }

  private void commit(List<UpdateTableRequest> changes) {
    services
        .restApi()
        .commitTransaction(
            CATALOG,
            new CommitTransactionRequest(changes),
            UUID.randomUUID(),
            services.realmContext(),
            services.securityContext());
  }

  private TableMetadata load(String table) {
    try (Response response =
        services
            .restApi()
            .loadTable(
                CATALOG,
                NAMESPACE,
                table,
                null,
                null,
                null,
                null,
                services.realmContext(),
                services.securityContext())) {
      return ((LoadTableResponse) response.getEntity()).tableMetadata();
    }
  }

  private void createCatalogNamespaceAndTables() {
    StorageConfigInfo storage =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    Catalog catalog =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            CATALOG,
            CatalogProperties.builder()
                .setDefaultBaseLocation(catalogLocation + "/" + CATALOG)
                .build(),
            0L,
            0L,
            1,
            storage);
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
    services
        .restApi()
        .createNamespace(
            CATALOG,
            CreateNamespaceRequest.builder().withNamespace(Namespace.of(NAMESPACE)).build(),
            UUID.randomUUID(),
            services.realmContext(),
            services.securityContext())
        .close();
    for (String table : TABLES) {
      services
          .restApi()
          .createTable(
              CATALOG,
              NAMESPACE,
              CreateTableRequest.builder()
                  .withName(table)
                  .withLocation(catalogLocation + "/" + CATALOG + "/" + NAMESPACE + "/" + table)
                  .withSchema(SCHEMA)
                  .build(),
              null,
              UUID.randomUUID(),
              services.realmContext(),
              services.securityContext())
          .close();
    }
  }
}
