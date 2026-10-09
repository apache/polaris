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
package org.apache.polaris.service.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import java.sql.SQLException;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.persistence.AmbiguousWriteException;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.it.test.PolarisRestCatalogFileIntegrationTest;
import org.apache.polaris.service.task.TaskErrorHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(Profiles.RestCatalogFileIntegrationProfile.class)
public class RestCatalogFileIntegrationTest extends PolarisRestCatalogFileIntegrationTest {
  @Inject MetaStoreManagerFactory metaStoreManagerFactory;

  @Inject
  @Identifier("task-error-handler")
  TaskErrorHandler taskErrorHandler;

  @AfterEach
  void checkTaskExceptions() {
    taskErrorHandler.assertNoTaskExceptions();
  }

  @Test
  void appendRecoversWhenDatabaseWriteResultIsLost() {
    Namespace namespace = Namespace.of("ambiguous_commit");
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, "tbl");
    catalog().createNamespace(namespace);
    Table table = catalog().buildTable(tableIdentifier, SCHEMA).create();

    PolarisMetaStoreManager metaStoreManager =
        spy(metaStoreManagerFactory.getOrCreateMetaStoreManager(() -> "POLARIS"));
    MetaStoreManagerFactory factorySpy = spy(metaStoreManagerFactory);
    doReturn(metaStoreManager).when(factorySpy).getOrCreateMetaStoreManager(any());
    QuarkusMock.installMockForType(factorySpy, MetaStoreManagerFactory.class);

    doAnswer(
            invocation -> {
              invocation.callRealMethod();
              throw new AmbiguousWriteException(
                  new SQLException("I/O error occurred while sending to the backend"));
            })
        .when(metaStoreManager)
        .updateEntityPropertiesIfNotChangedWithAmbiguousWriteDetection(any(), any(), any());

    table.newAppend().appendFile(FILE_A).commit();

    verify(metaStoreManager)
        .updateEntityPropertiesIfNotChangedWithAmbiguousWriteDetection(any(), any(), any());

    Table committedTable = catalog().loadTable(tableIdentifier);
    var snapshot = committedTable.currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(committedTable.io().newInputFile(snapshot.manifestListLocation()).exists()).isTrue();
    assertThat(snapshot.allManifests(committedTable.io()))
        .allSatisfy(
            manifest ->
                assertThat(committedTable.io().newInputFile(manifest.path().toString()).exists())
                    .isTrue());
  }
}
