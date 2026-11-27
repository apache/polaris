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
package org.apache.polaris.persistence.nosql.cassandra;

import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_OBJ_KEY;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_REALM;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_REF_CREATED_AT;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_REF_NAME;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.CQL_COL_REF_PREVIOUS;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.EXPECTED_OBJ_COLUMNS;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.EXPECTED_REF_COLUMNS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_OBJS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_REFS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class CassandraSchemaTest {
  private final CqlSession session = mock(CqlSession.class);
  private final Metadata metadata = mock(Metadata.class);
  private final KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
  private final Map<CqlIdentifier, ColumnMetadata> refColumns = columns(EXPECTED_REF_COLUMNS);
  private final TableMetadata refs = table(refColumns, List.of(CQL_COL_REALM, CQL_COL_REF_NAME));
  private final CassandraBackend backend =
      new CassandraBackend(
          new CassandraBackendConfig(
              session,
              "polaris",
              Duration.ofSeconds(5),
              Duration.ofSeconds(3),
              DefaultConsistencyLevel.LOCAL_QUORUM,
              DefaultConsistencyLevel.LOCAL_SERIAL,
              false));

  @BeforeEach
  void existingSchema() {
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace("polaris")).thenReturn(Optional.of(keyspace));
    when(keyspace.getName()).thenReturn(CqlIdentifier.fromInternal("polaris"));
    when(keyspace.getTable(TABLE_REFS)).thenReturn(Optional.of(refs));
    var objs = table(columns(EXPECTED_OBJ_COLUMNS), List.of(CQL_COL_REALM, CQL_COL_OBJ_KEY));
    when(keyspace.getTable(TABLE_OBJS)).thenReturn(Optional.of(objs));
  }

  @Test
  void compatibleSchemaDoesNotIssueDdl() {
    assertThat(backend.setupSchema())
        .hasValueSatisfying(info -> assertThat(info).contains("polaris"));

    verify(session, never()).execute(any(SimpleStatement.class));
  }

  @Test
  void missingKeyspaceIsRejected() {
    when(metadata.getKeyspace("polaris")).thenReturn(Optional.empty());

    assertThatIllegalStateException()
        .isThrownBy(backend::setupSchema)
        .withMessageContaining("Keyspace 'polaris' must exist");
    verify(session, never()).execute(any(SimpleStatement.class));
  }

  @Test
  void missingTablesAreCreated() {
    when(keyspace.getTable(anyString())).thenReturn(Optional.empty());

    backend.setupSchema();

    verify(session, times(2)).execute(any(SimpleStatement.class));
  }

  @ParameterizedTest
  @CsvSource({
    "missing-column,incompatible mandatory columns",
    "wrong-type,incompatible mandatory columns",
    "wrong-key,primary key columns",
    "reordered-key,primary key columns",
    "clustering,primary key columns"
  })
  void incompatibleSchemaIsRejected(String problem, String message) {
    var realm = refColumns.get(CqlIdentifier.fromInternal(CQL_COL_REALM.name()));
    var name = refColumns.get(CqlIdentifier.fromInternal(CQL_COL_REF_NAME.name()));
    var previous = CqlIdentifier.fromInternal(CQL_COL_REF_PREVIOUS.name());
    switch (problem) {
      case "missing-column" -> refColumns.remove(previous);
      case "wrong-type" -> when(refColumns.get(previous).getType()).thenReturn(DataTypes.INT);
      case "wrong-key" -> when(refs.getPartitionKey()).thenReturn(List.of(name));
      case "reordered-key" -> when(refs.getPartitionKey()).thenReturn(List.of(name, realm));
      case "clustering" ->
          when(refs.getClusteringColumns())
              .thenReturn(
                  Map.of(
                      refColumns.get(CqlIdentifier.fromInternal(CQL_COL_REF_CREATED_AT.name())),
                      ClusteringOrder.ASC));
      default -> throw new IllegalArgumentException(problem);
    }

    assertThatIllegalStateException()
        .isThrownBy(backend::setupSchema)
        .withMessageContaining(message);
    verify(session, never()).execute(any(SimpleStatement.class));
  }

  private static Map<CqlIdentifier, ColumnMetadata> columns(Set<CqlColumn> definitions) {
    var columns = new HashMap<CqlIdentifier, ColumnMetadata>();
    for (var definition : definitions) {
      var name = CqlIdentifier.fromInternal(definition.name());
      var column = mock(ColumnMetadata.class);
      when(column.getName()).thenReturn(name);
      when(column.getType()).thenReturn(definition.type().dataType());
      columns.put(name, column);
    }
    return columns;
  }

  private static TableMetadata table(
      Map<CqlIdentifier, ColumnMetadata> columns, List<CqlColumn> primaryKey) {
    var table = mock(TableMetadata.class);
    when(table.getColumns()).thenReturn(columns);
    when(table.getColumn(anyString()))
        .thenAnswer(invocation ->
            Optional.ofNullable(
                columns.get(CqlIdentifier.fromInternal(invocation.getArgument(0)))));
    when(table.getPartitionKey())
        .thenReturn(
            primaryKey.stream()
                .map(column -> columns.get(CqlIdentifier.fromInternal(column.name())))
                .toList());
    when(table.getClusteringColumns()).thenReturn(Map.of());
    return table;
  }
}
