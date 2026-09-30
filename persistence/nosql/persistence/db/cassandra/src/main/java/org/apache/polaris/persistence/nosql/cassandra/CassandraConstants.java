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

import static org.apache.polaris.persistence.nosql.cassandra.CqlColumnType.BIGINT;
import static org.apache.polaris.persistence.nosql.cassandra.CqlColumnType.INT;
import static org.apache.polaris.persistence.nosql.cassandra.CqlColumnType.NAME;
import static org.apache.polaris.persistence.nosql.cassandra.CqlColumnType.VARBINARY;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_CREATED_AT;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_ID;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_REAL_PART_NUM;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_TYPE;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_VALUE;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_VERSION;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REALM;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_CREATED_AT;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_NAME;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_POINTER;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_PREVIOUS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_OBJS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_REFS;

import java.util.Set;

final class CassandraConstants {
  private CassandraConstants() {}

  static final int SELECT_BATCH_SIZE = 20;
  static final int MAX_CONCURRENT_BATCH_READS = 20;
  static final int MAX_CONCURRENT_DELETES = 20;
  static final int MAX_CONCURRENT_STORES = 20;

  static final CqlColumn CQL_COL_REALM = new CqlColumn(COL_REALM, NAME);
  static final CqlColumn CQL_COL_REF_NAME = new CqlColumn(COL_REF_NAME, NAME);
  static final CqlColumn CQL_COL_REF_POINTER = new CqlColumn(COL_REF_POINTER, VARBINARY);
  static final CqlColumn CQL_COL_REF_CREATED_AT = new CqlColumn(COL_REF_CREATED_AT, BIGINT);
  static final CqlColumn CQL_COL_REF_PREVIOUS = new CqlColumn(COL_REF_PREVIOUS, VARBINARY);
  static final Set<CqlColumn> EXPECTED_REF_COLUMNS =
      Set.of(
          CQL_COL_REALM,
          CQL_COL_REF_NAME,
          CQL_COL_REF_POINTER,
          CQL_COL_REF_CREATED_AT,
          CQL_COL_REF_PREVIOUS);
  static final CqlColumn CQL_COL_OBJ_KEY = new CqlColumn(COL_OBJ_ID, VARBINARY);
  static final CqlColumn CQL_COL_OBJ_TYPE = new CqlColumn(COL_OBJ_TYPE, NAME);
  static final CqlColumn CQL_COL_OBJ_VALUE = new CqlColumn(COL_OBJ_VALUE, VARBINARY);
  static final CqlColumn CQL_COL_OBJ_VERSION = new CqlColumn(COL_OBJ_VERSION, NAME);
  static final CqlColumn CQL_COL_OBJ_CREATED_AT = new CqlColumn(COL_OBJ_CREATED_AT, BIGINT);
  static final CqlColumn CQL_COL_OBJ_REAL_PART_NUM = new CqlColumn(COL_OBJ_REAL_PART_NUM, INT);
  static final Set<CqlColumn> EXPECTED_OBJ_COLUMNS =
      Set.of(
          CQL_COL_REALM,
          CQL_COL_OBJ_KEY,
          CQL_COL_OBJ_TYPE,
          CQL_COL_OBJ_VERSION,
          CQL_COL_OBJ_VALUE,
          CQL_COL_OBJ_CREATED_AT,
          CQL_COL_OBJ_REAL_PART_NUM);

  static final String EXPECTED_SUFFIX = "_expected";

  // Keep the static DDL in site/content/in-dev/unreleased/metastores/nosql-cassandra.md in sync
  // with changes to either table schema.
  static final String CREATE_TABLE_OBJS =
      "CREATE TABLE IF NOT EXISTS %s."
          + TABLE_OBJS
          + "\n  (\n    "
          + CQL_COL_REALM
          + " "
          + CQL_COL_REALM.type().cqlName()
          + ",\n    "
          + CQL_COL_OBJ_KEY
          + " "
          + CQL_COL_OBJ_KEY.type().cqlName()
          + ",\n    "
          + CQL_COL_OBJ_TYPE
          + " "
          + CQL_COL_OBJ_TYPE.type().cqlName()
          + ",\n    "
          + CQL_COL_OBJ_VERSION
          + " "
          + CQL_COL_OBJ_VERSION.type().cqlName()
          + ",\n    "
          + CQL_COL_OBJ_VALUE
          + " "
          + CQL_COL_OBJ_VALUE.type().cqlName()
          + ",\n    "
          + CQL_COL_OBJ_CREATED_AT
          + " "
          + CQL_COL_OBJ_CREATED_AT.type().cqlName()
          + ",\n    "
          + CQL_COL_OBJ_REAL_PART_NUM
          + " "
          + CQL_COL_OBJ_REAL_PART_NUM.type().cqlName()
          + ",\n    PRIMARY KEY (("
          + CQL_COL_REALM
          + ", "
          + CQL_COL_OBJ_KEY
          + "))\n  ) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T4'}";

  static final String CREATE_TABLE_REFS =
      "CREATE TABLE IF NOT EXISTS %s."
          + TABLE_REFS
          + "\n  (\n    "
          + CQL_COL_REALM
          + " "
          + CQL_COL_REALM.type().cqlName()
          + ",\n    "
          + CQL_COL_REF_NAME
          + " "
          + CQL_COL_REF_NAME.type().cqlName()
          + ",\n    "
          + CQL_COL_REF_POINTER
          + " "
          + CQL_COL_REF_POINTER.type().cqlName()
          + ",\n    "
          + CQL_COL_REF_CREATED_AT
          + " "
          + CQL_COL_REF_CREATED_AT.type().cqlName()
          + ",\n    "
          + CQL_COL_REF_PREVIOUS
          + " "
          + CQL_COL_REF_PREVIOUS.type().cqlName()
          + ",\n    PRIMARY KEY (("
          + CQL_COL_REALM
          + ", "
          + CQL_COL_REF_NAME
          + "))\n  ) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T4'}";

  static final String ADD_REFERENCE =
      "INSERT INTO %s."
          + TABLE_REFS
          + " ("
          + CQL_COL_REALM
          + ", "
          + CQL_COL_REF_NAME
          + ", "
          + CQL_COL_REF_POINTER
          + ", "
          + CQL_COL_REF_CREATED_AT
          + ", "
          + CQL_COL_REF_PREVIOUS
          + ") VALUES (?, ?, ?, ?, ?) IF NOT EXISTS";
  static final String UPDATE_REFERENCE_POINTER =
      "UPDATE %s."
          + TABLE_REFS
          + " SET "
          + CQL_COL_REF_POINTER
          + "=?, "
          + CQL_COL_REF_PREVIOUS
          + "=? WHERE "
          + CQL_COL_REALM
          + "=? AND "
          + CQL_COL_REF_NAME
          + "=? IF "
          + CQL_COL_REF_POINTER
          + "=? AND "
          + CQL_COL_REF_CREATED_AT
          + "=?";
  static final String FIND_REFERENCES =
      "SELECT "
          + CQL_COL_REF_NAME
          + ", "
          + CQL_COL_REF_POINTER
          + ", "
          + CQL_COL_REF_CREATED_AT
          + ", "
          + CQL_COL_REF_PREVIOUS
          + " FROM %s."
          + TABLE_REFS
          + " WHERE "
          + CQL_COL_REALM
          + "=? AND "
          + CQL_COL_REF_NAME
          + " IN ?";
  static final String DELETE_REF =
      "DELETE FROM %s."
          + TABLE_REFS
          + " WHERE "
          + CQL_COL_REALM
          + "=? AND "
          + CQL_COL_REF_NAME
          + "=?";
  static final String FIND_OBJS =
      "SELECT "
          + CQL_COL_OBJ_KEY
          + ", "
          + COL_OBJ_TYPE
          + ", "
          + COL_OBJ_VERSION
          + ", "
          + COL_OBJ_VALUE
          + ", "
          + COL_OBJ_CREATED_AT
          + ", "
          + COL_OBJ_REAL_PART_NUM
          + " FROM %s."
          + TABLE_OBJS
          + " WHERE "
          + CQL_COL_REALM
          + "=? AND "
          + CQL_COL_OBJ_KEY
          + " IN ?";

  static final String WRITE_OBJ =
      "INSERT INTO %s."
          + TABLE_OBJS
          + " ("
          + CQL_COL_REALM
          + ", "
          + CQL_COL_OBJ_KEY
          + ", "
          + CQL_COL_OBJ_TYPE
          + ", "
          + CQL_COL_OBJ_VERSION
          + ", "
          + CQL_COL_OBJ_VALUE
          + ", "
          + CQL_COL_OBJ_CREATED_AT
          + ", "
          + CQL_COL_OBJ_REAL_PART_NUM
          + ") VALUES (:"
          + CQL_COL_REALM
          + ", :"
          + CQL_COL_OBJ_KEY
          + ", :"
          + CQL_COL_OBJ_TYPE
          + ", :"
          + CQL_COL_OBJ_VERSION
          + ", :"
          + CQL_COL_OBJ_VALUE
          + ", :"
          + CQL_COL_OBJ_CREATED_AT
          + ", :"
          + CQL_COL_OBJ_REAL_PART_NUM
          + ")";
  static final String DELETE_OBJ =
      "DELETE FROM %s."
          + TABLE_OBJS
          + " WHERE "
          + CQL_COL_REALM
          + "=? AND "
          + CQL_COL_OBJ_KEY
          + "=?";

  static final String STORE_OBJ = WRITE_OBJ + " IF NOT EXISTS";
  static final String UPDATE_OBJ =
      "UPDATE %s."
          + TABLE_OBJS
          + " SET "
          + CQL_COL_OBJ_TYPE
          + "=:"
          + CQL_COL_OBJ_TYPE
          + ", "
          + CQL_COL_OBJ_VERSION
          + "=:"
          + CQL_COL_OBJ_VERSION
          + ", "
          + CQL_COL_OBJ_VALUE
          + "=:"
          + CQL_COL_OBJ_VALUE
          + ", "
          + CQL_COL_OBJ_CREATED_AT
          + "=:"
          + CQL_COL_OBJ_CREATED_AT
          + " WHERE "
          + CQL_COL_REALM
          + "=:"
          + CQL_COL_REALM
          + " AND "
          + CQL_COL_OBJ_KEY
          + "=:"
          + CQL_COL_OBJ_KEY
          + " IF "
          + CQL_COL_OBJ_VERSION
          + "=:"
          + CQL_COL_OBJ_VERSION
          + EXPECTED_SUFFIX;
  static final String DELETE_OBJ_CONDITIONAL =
      "DELETE FROM %s."
          + TABLE_OBJS
          + " WHERE "
          + CQL_COL_REALM
          + "=:"
          + CQL_COL_REALM
          + " AND "
          + CQL_COL_OBJ_KEY
          + "=:"
          + CQL_COL_OBJ_KEY
          + " IF "
          + CQL_COL_OBJ_VERSION
          + "=:"
          + CQL_COL_OBJ_VERSION;

  static final String SCAN_REFS =
      "SELECT "
          + CQL_COL_REALM
          + ", "
          + CQL_COL_REF_NAME
          + ", "
          + CQL_COL_REF_CREATED_AT
          + " FROM %s."
          + TABLE_REFS;
  static final String SCAN_OBJS =
      "SELECT "
          + CQL_COL_REALM
          + ", "
          + CQL_COL_OBJ_KEY
          + ", "
          + CQL_COL_OBJ_TYPE
          + ", "
          + CQL_COL_OBJ_CREATED_AT
          + " FROM %s."
          + TABLE_OBJS;
}
