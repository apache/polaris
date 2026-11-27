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

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

enum CqlColumnType {
  NAME(DataTypes.TEXT),
  BOOL(DataTypes.BOOLEAN),
  VARBINARY(DataTypes.BLOB),
  BIGINT(DataTypes.BIGINT),
  INT(DataTypes.INT),
  ;

  private final DataType dataType;

  CqlColumnType(DataType dataType) {
    this.dataType = dataType;
  }

  String cqlName() {
    return dataType.asCql(false, false);
  }

  DataType dataType() {
    return dataType;
  }
}
