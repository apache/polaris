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
package org.apache.polaris.persistence.relational.jdbc.models;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface Converter<T> {
  /**
   * Converts a ResultSet to model.
   *
   * @param rs : ResultSet from JDBC.
   * @return the corresponding business entity
   * @throws SQLException : Exception while fetching from ResultSet.
   */
  T fromResultSet(ResultSet rs) throws SQLException;

  /**
   * Convert a model into a Map with keys as snake case names, where as values as values of member
   * of model obj.
   */
  Map<String, Object> toMap();
}
