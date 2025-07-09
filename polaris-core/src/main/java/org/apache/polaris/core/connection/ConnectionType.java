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
package org.apache.polaris.core.connection;

import jakarta.annotation.Nonnull;
import java.util.Arrays;

/**
 * The internal persistence-object counterpart to ConnectionConfigInfo.ConnectionTypeEnum defined in
 * the API model. We define integer type codes in this enum for better compatibility within
 * persisted data in case the names of enum types are ever changed in place.
 *
 * <p>Important: Codes must be kept in-sync wi th JsonSubTypes annotated within {@link
 * ConnectionConfigInfoDpo}.
 */
public enum ConnectionType {
  NULL_TYPE(0),
  ICEBERG_REST(1),
  HADOOP(2),
  ;

  private static final ConnectionType[] REVERSE_MAPPING_ARRAY;

  static {
    // find max array size
    int maxCode =
        Arrays.stream(ConnectionType.values()).mapToInt(ConnectionType::getCode).max().orElse(0);

    // allocate mapping array
    REVERSE_MAPPING_ARRAY = new ConnectionType[maxCode + 1];

    // populate mapping array
    for (ConnectionType connectionType : ConnectionType.values()) {
      REVERSE_MAPPING_ARRAY[connectionType.code] = connectionType;
    }
  }

  private final int code;

  ConnectionType(int code) {
    this.code = code;
  }

  /**
   * Given the code associated to the type, return the associated ConnectionType. Return null if not
   * found
   *
   * @param connectionTypeCode code associated to the connection type
   * @return ConnectionType corresponding to that code or null if mapping not found
   */
  public static @Nonnull ConnectionType fromCode(int connectionTypeCode) {
    // ensure it is within bounds
    if (connectionTypeCode < 0 || connectionTypeCode >= REVERSE_MAPPING_ARRAY.length) {
      return ConnectionType.NULL_TYPE;
    }

    // get value
    return REVERSE_MAPPING_ARRAY[connectionTypeCode];
  }

  public int getCode() {
    return this.code;
  }
}
