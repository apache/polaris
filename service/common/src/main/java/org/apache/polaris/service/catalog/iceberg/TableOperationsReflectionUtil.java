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

import java.lang.reflect.Field;
import java.util.ArrayList;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

/**
 * Used to handle reflection-related fields for TableOperations and TableMetadata TODO remove the
 * following if the Iceberg library allows us to do this without reflection For more details see <a
 * href="https://github.com/apache/polaris/pull/1378">#1378</a>
 */
public final class TableOperationsReflectionUtil {
  private static Logger LOGGER = LoggerFactory.getLogger(TableOperationsReflectionUtil.class);

  private static class Instance {
    private static TableOperationsReflectionUtil instance = new TableOperationsReflectionUtil();
  }

  static TableOperationsReflectionUtil getInstance() {
    return Instance.instance;
  }

  public void setMetadataFileLocation(TableMetadata tableMetadata, String metadataFileLocation) {
    try {
      tableMetadataField.set(tableMetadata, metadataFileLocation);
      unsafe.putObject(tableMetadata, changesFieldOffset, new ArrayList<MetadataUpdate>());
    } catch (IllegalAccessException e) {
      LOGGER.error(
          "Encountered an unexpected error while attempting to access private fields"
              + " in TableMetadata",
          e);
    }
  }

  public void setCurrentMetadata(
      BaseMetastoreTableOperations tableOperations,
      TableMetadata tableMetadata,
      String metadataFileLocation) {
    try {
      currentMetadataLocationField.set(tableOperations, metadataFileLocation);
      currentMetadataField.set(tableOperations, tableMetadata);
    } catch (IllegalAccessException e) {
      LOGGER.error(
          "Encountered an unexpected error while attempting to access private fields"
              + " in BaseMetastoreTableOperations",
          e);
    }
  }

  private final Unsafe unsafe;
  private final Field tableMetadataField;
  private final long changesFieldOffset;
  private final Field currentMetadataLocationField;
  private final Field currentMetadataField;

  private TableOperationsReflectionUtil() {
    try {
      tableMetadataField = TableMetadata.class.getDeclaredField("metadataFileLocation");
      tableMetadataField.setAccessible(true);

      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (Unsafe) unsafeField.get(null);
      Field changesField = TableMetadata.class.getDeclaredField("changes");
      changesField.setAccessible(true);
      changesFieldOffset = unsafe.objectFieldOffset(changesField);

      currentMetadataLocationField =
          BaseMetastoreTableOperations.class.getDeclaredField("currentMetadataLocation");
      currentMetadataLocationField.setAccessible(true);

      currentMetadataField = BaseMetastoreTableOperations.class.getDeclaredField("currentMetadata");
      currentMetadataField.setAccessible(true);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      LOGGER.error(
          "Encountered an unexpected error while attempting to access private fields in TableMetadata",
          e);
      throw new RuntimeException(e);
    }
  }
}
