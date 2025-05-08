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
package org.apache.polaris.service.catalog.validation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;

enum StorageTypeFileIO {
  S3("org.apache.iceberg.aws.s3.S3FileIO", true),

  GCS("org.apache.iceberg.gcp.gcs.GCSFileIO", true),

  AZURE("org.apache.iceberg.azure.adlsv2.ADLSFileIO", true),

  FILE("org.apache.iceberg.hadoop.HadoopFileIO", false),

  // Iceberg tests
  IN_MEMORY("org.apache.iceberg.inmemory.InMemoryFileIO", false, false),
  ;

  private final String fileIoImplementation;
  private final boolean safe;
  private final boolean validateAllowedStorageType;

  StorageTypeFileIO(String fileIoImplementation, boolean safe) {
    this(fileIoImplementation, safe, true);
  }

  StorageTypeFileIO(String fileIoImplementation, boolean safe, boolean validateAllowedStorageType) {
    this.fileIoImplementation = fileIoImplementation;
    this.safe = safe;
    this.validateAllowedStorageType = validateAllowedStorageType;
  }

  boolean safe() {
    return safe;
  }

  boolean validateAllowedStorageType() {
    return validateAllowedStorageType;
  }

  static StorageTypeFileIO fromFileIoImplementation(String fileIoImplementation) {
    var type = FILE_TO_TO_STORAGE_TYPE.get(fileIoImplementation);
    if (type == null) {
      throw new IllegalArgumentException("Unknown FileIO implementation: " + fileIoImplementation);
    }
    return type;
  }

  private static final Map<String, StorageTypeFileIO> FILE_TO_TO_STORAGE_TYPE;

  static {
    var map = new HashMap<String, StorageTypeFileIO>();
    for (var st : PolarisStorageConfigurationInfo.StorageType.values()) {
      // Ensure all storage types are included in this enum
      valueOf(st.name());
    }
    for (var value : StorageTypeFileIO.values()) {
      if (value.validateAllowedStorageType()) {
        // Ensure that the storage type in this enum has a corresponding value
        PolarisStorageConfigurationInfo.StorageType.valueOf(value.name());
      }
      map.put(value.fileIoImplementation, value);
    }
    FILE_TO_TO_STORAGE_TYPE = Collections.unmodifiableMap(map);
  }
}
