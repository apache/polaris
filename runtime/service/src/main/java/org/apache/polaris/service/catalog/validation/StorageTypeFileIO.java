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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo.StorageType;
import org.jspecify.annotations.Nullable;

enum StorageTypeFileIO {
  S3("org.apache.iceberg.aws.s3.S3FileIO", true),

  // Cloudflare R2 speaks the S3 API and shares S3FileIO with S3. Resolution from a FileIO
  // class back to a storage type therefore prefers the catalog's declared type when known
  // (see fromFileIoImplementation(String, StorageType)); the bare lookup returns the first
  // declared candidate, S3, for backward compatibility.
  R2("org.apache.iceberg.aws.s3.S3FileIO", true),

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

  /** Resolves a FileIO class to a storage type; the first declared candidate wins on a tie. */
  static StorageTypeFileIO fromFileIoImplementation(String fileIoImplementation) {
    return fromFileIoImplementation(fileIoImplementation, null);
  }

  /**
   * Resolves a FileIO class to a storage type. When several storage types share the FileIO class
   * (S3 and R2 both use S3FileIO) and {@code preferred} is one of them, {@code preferred} wins;
   * otherwise the first declared candidate wins.
   */
  static StorageTypeFileIO fromFileIoImplementation(
      String fileIoImplementation, @Nullable StorageType preferred) {
    List<StorageTypeFileIO> candidates = FILE_IO_TO_STORAGE_TYPES.get(fileIoImplementation);
    if (candidates == null || candidates.isEmpty()) {
      throw new IllegalArgumentException("Unknown FileIO implementation: " + fileIoImplementation);
    }
    if (preferred != null) {
      for (StorageTypeFileIO candidate : candidates) {
        if (candidate.name().equals(preferred.name())) {
          return candidate;
        }
      }
    }
    return candidates.get(0);
  }

  private static final Map<String, List<StorageTypeFileIO>> FILE_IO_TO_STORAGE_TYPES;

  static {
    var map = new HashMap<String, List<StorageTypeFileIO>>();
    for (var st : PolarisStorageConfigurationInfo.StorageType.values()) {
      // Ensure all storage types are included in this enum
      valueOf(st.name());
    }
    for (var value : StorageTypeFileIO.values()) {
      if (value.validateAllowedStorageType()) {
        // Ensure that the storage type in this enum has a corresponding value
        PolarisStorageConfigurationInfo.StorageType.valueOf(value.name());
      }
      map.computeIfAbsent(value.fileIoImplementation, k -> new ArrayList<>()).add(value);
    }
    map.replaceAll((k, v) -> List.copyOf(v));
    FILE_IO_TO_STORAGE_TYPES = Collections.unmodifiableMap(map);
  }
}
