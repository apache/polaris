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

package org.apache.polaris.storage.files.api;

import org.apache.iceberg.ContentFile;

public enum FileType {
  UNKNOWN(),
  ICEBERG_METADATA(),
  ICEBERG_STATISTICS(),
  ICEBERG_MANIFEST_LIST(),
  ICEBERG_MANIFEST_FILE(),
  ICEBERG_DATA_FILE(),
  ICEBERG_DELETE_FILE(),
  ;

  @SuppressWarnings("UnnecessaryDefault")
  public static FileType fromContentFile(ContentFile<? extends ContentFile<?>> contentFile) {
    return switch (contentFile.content()) {
      case DATA -> ICEBERG_DATA_FILE;
      case EQUALITY_DELETES, POSITION_DELETES -> ICEBERG_DELETE_FILE;
      default -> UNKNOWN;
    };
  }
}
