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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Describes a single file/object in an object storage.
 *
 * <p>Not all attributes are populated by every {@link FileOperations} function.
 */
@PolarisImmutable
public interface FileSpec {

  /** The full object storage URI. */
  String location();

  /**
   * The type of the file, if known.
   *
   * @see #guessTypeFromName()
   */
  Optional<FileType> fileType();

  /** The size of the file in bytes, if available. */
  OptionalLong size();

  /** The creation timestamp in milliseconds since the epoch, if available. */
  OptionalLong createdAtMillis();

  static Builder builder() {
    return ImmutableFileSpec.builder();
  }

  static Builder fromLocation(String location) {
    return builder().location(location);
  }

  static Builder fromLocationAndSize(String location, long size) {
    var b = fromLocation(location);
    if (size > 0L) {
      b.size(size);
    }
    return b;
  }

  default FileType guessTypeFromName() {
    var location = location();
    var lastSlash = location.lastIndexOf('/');
    var fileName = lastSlash > 0 ? location.substring(lastSlash + 1) : location;

    if (fileName.contains(".metadata.json")) {
      return FileType.ICEBERG_METADATA;
    } else if (fileName.startsWith("snap-")) {
      return FileType.ICEBERG_MANIFEST_LIST;
    } else if (fileName.contains("-m")) {
      return FileType.ICEBERG_MANIFEST_FILE;
    }
    return FileType.UNKNOWN;
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder fileType(FileType fileType);

    @CanIgnoreReturnValue
    Builder location(String location);

    @CanIgnoreReturnValue
    Builder size(long size);

    @CanIgnoreReturnValue
    Builder createdAtMillis(long createdAtMillis);

    FileSpec build();
  }
}
