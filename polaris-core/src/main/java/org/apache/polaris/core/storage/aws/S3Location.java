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

package org.apache.polaris.core.storage.aws;

import jakarta.annotation.Nonnull;
import org.apache.polaris.core.storage.StorageLocation;

public class S3Location extends StorageLocation {
  private final String scheme;
  private final String locationWithoutScheme;

  public S3Location(@Nonnull String location) {
    super(location);
    if (location.startsWith("s3:")) {
      this.scheme = "s3";
      this.locationWithoutScheme = location.substring(3);
    } else if (location.startsWith("s3a:")) {
      this.scheme = "s3a";
      this.locationWithoutScheme = location.substring(4);
    } else {
      throw new IllegalArgumentException("Invalid S3 location uri " + location);
    }
  }

  public static boolean isS3Location(String location) {
    if (location == null) {
      return false;
    }
    return location.startsWith("s3:") || location.startsWith("s3a:");
  }

  @Override
  public boolean isChildOf(StorageLocation potentialParent) {
    if (potentialParent instanceof S3Location that) {
      // Given that S3 and S3A are to be treated similarly, the parent check ignores the prefix
      String slashTerminatedObjectKey = ensureTrailingSlash(this.locationWithoutScheme);
      String slashTerminatedObjectKeyThat = ensureTrailingSlash(that.locationWithoutScheme);
      return slashTerminatedObjectKey.startsWith(slashTerminatedObjectKeyThat);
    }
    return false;
  }

  public String getScheme() {
    return scheme;
  }

  @Override
  public String withoutScheme() {
    return locationWithoutScheme;
  }

  @Override
  public S3Location normalize() {
    if (scheme.equals("s3a")) {
      return new S3Location("s3:" + locationWithoutScheme);
    }
    return this;
  }

  @Override
  public int hashCode() {
    return withoutScheme().hashCode();
  }

  /** Checks if two S3Location instances represent the same physical location. */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof S3Location) {
      return withoutScheme().equals(((StorageLocation) obj).withoutScheme());
    } else {
      return false;
    }
  }
}
