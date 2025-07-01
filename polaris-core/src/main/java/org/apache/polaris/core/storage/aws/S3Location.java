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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.polaris.core.storage.StorageLocation;

public class S3Location extends StorageLocation {
  private static final Pattern URI_PATTERN = Pattern.compile("^(s3a?):(.+)$");
  private final String scheme;
  private final String locationWithoutScheme;

  public S3Location(@Nonnull String location) {
    super(location);
    Matcher matcher = URI_PATTERN.matcher(location);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid S3 location uri " + location);
    }
    this.scheme = matcher.group(1);
    this.locationWithoutScheme = matcher.group(2);
  }

  public static boolean isS3Location(String location) {
    if (location == null) {
      return false;
    }
    Matcher matcher = URI_PATTERN.matcher(location);
    return matcher.matches();
  }

  @Override
  public boolean isChildOf(StorageLocation potentialParent) {
    if (potentialParent instanceof S3Location) {
      S3Location that = (S3Location) potentialParent;
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
}
