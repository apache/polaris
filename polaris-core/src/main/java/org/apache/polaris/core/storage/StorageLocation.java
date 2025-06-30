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
package org.apache.polaris.core.storage;

import jakarta.annotation.Nonnull;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.polaris.core.storage.aws.S3Location;
import org.apache.polaris.core.storage.azure.AzureLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An abstraction over a storage location */
public class StorageLocation {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageLocation.class);
  private static final Pattern SCHEME_PATTERN = Pattern.compile("^(.+?):(.+)");

  public static final String LOCAL_PATH_PREFIX = "file:///";

  private final String location;

  /** Create a StorageLocation from a String path */
  public static StorageLocation of(String location) {
    // TODO implement StorageLocation for all supported file systems and add isValidLocation
    if (AzureLocation.isAzureLocation(location)) {
      return new AzureLocation(location);
    } else if (S3Location.isS3Location(location)) {
      return new S3Location(location);
    } else {
      return new StorageLocation(location);
    }
  }

  protected StorageLocation(@Nonnull String location) {
    if (location == null) {
      this.location = null;
    } else if (location.startsWith("file:/")) {
      this.location = URI.create(location.replaceFirst("file:/+", LOCAL_PATH_PREFIX)).toString();
    } else if (location.startsWith("/")) {
      this.location = URI.create(location.replaceFirst("/+", LOCAL_PATH_PREFIX)).toString();
    } else {
      this.location = location;
    }
  }

  /** If a path doesn't end in `/`, this will add one */
  protected final String ensureTrailingSlash(String location) {
    if (location == null || location.endsWith("/")) {
      return location;
    } else {
      return location + "/";
    }
  }

  /** If a path doesn't start with `/`, this will add one */
  protected final @Nonnull String ensureLeadingSlash(@Nonnull String location) {
    if (location.startsWith("/")) {
      return location;
    } else {
      return "/" + location;
    }
  }

  @Override
  public int hashCode() {
    return location.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StorageLocation) {
      return location.equals(((StorageLocation) obj).location);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return location;
  }

  /**
   * Returns true if this StorageLocation's location string starts with the other StorageLocation's
   * location string
   */
  public boolean isChildOf(StorageLocation potentialParent) {
    if (this.location == null || potentialParent.location == null) {
      return false;
    } else {
      String slashTerminatedLocation = ensureTrailingSlash(this.location);
      String slashTerminatedParentLocation = ensureTrailingSlash(potentialParent.location);
      return slashTerminatedLocation.startsWith(slashTerminatedParentLocation);
    }
  }

  /** Returns a string representation of the location but without a scheme */
  public String withoutScheme() {
    if (location == null) {
      return null;
    }
    Matcher matcher = SCHEME_PATTERN.matcher(location);
    if (matcher.matches()) {
      String locationWithoutScheme = matcher.group(2);
      LOGGER.debug("Extracted {} from location {}", locationWithoutScheme, location);
      return locationWithoutScheme;
    } else {
      LOGGER.debug("Found no scheme in location {}", location);
      return location;
    }
  }
}
