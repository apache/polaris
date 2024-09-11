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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Support for file:// URLs in storage configuration. This is pretty-much only used for testing.
 * Supports URLs that start with file:// or /, but also supports wildcard (*) to support certain
 * test cases.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableFileStorageConfigurationInfo.class)
@JsonDeserialize(as = ImmutableFileStorageConfigurationInfo.class)
public abstract class FileStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  public static FileStorageConfigurationInfo of(Iterable<String> allowedLocations) {
    return ImmutableFileStorageConfigurationInfo.builder()
        .allowedLocations(allowedLocations)
        .build();
  }

  @Override
  public abstract List<String> getAllowedLocations();

  @Override
  public StorageType getStorageType() {
    return StorageType.FILE;
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.hadoop.HadoopFileIO";
  }

  @Override
  protected void validatePrefixForStorageType(String loc) {
    if (!loc.startsWith(getStorageType().getPrefix())
        && !loc.startsWith("file:/")
        && !loc.startsWith("/")
        && !loc.equals("*")) {
      throw new IllegalArgumentException(
          String.format(
              "Location prefix not allowed: '%s', expected prefix: file:// or / or *", loc));
    }
  }
}
