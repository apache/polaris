/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.core.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * Support for file:// URLs in storage configuration. This is pretty-much only used for testing.
 * Supports URLs that start with file:// or /, but also supports wildcard (*) to support certain
 * test cases.
 */
public class FileStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  public FileStorageConfigurationInfo(
      @JsonProperty(value = "allowedLocations", required = true) @NotNull
          List<String> allowedLocations) {
    super(StorageType.FILE, allowedLocations);
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.hadoop.HadoopFileIO";
  }

  @Override
  public void validatePrefixForStorageType(String loc) {
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
