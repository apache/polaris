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
package org.apache.polaris.core.storage.hadoop;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.immutables.PolarisImmutable;

/** HDFS Polaris Storage Configuration information */
@PolarisImmutable
@JsonSerialize(as = ImmutableHadoopStorageConfigurationInfo.class)
@JsonDeserialize(as = ImmutableHadoopStorageConfigurationInfo.class)
@JsonTypeName("HadoopStorageConfigurationInfo")
public abstract class HadoopStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  public static ImmutableHadoopStorageConfigurationInfo.Builder builder() {
    return ImmutableHadoopStorageConfigurationInfo.builder();
  }

// TODO (RDS) : see if the validation can be done elsewhere
//  /** Validate user supplied Hadoop resources are readable. */
//  private void validateHadoopResources() {
//    for (String resource : getResourcesArray()) {
//      try {
//        File file = new File(resource.trim());
//        if (!file.exists()) {
//          throw new IllegalArgumentException(
//                  "Hadoop resource supplied that does not exist: " + resource);
//        }
//        if (!file.canRead()) {
//          throw new IllegalArgumentException(
//                  "Unreadable Hadoop resource supplied, please check permissions: " + resource);
//        }
//      } catch (IllegalArgumentException e) {
//        throw e;
//      } catch (Exception e) {
//        throw new IllegalArgumentException("Error validating Hadoop resource: " + resource, e);
//      }
//    }
//  }

  @Override
  public void validatePrefixForStorageType(String loc) {
    if (!loc.startsWith("hdfs://")) {
      throw new IllegalArgumentException(
              String.format(
                      "Location prefix not allowed: '%s', expected prefix: hdfs://", loc));
    }
  }

  @Override
  public StorageType getStorageType() {
    return StorageType.HDFS;
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.hadoop.HadoopFileIO";
  }

  public abstract String getResources();

  @Nullable
  public abstract String getUsername();
}
