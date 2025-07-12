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

import static org.apache.polaris.core.storage.PolarisStorageConfigurationInfo.StorageType.S3;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AwsStorageConfigurationInfoTest {

  private static AwsStorageConfigurationInfo config(String endpoint, String stsEndpoint) {
    return config(endpoint, stsEndpoint, false);
  }

  private static AwsStorageConfigurationInfo config(
      String endpoint, String stsEndpoint, Boolean pathStyle) {
    return new AwsStorageConfigurationInfo(
        S3, List.of(), "role", null, null, endpoint, stsEndpoint, pathStyle);
  }

  @Test
  public void testStsEndpoint() {
    assertThat(config(null, null))
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getStsEndpointUri)
        .containsExactly(null, null);
    assertThat(config(null, "http://sts.example.com"))
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getStsEndpointUri)
        .containsExactly(null, URI.create("http://sts.example.com"));
    assertThat(config("http://s3.example.com", null))
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getStsEndpointUri)
        .containsExactly(URI.create("http://s3.example.com"), URI.create("http://s3.example.com"));
    assertThat(config("http://s3.example.com", "http://sts.example.com"))
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getStsEndpointUri)
        .containsExactly(URI.create("http://s3.example.com"), URI.create("http://sts.example.com"));
  }

  @Test
  public void testPathStyleAccess() {
    assertThat(config(null, null, null).getPathStyleAccess()).isNull();
    assertThat(config(null, null, false).getPathStyleAccess()).isFalse();
    assertThat(config(null, null, true).getPathStyleAccess()).isTrue();
  }
}
