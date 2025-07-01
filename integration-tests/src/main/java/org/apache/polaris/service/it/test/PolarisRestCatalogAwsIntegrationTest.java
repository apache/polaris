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
package org.apache.polaris.service.it.test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.assertj.core.util.Strings;

/** Runs PolarisRestCatalogIntegrationBase test on AWS. */
public class PolarisRestCatalogAwsIntegrationTest extends PolarisRestCatalogIntegrationBase {
  public static final String ROLE_ARN =
      Optional.ofNullable(System.getenv("INTEGRATION_TEST_ROLE_ARN"))
          .or(() -> Optional.ofNullable(System.getenv("INTEGRATION_TEST_S3_ROLE_ARN")))
          .orElse("arn:aws:iam::123456789012:role/my-role");
  public static final String BASE_LOCATION = System.getenv("INTEGRATION_TEST_S3_PATH");

  @Override
  protected StorageConfigInfo getStorageConfigInfo() {
    return AwsStorageConfigInfo.builder()
        .setRoleArn(ROLE_ARN)
        .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
        .setAllowedLocations(List.of(BASE_LOCATION))
        .build();
  }

  @Override
  protected boolean shouldSkip() {
    return Stream.of(BASE_LOCATION, ROLE_ARN).anyMatch(Strings::isNullOrEmpty);
  }
}
