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
package org.apache.polaris.service.it;

import static org.apache.polaris.test.commons.MinioRustProfile.ACCESS_KEY;
import static org.apache.polaris.test.commons.MinioRustProfile.SECRET_KEY;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.test.commons.MinioRustProfile;
import org.apache.polaris.test.floci.aws.FlociAwsTestResource;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Validates that Floci can exercise Polaris credential vending when AWS KMS policy generation is
 * enabled.
 */
@QuarkusIntegrationTest
@TestProfile(MinioRustProfile.class)
@QuarkusTestResource(
    value = FlociAwsTestResource.class,
    initArgs = {
      @ResourceArg(name = "accessKey", value = ACCESS_KEY),
      @ResourceArg(name = "secretKey", value = SECRET_KEY),
      @ResourceArg(name = "bucket", value = "floci-s3-special-kms-test"),
      @ResourceArg(name = "region", value = AbstractRestCatalogFlociS3SpecialIT.TEST_REGION),
      @ResourceArg(name = "iamEnforcement", value = "true")
    })
@ExtendWith(PolarisIntegrationTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestCatalogFlociS3SpecialKmsIT extends RestCatalogFlociS3SpecialIT {

  @Override
  protected boolean kmsUnavailableForStsCatalogs() {
    return false;
  }
}
