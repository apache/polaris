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
package org.apache.polaris.extension.storage.cloudflare.r2;

import java.util.List;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;

/**
 * The environment of the live R2 suite, shared by its test classes: the bucket location, the
 * account, the jurisdiction, and the storage config of an {@code R2_LOCAL_SIGNING} catalog. The
 * parent token comes from the environment of the Gradle process only, and nothing here prints it.
 */
final class R2LiveEnvironment {

  private R2LiveEnvironment() {}

  static final String ACCESS_KEY_ENV = "POLARIS_STORAGE_CLOUDFLARE_R2_ACCESS_KEY";
  static final String SECRET_KEY_ENV = "POLARIS_STORAGE_CLOUDFLARE_R2_SECRET_KEY";

  // Strip one trailing slash so base + "/" + subpath never forms "//", an empty path segment that
  // the module refuses.
  static final String BASE_LOCATION = stripTrailingSlash(System.getenv("INTEGRATION_TEST_R2_PATH"));
  static final String JURISDICTION = System.getenv("INTEGRATION_TEST_R2_JURISDICTION");

  /**
   * The R2 account ID. The suites run when {@code INTEGRATION_TEST_R2_PATH} is set, so a missing
   * account ID fails here with the variable name, not as an endpoint for the host {@code null}.
   */
  static String accountId() {
    return requiredEnv("INTEGRATION_TEST_R2_ACCOUNT_ID");
  }

  private static String stripTrailingSlash(String path) {
    return path != null && path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }

  static AwsStorageConfigInfo cloudflareR2StorageConfig(
      String endpoint, List<String> allowedLocations) {
    return AwsStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.S3)
        .setCredentialVendingMechanism(R2LocalSigningCredentialVendingMechanism.R2_LOCAL_SIGNING)
        .setEndpoint(endpoint)
        .setPathStyleAccess(true)
        .setRegion("auto")
        .setAllowedLocations(allowedLocations)
        .build();
  }

  static String endpoint(String accountId) {
    String host =
        JURISDICTION == null || JURISDICTION.isEmpty()
            ? accountId + ".r2.cloudflarestorage.com"
            : accountId + "." + JURISDICTION + ".r2.cloudflarestorage.com";
    return "https://" + host;
  }

  /**
   * Reads an environment variable the R2 suite cannot run without. Fails with the variable name
   * rather than letting a null reach the caller.
   */
  static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isEmpty()) {
      throw new IllegalStateException(
          "Environment variable " + name + " must be set to run the R2 integration tests");
    }
    return value;
  }
}
