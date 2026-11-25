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

import static org.apache.polaris.extension.storage.cloudflare.r2.R2LiveEnvironment.ACCESS_KEY_ENV;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LiveEnvironment.BASE_LOCATION;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LiveEnvironment.SECRET_KEY_ENV;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LiveEnvironment.accountId;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LiveEnvironment.cloudflareR2StorageConfig;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LiveEnvironment.endpoint;
import static org.apache.polaris.extension.storage.cloudflare.r2.R2LiveEnvironment.requiredEnv;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.it.env.RestCatalogConfig;
import org.apache.polaris.service.it.test.PolarisRestCatalogIntegrationBase;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Runs the shared REST catalog suite against a real Cloudflare R2 bucket through an S3 catalog
 * whose {@code credentialVendingMechanism} is {@code R2_LOCAL_SIGNING}. The class only sets the
 * storage config and the properties of the client FileIO. {@link R2LocalSigningLiveIT} holds the
 * tests that are specific to R2.
 *
 * <p>The Gradle task {@code :polaris-extensions-storage-cloudflare-r2:cloudManualTest} starts the
 * packaged Polaris server through the polaris-server-test-runner plugin and runs this class against
 * it. The suite runs only when the {@code INTEGRATION_TEST_R2_*} environment is present. The server
 * needs the R2 parent token in {@code polaris.storage.cloudflare-r2.access-key} and {@code
 * polaris.storage.cloudflare-r2.secret-key}. The server process inherits the environment of the
 * Gradle process, so exporting {@code POLARIS_STORAGE_CLOUDFLARE_R2_ACCESS_KEY} and {@code
 * POLARIS_STORAGE_CLOUDFLARE_R2_SECRET_KEY} is the only supported way to supply it. Never pass the
 * parent token as a system property or a Gradle task input: both reach the command line of the
 * server process or the task inputs.
 *
 * <p>Tests never print the parent token, never put it in an assertion message, and never log client
 * properties.
 */
@EnabledIfEnvironmentVariable(named = "INTEGRATION_TEST_R2_PATH", matches = ".+")
@RestCatalogConfig({"header.X-Iceberg-Access-Delegation", "vended-credentials"})
public class RestCatalogR2IT extends PolarisRestCatalogIntegrationBase {

  @Override
  protected StorageConfigInfo getStorageConfigInfo() {
    return cloudflareR2StorageConfig(endpoint(accountId()), List.of(BASE_LOCATION));
  }

  /** Tests that write to storage directly need the R2 endpoint and the parent token. */
  @Override
  protected ImmutableMap.Builder<String, String> clientFileIOProperties() {
    return super.clientFileIOProperties()
        .put("s3.endpoint", endpoint(accountId()))
        .put("s3.path-style-access", "true")
        .put("client.region", "auto")
        .put("s3.access-key-id", requiredEnv(ACCESS_KEY_ENV))
        .put("s3.secret-access-key", requiredEnv(SECRET_KEY_ENV));
  }
}
