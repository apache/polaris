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

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.apache.polaris.service.it.env.RestCatalogConfig;
import org.apache.polaris.service.it.test.PolarisRestCatalogR2IntegrationTestBase;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Runs the shared REST catalog suite against a real Cloudflare R2 bucket.
 *
 * <p>The server under test needs the R2 parent token in {@code polaris.storage.r2.access-key} and
 * {@code polaris.storage.r2.secret-key}. The launched server inherits this JVM's environment, so
 * exporting {@code POLARIS_STORAGE_R2_ACCESS_KEY} and {@code POLARIS_STORAGE_R2_SECRET_KEY} is the
 * only supported way to supply it. Never pass the parent token as a Quarkus config override: for
 * {@link QuarkusIntegrationTest} every override becomes a {@code -Dkey=value} argument on the
 * launched server's command line, and the launcher prints that command line to test output.
 */
@QuarkusIntegrationTest
@TestProfile(CredentialVendingProfile.class)
@EnabledIfEnvironmentVariable(named = "INTEGRATION_TEST_R2_PATH", matches = ".+")
@RestCatalogConfig({"header.X-Iceberg-Access-Delegation", "vended-credentials"})
public class RestCatalogR2IT extends PolarisRestCatalogR2IntegrationTestBase {}
