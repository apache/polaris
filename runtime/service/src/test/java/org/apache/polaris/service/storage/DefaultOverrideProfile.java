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
package org.apache.polaris.service.storage;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;
import java.util.Set;

/**
 * Installs {@link TestDefaultOverrideMechanism} as a CDI alternative for the DEFAULT identifier.
 * Only {@code STS} is allowlisted explicitly: an empty {@code credentialVendingMechanism}, which
 * resolves to DEFAULT, needs no allowlist entry of its own.
 */
public class DefaultOverrideProfile implements QuarkusTestProfile {

  @Override
  public Set<Class<?>> getEnabledAlternatives() {
    return Set.of(TestDefaultOverrideMechanism.class);
  }

  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(
        "polaris.realm-context.realms",
        ThirdMechanismProfile.ALLOWLISTED_REALM,
        "polaris.file-io.type",
        "test-in-memory",
        "polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"",
        "[\"S3\"]",
        "polaris.features.\"SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS\"",
        "[\"STS\"]",
        "polaris.event-listener.type",
        "test",
        "polaris.authentication.token-broker.type",
        "symmetric-key",
        "polaris.authentication.token-broker.symmetric-key.secret",
        "secret");
  }
}
