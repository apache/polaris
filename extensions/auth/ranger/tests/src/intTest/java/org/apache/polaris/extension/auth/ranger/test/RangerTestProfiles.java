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
package org.apache.polaris.extension.auth.ranger.test;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

/** Quarkus test profiles for Ranger integration tests using embedded policy fixtures. */
public final class RangerTestProfiles {

  private RangerTestProfiles() {}

  /** Ranger authorizer with policies loaded from classpath ({@code /authz_tests}). */
  public static class EmbeddedPolicy implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.ofEntries(
          Map.entry("polaris.authorization.type", "ranger"),
          Map.entry("polaris.authorization.ranger.service-name", "dev_polaris"),
          Map.entry(
              "polaris.authorization.ranger.authz.default.policy.source.impl",
              "org.apache.ranger.admin.client.EmbeddedResourcePolicySource"),
          Map.entry(
              "polaris.authorization.ranger.authz.default.enable.implicit.userstore.enricher",
              "true"),
          Map.entry(
              "polaris.authorization.ranger.authz.default.policy.source.embedded_resource.path",
              "/authz_it_tests"),
          Map.entry("polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"", "[\"FILE\"]"),
          Map.entry("polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"", "true"),
          Map.entry("polaris.readiness.ignore-severe-issues", "true"));
    }
  }
}
