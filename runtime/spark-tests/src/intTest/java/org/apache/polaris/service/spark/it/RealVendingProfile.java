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
package org.apache.polaris.service.spark.it;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class RealVendingProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.ofEntries(
        Map.entry("polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"", "false"),
        Map.entry("polaris.storage.aws.access-key", "foo"),
        Map.entry("polaris.storage.aws.secret-key", "bar"),
        Map.entry("polaris.storage.aws.validstorage.access-key", "foo"),
        Map.entry("polaris.storage.aws.validstorage.secret-key", "bar"),
        Map.entry("polaris.storage.aws.ns-named.access-key", "foo"),
        Map.entry("polaris.storage.aws.ns-named.secret-key", "bar"),
        Map.entry("polaris.storage.aws.tbl-named.access-key", "foo"),
        Map.entry("polaris.storage.aws.tbl-named.secret-key", "bar"),
        Map.entry("polaris.storage.aws.ns.access-key", "foo"),
        Map.entry("polaris.storage.aws.ns.secret-key", "bar"),
        Map.entry("polaris.storage.aws.tbl.access-key", "foo"),
        Map.entry("polaris.storage.aws.tbl.secret-key", "bar"),
        Map.entry("polaris.storage.aws.billing-creds.access-key", "foo"),
        Map.entry("polaris.storage.aws.billing-creds.secret-key", "bar"),
        Map.entry("polaris.storage.aws.mid.access-key", "foo"),
        Map.entry("polaris.storage.aws.mid.secret-key", "bar"),
        Map.entry("polaris.storage.aws.tbl-only.access-key", "foo"),
        Map.entry("polaris.storage.aws.tbl-only.secret-key", "bar"),
        Map.entry("polaris.storage.aws.ns-shared.access-key", "foo"),
        Map.entry("polaris.storage.aws.ns-shared.secret-key", "bar"),
        Map.entry("polaris.storage.aws.tbl-override.access-key", "foo"),
        Map.entry("polaris.storage.aws.tbl-override.secret-key", "bar"),
        Map.entry("polaris.storage.aws.ns-v1.access-key", "foo"),
        Map.entry("polaris.storage.aws.ns-v1.secret-key", "bar"),
        Map.entry("polaris.storage.aws.ns-v2.access-key", "foo"),
        Map.entry("polaris.storage.aws.ns-v2.secret-key", "bar"));
  }
}
