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
package org.apache.polaris.service.it.relational.jdbc;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import org.apache.polaris.service.it.test.PolarisTagServiceIntegrationTest;
import org.apache.polaris.test.commons.RelationalJdbcProfile;

@QuarkusIntegrationTest
@TestProfile(JdbcTagServiceIT.Profile.class)
public class JdbcTagServiceIT extends PolarisTagServiceIntegrationTest {
  public static class Profile extends RelationalJdbcProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      // The tag store ships disabled by default; these suites exercise it, so turn it on.
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("polaris.features.\"ENABLE_TAG_STORE\"", "true")
          .build();
    }
  }
}
