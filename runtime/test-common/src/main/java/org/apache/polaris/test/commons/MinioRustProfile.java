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

package org.apache.polaris.test.commons;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class MinioRustProfile implements QuarkusTestProfile {
  public static final String SECRET_KEY = "test-sk-123";
  public static final String ACCESS_KEY = "test-ak-123";
  public static final Map<String, String> CONFIG_OVERRIDES =
      ImmutableMap.<String, String>builder()
          .put("polaris.storage.aws.access-key", ACCESS_KEY)
          .put("polaris.storage.aws.secret-key", SECRET_KEY)
          .put("polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"", "false")
          .build();

  @Override
  public Map<String, String> getConfigOverrides() {
    return CONFIG_OVERRIDES;
  }
}
