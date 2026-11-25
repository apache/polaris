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

package org.apache.polaris.extension.auth.ranger;

import static org.apache.polaris.extension.auth.ranger.RangerTestUtils.createConfig;
import static org.apache.polaris.extension.auth.ranger.RangerTestUtils.createRealmContext;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class RangerPolarisAuthorizerFactoryTest {
  @Test
  public void testAuthorizerInstantiation(@TempDir Path tempDir) {
    RangerPolarisAuthorizerFactory factory =
        new RangerPolarisAuthorizerFactory(createConfig(tempDir));
    RangerPolarisAuthorizer authorizer = factory.create(null);
    assertNotNull(authorizer);
    authorizer.setRealmContext(createRealmContext());
  }

  @Test
  public void testAuthorizerInstantiationWithTenantKey(@TempDir Path tempDir) {
    RangerPolarisAuthorizerConfig base = createConfig(tempDir);
    RangerPolarisAuthorizerConfig config =
        createConfig(null, base.properties(), Map.of("tenant1", () -> Optional.of("dev_polaris")));
    RangerPolarisAuthorizerFactory factory = new RangerPolarisAuthorizerFactory(config);
    assertNotNull(factory.create("tenant1"));
    assertThrows(IllegalStateException.class, () -> factory.create("unknown"));
    assertThrows(IllegalStateException.class, () -> factory.create(null));
    for (String reserved : RangerPolarisAuthorizerConfig.RESERVED_TENANT_KEYS) {
      assertThrows(IllegalStateException.class, () -> factory.create(reserved));
    }
  }

  @Test
  public void testAuthorizerInitMissingServiceName() {
    RangerPolarisAuthorizerConfig config = createConfig(null, Collections.emptyMap());
    assertThrows(IllegalStateException.class, config::validate);
  }
}
