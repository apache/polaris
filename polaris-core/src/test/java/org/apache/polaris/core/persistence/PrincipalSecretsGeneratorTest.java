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
package org.apache.polaris.core.persistence;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.bootstrap;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.junit.jupiter.api.Test;

class PrincipalSecretsGeneratorTest {

  @Test
  void testRandomSecrets() {
    PolarisPrincipalSecrets s = bootstrap("test", null).produceSecrets("name1", 123);
    assertThat(s).isNotNull();
    assertThat(s.getPrincipalId()).isEqualTo(123);
    assertThat(s.getPrincipalClientId()).isNotNull();
    assertThat(s.getMainSecret()).isNotNull();
    assertThat(s.getSecondarySecret()).isNotNull();
  }

  @Test
  void testSecretOverride() {
    PrincipalSecretsGenerator gen =
        bootstrap("test-Realm", RootCredentialsSet.fromString("test-Realm,client1,sec2"));
    PolarisPrincipalSecrets s = gen.produceSecrets("root", 123);
    assertThat(s).isNotNull();
    assertThat(s.getPrincipalId()).isEqualTo(123);
    assertThat(s.getPrincipalClientId()).isEqualTo("client1");
    assertThat(s.getMainSecret()).isEqualTo("sec2");
    assertThat(s.getSecondarySecret()).isEqualTo("sec2");
  }
}
