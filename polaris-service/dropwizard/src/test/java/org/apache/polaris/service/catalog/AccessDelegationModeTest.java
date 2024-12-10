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
package org.apache.polaris.service.catalog;

import static org.apache.polaris.service.catalog.AccessDelegationMode.*;
import static org.apache.polaris.service.catalog.AccessDelegationMode.REMOTE_SIGNING;
import static org.apache.polaris.service.catalog.AccessDelegationMode.VENDED_CREDENTIALS;
import static org.apache.polaris.service.catalog.AccessDelegationMode.fromProtocolValuesList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.EnumSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class AccessDelegationModeTest {

  @ParameterizedTest
  @EnumSource(AccessDelegationMode.class)
  void testSingle(AccessDelegationMode mode) {
    assertThat(fromProtocolValuesList(mode.protocolValue())).isEqualTo(EnumSet.of(mode));
  }

  @Test
  void testSeveral() {
    assertThat(fromProtocolValuesList("vended-credentials, remote-signing"))
        .isEqualTo(EnumSet.of(VENDED_CREDENTIALS, REMOTE_SIGNING));
  }

  @Test
  void testEmpty() {
    assertThat(fromProtocolValuesList(null)).isEqualTo(EnumSet.noneOf(AccessDelegationMode.class));
    assertThat(fromProtocolValuesList("")).isEqualTo(EnumSet.noneOf(AccessDelegationMode.class));
  }

  @Test
  void testUnknown() {
    assertThat(fromProtocolValuesList("abc")).isEqualTo(EnumSet.of(UNKNOWN));
    assertThat(fromProtocolValuesList("abc,def")).isEqualTo(EnumSet.of(UNKNOWN));
    assertThat(fromProtocolValuesList("abc,remote-signing"))
        .isEqualTo(EnumSet.of(REMOTE_SIGNING, UNKNOWN));
  }

  @Test
  void testLegacy() {
    assertThat(fromProtocolValuesList("true")).isEqualTo(EnumSet.of(VENDED_CREDENTIALS));
    assertThat(fromProtocolValuesList("true, vended-credentials"))
        .isEqualTo(EnumSet.of(UNKNOWN, VENDED_CREDENTIALS));
  }
}
