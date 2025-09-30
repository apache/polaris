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
package org.apache.polaris.core.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class StaticTokenProviderTest {

  @Test
  public void testStaticTokenProvider() {
    String expectedToken = "static-bearer-token";
    StaticTokenProvider provider = new StaticTokenProvider(expectedToken);

    String actualToken = provider.getToken();
    assertEquals(expectedToken, actualToken);
  }

  @Test
  public void testStaticTokenProviderWithNull() {
    StaticTokenProvider provider = new StaticTokenProvider(null);

    String token = provider.getToken();
    assertNull(token);
  }

  @Test
  public void testStaticTokenProviderWithEmptyString() {
    StaticTokenProvider provider = new StaticTokenProvider("");

    String token = provider.getToken();
    assertEquals("", token);
  }
}
