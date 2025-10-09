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
package org.apache.polaris.service.config;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for OpaHttpClientFactory.
 */
public class OpaHttpClientFactoryTest {

  @Test
  void testCreateHttpClientWithHttpUrl() throws Exception {
    AuthorizationConfiguration.OpaConfig.HttpConfig httpConfig = createMockHttpConfig(5000, true, null, null);
    
    CloseableHttpClient client = OpaHttpClientFactory.createHttpClient(httpConfig);
    
    assertNotNull(client);
    client.close();
  }

  @Test
  void testCreateHttpClientWithHttpsUrl() throws Exception {
    AuthorizationConfiguration.OpaConfig.HttpConfig httpConfig = createMockHttpConfig(5000, false, null, null);
    
    CloseableHttpClient client = OpaHttpClientFactory.createHttpClient(httpConfig);
    
    assertNotNull(client);
    client.close();
  }

  @Test
  void testCreateHttpClientWithCustomTimeout() throws Exception {
    AuthorizationConfiguration.OpaConfig.HttpConfig httpConfig = createMockHttpConfig(10000, true, null, null);
    
    CloseableHttpClient client = OpaHttpClientFactory.createHttpClient(httpConfig);
    
    assertNotNull(client);
    client.close();
  }

  private AuthorizationConfiguration.OpaConfig.HttpConfig createMockHttpConfig(
      int timeoutMs, boolean verifySsl, String trustStorePath, String trustStorePassword) {
    AuthorizationConfiguration.OpaConfig.HttpConfig httpConfig = mock(AuthorizationConfiguration.OpaConfig.HttpConfig.class);
    when(httpConfig.timeoutMs()).thenReturn(timeoutMs);
    when(httpConfig.verifySsl()).thenReturn(verifySsl);
    when(httpConfig.trustStorePath()).thenReturn(Optional.ofNullable(trustStorePath));
    when(httpConfig.trustStorePassword()).thenReturn(Optional.ofNullable(trustStorePassword));
    return httpConfig;
  }
}