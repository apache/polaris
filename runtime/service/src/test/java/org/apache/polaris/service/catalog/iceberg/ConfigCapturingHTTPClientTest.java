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
package org.apache.polaris.service.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConfigCapturingHTTPClientTest {

  private RESTClient delegate;
  private CapturedConfigHolder holder;
  private ConfigCapturingHTTPClient client;

  @BeforeEach
  void setUp() {
    delegate = mock(RESTClient.class);
    holder = new CapturedConfigHolder();
    client = new ConfigCapturingHTTPClient(delegate, holder);
  }

  @Test
  void capturesConfigFromLoadTableResponse() {
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn("s3://test-bucket/table");
    LoadTableResponse response =
        LoadTableResponse.builder()
            .withTableMetadata(metadata)
            .addAllConfig(
                Map.of(
                    "tableId", "73031312-6e6f-432c-ab35-ae23561f6472",
                    "tableBucketId", "d7290d06"))
            .build();

    when(delegate.get(
            any(String.class),
            any(Map.class),
            eq(LoadTableResponse.class),
            any(Map.class),
            any(java.util.function.Consumer.class)))
        .thenReturn(response);

    client.get("path", Map.of(), LoadTableResponse.class, Map.of(), (ErrorResponse e) -> {});

    assertThat(holder.getTableId()).hasValue("73031312-6e6f-432c-ab35-ae23561f6472");
  }

  @Test
  void doesNotCaptureFromNonLoadTableResponse() {
    when(delegate.delete(
            any(String.class),
            eq(LoadTableResponse.class),
            any(Map.class),
            any(java.util.function.Consumer.class)))
        .thenReturn(null);

    client.delete("path", LoadTableResponse.class, Map.of(), (ErrorResponse e) -> {});

    assertThat(holder.getTableId()).isEmpty();
  }

  @Test
  void withAuthSessionDelegatesToInnerClient() {
    AuthSession session = mock(AuthSession.class);
    RESTClient wrappedDelegate = mock(RESTClient.class);
    when(delegate.withAuthSession(session)).thenReturn(wrappedDelegate);

    RESTClient result = client.withAuthSession(session);

    assertThat(result).isInstanceOf(ConfigCapturingHTTPClient.class);
    verify(delegate).withAuthSession(session);
  }
}
