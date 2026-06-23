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
package org.apache.polaris.service.it.test;

import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;
import org.apache.polaris.service.it.env.ClientCredentials;
import org.apache.polaris.service.it.env.OpenLineageApi;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for the OpenLineage ingest endpoint ({@code POST /api/v1/lineage}).
 *
 * <p>The endpoint is currently a no-op: every authenticated, well-formed event returns {@code
 * 201 Created}. These tests pin down the wire-level contract so that follow-up PRs (persistence,
 * dataset resolution, downstream forwarding) cannot accidentally regress dispatch or auth
 * behavior.
 */
@ExtendWith(PolarisIntegrationTestExtension.class)
public class PolarisOpenLineageServiceIntegrationTest {

  private static final String SCHEMA_BASE = "https://openlineage.io/spec/2-0-2/OpenLineage.json";

  private static final String RUN_EVENT =
      """
      {
        "eventTime": "2024-01-01T00:00:00Z",
        "eventType": "START",
        "run": {"runId": "123e4567-e89b-12d3-a456-426614174000"},
        "job": {"namespace": "test", "name": "job"},
        "producer": "https://example.com",
        "schemaURL": "%s#/$defs/RunEvent",
        "inputs": [{"namespace": "db", "name": "orders_raw"}],
        "outputs": [{"namespace": "db", "name": "orders_daily"}]
      }
      """
          .formatted(SCHEMA_BASE);

  private static final String JOB_EVENT =
      """
      {
        "eventTime": "2024-01-01T00:00:00Z",
        "job": {"namespace": "test", "name": "job"},
        "producer": "https://example.com",
        "schemaURL": "%s#/$defs/JobEvent"
      }
      """
          .formatted(SCHEMA_BASE);

  private static final String DATASET_EVENT =
      """
      {
        "eventTime": "2024-01-01T00:00:00Z",
        "dataset": {"namespace": "db", "name": "orders_daily"},
        "producer": "https://example.com",
        "schemaURL": "%s#/$defs/DatasetEvent"
      }
      """
          .formatted(SCHEMA_BASE);

  private static final String UNKNOWN_SCHEMA_URL_EVENT =
      """
      {
        "eventTime": "2024-01-01T00:00:00Z",
        "eventType": "START",
        "run": {"runId": "123e4567-e89b-12d3-a456-426614174000"},
        "job": {"namespace": "test", "name": "job"},
        "producer": "https://example.com",
        "schemaURL": "https://example.com/UnknownEvent"
      }
      """;

  private static final String MISSING_SCHEMA_URL_EVENT =
      """
      {
        "eventTime": "2024-01-01T00:00:00Z",
        "eventType": "START",
        "run": {"runId": "123e4567-e89b-12d3-a456-426614174000"},
        "job": {"namespace": "test", "name": "job"},
        "producer": "https://example.com"
      }
      """;

  private static PolarisClient client;
  private static OpenLineageApi authenticated;
  private static OpenLineageApi anonymous;

  @BeforeAll
  public static void setup(PolarisApiEndpoints endpoints, ClientCredentials credentials) {
    client = polarisClient(endpoints);
    String token = client.obtainToken(credentials);
    authenticated = client.openLineageApi(token);
    anonymous = client.openLineageApiPlain();
  }

  @AfterAll
  public static void close() throws Exception {
    client.close();
  }

  @Test
  public void runEventReturns201() {
    try (Response res = authenticated.sendEvent(RUN_EVENT)) {
      assertThat(res.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  @Test
  public void jobEventReturns201() {
    try (Response res = authenticated.sendEvent(JOB_EVENT)) {
      assertThat(res.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  @Test
  public void datasetEventReturns201() {
    try (Response res = authenticated.sendEvent(DATASET_EVENT)) {
      assertThat(res.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  @Test
  public void unknownSchemaUrlIsAccepted() {
    try (Response res = authenticated.sendEvent(UNKNOWN_SCHEMA_URL_EVENT)) {
      assertThat(res.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  @Test
  public void missingSchemaUrlIsAccepted() {
    try (Response res = authenticated.sendEvent(MISSING_SCHEMA_URL_EVENT)) {
      assertThat(res.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  @Test
  public void unauthenticatedReturns401() {
    try (Response res = anonymous.sendEvent(RUN_EVENT)) {
      assertThat(res.getStatus()).isEqualTo(Response.Status.UNAUTHORIZED.getStatusCode());
    }
  }

  @Test
  public void malformedJsonReturns400() {
    try (Response res = authenticated.sendEvent("not-json")) {
      assertThat(res.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    }
  }
}
