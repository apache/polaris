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

package org.apache.polaris.service.tracing;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2Api;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(TracingFilterTest.Profile.class)
@TestHTTPEndpoint(IcebergRestOAuth2Api.class)
public class TracingFilterTest {

  public static class Profile implements QuarkusTestProfile {

    @Produces
    @Singleton
    InMemorySpanExporter inMemorySpanExporter() {
      return InMemorySpanExporter.create();
    }

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("quarkus.otel.sdk.disabled", "false");
    }
  }

  @Inject InMemorySpanExporter inMemorySpanExporter;

  @Test
  void testW3CTraceContextPropagation() {

    // Emulate an incoming request with a W3C trace context
    // Example taken from:
    // https://www.w3.org/TR/trace-context/#traceparent-header-field-values
    String traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
    String spanId = "00f067aa0ba902b7";
    String traceparent = "00-" + traceId + "-" + spanId + "-01";
    String rojoState = spanId;
    String congoState = "t61rcWkgMzE";
    String tracestate = "rojo=" + rojoState + ",congo=" + congoState;

    given()
        .contentType(ContentType.URLENC)
        .formParam("grant_type", "client_credentials")
        .formParam("scope", "PRINCIPAL_ROLE:ALL")
        .formParam("client_id", "test-admin")
        .formParam("client_secret", "test-secret")
        // W3C headers
        .header("traceparent", traceparent)
        .header("tracestate", tracestate)
        // Polaris request ID
        .header("X-Request-ID", "12345")
        .when()
        .post()
        .then()
        .statusCode(200)
        .header("X-Request-ID", "12345");

    List<SpanData> spans =
        await()
            .atMost(Duration.ofSeconds(30))
            .until(inMemorySpanExporter::getFinishedSpanItems, sp -> !sp.isEmpty());

    SpanData span = spans.getFirst();

    Map<AttributeKey<?>, Object> attributes = span.getAttributes().asMap();
    assertThat(attributes)
        .containsEntry(stringKey(TracingFilter.REALM_ID_ATTRIBUTE), "POLARIS")
        .containsEntry(stringKey(TracingFilter.REQUEST_ID_ATTRIBUTE), "12345");

    SpanContext parent = span.getParentSpanContext();
    assertThat(parent.getTraceId()).isEqualTo(traceId);
    assertThat(parent.getSpanId()).isEqualTo(spanId);
    assertThat(parent.isRemote()).isTrue();
    assertThat(parent.getTraceFlags().asByte()).isEqualTo((byte) 1);
    assertThat(parent.getTraceState().asMap())
        .containsEntry("rojo", rojoState)
        .containsEntry("congo", congoState);
  }
}
