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
package org.apache.polaris.extension.auth.opa;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.HttpEntities;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.apache.polaris.core.auth.AuthorizationBatchRequest;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationIntent;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.auth.PathSegment;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.collection.AttributeMap;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.extension.auth.opa.token.StaticBearerTokenProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class OpaPolarisAuthorizerBatchTest {

  private static final URI POLICY_URI =
      URI.create("http://opa.example.com:8181/v1/data/polaris/allow");
  private static final URI BATCH_URI =
      URI.create("http://opa.example.com:8181/v1/data/polaris/batch_allow");
  private static final ObjectMapper MAPPER = JsonMapper.builder().build();

  /** One HTTP call made by the authorizer under test. */
  private record Call(URI uri, JsonNode body, String authorization) {}

  /** Answers a call: the HTTP status and the response body. */
  private record Reply(int status, String body) {}

  /** An authorizer whose HTTP calls are recorded and answered by {@code replier}. */
  private static OpaPolarisAuthorizer authorizer(
      URI batchUri,
      int maxBatchSize,
      List<Call> calls,
      Function<Call, Reply> replier,
      String requestId) {
    return new OpaPolarisAuthorizer(
        POLICY_URI,
        batchUri,
        maxBatchSize,
        mock(CloseableHttpClient.class),
        MAPPER,
        new StaticBearerTokenProvider("test-token"),
        requestId,
        "test-realm",
        Map.of("environment", "test")) {
      @Override
      <T> T httpClientExecute(
          ClassicHttpRequest request, HttpClientResponseHandler<? extends T> responseHandler)
          throws HttpException, IOException {
        try {
          JsonNode body =
              MAPPER.readTree(
                  new String(
                      request.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8));
          var authorization = request.getFirstHeader("Authorization");
          Call call =
              new Call(
                  request.getUri(), body, authorization == null ? null : authorization.getValue());
          calls.add(call);
          Reply reply = replier.apply(call);
          @SuppressWarnings("resource")
          ClassicHttpResponse response = new BasicClassicHttpResponse(reply.status());
          response.setEntity(HttpEntities.create(reply.body()));
          return responseHandler.handleResponse(response);
        } catch (URISyntaxException e) {
          throw new IOException(e);
        }
      }
    };
  }

  private static AuthorizationIntent tableIntent(int index) {
    return new SingleTargetAuthorizationIntent(
        PolarisAuthorizableOperation.LIST_TABLE,
        PolarisSecurable.of(
            new PathSegment(PolarisEntityType.CATALOG, "federated_catalog"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns1"),
            new PathSegment(PolarisEntityType.TABLE_LIKE, "table" + index)));
  }

  private static AuthorizationBatchRequest batchOf(int size) {
    return new AuthorizationBatchRequest(
        PolarisPrincipal.of("alice", AttributeMap.EMPTY, Set.of("role-1")),
        IntStream.range(0, size).mapToObj(OpaPolarisAuthorizerBatchTest::tableIntent).toList());
  }

  private static AuthorizationState state() {
    return new AuthorizationState(mock(PolarisResolutionManifest.class));
  }

  /** The table index (the number in "tableN") of a question's first target. */
  private static int tableIndex(JsonNode entry) {
    String name = entry.path("resource").path("targets").get(0).path("name").asText();
    return Integer.parseInt(name.substring("table".length()));
  }

  /** The policy used by these tests: even-numbered tables are allowed, odd ones denied. */
  private static boolean allowed(int tableIndex) {
    return tableIndex % 2 == 0;
  }

  /** What a correct batch rule answers: question ID to the policy's decision. */
  private static Reply batchReply(Call call) {
    ObjectNode result = MAPPER.createObjectNode();
    for (JsonNode entry : call.body().path("input").path("requests")) {
      result.put(entry.path("id").asText(), allowed(tableIndex(entry)));
    }
    return new Reply(200, MAPPER.createObjectNode().set("result", result).toString());
  }

  /** What a correct single rule answers. */
  private static Reply singleReply(Call call) {
    int index = tableIndex(call.body().path("input"));
    return new Reply(200, "{\"result\":{\"allow\":" + allowed(index) + "}}");
  }

  private static Reply reply(Call call) {
    return call.uri().equals(BATCH_URI) ? batchReply(call) : singleReply(call);
  }

  @Test
  void withoutBatchPolicyUriEveryIntentIsOneCallToThePolicyUri() {
    List<Call> calls = new ArrayList<>();
    var authorizer = authorizer(null, 100, calls, OpaPolarisAuthorizerBatchTest::reply, null);

    List<AuthorizationDecision> decisions = authorizer.authorize(state(), batchOf(3));

    assertThat(calls).hasSize(3).allSatisfy(call -> assertThat(call.uri()).isEqualTo(POLICY_URI));
    assertThat(decisions)
        .extracting(AuthorizationDecision::isAllowed)
        .containsExactly(true, false, true);
  }

  @Test
  void emptyBatchMakesNoCalls() {
    List<Call> calls = new ArrayList<>();
    var authorizer = authorizer(BATCH_URI, 100, calls, OpaPolarisAuthorizerBatchTest::reply, null);

    assertThat(authorizer.authorize(state(), batchOf(0))).isEmpty();
    assertThat(calls).isEmpty();
  }

  @Test
  void sendsOneCallWithActorAndContextOnceAndTheFullResourcePerQuestion() {
    List<Call> calls = new ArrayList<>();
    var authorizer =
        authorizer(BATCH_URI, 100, calls, OpaPolarisAuthorizerBatchTest::reply, "req-1");

    List<AuthorizationDecision> decisions = authorizer.authorize(state(), batchOf(2));

    assertThat(calls).hasSize(1);
    Call call = calls.get(0);
    assertThat(call.uri()).isEqualTo(BATCH_URI);
    assertThat(call.authorization()).isEqualTo("Bearer test-token");
    JsonNode input = call.body().path("input");
    assertThat(input.path("actor").path("principal").asText()).isEqualTo("alice");
    assertThat(input.path("actor").path("roles").get(0).asText()).isEqualTo("role-1");
    assertThat(input.path("context").path("request_id").asText()).isEqualTo("req-1");
    assertThat(input.path("context").path("realm").asText()).isEqualTo("test-realm");
    assertThat(input.path("context").path("properties").path("environment").asText())
        .isEqualTo("test");
    JsonNode requests = input.path("requests");
    assertThat(requests).hasSize(2);
    assertThat(requests.get(0).path("id").asText()).isEqualTo("0");
    assertThat(requests.get(1).path("id").asText()).isEqualTo("1");
    assertThat(requests.get(1).path("action").asText()).isEqualTo("LIST_TABLE");
    // The full path is sent, as for a remote-only table of a federated catalog.
    JsonNode target = requests.get(1).path("resource").path("targets").get(0);
    assertThat(target.path("type").asText()).isEqualTo("TABLE_LIKE");
    assertThat(target.path("name").asText()).isEqualTo("table1");
    assertThat(target.path("parents").get(0).path("name").asText()).isEqualTo("federated_catalog");
    assertThat(target.path("parents").get(1).path("name").asText()).isEqualTo("ns1");
    assertThat(decisions).extracting(AuthorizationDecision::isAllowed).containsExactly(true, false);
  }

  @Test
  void matchesAnswersByIdNotByOrder() {
    List<Call> calls = new ArrayList<>();
    var authorizer =
        authorizer(
            BATCH_URI,
            100,
            calls,
            call -> new Reply(200, "{\"result\":{\"2\":true,\"0\":false,\"1\":true}}"),
            null);

    assertThat(authorizer.authorize(state(), batchOf(3)))
        .extracting(AuthorizationDecision::isAllowed)
        .containsExactly(false, true, true);
  }

  @Test
  void splitsIntoChunksAndKeepsDecisionsInOrder() {
    List<Call> calls = new ArrayList<>();
    var authorizer = authorizer(BATCH_URI, 100, calls, OpaPolarisAuthorizerBatchTest::reply, null);

    List<AuthorizationDecision> decisions = authorizer.authorize(state(), batchOf(250));

    assertThat(calls)
        .extracting(call -> call.body().path("input").path("requests").size())
        .containsExactly(100, 100, 50);
    // IDs restart at 0 in every chunk, and every chunk shares one request ID.
    assertThat(calls)
        .allSatisfy(call ->
            assertThat(call.body().path("input").path("requests").get(0).path("id").asText())
                .isEqualTo("0"));
    assertThat(calls.stream().map(c -> c.body().path("input").path("context").path("request_id")))
        .doesNotContainNull()
        .containsOnly(calls.get(0).body().path("input").path("context").path("request_id"));
    assertThat(decisions).hasSize(250);
    for (int i = 0; i < decisions.size(); i++) {
      assertThat(decisions.get(i).isAllowed()).as("table%s", i).isEqualTo(allowed(i));
    }
  }

  @Test
  void givesTheSameDecisionsAsOneCallPerIntent() {
    AuthorizationBatchRequest batch = batchOf(25);
    var batched =
        authorizer(BATCH_URI, 10, new ArrayList<>(), OpaPolarisAuthorizerBatchTest::reply, null);
    var single =
        authorizer(null, 10, new ArrayList<>(), OpaPolarisAuthorizerBatchTest::reply, null);

    assertThat(batched.authorize(state(), batch))
        .extracting(AuthorizationDecision::isAllowed)
        .isEqualTo(
            single.authorize(state(), batch).stream()
                .map(AuthorizationDecision::isAllowed)
                .toList());
  }

  static Stream<Arguments> badResponses() {
    return Stream.of(
        Arguments.of("not 200", 500, "{\"result\":{\"0\":true,\"1\":true}}"),
        Arguments.of("not JSON", 200, "this is not json"),
        Arguments.of("no result (batch rule undefined)", 200, "{}"),
        Arguments.of("missing an ID", 200, "{\"result\":{\"0\":true}}"),
        Arguments.of("an extra ID", 200, "{\"result\":{\"0\":true,\"1\":true,\"2\":true}}"),
        Arguments.of("a non-boolean answer", 200, "{\"result\":{\"0\":true,\"1\":\"yes\"}}"),
        Arguments.of("a null answer", 200, "{\"result\":{\"0\":true,\"1\":null}}"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("badResponses")
  void failsTheWholeBatchOnABadResponse(String description, int status, String body) {
    var authorizer =
        authorizer(BATCH_URI, 100, new ArrayList<>(), call -> new Reply(status, body), null);

    assertThatThrownBy(() -> authorizer.authorize(state(), batchOf(2)))
        .isInstanceOf(RuntimeException.class)
        .hasMessageStartingWith("OPA batch query failed");
  }

  @Test
  void aFailureInALaterChunkFailsTheWholeBatch() {
    List<Call> calls = new ArrayList<>();
    var authorizer =
        authorizer(
            BATCH_URI,
            2,
            calls,
            call -> calls.size() < 2 ? batchReply(call) : new Reply(200, "{\"result\":{}}"),
            null);

    assertThatThrownBy(() -> authorizer.authorize(state(), batchOf(4)))
        .hasMessageStartingWith("OPA batch query failed");
  }
}
