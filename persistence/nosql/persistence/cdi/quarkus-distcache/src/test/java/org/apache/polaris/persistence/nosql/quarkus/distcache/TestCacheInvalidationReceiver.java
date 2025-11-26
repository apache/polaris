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
package org.apache.polaris.persistence.nosql.quarkus.distcache;

import static java.util.Collections.singletonList;
import static org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidationEvictObj.cacheInvalidationEvictObj;
import static org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidationEvictReference.cacheInvalidationEvictReference;
import static org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.cacheInvalidations;
import static org.apache.polaris.persistence.nosql.quarkus.distcache.CacheInvalidationReceiver.CACHE_INVALIDATION_TOKEN_HEADER;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations;
import org.apache.polaris.persistence.nosql.api.cache.DistributedCacheInvalidation;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.junit.jupiter.api.Test;

public class TestCacheInvalidationReceiver {
  private static final ObjRef SOME_OBJ_REF = ObjRef.objRef("foo", 1234);

  @Test
  public void senderReceiver() throws Exception {
    var distributedCacheInvalidation = mock(DistributedCacheInvalidation.Receiver.class);

    var token = "cafe";
    var tokens = singletonList(token);
    var receiverId = ServerInstanceId.of("receiverId");
    var senderId = ServerInstanceId.of("senderId");

    var receiver = buildReceiver(tokens, receiverId, distributedCacheInvalidation);

    var invalidations = cacheInvalidations(allInvalidationTypes());

    var rc =
        expectResponse(
            r -> {
              when(r.getParam("sender")).thenReturn(senderId.instanceId());
              when(r.getHeader(CACHE_INVALIDATION_TOKEN_HEADER)).thenReturn(token);
            });
    var reqBody = mock(RequestBody.class);
    when(reqBody.asString()).thenReturn(new ObjectMapper().writeValueAsString(invalidations));
    when(rc.body()).thenReturn(reqBody);

    receiver.cacheInvalidations(rc);

    verify(rc.response()).setStatusCode(204);
    verify(rc.response()).setStatusMessage("No content");

    verify(distributedCacheInvalidation).evictObj("repo", SOME_OBJ_REF);
    verify(distributedCacheInvalidation).evictReference("repo", "refs/foo/bar");
    verifyNoMoreInteractions(distributedCacheInvalidation);
  }

  @Test
  public void doesNotAcceptInvalidationsWithoutTokens() {
    var distributedCacheInvalidation = mock(DistributedCacheInvalidation.Receiver.class);

    var token = "cafe";
    var tokens = List.<String>of();
    var receiverId = ServerInstanceId.of("receiverId");
    var senderId = ServerInstanceId.of("senderId");

    var receiver = buildReceiver(tokens, receiverId, distributedCacheInvalidation);

    var rc = expectResponse();
    receiver.cacheInvalidations(
        rc, () -> cacheInvalidations(allInvalidationTypes()), senderId.instanceId(), token);

    verify(rc.response()).setStatusCode(400);
    verify(rc.response()).setStatusMessage("Invalid token");

    verifyNoMoreInteractions(distributedCacheInvalidation);
  }

  @Test
  public void receiveFromSelf() {
    var distributedCacheInvalidation = mock(DistributedCacheInvalidation.Receiver.class);

    var token = "cafe";
    var tokens = singletonList(token);
    var receiverId = ServerInstanceId.of("receiverId");

    var receiver = buildReceiver(tokens, receiverId, distributedCacheInvalidation);

    var rc = expectResponse();
    receiver.cacheInvalidations(
        rc, () -> cacheInvalidations(allInvalidationTypes()), receiverId.instanceId(), token);

    verify(rc.response()).setStatusCode(204);
    verify(rc.response()).setStatusMessage("No content");

    verifyNoMoreInteractions(distributedCacheInvalidation);
  }

  @Test
  public void unknownToken() {
    var distributedCacheInvalidation = mock(DistributedCacheInvalidation.Receiver.class);

    var token = "cafe";
    var tokens = singletonList(token);
    var differentToken = "otherToken";
    var receiverId = ServerInstanceId.of("receiverId");
    var senderId = ServerInstanceId.of("senderId");

    CacheInvalidationReceiver receiver =
        buildReceiver(tokens, receiverId, distributedCacheInvalidation);

    RoutingContext rc = expectResponse();
    receiver.cacheInvalidations(
        rc,
        () -> cacheInvalidations(allInvalidationTypes()),
        senderId.instanceId(),
        differentToken);

    verify(rc.response()).setStatusCode(400);
    verify(rc.response()).setStatusMessage("Invalid token");

    verifyNoMoreInteractions(distributedCacheInvalidation);
  }

  private RoutingContext expectResponse() {
    return expectResponse(r -> {});
  }

  private RoutingContext expectResponse(Consumer<HttpServerRequest> requestMocker) {
    var response = mock(HttpServerResponse.class);
    when(response.setStatusCode(anyInt())).thenReturn(response);
    when(response.setStatusMessage(anyString())).thenReturn(response);
    when(response.end()).thenReturn(Future.succeededFuture());

    var request = mock(HttpServerRequest.class);
    when(request.getHeader("Content-Type")).thenReturn("application/json");
    requestMocker.accept(request);

    var rc = mock(RoutingContext.class);
    when(rc.response()).thenReturn(response);
    when(rc.request()).thenReturn(request);
    return rc;
  }

  private static CacheInvalidationReceiver buildReceiver(
      List<String> tokens,
      ServerInstanceId receiverId,
      DistributedCacheInvalidation.Receiver distCacheInvalidation) {
    QuarkusDistributedCacheInvalidationsConfig config =
        mock(QuarkusDistributedCacheInvalidationsConfig.class);
    when(config.cacheInvalidationValidTokens()).thenReturn(Optional.of(tokens));

    return new CacheInvalidationReceiver(config, receiverId, distCacheInvalidation);
  }

  List<CacheInvalidations.CacheInvalidation> allInvalidationTypes() {
    return List.of(
        cacheInvalidationEvictReference("repo", "refs/foo/bar"),
        cacheInvalidationEvictObj("repo", SOME_OBJ_REF));
  }
}
