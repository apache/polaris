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

package org.apache.polaris.tools.mcp;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Flow;
import javax.net.ssl.SSLSession;

final class TestHttpExecutor implements HttpExecutor {
  private final int statusCode;
  private final String responseBody;
  private HttpRequest lastRequest;
  private String lastRequestBody;

  TestHttpExecutor(int statusCode, String responseBody) {
    this.statusCode = statusCode;
    this.responseBody = responseBody;
  }

  @Override
  public HttpResponse<String> execute(HttpRequest request) throws IOException, InterruptedException {
    this.lastRequest = request;
    this.lastRequestBody = readBody(request);
    Map<String, List<String>> headers = Collections.emptyMap();
    return new StubHttpResponse(request, statusCode, responseBody, headers);
  }

  HttpRequest lastRequest() {
    return lastRequest;
  }

  String lastRequestBody() {
    return lastRequestBody;
  }

  private String readBody(HttpRequest request) throws IOException, InterruptedException {
    return request
        .bodyPublisher()
        .map(
            publisher -> {
              BodyCaptureSubscriber subscriber = new BodyCaptureSubscriber();
              publisher.subscribe(subscriber);
              return subscriber.awaitBody();
            })
        .orElse("");
  }

  private static final class StubHttpResponse implements HttpResponse<String> {
    private final HttpRequest request;
    private final int status;
    private final String body;
    private final HttpHeaders headers;

    StubHttpResponse(
        HttpRequest request, int statusCode, String body, Map<String, List<String>> headers) {
      this.request = request;
      this.status = statusCode;
      this.body = body;
      this.headers = HttpHeaders.of(headers, (k, v) -> true);
    }

    @Override
    public int statusCode() {
      return status;
    }

    @Override
    public HttpRequest request() {
      return request;
    }

    @Override
    public Optional<HttpResponse<String>> previousResponse() {
      return Optional.empty();
    }

    @Override
    public HttpHeaders headers() {
      return headers;
    }

    @Override
    public String body() {
      return body;
    }

    @Override
    public Optional<SSLSession> sslSession() {
      return Optional.empty();
    }

    @Override
    public URI uri() {
      return request.uri();
    }

    @Override
    public HttpClient.Version version() {
      return HttpClient.Version.HTTP_1_1;
    }

  }

  private static final class BodyCaptureSubscriber implements Flow.Subscriber<ByteBuffer> {
    private final StringBuilder builder = new StringBuilder();

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(ByteBuffer item) {
      byte[] bytes = new byte[item.remaining()];
      item.get(bytes);
      builder.append(new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public void onError(Throwable throwable) {
      // No-op for tests
    }

    @Override
    public void onComplete() {
      // No-op for tests
    }

    String awaitBody() {
      return builder.toString();
    }
  }
}
