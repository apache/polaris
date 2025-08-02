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
package org.apache.polaris.service.exception;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Fake version of an Azure HttpResponse that can be forced to return a fixed statusCode. */
public class FakeAzureHttpResponse extends HttpResponse {
  private final int mockStatusCode;

  public FakeAzureHttpResponse(int mockStatusCode) {
    super(null);
    this.mockStatusCode = mockStatusCode;
  }

  @Override
  public int getStatusCode() {
    return mockStatusCode;
  }

  @Override
  @Deprecated
  public String getHeaderValue(String name) {
    return "";
  }

  @Override
  public HttpHeaders getHeaders() {
    return null;
  }

  @Override
  public Flux<ByteBuffer> getBody() {
    return null;
  }

  @Override
  public Mono<byte[]> getBodyAsByteArray() {
    return null;
  }

  @Override
  public Mono<String> getBodyAsString() {
    return null;
  }

  @Override
  public Mono<String> getBodyAsString(Charset charset) {
    return null;
  }
}
