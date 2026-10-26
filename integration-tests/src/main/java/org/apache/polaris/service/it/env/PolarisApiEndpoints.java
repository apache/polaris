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
package org.apache.polaris.service.it.env;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class contains the most fundamental information for accessing Polaris APIs, such as the base
 * URI and realm ID and provides methods for obtaining Iceberg REST API and Polaris Management
 * endpoints.
 */
public final class PolarisApiEndpoints implements Serializable {

  private final Supplier<URI> baseUriSupplier;
  private final String realmId;
  private final Map<String, String> headers;
  private URI baseUri;

  public PolarisApiEndpoints(URI baseUri, String realmId, Map<String, String> headers) {
    this(() -> baseUri, realmId, headers);
  }

  public PolarisApiEndpoints(
      Supplier<URI> baseUriSupplier, String realmId, Map<String, String> headers) {
    this.baseUriSupplier = baseUriSupplier;
    this.realmId = realmId;
    this.headers = headers;
  }

  private URI baseUri() {
    if (baseUri == null) {
      baseUri = baseUriSupplier.get();
    }
    return baseUri;
  }

  public URI catalogApiEndpoint() {
    return appendPath(baseUri(), "api/catalog");
  }

  public URI managementApiEndpoint() {
    return appendPath(baseUri(), "api/management");
  }

  public URI openLineageApiEndpoint() {
    return appendPath(baseUri(), "api/openlineage/v1");
  }

  public String realmId() {
    return realmId;
  }

  public Map<String, String> extraHeaders() {
    return headers;
  }

  public Map<String, String> extraHeaders(String keyPrefix) {
    return headers.entrySet().stream()
        .map(e -> Map.entry(keyPrefix + e.getKey(), e.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static URI appendPath(URI base, String path) {
    String baseStr = base.toString();
    if (baseStr.endsWith("/")) {
      baseStr = baseStr.substring(0, baseStr.length() - 1);
    }
    return URI.create(baseStr + "/" + path).normalize();
  }
}
