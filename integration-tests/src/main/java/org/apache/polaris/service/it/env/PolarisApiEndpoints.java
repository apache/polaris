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
import java.util.stream.Collectors;

/**
 * This class contains the most fundamental information for accessing Polaris APIs, such as the base
 * URI and realm ID and provides methods for obtaining Iceberg REST API and Polaris Management
 * endpoints.
 */
public final class PolarisApiEndpoints implements Serializable {

  private final URI baseUri;
  private final String realmId;
  private final Map<String, String> headers;

  public PolarisApiEndpoints(URI baseUri, String realmId, String realmHeaderName) {
    this(baseUri, realmId, Map.of(realmHeaderName, realmId));
  }

  public PolarisApiEndpoints(URI baseUri, String realmId, Map<String, String> headers) {
    this.baseUri = baseUri;
    this.realmId = realmId;
    this.headers = headers;
  }

  public URI catalogApiEndpoint() {
    return baseUri.resolve(baseUri.getRawPath() + "/api/catalog").normalize();
  }

  public URI managementApiEndpoint() {
    return baseUri.resolve(baseUri.getRawPath() + "/api/management").normalize();
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
}
