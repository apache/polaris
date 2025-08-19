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

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Invocation;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * A base class for all Polaris APIs. This class assumes that the client is already authenticated,
 * and therefore, possesses a valid token.
 */
public class PolarisRestApi extends RestApi {
  private final PolarisApiEndpoints endpoints;
  private final String authToken;

  PolarisRestApi(Client client, PolarisApiEndpoints endpoints, String authToken, URI uri) {
    super(client, uri);
    this.endpoints = endpoints;
    this.authToken = authToken;
  }

  protected Map<String, String> defaultHeaders() {
    Map<String, String> headers = new HashMap<>();
    headers.put(endpoints.realmHeaderName(), endpoints.realmId());
    if (authToken != null) {
      headers.put("Authorization", "Bearer " + authToken);
    }
    return headers;
  }

  @Override
  public Invocation.Builder request(
      String path, Map<String, String> templateValues, Map<String, String> queryParams) {
    return request(path, templateValues, queryParams, defaultHeaders());
  }
}
