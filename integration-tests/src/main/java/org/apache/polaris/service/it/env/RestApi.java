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
import jakarta.ws.rs.client.WebTarget;
import java.net.URI;
import java.util.Map;

public class RestApi {
  private final Client client;
  private final PolarisApiEndpoints endpoints;
  private final String authToken;
  private final URI uri;

  RestApi(Client client, PolarisApiEndpoints endpoints, String authToken, URI uri) {
    this.client = client;
    this.endpoints = endpoints;
    this.authToken = authToken;
    this.uri = uri;
  }

  public Invocation.Builder request(String path) {
    return request(path, Map.of());
  }

  public Invocation.Builder request(String path, Map<String, String> templateValues) {
    return request(path, templateValues, Map.of());
  }

  public Invocation.Builder request(
      String path, Map<String, String> templateValues, Map<String, String> queryParams) {
    WebTarget target = client.target(uri).path(path);
    for (Map.Entry<String, String> entry : templateValues.entrySet()) {
      target = target.resolveTemplate(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : queryParams.entrySet()) {
      target = target.queryParam(entry.getKey(), entry.getValue());
    }
    Invocation.Builder request = target.request("application/json");
    request = request.header(PolarisApiEndpoints.REALM_HEADER, endpoints.realm());
    if (authToken != null) {
      request = request.header("Authorization", "Bearer " + authToken);
    }
    return request;
  }
}
