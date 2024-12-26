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

import static jakarta.ws.rs.core.Response.Status.NO_CONTENT;
import static jakarta.ws.rs.core.Response.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;

public class CatalogApi extends RestApi {
  CatalogApi(Client client, PolarisApiEndpoints endpoints, String authToken, URI uri) {
    super(client, endpoints, authToken, uri);
  }

  public String obtainToken(ClientCredentials credentials) {
    try (Response response =
        request("v1/oauth/tokens")
            .post(
                Entity.form(
                    new MultivaluedHashMap<>(
                        Map.of(
                            "grant_type",
                            "client_credentials",
                            "scope",
                            "PRINCIPAL_ROLE:ALL",
                            "client_id",
                            credentials.clientId(),
                            "client_secret",
                            credentials.clientSecret()))))) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
      return response.readEntity(OAuthTokenResponse.class).token();
    }
  }

  public void createNamespace(String catalogName, String namespaceName) {
    try (Response response =
        request("v1/{cat}/namespaces", Map.of("cat", catalogName))
            .post(
                Entity.json(
                    CreateNamespaceRequest.builder()
                        .withNamespace(Namespace.of(namespaceName))
                        .build()))) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    }
  }

  public List<Namespace> listNamespaces(String catalog) {
    try (Response response = request("v1/{cat}/namespaces", Map.of("cat", catalog)).get()) {
      assertThat(response.getStatus()).isEqualTo(OK.getStatusCode());
      ListNamespacesResponse res = response.readEntity(ListNamespacesResponse.class);
      return res.namespaces();
    }
  }

  public void deleteNamespaces(String catalog, Namespace namespace) {
    try (Response response =
        request(
                "v1/{cat}/namespaces/{ns}",
                Map.of("cat", catalog, "ns", RESTUtil.encodeNamespace(namespace)))
            .delete()) {
      assertThat(response.getStatus()).isEqualTo(NO_CONTENT.getStatusCode());
    }
  }
}
