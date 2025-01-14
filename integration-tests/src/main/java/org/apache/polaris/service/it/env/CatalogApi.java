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

import com.google.common.base.Joiner;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;

/**
 * A simple, non-exhaustive set of helper methods for accessing the Iceberg REST API.
 *
 * @see PolarisClient#catalogApi(ClientCredentials)
 */
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

  public List<Namespace> listNamespaces(String catalog, Namespace parent) {
    Map<String, String> queryParams = new HashMap<>();
    if (!parent.isEmpty()) {
      // TODO change this for Iceberg 1.7.2:
      //   queryParams.put("parent", RESTUtil.encodeNamespace(parent));
      queryParams.put("parent", Joiner.on('\u001f').join(parent.levels()));
    }
    try (Response response =
        request("v1/{cat}/namespaces", Map.of("cat", catalog), queryParams).get()) {
      assertThat(response.getStatus()).isEqualTo(OK.getStatusCode());
      ListNamespacesResponse res = response.readEntity(ListNamespacesResponse.class);
      return res.namespaces();
    }
  }

  public List<Namespace> listAllNamespacesChildFirst(String catalog) {
    List<Namespace> result = new ArrayList<>();
    for (int idx = -1; idx < result.size(); idx++) {
      Namespace parent = Namespace.empty();
      if (idx >= 0) {
        parent = result.get(idx);
      }

      result.addAll(listNamespaces(catalog, parent));
    }

    return result.reversed();
  }

  public void deleteNamespace(String catalog, Namespace namespace) {
    String ns = RESTUtil.encodeNamespace(namespace);
    try (Response response =
        request("v1/{cat}/namespaces/" + ns, Map.of("cat", catalog)).delete()) {
      assertThat(response.getStatus()).isEqualTo(NO_CONTENT.getStatusCode());
    }
  }

  public void purge(String catalog) {
    listAllNamespacesChildFirst(catalog).forEach(ns -> purge(catalog, ns));
  }

  public void purge(String catalog, Namespace ns) {
    listTables(catalog, ns).forEach(t -> dropTable(catalog, t));
    listViews(catalog, ns).forEach(t -> dropView(catalog, t));
    deleteNamespace(catalog, ns);
  }

  public List<TableIdentifier> listTables(String catalog, Namespace namespace) {
    String ns = RESTUtil.encodeNamespace(namespace);
    try (Response res =
        request("v1/{cat}/namespaces/" + ns + "/tables", Map.of("cat", catalog)).get()) {
      assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(ListTablesResponse.class).identifiers();
    }
  }

  public void dropTable(String catalog, TableIdentifier id) {
    String ns = RESTUtil.encodeNamespace(id.namespace());
    try (Response res =
        request(
                "v1/{cat}/namespaces/" + ns + "/tables/{table}",
                Map.of("cat", catalog, "table", id.name()))
            .delete()) {
      assertThat(res.getStatus()).isEqualTo(NO_CONTENT.getStatusCode());
    }
  }

  public List<TableIdentifier> listViews(String catalog, Namespace namespace) {
    String ns = RESTUtil.encodeNamespace(namespace);
    try (Response res =
        request("v1/{cat}/namespaces/" + ns + "/views", Map.of("cat", catalog)).get()) {
      assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(ListTablesResponse.class).identifiers();
    }
  }

  public void dropView(String catalog, TableIdentifier id) {
    String ns = RESTUtil.encodeNamespace(id.namespace());
    try (Response res =
        request(
                "v1/{cat}/namespaces/" + ns + "/views/{view}",
                Map.of("cat", catalog, "view", id.name()))
            .delete()) {
      assertThat(res.getStatus()).isEqualTo(NO_CONTENT.getStatusCode());
    }
  }
}
