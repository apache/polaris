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
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.apache.polaris.service.types.GenericTable;
import org.apache.polaris.service.types.ListGenericTablesResponse;
import org.apache.polaris.service.types.LoadGenericTableResponse;

/**
 * A simple, non-exhaustive set of helper methods for accessing the generic tables REST API
 *
 * @see PolarisClient#genericTableApi(ClientCredentials)
 */
public class GenericTableApi extends RestApi {
  GenericTableApi(Client client, PolarisApiEndpoints endpoints, String authToken, URI uri) {
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

  public void purge(String catalog, Namespace ns) {
    listGenericTables(catalog, ns).forEach(t -> dropGenericTable(catalog, t));
  }

  public List<TableIdentifier> listGenericTables(String catalog, Namespace namespace) {
    String ns = RESTUtil.encodeNamespace(namespace);
    try (Response res =
        request("v1/{cat}/namespaces/{ns}/generic-tables", Map.of("cat", catalog, "ns", ns))
            .get()) {
      assertThat(res.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      return res.readEntity(ListGenericTablesResponse.class).getIdentifiers().stream().toList();
    }
  }

  public void dropGenericTable(String catalog, TableIdentifier id) {
    String ns = RESTUtil.encodeNamespace(id.namespace());
    try (Response res =
        request(
                "v1/{cat}/namespaces/{ns}/generic-tables/{table}",
                Map.of("cat", catalog, "table", id.name(), "ns", ns))
            .delete()) {
      assertThat(res.getStatus()).isEqualTo(NO_CONTENT.getStatusCode());
    }
  }

  public GenericTable createGenericTable(
      String catalog, TableIdentifier id, String format, Map<String, String> properties) {
    String ns = RESTUtil.encodeNamespace(id.namespace());
    try (Response res =
             request(
                 "v1/{cat}/namespaces/{ns}/generic-tables/}",
                 Map.of("cat", catalog, "ns", ns))
                 .post(Entity.json(new CreateGenericTableRequest(id.name(), format, "doc", properties)))) {
      return res.readEntity(LoadGenericTableResponse.class).getTable();
    }
  }
}
