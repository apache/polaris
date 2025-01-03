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
import java.net.URI;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;

public record PolarisApiClient(Client client, String realm, URI baseUri) {

  public static String REALM_HEADER = "realm";

  public URI catalogApiEndpoint() {
    return baseUri.resolve("api/catalog");
  }

  public CatalogApi catalogApi(AuthToken token) {
    return new CatalogApi(this, token, catalogApiEndpoint());
  }

  public CatalogApi catalogApi(PrincipalWithCredentials principal) {
    return catalogApi(obtainToken(principal));
  }

  public AuthToken obtainToken(PrincipalWithCredentials principal) {
    CatalogApi anon = new CatalogApi(this, null, catalogApiEndpoint());
    return anon.obtainToken(principal);
  }

  public ManagementApi managementApi(AuthToken token) {
    return new ManagementApi(this, token, baseUri.resolve("api/management"));
  }
}
