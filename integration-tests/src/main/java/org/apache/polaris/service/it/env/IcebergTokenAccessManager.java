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
import org.apache.polaris.service.it.ext.PolarisAccessManager;

/**
 * This class obtains access tokens from the {@code v1/oauth/tokens} endpoint defined by the Iceberg
 * REST Catalog spec.
 *
 * <p>Note: even though this endpoint is still part of the Iceberg REST Catalog spec it has been
 * deprecated per <a href="https://github.com/apache/iceberg/pull/10603">Iceberg PR#10603</a>.
 */
public class IcebergTokenAccessManager implements PolarisAccessManager {
  private final Client client;

  public IcebergTokenAccessManager(Client client) {
    this.client = client;
  }

  @Override
  public String obtainAccessToken(PolarisApiEndpoints endpoints, ClientCredentials credentials) {
    OAuth2Api api = new OAuth2Api(client, endpoints.catalogApiEndpoint(), "v1/oauth/tokens");
    return api.obtainAccessToken(credentials, "PRINCIPAL_ROLE:ALL");
  }
}
