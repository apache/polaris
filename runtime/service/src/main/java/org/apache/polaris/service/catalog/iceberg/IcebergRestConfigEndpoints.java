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
package org.apache.polaris.service.catalog.iceberg;

import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;
import org.apache.iceberg.rest.Endpoint;
import org.apache.polaris.service.catalog.spi.CatalogConfigEndpointContributor;
import org.apache.polaris.service.idempotency.IdempotencyConfiguration;

@ApplicationScoped
@Priority(100)
public class IcebergRestConfigEndpoints implements CatalogConfigEndpointContributor {
  public static final Endpoint UNREGISTER_TABLE_ENDPOINT =
      Endpoint.create("POST", "/v1/{prefix}/namespaces/{namespace}/tables/{table}/unregister");

  public static final Set<Endpoint> ICEBERG_REST_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(Endpoint.V1_LIST_NAMESPACES)
          .add(Endpoint.V1_LOAD_NAMESPACE)
          .add(Endpoint.V1_NAMESPACE_EXISTS)
          .add(Endpoint.V1_CREATE_NAMESPACE)
          .add(Endpoint.V1_UPDATE_NAMESPACE)
          .add(Endpoint.V1_DELETE_NAMESPACE)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_LOAD_TABLE)
          .add(Endpoint.V1_TABLE_EXISTS)
          .add(Endpoint.V1_CREATE_TABLE)
          .add(Endpoint.V1_UPDATE_TABLE)
          .add(Endpoint.V1_DELETE_TABLE)
          .add(Endpoint.V1_RENAME_TABLE)
          .add(Endpoint.V1_REGISTER_TABLE)
          .add(UNREGISTER_TABLE_ENDPOINT)
          .add(Endpoint.V1_REPORT_METRICS)
          .add(Endpoint.V1_COMMIT_TRANSACTION)
          .add(Endpoint.V1_TABLE_CREDENTIALS)
          .build();

  private final IdempotencyConfiguration idempotencyConfiguration;

  @Inject
  public IcebergRestConfigEndpoints(IdempotencyConfiguration idempotencyConfiguration) {
    this.idempotencyConfiguration = idempotencyConfiguration;
  }

  @Override
  public Set<Endpoint> endpoints() {
    if (idempotencyConfiguration.enabled()) {
      return ICEBERG_REST_ENDPOINTS.stream()
          .filter(endpoint -> !endpoint.equals(UNREGISTER_TABLE_ENDPOINT))
          .collect(ImmutableSet.toImmutableSet());
    }
    return ICEBERG_REST_ENDPOINTS;
  }
}
