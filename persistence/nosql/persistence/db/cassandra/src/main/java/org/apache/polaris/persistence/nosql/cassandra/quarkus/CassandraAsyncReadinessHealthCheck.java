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
package org.apache.polaris.persistence.nosql.cassandra.quarkus;

import com.datastax.oss.driver.api.core.CqlSession;
import io.smallrye.health.api.AsyncHealthCheck;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.polaris.persistence.nosql.api.backend.BackendConfiguration;
import org.apache.polaris.persistence.nosql.cassandra.CassandraBackendFactory;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

@Readiness
@ApplicationScoped
class CassandraAsyncReadinessHealthCheck implements AsyncHealthCheck {
  private static final String HEALTH_CHECK_NAME = "Cassandra health check";
  private static final String HEALTH_CHECK_QUERY = "SELECT release_version FROM system.local";

  private final BackendConfiguration backendConfiguration;
  private final Instance<CqlSession> cqlSession;

  @Inject
  CassandraAsyncReadinessHealthCheck(
      BackendConfiguration backendConfiguration, @Any Instance<CqlSession> cqlSession) {
    this.backendConfiguration = backendConfiguration;
    this.cqlSession = cqlSession;
  }

  @Override
  public Uni<HealthCheckResponse> call() {
    if (backendConfiguration.backend().filter(CassandraBackendFactory.NAME::equals).isEmpty()) {
      return Uni.createFrom().item(up());
    }
    if (!cqlSession.isResolvable()) {
      return Uni.createFrom().item(down("Cassandra session is unavailable"));
    }
    return Uni.createFrom()
        .completionStage(cqlSession.get().executeAsync(HEALTH_CHECK_QUERY))
        .onItem()
        .transform(
            resultSet -> resultSet.one() == null ? down("system.local returned no row") : up())
        .onFailure()
        .recoverWithItem(ignored -> down("Cassandra health query failed"));
  }

  private static HealthCheckResponse up() {
    return HealthCheckResponse.named(HEALTH_CHECK_NAME).up().build();
  }

  private static HealthCheckResponse down(String reason) {
    return HealthCheckResponse.named(HEALTH_CHECK_NAME).down().withData("reason", reason).build();
  }
}
