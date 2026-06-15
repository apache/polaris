/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.polaris.persistence.relational.jdbc.idempotency;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;

/**
 * Produces the {@code "relational-jdbc"} {@link IdempotencyStore}, backed by the same JDBC {@link
 * DatasourceOperations} used by the primary metastore.
 *
 * <p>The store is request-scoped and bound to the current request's realm (mirroring {@code
 * JdbcBasePersistenceImpl}); realm scoping is enforced inside SQL via the {@code realm_id} column.
 * The {@link RealmContext} arrives as a producer-method argument rather than being injected into a
 * field, so the {@link ApplicationScoped} bean itself has no request-scoped dependency.
 */
@ApplicationScoped
public class RelationalJdbcIdempotencyStoreProducer {

  private final DatasourceOperations datasourceOperations;

  @Inject
  public RelationalJdbcIdempotencyStoreProducer(DatasourceOperations datasourceOperations) {
    this.datasourceOperations = datasourceOperations;
  }

  @Produces
  @RequestScoped
  @Identifier("relational-jdbc")
  public IdempotencyStore idempotencyStore(RealmContext realmContext) {
    return new RelationalJdbcIdempotencyStore(
        datasourceOperations, realmContext.getRealmIdentifier());
  }
}
