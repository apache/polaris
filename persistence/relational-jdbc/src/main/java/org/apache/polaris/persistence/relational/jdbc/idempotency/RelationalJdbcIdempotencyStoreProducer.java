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
 * JDBC idempotency backend, selected via {@code polaris.idempotency.type=relational-jdbc}. Backed
 * by the same {@link DatasourceOperations} as the primary metastore.
 *
 * <p>Each request gets a lightweight {@link RelationalJdbcIdempotencyStore} bound to the request's
 * realm (mirroring {@code JdbcBasePersistenceImpl}); realm scoping is enforced inside SQL via the
 * {@code realm_id} column. The {@code @Identifier} lets {@code IdempotencyStoreProducer} pick this
 * backend without a compile-time dependency on it.
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
