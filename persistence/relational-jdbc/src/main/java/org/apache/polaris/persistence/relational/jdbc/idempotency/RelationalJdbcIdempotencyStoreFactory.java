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
package org.apache.polaris.persistence.relational.jdbc.idempotency;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.inject.Inject;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.core.persistence.IdempotencyStoreFactory;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;

@ApplicationScoped
@Identifier("relational-jdbc")
public class RelationalJdbcIdempotencyStoreFactory implements IdempotencyStoreFactory {

  @Inject Instance<DataSource> dataSource;
  @Inject @Any Instance<DataSource> anyDataSources;
  @Inject RelationalJdbcConfiguration relationalJdbcConfiguration;

  @Override
  public IdempotencyStore create() {
    try {
      Instance<DataSource> idempotencyDs = anyDataSources.select(NamedLiteral.of("idempotency"));
      DataSource ds = idempotencyDs.isResolvable() ? idempotencyDs.get() : dataSource.get();
      return new RelationalJdbcIdempotencyStore(ds, relationalJdbcConfiguration);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to create RelationalJdbcIdempotencyStore", e);
    }
  }
}
