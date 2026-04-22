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
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import javax.sql.DataSource;
import org.apache.polaris.core.persistence.IdempotencyStore;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Produces an {@link IdempotencyStore} backed by the JDBC datasource. Qualified with
 * {@code @Identifier("relational-jdbc")} so the runtime can select it based on {@code
 * polaris.persistence.type}.
 */
@ApplicationScoped
public class RelationalJdbcIdempotencyStoreProducer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(RelationalJdbcIdempotencyStoreProducer.class);

  @Inject Instance<DataSource> dataSource;
  @Inject RelationalJdbcConfiguration relationalJdbcConfiguration;

  @Produces
  @ApplicationScoped
  @Identifier("relational-jdbc")
  IdempotencyStore idempotencyStore() {
    if (dataSource.isUnsatisfied()) {
      throw new IllegalStateException(
          "relational-jdbc IdempotencyStore requested but no DataSource is wired");
    }
    try {
      LOGGER.info("Wiring RelationalJdbcIdempotencyStore");
      return new RelationalJdbcIdempotencyStore(dataSource.get(), relationalJdbcConfiguration);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to construct RelationalJdbcIdempotencyStore", e);
    }
  }
}
