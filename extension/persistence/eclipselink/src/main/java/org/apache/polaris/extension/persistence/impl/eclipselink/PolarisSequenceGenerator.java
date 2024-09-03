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
package org.apache.polaris.extension.persistence.impl.eclipselink;

import jakarta.persistence.*;
import java.sql.SQLSyntaxErrorException;
import org.eclipse.persistence.internal.jpa.EntityManagerImpl;
import org.eclipse.persistence.platform.database.DatabasePlatform;
import org.eclipse.persistence.platform.database.PostgreSQLPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to generate sequence IDs for Polaris entities. When the `POLARIS_SEQ` generator is available
 * it should be used. Otherwise, will use the `POLARIS_SEQUENCE` table for ID generation.
 * `generateId` must be called inside a transaction.
 */
public class PolarisSequenceGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisSequenceGenerator.class);

  /* Get the `DatabasePlatform` associated with the `EntityManager` */
  private static DatabasePlatform getDatabasePlatform(EntityManager session) {
    EntityManagerImpl entityManagerImpl = session.unwrap(EntityManagerImpl.class);
    return entityManagerImpl.getDatabaseSession().getPlatform();
  }

  /* Generate an ID from the `ModelSequenceId` table instead of using a generator */
  private static Long generateSequenceFromTable(EntityManager session) {
    TypedQuery<Long> query =
        session.createQuery(
            "SELECT COALESCE(MAX(e.id), 999) + 1 FROM ModelSequenceId e", Long.class);
    return query.getSingleResult();
  }

  /* Returns `true` is NEXTVAL is supported in native queries */
  private static boolean canUseNextVal(EntityManager session) {
    DatabasePlatform platform = getDatabasePlatform(session);
    return platform instanceof PostgreSQLPlatform;
  }

  /** Generates a new ID from `POLARIS_SEQUENCE` or `POLARIS_SEQ` depending on availability. */
  public static Long generateId(EntityManager session) {
    if (canUseNextVal(session)) {
      try {
        return (Long) session.createNativeQuery("SELECT NEXTVAL('POLARIS_SEQ')").getSingleResult();
      } catch (PersistenceException e) {
        if (e.getCause() instanceof SQLSyntaxErrorException) {
          LOGGER.debug(
              "Failed to use NEXTVAL to generate a sequence ID; falling back to the POLARIS_SEQUENCE table");
          return generateSequenceFromTable(session);
        } else {
          throw e;
        }
      }
    } else {
      return generateSequenceFromTable(session);
    }
  }
}
