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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.polaris.core.persistence.models.ModelSequenceId;
import org.eclipse.persistence.internal.jpa.EntityManagerImpl;
import org.eclipse.persistence.platform.database.DatabasePlatform;
import org.eclipse.persistence.platform.database.PostgreSQLPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to generate sequence IDs for Polaris entities. If the legacy `POLARIS_SEQ` generator is
 * available it will be used then cleaned up. In all other cases the `POLARIS_SEQUENCE` table is
 * used directly.
 */
class PolarisSequenceUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisSequenceUtil.class);

  private static final AtomicBoolean initialized = new AtomicBoolean(false);

  private PolarisSequenceUtil() {}

  /* If `initialize` was never called, throw an exception */
  private static void throwIfNotInitialized() {
    if (!initialized.get()) {
      throw new IllegalStateException("Sequence util has not been initialized");
    }
  }

  /* Get the database platform associated with the `EntityManager` */
  private static DatabasePlatform getDatabasePlatform(EntityManager session) {
    EntityManagerImpl entityManagerImpl = session.unwrap(EntityManagerImpl.class);
    return entityManagerImpl.getDatabaseSession().getPlatform();
  }

  private static void removeSequence(EntityManager session) {
    LOGGER.info("Renaming legacy sequence `POLARIS_SEQ` to `POLARIS_SEQ_UNUSED`");
    String renameSequenceQuery = "ALTER SEQUENCE POLARIS_SEQ RENAME TO POLARIS_SEQ_UNUSED";
    session.createNativeQuery(renameSequenceQuery).executeUpdate();
  }

  /**
   * Prepare the `PolarisSequenceUtil` to generate IDs. This may run a failing query, so it should
   * be called for the first time outside the context of a transaction. This method should be called
   * before any other methods. TODO: after a sufficient this can eventually be removed or altered
   */
  public static void initialize(EntityManager session) {
    // Trigger cleanup of the POLARIS_SEQ if it is present
    DatabasePlatform databasePlatform = getDatabasePlatform(session);
    if (!initialized.get()) {
      if (databasePlatform instanceof PostgreSQLPlatform) {
        Optional<Long> result = Optional.empty();
        LOGGER.info("Checking if the sequence POLARIS_SEQ exists");
        String checkSequenceQuery =
            "SELECT COUNT(*) FROM information_schema.sequences WHERE sequence_name IN "
                + "('polaris_seq', 'POLARIS_SEQ')";
        int sequenceExists =
            ((Number) session.createNativeQuery(checkSequenceQuery).getSingleResult()).intValue();

        if (sequenceExists > 0) {
          LOGGER.info("POLARIS_SEQ exists, calling NEXTVAL");
          long queryResult =
              (long) session.createNativeQuery("SELECT NEXTVAL('POLARIS_SEQ')").getSingleResult();
          result = Optional.of(queryResult);
        } else {
          LOGGER.info("POLARIS_SEQ does not exist, skipping NEXTVAL");
        }
        result.ifPresent(
            r -> {
              ModelSequenceId modelSequenceId = new ModelSequenceId();
              modelSequenceId.setId(r);

              // Persist the new ID:
              session.persist(modelSequenceId);
              session.flush();

              // Clean the sequence:
              removeSequence(session);
            });
      }
      initialized.set(true);
    }
  }

  /**
   * Generates a new ID from `POLARIS_SEQUENCE` or `POLARIS_SEQ` depending on availability.
   * `initialize` should be called before this method.
   */
  public static Long getNewId(EntityManager session) {
    throwIfNotInitialized();

    ModelSequenceId modelSequenceId = new ModelSequenceId();

    // Persist the new ID:
    session.persist(modelSequenceId);
    session.flush();

    return modelSequenceId.getId();
  }
}
