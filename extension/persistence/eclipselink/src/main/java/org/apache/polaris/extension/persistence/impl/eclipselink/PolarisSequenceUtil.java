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

  private static AtomicBoolean sequenceCleaned = new AtomicBoolean(false);
  private static AtomicBoolean initialized = new AtomicBoolean(false);

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

  // TODO simplify this logic once all usage can safely be assumed to have migrated from POLARIS_SEQ
  private static synchronized Optional<Long> getSequenceId(EntityManager session) {
    DatabasePlatform databasePlatform = getDatabasePlatform(session);
    if (databasePlatform instanceof PostgreSQLPlatform) {
      Optional<Long> result = Optional.empty();
      if (!sequenceCleaned.get()) {
        try {
          LOGGER.info("Checking if the sequence POLARIS_SEQ exists");
          String checkSequenceQuery =
              "SELECT COUNT(*) FROM information_schema.sequences WHERE sequence_name IN ('polaris_seq', 'POLARIS_SEQ')";
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
        } catch (Exception e) {
          LOGGER.info(
              "Encountered an exception when checking sequence or calling `NEXTVAL('POLARIS_SEQ')`",
              e);
        }
        if (result.isPresent()) {
          removeSequence(session);
        }
        sequenceCleaned.set(true);
      }
      return result;
    } else {
      return Optional.empty();
    }
  }

  /**
   * Prepare the `PolarisSequenceUtil` to generate IDs. This may run a failing query, so it should
   * be called for the first time outside the context of a transaction.
   */
  public static void initialize(EntityManager session) {
    // Trigger cleanup of the POLARIS_SEQ if it is present
    getSequenceId(session);
    initialized.set(true);
  }

  /**
   * Generates a new ID from `POLARIS_SEQUENCE` or `POLARIS_SEQ` depending on availability. If
   * `POLARIS_SEQ` exists, it will be renamed to `POLARIS_SEQ_UNUSED`.
   */
  public static Long getNewId(EntityManager session) {
    throwIfNotInitialized();

    ModelSequenceId modelSequenceId = new ModelSequenceId();

    // If a legacy sequence ID is present, use that as an override:
    getSequenceId(session).ifPresent(modelSequenceId::setId);

    // Persist the new ID:
    session.persist(modelSequenceId);
    session.flush();

    return modelSequenceId.getId();
  }
}
