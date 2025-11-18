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
package org.apache.polaris.persistence.nosql.realms.store;

import static org.apache.polaris.persistence.nosql.realms.store.RealmsStateObj.REALMS_REF_NAME;

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.nosql.maintenance.spi.CountDownPredicate;
import org.apache.polaris.persistence.nosql.maintenance.spi.PerRealmRetainedIdentifier;
import org.apache.polaris.persistence.nosql.maintenance.spi.RetainedCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
class RealmManagementRetainedIdentifier implements PerRealmRetainedIdentifier {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(RealmManagementRetainedIdentifier.class);

  @Override
  public String name() {
    return "Realm management";
  }

  @Override
  public boolean identifyRetained(@Nonnull RetainedCollector collector) {
    if (!collector.isSystemRealm()) {
      return false;
    }

    // TODO follow-up: configurable limit number of historic realm states to retain
    try {
      collector.refRetainIndexToSingleObj(
          REALMS_REF_NAME,
          RealmsStateObj.class,
          new CountDownPredicate<>(10),
          RealmsStateObj::realmIndex);
    } catch (ReferenceNotFoundException e) {
      // logged, but otherwise ignored
      LOGGER.debug(
          "Reference '{}' not found while identifying retained items: {}, this might be expected",
          REALMS_REF_NAME,
          e.getMessage());
    }

    // Intentionally return false, let the maintenance service's identifier decide
    return false;
  }
}
